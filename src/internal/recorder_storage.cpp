#include "jojo/rec/detail/recorder_storage.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace jojo::rec::internal {

bool RecorderStorage::Initialize(const std::filesystem::path& output_root, const std::string& recording_label,
                                 const std::map<std::uint32_t, std::string>& message_type_names, std::string* error) {
  Reset();
  if (!EnsureDirectory(output_root, error)) {
    return false;
  }

  const auto start_time = std::chrono::system_clock::now();
  manifest_.start_utc = FormatUtcCompact(start_time);
  manifest_.recording_label = recording_label;
  manifest_.message_type_names = message_type_names;
  recording_path_ = output_root / ("recording-" + manifest_.start_utc);
  if (!EnsureDirectory(recording_path_, error)) {
    Reset();
    return false;
  }
  if (!WriteManifest(recording_path_, manifest_, error)) {
    Reset();
    return false;
  }

  manifest_dirty_ = true;
  return true;
}

const std::filesystem::path& RecorderStorage::recording_path() const { return recording_path_; }

void RecorderStorage::SetRecordingPath(std::filesystem::path recording_path) { recording_path_ = std::move(recording_path); }

const ManifestData& RecorderStorage::manifest() const { return manifest_; }

void RecorderStorage::MarkDegraded() {
  manifest_.degraded = true;
  manifest_dirty_ = true;
}

void RecorderStorage::HandleAbortedEntry() {
  manifest_.aborted_entries += 1;
  MarkDegraded();
}

ManifestData RecorderStorage::BuildManifestSnapshot() const {
  ManifestData snapshot = manifest_;
  if (has_current_segment_) {
    snapshot.segments.push_back(current_segment_.summary);
  }
  return snapshot;
}

bool RecorderStorage::FlushManifestSnapshot(std::string* error) {
  return WriteManifest(recording_path_, BuildManifestSnapshot(), error);
}

bool RecorderStorage::ShouldSyncOnExplicitFlush() const {
  switch (fsync_policy_) {
    case FsyncPolicy::kNever:
      return false;
    case FsyncPolicy::kInterval:
      if (last_sync_time_ == std::chrono::steady_clock::time_point::min()) {
        return true;
      }
      return std::chrono::steady_clock::now() - last_sync_time_ >= ClampFlushInterval(flush_interval_ms_);
    case FsyncPolicy::kEveryFlush:
      return true;
  }

  return false;
}

bool RecorderStorage::ShouldSyncOnClose() const { return fsync_policy_ != FsyncPolicy::kNever; }

bool RecorderStorage::PerformDurabilityCheckpoint(std::string* error) {
  if (!SyncUnsyncedFinalizedSegments(error) || !SyncCurrentSegmentFile(error)) {
    return false;
  }
  if (!FlushManifestSnapshot(error) || !SyncManifestSnapshot(error)) {
    return false;
  }
  MarkDurabilityCheckpointComplete();
  return true;
}

bool RecorderStorage::WriteRecord(const ReplayMessage& message, std::uint64_t segment_max_bytes,
                                  std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                                  std::size_t sparse_index_max_bytes, std::string* error) {
  if (!RotateIfNeeded(message, segment_max_bytes, sparse_index_interval_us, sparse_index_max_records,
                      sparse_index_max_bytes, error) ||
      !WriteRecordToSegment(&current_segment_, message, error)) {
    return false;
  }

  manifest_.total_records += 1;
  manifest_.total_payload_bytes += message.payload.size();
  manifest_dirty_ = true;
  current_segment_dirty_ = true;
  return true;
}

void RecorderStorage::FinishCurrentSegment(std::string* error) {
  if (!has_current_segment_) {
    return;
  }
  if (!FinalizeSegment(&current_segment_, error)) {
    return;
  }
  const bool finalized_segment_was_already_synced =
      !current_segment_dirty_ && synced_segment_count_ == manifest_.segments.size();
  manifest_.segments.push_back(current_segment_.summary);
  if (finalized_segment_was_already_synced) {
    ++synced_segment_count_;
  }
  has_current_segment_ = false;
  current_segment_dirty_ = false;
  manifest_dirty_ = true;
}

void RecorderStorage::MarkClosedAt(std::chrono::system_clock::time_point time_point) {
  manifest_.stop_utc = FormatUtcCompact(time_point);
  manifest_.incomplete = false;
  manifest_dirty_ = true;
}

std::filesystem::path RecorderStorage::MakeFinalizedPath(const std::filesystem::path& output_root) const {
  if (!manifest_.stop_utc.has_value()) {
    return recording_path_;
  }
  return output_root / ("recording-" + manifest_.start_utc + "-to-" + *manifest_.stop_utc);
}

void RecorderStorage::Reset() {
  recording_path_.clear();
  manifest_ = ManifestData{};
  current_segment_ = SegmentWriteContext{};
  has_current_segment_ = false;
  synced_segment_count_ = 0;
  current_segment_dirty_ = false;
  manifest_dirty_ = false;
  last_sync_time_ = std::chrono::steady_clock::time_point::min();
  fsync_policy_ = FsyncPolicy::kNever;
  flush_interval_ms_ = 0;
}

void RecorderStorage::SetFlushPolicy(FsyncPolicy policy, std::uint64_t flush_interval_ms) {
  fsync_policy_ = policy;
  flush_interval_ms_ = flush_interval_ms;
}

std::chrono::milliseconds RecorderStorage::ClampFlushInterval(std::uint64_t flush_interval_ms) {
  const auto max_millis = static_cast<std::uint64_t>(std::numeric_limits<std::chrono::milliseconds::rep>::max());
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(std::min(flush_interval_ms, max_millis)));
}

bool RecorderStorage::OpenCurrentSegment(std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                                         std::size_t sparse_index_max_bytes, std::string* error) {
  if (has_current_segment_) {
    return true;
  }
  has_current_segment_ = true;
  return OpenSegment(recording_path_, static_cast<std::uint32_t>(manifest_.segments.size()), sparse_index_interval_us,
                     sparse_index_max_records, sparse_index_max_bytes, &current_segment_, error);
}

bool RecorderStorage::FlushOpenSegmentStream(std::string* error) {
  if (!has_current_segment_ || current_segment_.summary.record_count == 0) {
    return true;
  }
  current_segment_.stream.flush();
  if (!current_segment_.stream.good()) {
    if (error != nullptr) {
      *error = "failed to flush active segment stream: " + current_segment_.file_path.string();
    }
    return false;
  }
  return true;
}

bool RecorderStorage::SyncUnsyncedFinalizedSegments(std::string* error) {
  for (std::size_t index = synced_segment_count_; index < manifest_.segments.size(); ++index) {
    if (!SyncFile(recording_path_ / manifest_.segments[index].file_name, error)) {
      return false;
    }
  }
  return true;
}

bool RecorderStorage::SyncCurrentSegmentFile(std::string* error) {
  if (!has_current_segment_ || current_segment_.summary.record_count == 0 || !current_segment_dirty_) {
    return true;
  }
  if (!FlushOpenSegmentStream(error)) {
    return false;
  }
  return SyncFile(current_segment_.file_path, error);
}

bool RecorderStorage::SyncManifestSnapshot(std::string* error) {
  if (!SyncFile(recording_path_ / "manifest.json", error)) {
    return false;
  }
  return SyncDirectory(recording_path_, error);
}

void RecorderStorage::MarkDurabilityCheckpointComplete() {
  synced_segment_count_ = manifest_.segments.size();
  current_segment_dirty_ = false;
  manifest_dirty_ = false;
  last_sync_time_ = std::chrono::steady_clock::now();
}

bool RecorderStorage::RotateIfNeeded(const ReplayMessage& message, std::uint64_t segment_max_bytes,
                                     std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                                     std::size_t sparse_index_max_bytes, std::string* error) {
  if (!has_current_segment_) {
    return OpenCurrentSegment(sparse_index_interval_us, sparse_index_max_records, sparse_index_max_bytes, error);
  }
  const RecordedMessage estimated{message.session_id, message.message_type,
                                  ByteView{message.payload.data(), message.payload.size()}};
  if (!ShouldRotateSegment(current_segment_, segment_max_bytes, EstimateRecordBytes(estimated))) {
    return true;
  }
  if (!FinalizeSegment(&current_segment_, error)) {
    return false;
  }
  const bool finalized_segment_was_already_synced =
      !current_segment_dirty_ && synced_segment_count_ == manifest_.segments.size();
  manifest_.segments.push_back(current_segment_.summary);
  if (finalized_segment_was_already_synced) {
    ++synced_segment_count_;
  }
  has_current_segment_ = false;
  current_segment_dirty_ = false;
  manifest_dirty_ = true;
  if (!FlushManifestSnapshot(error)) {
    return false;
  }
  return OpenCurrentSegment(sparse_index_interval_us, sparse_index_max_records, sparse_index_max_bytes, error);
}

}  // namespace jojo::rec::internal
