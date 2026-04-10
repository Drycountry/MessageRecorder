#include "jojo/rec/recorder.hpp"

#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
#include <utility>

namespace jojo::rec {

Recorder::QueueBuffer::QueueBuffer() = default;

Recorder::QueueBuffer::QueueBuffer(std::size_t capacity) : slots(capacity, nullptr) {}

bool Recorder::QueueBuffer::Empty() const { return size == 0; }

bool Recorder::QueueBuffer::Full() const { return size == slots.size(); }

Recorder::QueuedEntry* Recorder::QueueBuffer::Front() const { return Empty() ? nullptr : slots[head_index]; }

void Recorder::QueueBuffer::Push(QueuedEntry* entry) {
  const std::size_t tail_index = (head_index + size) % slots.size();
  slots[tail_index] = entry;
  ++size;
}

void Recorder::QueueBuffer::Pop() {
  slots[head_index] = nullptr;
  head_index = (head_index + 1) % slots.size();
  --size;
  if (size == 0) {
    head_index = 0;
  }
}

Recorder::Recorder(const RecorderConfig& config, std::string* error) : config_(config) {
  if (config_.output_root.empty()) {
    SetError("output_root must not be empty", error);
    return;
  }
  if (config_.queue_capacity_mb == 0 || config_.segment_max_mb == 0) {
    SetError("queue_capacity_mb and segment_max_mb must be non-zero", error);
    return;
  }
  if (config_.queue_buffer_count < 2) {
    SetError("queue_buffer_count must be at least 2", error);
    return;
  }

  std::uint64_t queue_capacity_bytes = 0;
  if (!ConvertMegabytesToBytes(config_.queue_capacity_mb, &queue_capacity_bytes) ||
      queue_capacity_bytes > std::numeric_limits<std::size_t>::max()) {
    SetError("queue_capacity_mb is too large", error);
    return;
  }
  queue_capacity_bytes_ = static_cast<std::size_t>(queue_capacity_bytes);
  queues_.assign(config_.queue_buffer_count, QueueBuffer(ComputeQueueSlotCapacity(queue_capacity_bytes_)));
  // 写线程可能暂时持有一个已经出队、但尚未回收到空闲池的条目，所以对象池需要比总槽位数多 1。
  std::size_t entry_pool_size = 1;
  for (const QueueBuffer& buffer : queues_) {
    if (buffer.slots.size() > std::numeric_limits<std::size_t>::max() - entry_pool_size) {
      SetError("entry pool is too large", error);
      return;
    }
    entry_pool_size += buffer.slots.size();
  }
  try {
    entry_storage_ = std::make_unique<QueuedEntry[]>(entry_pool_size);
    entry_storage_size_ = entry_pool_size;
    free_entries_.reserve(entry_pool_size);
  } catch (const std::exception&) {
    SetError("failed to allocate entry pool", error);
    return;
  }
  for (std::size_t index = 0; index < entry_pool_size; ++index) {
    free_entries_.push_back(&entry_storage_[index]);
  }

  if (!ConvertMegabytesToBytes(config_.segment_max_mb, &segment_max_bytes_)) {
    SetError("segment_max_mb is too large", error);
    return;
  }
  if (!internal::EnsureDirectory(config_.output_root, error)) {
    return;
  }

  const auto start_time = std::chrono::system_clock::now();
  manifest_.start_utc = internal::FormatUtcCompact(start_time);
  manifest_.recording_label = config_.recording_label;
  manifest_.message_type_names = config_.message_type_names;
  recording_path_ = config_.output_root / ("recording-" + manifest_.start_utc);
  if (!internal::EnsureDirectory(recording_path_, error)) {
    return;
  }
  if (!internal::WriteManifest(recording_path_, manifest_, error)) {
    return;
  }

  manifest_dirty_ = true;
  open_ = true;
  writer_thread_ = std::thread([this]() { WriterLoop(); });
}

Recorder::~Recorder() { CloseNoexcept(); }

Recorder::Recorder(Recorder&& other) noexcept {
  other.CloseNoexcept();

  std::scoped_lock<std::mutex, std::mutex> lock(mutex_, other.mutex_);
  MoveStateFromLocked(std::move(other));
  other.ResetMovedFromStateLocked();
}

Recorder& Recorder::operator=(Recorder&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  CloseNoexcept();
  other.CloseNoexcept();

  std::scoped_lock<std::mutex, std::mutex> lock(mutex_, other.mutex_);
  MoveStateFromLocked(std::move(other));
  other.ResetMovedFromStateLocked();
  return *this;
}

bool Recorder::IsOpen() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return open_ && fatal_error_.empty();
}

const RecorderConfig& Recorder::Config() const { return config_; }

std::filesystem::path Recorder::RecordingPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return recording_path_;
}

void Recorder::CloseNoexcept() noexcept {
  std::string ignored;
  Close(&ignored);
}

bool Recorder::ConvertMegabytesToBytes(std::size_t megabytes, std::uint64_t* bytes) {
  if (megabytes > std::numeric_limits<std::uint64_t>::max() / kBytesPerMb) {
    return false;
  }
  *bytes = static_cast<std::uint64_t>(megabytes) * kBytesPerMb;
  return true;
}

std::size_t Recorder::ComputeQueueSlotCapacity(std::size_t queue_capacity_bytes) {
  const std::size_t min_record_bytes = internal::EstimateRecordBytes(RecordedMessage{});
  if (min_record_bytes == 0) {
    return 2;
  }
  const std::uint64_t max_entries = static_cast<std::uint64_t>(queue_capacity_bytes) / min_record_bytes + 2ULL;
  const std::uint64_t clamped = std::min<std::uint64_t>(max_entries, std::numeric_limits<std::size_t>::max());
  return static_cast<std::size_t>(std::max<std::uint64_t>(clamped, 2ULL));
}

void Recorder::SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

std::chrono::milliseconds Recorder::ClampFlushInterval(std::uint64_t flush_interval_ms) {
  const auto max_millis = static_cast<std::uint64_t>(std::numeric_limits<std::chrono::milliseconds::rep>::max());
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(std::min(flush_interval_ms, max_millis)));
}

void Recorder::PrepareQueuedMessage(ReplayMessage* owned, const RecordedMessage& message, std::uint64_t record_seq,
                                    std::uint64_t event_mono_ts_us, std::uint64_t event_utc_ts_us) {
  owned->record_seq = record_seq;
  owned->event_mono_ts_us = event_mono_ts_us;
  owned->event_utc_ts_us = event_utc_ts_us;
  owned->session_id = message.session_id;
  owned->message_type = message.message_type;
  owned->payload.clear();
}

void Recorder::CopyByteView(const ByteView& source, std::vector<std::uint8_t>* destination) {
  destination->resize(source.size);
  if (source.size != 0) {
    std::memcpy(destination->data(), source.data, source.size);
  }
}

void Recorder::PublishReservedEntry(AcceptQueueStatus accept_status) {
  if (accept_status == AcceptQueueStatus::kAcceptedAfterSwitch) {
    producer_cv_.notify_one();
  }
}

void Recorder::MoveStateFromLocked(Recorder&& other) {
  config_ = std::move(other.config_);
  queue_capacity_bytes_ = other.queue_capacity_bytes_;
  segment_max_bytes_ = other.segment_max_bytes_;
  recording_path_ = std::move(other.recording_path_);
  manifest_ = std::move(other.manifest_);
  current_segment_ = std::move(other.current_segment_);
  has_current_segment_ = other.has_current_segment_;
  synced_segment_count_ = other.synced_segment_count_;
  current_segment_dirty_ = other.current_segment_dirty_;
  manifest_dirty_ = other.manifest_dirty_;
  last_sync_time_ = other.last_sync_time_;
  open_ = other.open_;
  closing_requested_ = other.closing_requested_;
  closed_ = other.closed_;
  fatal_error_ = std::move(other.fatal_error_);
  next_record_seq_ = other.next_record_seq_;
  queues_ = std::move(other.queues_);
  accept_queue_index_ = other.accept_queue_index_;
  drain_queue_index_ = other.drain_queue_index_;
  entry_storage_ = std::move(other.entry_storage_);
  entry_storage_size_ = other.entry_storage_size_;
  free_entries_ = std::move(other.free_entries_);
  flush_requested_id_ = other.flush_requested_id_;
  flush_completed_id_ = other.flush_completed_id_;
  writer_thread_ = std::move(other.writer_thread_);
}

void Recorder::ResetMovedFromStateLocked() {
  config_ = RecorderConfig{};
  queue_capacity_bytes_ = 0;
  segment_max_bytes_ = 0;
  recording_path_.clear();
  manifest_ = internal::ManifestData{};
  current_segment_ = internal::SegmentWriteContext{};
  has_current_segment_ = false;
  synced_segment_count_ = 0;
  current_segment_dirty_ = false;
  manifest_dirty_ = false;
  last_sync_time_ = std::chrono::steady_clock::time_point::min();
  open_ = false;
  closing_requested_ = false;
  closed_ = false;
  fatal_error_.clear();
  next_record_seq_ = 0;
  queues_.clear();
  accept_queue_index_ = 0;
  drain_queue_index_ = 0;
  entry_storage_.reset();
  entry_storage_size_ = 0;
  free_entries_.clear();
  flush_requested_id_ = 0;
  flush_completed_id_ = 0;
  writer_thread_ = std::thread();
}

Recorder::QueuedEntry* Recorder::AcquireEntryLocked() {
  if (free_entries_.empty()) {
    return nullptr;
  }
  QueuedEntry* entry = free_entries_.back();
  free_entries_.pop_back();
  return entry;
}

void Recorder::RecycleEntryLocked(QueuedEntry* entry) {
  entry->state.store(QueuedEntry::State::kConsumed, std::memory_order_relaxed);
  entry->reserved_bytes = 0;
  entry->message.record_seq = 0;
  entry->message.event_mono_ts_us = 0;
  entry->message.event_utc_ts_us = 0;
  entry->message.session_id = 0;
  entry->message.message_type = 0;
  entry->message.payload.clear();
  free_entries_.push_back(entry);
}

bool Recorder::CanAcceptInBuffer(const QueueBuffer& buffer, std::size_t reserve_bytes) const {
  if (buffer.Full()) {
    return false;
  }
  const bool oversized_only = buffer.Empty() && reserve_bytes > queue_capacity_bytes_;
  return oversized_only || buffer.reserved_bytes + reserve_bytes <= queue_capacity_bytes_;
}

bool Recorder::AnyQueueHasEntriesLocked() const {
  for (const QueueBuffer& buffer : queues_) {
    if (!buffer.Empty()) {
      return true;
    }
  }
  return false;
}

bool Recorder::SwitchAcceptQueueLocked() {
  for (std::size_t offset = 1; offset < queues_.size(); ++offset) {
    const std::size_t index = (accept_queue_index_ + offset) % queues_.size();
    if (queues_[index].Empty()) {
      accept_queue_index_ = index;
      return true;
    }
  }
  return false;
}

bool Recorder::SelectNextDrainQueueLocked() {
  for (std::size_t offset = 0; offset < queues_.size(); ++offset) {
    const std::size_t index = (drain_queue_index_ + offset) % queues_.size();
    if (!queues_[index].Empty()) {
      drain_queue_index_ = index;
      return true;
    }
  }
  return false;
}

Recorder::AcceptQueueStatus Recorder::PrepareAcceptQueueLocked(std::size_t reserve_bytes) {
  if (CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes)) {
    return AcceptQueueStatus::kAccepted;
  }
  if (!SwitchAcceptQueueLocked()) {
    return AcceptQueueStatus::kBlocked;
  }
  return CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes) ? AcceptQueueStatus::kAcceptedAfterSwitch
                                                                        : AcceptQueueStatus::kBlocked;
}

bool Recorder::OpenCurrentSegment(std::string* error) {
  if (has_current_segment_) {
    return true;
  }
  has_current_segment_ = true;
  return internal::OpenSegment(recording_path_, static_cast<std::uint32_t>(manifest_.segments.size()),
                               &current_segment_, error);
}

void Recorder::HandleAbortedEntry() {
  manifest_.aborted_entries += 1;
  manifest_.degraded = true;
  manifest_dirty_ = true;
}

internal::ManifestData Recorder::BuildManifestSnapshot() const {
  internal::ManifestData snapshot = manifest_;
  if (has_current_segment_) {
    snapshot.segments.push_back(current_segment_.summary);
  }
  return snapshot;
}

bool Recorder::FlushManifestSnapshot(std::string* error) {
  return internal::WriteManifest(recording_path_, BuildManifestSnapshot(), error);
}

bool Recorder::ShouldSyncOnExplicitFlush() const {
  switch (config_.fsync_policy) {
    case FsyncPolicy::kNever:
      return false;
    case FsyncPolicy::kInterval:
      if (last_sync_time_ == std::chrono::steady_clock::time_point::min()) {
        return true;
      }
      return std::chrono::steady_clock::now() - last_sync_time_ >= ClampFlushInterval(config_.flush_interval_ms);
    case FsyncPolicy::kEveryFlush:
      return true;
  }

  return false;
}

bool Recorder::ShouldSyncOnClose() const { return config_.fsync_policy != FsyncPolicy::kNever; }

bool Recorder::FlushOpenSegmentStream(std::string* error) {
  if (!has_current_segment_ || current_segment_.summary.record_count == 0) {
    return true;
  }
  current_segment_.stream.flush();
  if (!current_segment_.stream.good()) {
    SetError("failed to flush active segment stream: " + current_segment_.file_path.string(), error);
    return false;
  }
  return true;
}

bool Recorder::SyncUnsyncedFinalizedSegments(std::string* error) {
  for (std::size_t index = synced_segment_count_; index < manifest_.segments.size(); ++index) {
    if (!internal::SyncFile(recording_path_ / manifest_.segments[index].file_name, error)) {
      return false;
    }
  }
  return true;
}

bool Recorder::SyncCurrentSegmentFile(std::string* error) {
  if (!has_current_segment_ || current_segment_.summary.record_count == 0 || !current_segment_dirty_) {
    return true;
  }
  if (!FlushOpenSegmentStream(error)) {
    return false;
  }
  return internal::SyncFile(current_segment_.file_path, error);
}

bool Recorder::SyncManifestSnapshot(std::string* error) {
  if (!internal::SyncFile(recording_path_ / "manifest.json", error)) {
    return false;
  }
  return internal::SyncDirectory(recording_path_, error);
}

void Recorder::MarkDurabilityCheckpointComplete() {
  synced_segment_count_ = manifest_.segments.size();
  current_segment_dirty_ = false;
  manifest_dirty_ = false;
  last_sync_time_ = std::chrono::steady_clock::now();
}

bool Recorder::PerformDurabilityCheckpoint(std::string* error) {
  if (!SyncUnsyncedFinalizedSegments(error) || !SyncCurrentSegmentFile(error)) {
    return false;
  }
  if (!FlushManifestSnapshot(error) || !SyncManifestSnapshot(error)) {
    return false;
  }
  MarkDurabilityCheckpointComplete();
  return true;
}

bool Recorder::RotateIfNeeded(const ReplayMessage& message, std::string* error) {
  if (!has_current_segment_) {
    return OpenCurrentSegment(error);
  }
  const RecordedMessage estimated{message.session_id, message.message_type,
                                  ByteView{message.payload.data(), message.payload.size()}};
  if (!internal::ShouldRotateSegment(current_segment_, segment_max_bytes_, internal::EstimateRecordBytes(estimated))) {
    return true;
  }
  if (!internal::FinalizeSegment(&current_segment_, error)) {
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
  return OpenCurrentSegment(error);
}

void Recorder::FinishCurrentSegment(std::string* error) {
  if (!has_current_segment_) {
    return;
  }
  if (!internal::FinalizeSegment(&current_segment_, error)) {
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

AppendResult Recorder::Append(const RecordedMessage& message) {
  QueuedEntry* entry = nullptr;
  AcceptQueueStatus accept_status = AcceptQueueStatus::kBlocked;
  const AppendResult reserve_result = ReserveAppendEntry(
      internal::EstimateRecordBytes(message),
      [&](QueuedEntry* reserved_entry, std::uint64_t record_seq, std::uint64_t mono_ts_us, std::uint64_t utc_ts_us) {
        PrepareQueuedMessage(&reserved_entry->message, message, record_seq, mono_ts_us, utc_ts_us);
      },
      &entry, &accept_status);
  if (reserve_result != AppendResult::kOk) {
    return reserve_result;
  }
  PublishReservedEntry(accept_status);

  try {
    // 只有在完整负载复制完成后再唤醒写线程，避免 pending 条目导致额外睡醒抖动。
    CopyByteView(message.payload, &entry->message.payload);
  } catch (const std::exception&) {
    entry->state.store(QueuedEntry::State::kAborted, std::memory_order_release);
    writer_cv_.notify_one();
    return AppendResult::kInternalError;
  }

  entry->state.store(QueuedEntry::State::kReady, std::memory_order_release);
  writer_cv_.notify_one();
  return AppendResult::kOk;
}

bool Recorder::Flush(std::string* error) {
  std::size_t target_id = 0;
  std::unique_lock<std::mutex> lock(mutex_);
  if (!fatal_error_.empty()) {
    SetError(fatal_error_, error);
    return false;
  }
  if (!open_ || closed_ || closing_requested_) {
    SetError("recorder is closed", error);
    return false;
  }

  target_id = ++flush_requested_id_;
  writer_cv_.notify_one();
  flush_cv_.wait(lock, [&]() { return flush_completed_id_ >= target_id || !fatal_error_.empty() || closed_; });
  if (!fatal_error_.empty()) {
    SetError(fatal_error_, error);
    return false;
  }
  return true;
}

bool Recorder::Close(std::string* error) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return fatal_error_.empty();
    }
    if (closing_requested_) {
      SetError(fatal_error_.empty() ? "recorder is closing" : fatal_error_, error);
      return false;
    }
    if (!open_) {
      SetError(fatal_error_.empty() ? "recorder is not open" : fatal_error_, error);
      return false;
    }
    open_ = false;
    closing_requested_ = true;
  }

  producer_cv_.notify_all();
  writer_cv_.notify_one();
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!fatal_error_.empty()) {
      SetError(fatal_error_, error);
      return false;
    }
  }

  auto remember_close_failure = [&](const std::string& message) {
    if (message.empty()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    fatal_error_ = message;
  };

  manifest_.stop_utc = internal::FormatUtcCompact(std::chrono::system_clock::now());
  manifest_.incomplete = false;
  manifest_dirty_ = true;

  const std::filesystem::path finalized_path =
      config_.output_root / ("recording-" + manifest_.start_utc + "-to-" + *manifest_.stop_utc);
  std::string close_error;
  if (ShouldSyncOnClose()) {
    if (!PerformDurabilityCheckpoint(&close_error)) {
      remember_close_failure(close_error);
      SetError(close_error, error);
      return false;
    }
  } else if (!internal::WriteManifest(recording_path_, manifest_, &close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }
  if (!internal::RenameDirectoryAtomically(recording_path_, finalized_path, &close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }
  if (ShouldSyncOnClose() && !internal::SyncDirectory(config_.output_root, &close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    recording_path_ = finalized_path;
    closed_ = true;
  }
  flush_cv_.notify_all();
  return true;
}

void Recorder::WriterLoop() {
  for (;;) {
    QueuedEntry* entry = nullptr;
    bool should_flush = false;
    bool should_exit = false;
    bool entry_aborted = false;
    bool notify_producers = false;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      writer_cv_.wait(lock, [&]() {
        return closing_requested_ || !fatal_error_.empty() || AnyQueueHasEntriesLocked() ||
               flush_requested_id_ > flush_completed_id_;
      });

      if (SelectNextDrainQueueLocked()) {
        QueueBuffer& drain_queue = queues_[drain_queue_index_];
        QueuedEntry* front = drain_queue.Front();
        if (front->state.load(std::memory_order_acquire) == QueuedEntry::State::kPending) {
          writer_cv_.wait(lock, [&]() {
            return front->state.load(std::memory_order_acquire) != QueuedEntry::State::kPending ||
                   !fatal_error_.empty();
          });
          continue;
        }
        entry = front;
        entry_aborted = entry->state.load(std::memory_order_acquire) == QueuedEntry::State::kAborted;
        drain_queue.Pop();
        drain_queue.reserved_bytes -= entry->reserved_bytes;
        entry->state.store(QueuedEntry::State::kConsumed, std::memory_order_relaxed);
        notify_producers = true;
      } else if (flush_requested_id_ > flush_completed_id_) {
        should_flush = true;
      } else if (closing_requested_) {
        should_exit = true;
      }
    }

    if (notify_producers) {
      producer_cv_.notify_one();
    }

    if (entry != nullptr) {
      if (entry_aborted) {
        HandleAbortedEntry();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          RecycleEntryLocked(entry);
        }
        continue;
      }

      std::string error;
      if (!RotateIfNeeded(entry->message, &error) ||
          !internal::WriteRecordToSegment(&current_segment_, entry->message, &error)) {
        manifest_.degraded = true;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          fatal_error_ = error;
          RecycleEntryLocked(entry);
        }
        producer_cv_.notify_all();
        flush_cv_.notify_all();
        writer_cv_.notify_all();
        break;
      }

      manifest_.total_records += 1;
      manifest_.total_payload_bytes += entry->message.payload.size();
      manifest_dirty_ = true;
      current_segment_dirty_ = true;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        RecycleEntryLocked(entry);
      }
      continue;
    }

    if (should_flush) {
      std::string error;
      if (ShouldSyncOnExplicitFlush() ? !PerformDurabilityCheckpoint(&error) : !FlushManifestSnapshot(&error)) {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          fatal_error_ = error;
        }
        producer_cv_.notify_all();
        flush_cv_.notify_all();
        writer_cv_.notify_all();
        break;
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        flush_completed_id_ = flush_requested_id_;
      }
      flush_cv_.notify_all();
      continue;
    }

    if (should_exit) {
      std::string error;
      FinishCurrentSegment(&error);
      if (!error.empty()) {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          fatal_error_ = error;
        }
        producer_cv_.notify_all();
        flush_cv_.notify_all();
        writer_cv_.notify_all();
      }
      break;
    }
  }
}

}  // namespace jojo::rec
