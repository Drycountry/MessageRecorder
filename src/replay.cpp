#include "jojo/rec/replay.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "jojo/rec/detail/manifest.hpp"
#include "jojo/rec/detail/segment.hpp"

namespace jojo::rec {
namespace {

constexpr auto kBusyWaitWindow = std::chrono::microseconds(200);
constexpr std::size_t kInvalidSegmentListIndex = std::numeric_limits<std::size_t>::max();
constexpr std::size_t kPrefetchMaxRecords = 64;
constexpr std::size_t kPrefetchMaxBytes = 1024 * 1024;
constexpr std::uint32_t kRecordMagic = 0x31524543U;
constexpr std::uint16_t kRecordHeaderSize = 48;

enum class PrepareEntryStatus {
  kOk,
  kEnd,
  kError,
};

struct SegmentFilePosition {
  std::size_t segment_list_index = kInvalidSegmentListIndex;
  std::uint64_t file_offset = 0;
};

struct PrefetchedRecord {
  ReplayMessage message;
  std::size_t encoded_size_bytes = 0;
};

/// @brief 在提供错误输出缓冲时写入错误消息。
void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

/// @brief 校验回放速度是否为有限且非负的数值。
bool IsValidReplaySpeed(double speed) { return std::isfinite(speed) && speed >= 0.0; }

/// @brief 判断某个 segment 是否可能包含指定记录序号。
bool SegmentMightContainRecordSequence(const SegmentSummary& segment, std::uint64_t value) {
  return segment.record_count != 0 && (!segment.last_record_seq.has_value() || *segment.last_record_seq >= value);
}

/// @brief 判断某个 segment 是否可能包含指定单调时间戳。
bool SegmentMightContainMonoTime(const SegmentSummary& segment, std::uint64_t value) {
  return segment.record_count != 0 &&
         (!segment.last_event_mono_ts_us.has_value() || *segment.last_event_mono_ts_us >= value);
}

/// @brief 判断某个 segment 是否可能包含指定 UTC 时间戳。
bool SegmentMightContainUtcTime(const SegmentSummary& segment, std::uint64_t value) {
  return segment.record_count != 0 &&
         (!segment.last_event_utc_ts_us.has_value() || *segment.last_event_utc_ts_us >= value);
}

/// @brief 根据回放游标找到第一个候选 segment 的列表索引。
bool FindSegmentListIndex(const internal::ManifestData& manifest, const ReplayCursor& cursor,
                          std::size_t* segment_list_index, std::string* error) {
  switch (cursor.kind) {
    case ReplayCursorKind::kRecordSequence:
      for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
        if (SegmentMightContainRecordSequence(manifest.segments[index], cursor.value)) {
          *segment_list_index = index;
          return true;
        }
      }
      SetError("record sequence cursor is beyond end of recording", error);
      return false;

    case ReplayCursorKind::kEventMonoTime:
      for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
        if (SegmentMightContainMonoTime(manifest.segments[index], cursor.value)) {
          *segment_list_index = index;
          return true;
        }
      }
      SetError("monotonic time cursor is beyond end of recording", error);
      return false;

    case ReplayCursorKind::kEventUtcTime:
      for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
        if (SegmentMightContainUtcTime(manifest.segments[index], cursor.value)) {
          *segment_list_index = index;
          return true;
        }
      }
      SetError("UTC time cursor is beyond end of recording", error);
      return false;

    case ReplayCursorKind::kSegmentCheckpoint:
      if (!cursor.segment_index.has_value()) {
        SetError("segment checkpoint cursor is missing segment_index", error);
        return false;
      }
      for (std::size_t index = 0; index < manifest.segments.size(); ++index) {
        if (manifest.segments[index].segment_index >= *cursor.segment_index) {
          *segment_list_index = index;
          return true;
        }
      }
      SetError("segment checkpoint is beyond end of recording", error);
      return false;
  }

  SetError("unknown replay cursor kind", error);
  return false;
}

/// @brief 判断一条记录在 lower-bound 意义下是否仍位于目标游标之前。
bool RecordIsBeforeCursor(const ReplayMessage& record, const ReplayCursor& cursor) {
  switch (cursor.kind) {
    case ReplayCursorKind::kRecordSequence:
      return record.record_seq < cursor.value;
    case ReplayCursorKind::kEventMonoTime:
      return record.event_mono_ts_us < cursor.value;
    case ReplayCursorKind::kEventUtcTime:
      return record.event_utc_ts_us < cursor.value;
    case ReplayCursorKind::kSegmentCheckpoint:
      return false;
  }
  return false;
}

/// @brief 判断某个稀疏索引点是否位于目标游标之前。
bool SegmentIndexEntryIsBeforeCursor(const internal::SegmentIndexEntry& entry, const ReplayCursor& cursor) {
  switch (cursor.kind) {
    case ReplayCursorKind::kRecordSequence:
      return entry.record_seq <= cursor.value;
    case ReplayCursorKind::kEventMonoTime:
      return entry.event_mono_ts_us <= cursor.value;
    case ReplayCursorKind::kEventUtcTime:
      return entry.event_utc_ts_us <= cursor.value;
    case ReplayCursorKind::kSegmentCheckpoint:
      return false;
  }
  return false;
}

/// @brief 从 segment 稀疏索引中选择一个不晚于目标游标的读取锚点。
std::uint64_t FindAnchorOffset(const internal::SegmentSparseIndexData& sparse_index, const ReplayCursor& cursor) {
  if (cursor.kind == ReplayCursorKind::kSegmentCheckpoint || sparse_index.entries.empty()) {
    return 0;
  }

  std::uint64_t anchor_offset = 0;
  for (const internal::SegmentIndexEntry& entry : sparse_index.entries) {
    if (!SegmentIndexEntryIsBeforeCursor(entry, cursor)) {
      break;
    }
    anchor_offset = entry.file_offset;
  }
  return anchor_offset;
}

/// @brief 判断录制中是否至少有一个包含记录的 segment。
bool HasReplayableSegments(const internal::ManifestData& manifest) {
  return std::any_of(manifest.segments.begin(), manifest.segments.end(),
                     [](const SegmentSummary& segment) { return segment.record_count != 0; });
}

/// @brief 估算一条回放消息在 segment 中占用的总字节数。
std::size_t MeasureEncodedSize(const ReplayMessage& message) { return kRecordHeaderSize + message.payload.size(); }

/// @brief 从原始字节指针按小端读取 16 位无符号整数。
std::uint16_t ReadU16(const std::uint8_t* bytes) {
  return static_cast<std::uint16_t>(bytes[0]) | (static_cast<std::uint16_t>(bytes[1]) << 8U);
}

/// @brief 从原始字节指针按小端读取 32 位无符号整数。
std::uint32_t ReadU32(const std::uint8_t* bytes) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(bytes[index]) << (index * 8);
  }
  return value;
}

/// @brief 从原始字节指针按小端读取 64 位无符号整数。
std::uint64_t ReadU64(const std::uint8_t* bytes) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(bytes[index]) << (index * 8);
  }
  return value;
}

/// @brief 从打开的 segment 流中读取固定长度字节。
bool ReadExact(std::ifstream* stream, std::uint64_t offset, std::uint8_t* destination, std::size_t size) {
  stream->clear();
  stream->seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  if (!stream->good()) {
    return false;
  }
  if (size == 0) {
    return true;
  }
  stream->read(reinterpret_cast<char*>(destination), static_cast<std::streamsize>(size));
  return stream->good();
}

/// @brief 只读取 record header，用于 seek 阶段快速判断 lower-bound。
bool ReadRecordHeaderAtOffset(internal::SegmentReadContext* context, std::uint64_t offset, ReplayMessage* record,
                              std::uint64_t* next_offset, std::string* error) {
  if (!context->open) {
    SetError("segment read context is not open", error);
    return false;
  }
  if (offset >= context->valid_bytes) {
    SetError("record offset is beyond segment valid bytes: " + context->file_path.string(), error);
    return false;
  }
  if (context->valid_bytes - offset < kRecordHeaderSize) {
    SetError("truncated record header near byte " + std::to_string(offset) + " in " + context->file_path.string(),
             error);
    return false;
  }

  std::array<std::uint8_t, kRecordHeaderSize> header{};
  if (!ReadExact(&context->stream, offset, header.data(), header.size())) {
    SetError("failed to read record header: " + context->file_path.string(), error);
    return false;
  }
  if (ReadU32(header.data()) != kRecordMagic) {
    SetError("record magic mismatch near byte " + std::to_string(offset) + " in " + context->file_path.string(), error);
    return false;
  }

  const std::uint16_t header_size = ReadU16(header.data() + 4);
  if (header_size != kRecordHeaderSize) {
    SetError("record header size mismatch near byte " + std::to_string(offset) + " in " + context->file_path.string(),
             error);
    return false;
  }

  const std::uint32_t payload_size = ReadU32(header.data() + 44);
  const std::uint64_t total_size = static_cast<std::uint64_t>(header_size) + payload_size;
  if (offset + total_size > context->valid_bytes) {
    SetError("truncated record body near byte " + std::to_string(offset) + " in " + context->file_path.string(), error);
    return false;
  }

  *record = ReplayMessage{};
  record->record_seq = ReadU64(header.data() + 8);
  record->event_mono_ts_us = ReadU64(header.data() + 16);
  record->event_utc_ts_us = ReadU64(header.data() + 24);
  record->session_id = ReadU64(header.data() + 32);
  record->message_type = ReadU32(header.data() + 40);
  *next_offset = offset + total_size;
  return true;
}

}  // namespace

class ReplaySession final : public IReplaySession {
 public:
  /// @brief 构造一个拥有 manifest 快照和目标对象的回放会话。
  ReplaySession(std::filesystem::path recording_path, internal::ManifestData manifest, ReplayOptions options,
                std::unique_ptr<IReplayTarget> target)
      : recording_path_(std::move(recording_path)),
        manifest_(std::move(manifest)),
        options_(options),
        target_(std::move(target)) {}

  /// @brief 析构会话并等待工作线程退出。
  ~ReplaySession() override {
    Stop();
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
  }

  /// @brief 在启动前读取稀疏索引并定位到初始游标位置。
  bool Initialize(const ReplayCursor& initial_cursor, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!LoadSparseIndexesLocked(error)) {
      return false;
    }
    return SeekLocked(initial_cursor, error);
  }

  /// @brief 启动异步回放线程。
  bool Start(std::string* error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) {
      SetError("replay session has already been started", error);
      return false;
    }

    started_ = true;
    worker_thread_ = std::thread([this]() { Run(); });
    return true;
  }

  /// @brief 请求在当前消息处理完成后进入暂停状态。
  bool Pause(std::string* error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_ || finished_) {
      SetError("replay session is not running", error);
      return false;
    }
    paused_ = true;
    return true;
  }

  /// @brief 从暂停状态恢复回放。
  bool Resume(std::string* error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_ || finished_) {
      SetError("replay session is not running", error);
      return false;
    }
    paused_ = false;
    cv_.notify_all();
    return true;
  }

  /// @brief 更新回放速度倍率。
  bool SetSpeed(double speed, std::string* error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsValidReplaySpeed(speed)) {
      SetError("replay speed must be finite and non-negative", error);
      return false;
    }
    if (finished_) {
      SetError("replay session is already finished", error);
      return false;
    }
    options_.speed = speed;
    cv_.notify_all();
    return true;
  }

  /// @brief 在运行中切换到新的回放游标。
  bool Seek(const ReplayCursor& cursor, std::string* error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!SeekLocked(cursor, error)) {
      return false;
    }
    cv_.notify_all();
    return true;
  }

  /// @brief 请求工作线程尽快平滑停止。
  void Stop() override {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = true;
    cv_.notify_all();
  }

  /// @brief 等待工作线程结束并返回最终结果。
  ReplayResult Wait() override {
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return result_;
  }

 private:
  /// @brief 仅在首次初始化时把所有 segment 的稀疏索引加载到内存。
  bool LoadSparseIndexesLocked(std::string* error) {
    if (sparse_indexes_loaded_) {
      return true;
    }

    segment_sparse_indexes_.clear();
    segment_sparse_indexes_.reserve(manifest_.segments.size());
    for (const SegmentSummary& segment : manifest_.segments) {
      internal::SegmentSparseIndexData sparse_index;
      if (segment.has_footer &&
          !internal::LoadSegmentSparseIndex(recording_path_ / segment.file_name, &sparse_index, error)) {
        return false;
      }
      segment_sparse_indexes_.push_back(std::move(sparse_index));
    }
    sparse_indexes_loaded_ = true;
    return true;
  }

  /// @brief 返回某个 segment 在回放路径上应该使用的逻辑有效字节数。
  std::uint64_t SegmentValidBytes(std::size_t segment_list_index) const {
    if (segment_list_index < segment_sparse_indexes_.size() &&
        segment_sparse_indexes_[segment_list_index].footer_valid) {
      return segment_sparse_indexes_[segment_list_index].valid_bytes;
    }
    return manifest_.segments[segment_list_index].valid_bytes;
  }

  /// @brief 返回某个 segment 的绝对路径。
  std::filesystem::path SegmentPath(std::size_t segment_list_index) const {
    return recording_path_ / manifest_.segments[segment_list_index].file_name;
  }

  /// @brief 清空预读窗口并重置累计字节数。
  void ClearPrefetchLocked() {
    std::deque<PrefetchedRecord>().swap(prefetch_window_);
    prefetched_bytes_ = 0;
    reached_end_ = false;
  }

  /// @brief 关闭当前复用中的 segment 读流。
  void ResetReaderLocked() {
    internal::CloseSegmentReadContext(&stream_reader_);
    reader_segment_list_index_ = kInvalidSegmentListIndex;
  }

  /// @brief 使用新的磁盘位置作为后续回放起点。
  void SetReadPositionLocked(const SegmentFilePosition& position) {
    ClearPrefetchLocked();
    ResetReaderLocked();
    next_read_position_ = position;
    timing_reset_requested_ = true;
    ++cursor_generation_;
  }

  /// @brief 将读取位置推进到下一个确实还有记录数据的 segment。
  bool MoveToNextReadablePositionLocked(SegmentFilePosition* position) {
    while (position->segment_list_index < manifest_.segments.size()) {
      const std::uint64_t valid_bytes = SegmentValidBytes(position->segment_list_index);
      if (position->file_offset < valid_bytes) {
        return true;
      }
      if (reader_segment_list_index_ == position->segment_list_index) {
        ResetReaderLocked();
      }
      ++position->segment_list_index;
      position->file_offset = 0;
    }
    return false;
  }

  /// @brief 确保当前读流已经打开到给定 segment。
  bool EnsureReaderForPositionLocked(const SegmentFilePosition& position, std::string* error) {
    const std::uint64_t valid_bytes = SegmentValidBytes(position.segment_list_index);
    if (reader_segment_list_index_ == position.segment_list_index && stream_reader_.open &&
        stream_reader_.valid_bytes == valid_bytes) {
      return true;
    }

    ResetReaderLocked();
    if (!internal::OpenSegmentReadContext(SegmentPath(position.segment_list_index), valid_bytes, &stream_reader_,
                                          error)) {
      return false;
    }
    reader_segment_list_index_ = position.segment_list_index;
    return true;
  }

  /// @brief 从当前读位置拉取一条完整消息放入预读窗口。
  bool PrefetchOneRecordLocked(std::string* error) {
    if (!MoveToNextReadablePositionLocked(&next_read_position_)) {
      reached_end_ = true;
      ResetReaderLocked();
      return true;
    }
    if (!EnsureReaderForPositionLocked(next_read_position_, error)) {
      return false;
    }

    ReplayMessage message;
    std::uint64_t next_offset = 0;
    if (!internal::ReadRecordAtOffset(&stream_reader_, next_read_position_.file_offset, &message, &next_offset,
                                      error)) {
      return false;
    }

    const std::size_t encoded_size = MeasureEncodedSize(message);
    prefetched_bytes_ += encoded_size;
    prefetch_window_.push_back(PrefetchedRecord{std::move(message), encoded_size});
    next_read_position_.file_offset = next_offset;

    if (!MoveToNextReadablePositionLocked(&next_read_position_)) {
      reached_end_ = true;
      ResetReaderLocked();
    } else if (reader_segment_list_index_ != next_read_position_.segment_list_index) {
      ResetReaderLocked();
    }
    return true;
  }

  /// @brief 按配置的记录数和字节上限填充一个小型预读窗口。
  bool FillPrefetchWindowLocked(std::string* error) {
    while (!reached_end_) {
      if (!prefetch_window_.empty() &&
          (prefetch_window_.size() >= kPrefetchMaxRecords || prefetched_bytes_ >= kPrefetchMaxBytes)) {
        break;
      }
      if (!PrefetchOneRecordLocked(error)) {
        return false;
      }
    }
    return true;
  }

  /// @brief 从某个 segment 起查找首条可回放记录的磁盘偏移。
  bool FindFirstRecordPositionFromSegment(std::size_t start_segment_list_index, const std::string& eof_message,
                                          SegmentFilePosition* position, std::string* error) const {
    for (std::size_t index = start_segment_list_index; index < manifest_.segments.size(); ++index) {
      const std::uint64_t valid_bytes = SegmentValidBytes(index);
      if (valid_bytes == 0) {
        continue;
      }

      internal::SegmentReadContext context;
      if (!internal::OpenSegmentReadContext(SegmentPath(index), valid_bytes, &context, error)) {
        return false;
      }

      ReplayMessage header_only;
      std::uint64_t next_offset = 0;
      const bool ok = ReadRecordHeaderAtOffset(&context, 0, &header_only, &next_offset, error);
      internal::CloseSegmentReadContext(&context);
      if (!ok) {
        return false;
      }

      *position = SegmentFilePosition{index, 0};
      return true;
    }

    SetError(eof_message, error);
    return false;
  }

  /// @brief 基于 segment 稀疏索引和少量 header 顺扫求出 lower-bound 起点。
  bool FindLowerBoundPosition(std::size_t start_segment_list_index, const ReplayCursor& cursor,
                              const std::string& eof_message, SegmentFilePosition* position, std::string* error) const {
    for (std::size_t index = start_segment_list_index; index < manifest_.segments.size(); ++index) {
      const std::uint64_t valid_bytes = SegmentValidBytes(index);
      if (valid_bytes == 0) {
        continue;
      }

      std::uint64_t offset = 0;
      if (index < segment_sparse_indexes_.size()) {
        offset = FindAnchorOffset(segment_sparse_indexes_[index], cursor);
      }
      if (offset >= valid_bytes) {
        offset = 0;
      }

      internal::SegmentReadContext context;
      if (!internal::OpenSegmentReadContext(SegmentPath(index), valid_bytes, &context, error)) {
        return false;
      }

      while (offset < valid_bytes) {
        ReplayMessage header_only;
        std::uint64_t next_offset = 0;
        if (!ReadRecordHeaderAtOffset(&context, offset, &header_only, &next_offset, error)) {
          internal::CloseSegmentReadContext(&context);
          return false;
        }
        if (!RecordIsBeforeCursor(header_only, cursor)) {
          internal::CloseSegmentReadContext(&context);
          *position = SegmentFilePosition{index, offset};
          return true;
        }
        offset = next_offset;
      }

      internal::CloseSegmentReadContext(&context);
    }

    SetError(eof_message, error);
    return false;
  }

  /// @brief 在持锁状态下完成一次游标跳转。
  bool SeekLocked(const ReplayCursor& cursor, std::string* error) {
    std::size_t segment_list_index = 0;
    if (!FindSegmentListIndex(manifest_, cursor, &segment_list_index, error)) {
      return false;
    }

    SegmentFilePosition position;
    switch (cursor.kind) {
      case ReplayCursorKind::kRecordSequence:
        if (!FindLowerBoundPosition(segment_list_index, cursor, "record sequence cursor is beyond end of recording",
                                    &position, error)) {
          return false;
        }
        break;

      case ReplayCursorKind::kEventMonoTime:
        if (!FindLowerBoundPosition(segment_list_index, cursor, "monotonic time cursor is beyond end of recording",
                                    &position, error)) {
          return false;
        }
        break;

      case ReplayCursorKind::kEventUtcTime:
        if (!FindLowerBoundPosition(segment_list_index, cursor, "UTC time cursor is beyond end of recording", &position,
                                    error)) {
          return false;
        }
        break;

      case ReplayCursorKind::kSegmentCheckpoint:
        if (!FindFirstRecordPositionFromSegment(segment_list_index, "segment checkpoint is beyond end of recording",
                                                &position, error)) {
          return false;
        }
        break;
    }

    SetReadPositionLocked(position);
    return true;
  }

  /// @brief 读取当前位置对应的当前消息，必要时再填充预读窗口。
  PrepareEntryStatus PrepareCurrentMessageLocked(ReplayMessage* message, std::uint64_t* generation,
                                                 std::string* error) {
    if (prefetch_window_.empty()) {
      if (!FillPrefetchWindowLocked(error)) {
        return PrepareEntryStatus::kError;
      }
    }
    if (prefetch_window_.empty()) {
      return reached_end_ ? PrepareEntryStatus::kEnd : PrepareEntryStatus::kError;
    }

    *message = prefetch_window_.front().message;
    *generation = cursor_generation_;
    return PrepareEntryStatus::kOk;
  }

  /// @brief 将当前位置推进到下一条消息。
  void AdvancePositionLocked() {
    if (prefetch_window_.empty()) {
      return;
    }
    prefetched_bytes_ -= prefetch_window_.front().encoded_size_bytes;
    prefetch_window_.pop_front();
  }

  /// @brief 按当前速度等待两条消息之间应有的回放延迟。
  bool WaitForPlaybackDelay(std::uint64_t logical_delta_us, const std::chrono::steady_clock::time_point& reference_time,
                            std::uint64_t observed_generation) {
    double remaining_logical_us = static_cast<double>(logical_delta_us);
    auto wall_checkpoint = reference_time;
    double applied_speed = 0.0;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      applied_speed = options_.speed;
      if (applied_speed == 0.0) {
        return true;
      }
    }

    for (;;) {
      std::unique_lock<std::mutex> lock(mutex_);
      const auto now = std::chrono::steady_clock::now();
      if (applied_speed > 0.0) {
        const double elapsed_wall_us = std::chrono::duration<double, std::micro>(now - wall_checkpoint).count();
        remaining_logical_us -= elapsed_wall_us * applied_speed;
      }
      wall_checkpoint = now;
      const double current_speed = options_.speed;
      applied_speed = current_speed;

      if (remaining_logical_us <= 0.0 || current_speed == 0.0) {
        return true;
      }
      if (stop_requested_ || paused_ || timing_reset_requested_ || cursor_generation_ != observed_generation) {
        return false;
      }

      const auto remaining_wall = std::chrono::duration<double, std::micro>(remaining_logical_us / current_speed);
      if (options_.high_precision_mode &&
          remaining_wall <= std::chrono::duration<double, std::micro>(kBusyWaitWindow)) {
        lock.unlock();
        while (std::chrono::steady_clock::now() - wall_checkpoint < remaining_wall) {
        }
        return true;
      }

      const auto sleep_duration = std::min(remaining_wall, std::chrono::duration<double, std::micro>(1000.0));
      cv_.wait_for(lock, sleep_duration, [&]() {
        return stop_requested_ || paused_ || timing_reset_requested_ || cursor_generation_ != observed_generation ||
               options_.speed != current_speed;
      });
    }
  }

  /// @brief 工作线程主循环，负责定时分发回放消息。
  void Run() {
    std::optional<std::uint64_t> previous_event_mono_ts_us;
    auto last_dispatch_time = std::chrono::steady_clock::now();

    while (true) {
      ReplayMessage entry;
      std::uint64_t observed_generation = 0;
      std::string load_error;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&]() { return stop_requested_ || !paused_; });
        if (stop_requested_) {
          result_.stopped_by_request = true;
          finished_ = true;
          return;
        }

        const PrepareEntryStatus prepare_status =
            PrepareCurrentMessageLocked(&entry, &observed_generation, &load_error);
        if (prepare_status == PrepareEntryStatus::kEnd) {
          result_.completed = true;
          finished_ = true;
          return;
        }
        if (prepare_status == PrepareEntryStatus::kError) {
          result_.failure =
              ReplayFailure{std::nullopt, std::nullopt, load_error.empty() ? "failed to load replay data" : load_error};
          finished_ = true;
          return;
        }
        if (timing_reset_requested_) {
          previous_event_mono_ts_us.reset();
          timing_reset_requested_ = false;
          last_dispatch_time = std::chrono::steady_clock::now();
        }
      }

      if (previous_event_mono_ts_us.has_value()) {
        const std::uint64_t delta_us = entry.event_mono_ts_us >= *previous_event_mono_ts_us
                                           ? entry.event_mono_ts_us - *previous_event_mono_ts_us
                                           : 0;
        if (!WaitForPlaybackDelay(delta_us, last_dispatch_time, observed_generation)) {
          continue;
        }
      }

      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_requested_) {
          result_.stopped_by_request = true;
          finished_ = true;
          return;
        }
        if (paused_ || timing_reset_requested_ || cursor_generation_ != observed_generation) {
          continue;
        }
      }

      std::string callback_error;
      if (!target_->OnMessage(entry, &callback_error)) {
        std::lock_guard<std::mutex> lock(mutex_);
        result_.failure = ReplayFailure{entry.record_seq, entry.event_utc_ts_us,
                                        callback_error.empty() ? "replay target rejected message" : callback_error};
        finished_ = true;
        return;
      }

      previous_event_mono_ts_us = entry.event_mono_ts_us;
      last_dispatch_time = std::chrono::steady_clock::now();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_requested_) {
          result_.stopped_by_request = true;
          finished_ = true;
          return;
        }
        if (paused_ || timing_reset_requested_ || cursor_generation_ != observed_generation) {
          continue;
        }
        AdvancePositionLocked();
      }
    }
  }

  std::filesystem::path recording_path_;
  internal::ManifestData manifest_;
  std::vector<internal::SegmentSparseIndexData> segment_sparse_indexes_;
  bool sparse_indexes_loaded_ = false;
  std::deque<PrefetchedRecord> prefetch_window_;
  std::size_t prefetched_bytes_ = 0;
  SegmentFilePosition next_read_position_;
  internal::SegmentReadContext stream_reader_;
  std::size_t reader_segment_list_index_ = kInvalidSegmentListIndex;
  bool reached_end_ = false;
  std::uint64_t cursor_generation_ = 0;
  ReplayOptions options_;
  std::unique_ptr<IReplayTarget> target_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread worker_thread_;
  bool started_ = false;
  bool paused_ = false;
  bool stop_requested_ = false;
  bool finished_ = false;
  bool timing_reset_requested_ = false;
  ReplayResult result_;
};

/// @brief 创建并初始化一个异步回放会话。
std::unique_ptr<IReplaySession> CreateReplaySession(const std::filesystem::path& recording_path,
                                                    const ReplayCursor& initial_cursor, const ReplayOptions& options,
                                                    std::unique_ptr<IReplayTarget> target, std::string* error) {
  if (target == nullptr) {
    SetError("replay target must not be null", error);
    return nullptr;
  }
  if (!IsValidReplaySpeed(options.speed)) {
    SetError("replay speed must be finite and non-negative", error);
    return nullptr;
  }

  internal::ManifestData manifest;
  if (!internal::LoadManifest(recording_path, &manifest, error)) {
    return nullptr;
  }
  if (!HasReplayableSegments(manifest)) {
    SetError("recording contains no replayable messages", error);
    return nullptr;
  }

  auto session = std::make_unique<ReplaySession>(recording_path, std::move(manifest), options, std::move(target));
  if (!session->Initialize(initial_cursor, error)) {
    return nullptr;
  }
  return session;
}

}  // namespace jojo::rec
