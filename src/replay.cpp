#include "jojo/rec/replay.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "internal/internal.hpp"

namespace jojo::rec {
namespace {

constexpr auto kBusyWaitWindow = std::chrono::microseconds(200);
constexpr std::size_t kInvalidSegmentListIndex = std::numeric_limits<std::size_t>::max();

enum class PrepareEntryStatus {
  kOk,
  kEnd,
  kError,
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

/// @brief 在已加载的 segment 记录数组中找到 lower-bound 位置。
bool FindRecordIndexInSegment(const std::vector<ReplayMessage>& records, const ReplayCursor& cursor,
                              std::size_t* record_index) {
  switch (cursor.kind) {
    case ReplayCursorKind::kRecordSequence: {
      const auto it =
          std::lower_bound(records.begin(), records.end(), cursor.value,
                           [](const ReplayMessage& record, std::uint64_t value) { return record.record_seq < value; });
      if (it == records.end()) {
        return false;
      }
      *record_index = static_cast<std::size_t>(std::distance(records.begin(), it));
      return true;
    }

    case ReplayCursorKind::kEventMonoTime: {
      const auto it = std::lower_bound(
          records.begin(), records.end(), cursor.value,
          [](const ReplayMessage& record, std::uint64_t value) { return record.event_mono_ts_us < value; });
      if (it == records.end()) {
        return false;
      }
      *record_index = static_cast<std::size_t>(std::distance(records.begin(), it));
      return true;
    }

    case ReplayCursorKind::kEventUtcTime: {
      const auto it = std::lower_bound(
          records.begin(), records.end(), cursor.value,
          [](const ReplayMessage& record, std::uint64_t value) { return record.event_utc_ts_us < value; });
      if (it == records.end()) {
        return false;
      }
      *record_index = static_cast<std::size_t>(std::distance(records.begin(), it));
      return true;
    }

    case ReplayCursorKind::kSegmentCheckpoint:
      *record_index = 0;
      return !records.empty();
  }

  return false;
}

/// @brief 判断录制中是否至少有一个包含记录的 segment。
bool HasReplayableSegments(const internal::ManifestData& manifest) {
  return std::any_of(manifest.segments.begin(), manifest.segments.end(),
                     [](const SegmentSummary& segment) { return segment.record_count != 0; });
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

  /// @brief 在启动前定位到初始游标位置。
  bool Initialize(const ReplayCursor& initial_cursor, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
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
  /// @brief 丢弃当前已加载的 segment 缓存。
  void DiscardLoadedSegmentLocked() {
    std::vector<ReplayMessage>().swap(loaded_segment_records_);
    loaded_segment_list_index_ = kInvalidSegmentListIndex;
  }

  /// @brief 加载指定 segment 的完整记录数组并缓存。
  bool LoadSegmentRecordsLocked(std::size_t segment_list_index, std::string* error) {
    if (loaded_segment_list_index_ == segment_list_index) {
      return true;
    }

    internal::SegmentScanOptions options;
    options.load_payloads = true;
    options.max_data_bytes = manifest_.segments[segment_list_index].valid_bytes;
    internal::SegmentScanResult scan_result;
    if (!internal::ScanSegment(recording_path_ / manifest_.segments[segment_list_index].file_name, options,
                               &scan_result, error)) {
      return false;
    }
    loaded_segment_records_ = std::move(scan_result.records);
    loaded_segment_list_index_ = segment_list_index;
    return true;
  }

  /// @brief 更新当前播放位置并请求重置时间基准。
  void SetPositionLocked(std::size_t segment_list_index, std::size_t record_index) {
    current_segment_list_index_ = segment_list_index;
    current_record_index_ = record_index;
    timing_reset_requested_ = true;
    ++cursor_generation_;
  }

  /// @brief 从某个 segment 开始定位到第一个可回放记录。
  bool SeekToFirstRecordFromSegmentLocked(std::size_t start_segment_list_index, const std::string& eof_message,
                                          std::string* error) {
    for (std::size_t index = start_segment_list_index; index < manifest_.segments.size(); ++index) {
      if (!LoadSegmentRecordsLocked(index, error)) {
        return false;
      }
      if (loaded_segment_records_.empty()) {
        continue;
      }
      SetPositionLocked(index, 0);
      return true;
    }

    SetError(eof_message, error);
    return false;
  }

  /// @brief 从候选 segment 起查找满足 lower-bound 的第一条记录。
  bool SeekToLowerBoundLocked(std::size_t start_segment_list_index, const ReplayCursor& cursor,
                              const std::string& eof_message, std::string* error) {
    for (std::size_t index = start_segment_list_index; index < manifest_.segments.size(); ++index) {
      if (!LoadSegmentRecordsLocked(index, error)) {
        return false;
      }
      if (loaded_segment_records_.empty()) {
        continue;
      }

      std::size_t record_index = 0;
      if (FindRecordIndexInSegment(loaded_segment_records_, cursor, &record_index)) {
        SetPositionLocked(index, record_index);
        return true;
      }
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

    switch (cursor.kind) {
      case ReplayCursorKind::kRecordSequence:
        return SeekToLowerBoundLocked(segment_list_index, cursor, "record sequence cursor is beyond end of recording",
                                      error);

      case ReplayCursorKind::kEventMonoTime:
        return SeekToLowerBoundLocked(segment_list_index, cursor, "monotonic time cursor is beyond end of recording",
                                      error);

      case ReplayCursorKind::kEventUtcTime:
        return SeekToLowerBoundLocked(segment_list_index, cursor, "UTC time cursor is beyond end of recording", error);

      case ReplayCursorKind::kSegmentCheckpoint:
        return SeekToFirstRecordFromSegmentLocked(segment_list_index, "segment checkpoint is beyond end of recording",
                                                  error);
    }

    SetError("unknown replay cursor kind", error);
    return false;
  }

  /// @brief 读取当前位置对应的当前消息，必要时跨 segment 前进。
  PrepareEntryStatus PrepareCurrentMessageLocked(ReplayMessage* message, std::uint64_t* generation,
                                                 std::string* error) {
    for (;;) {
      if (current_segment_list_index_ >= manifest_.segments.size()) {
        return PrepareEntryStatus::kEnd;
      }
      if (!LoadSegmentRecordsLocked(current_segment_list_index_, error)) {
        return PrepareEntryStatus::kError;
      }
      if (current_record_index_ < loaded_segment_records_.size()) {
        *message = loaded_segment_records_[current_record_index_];
        *generation = cursor_generation_;
        return PrepareEntryStatus::kOk;
      }

      ++current_segment_list_index_;
      current_record_index_ = 0;
      DiscardLoadedSegmentLocked();
    }
  }

  /// @brief 将当前位置推进到下一条消息。
  void AdvancePositionLocked() {
    ++current_record_index_;
    if (current_record_index_ >= loaded_segment_records_.size()) {
      ++current_segment_list_index_;
      current_record_index_ = 0;
      DiscardLoadedSegmentLocked();
    }
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

    for (;;) {
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
          result_.failure = ReplayFailure{std::nullopt, std::nullopt,
                                          load_error.empty() ? "failed to load replay segment" : load_error};
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
        const std::uint64_t delta_us = entry.event_mono_ts_us - *previous_event_mono_ts_us;
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
        if (timing_reset_requested_ || cursor_generation_ != observed_generation) {
          continue;
        }
        AdvancePositionLocked();
      }
    }
  }

  std::filesystem::path recording_path_;
  internal::ManifestData manifest_;
  std::size_t loaded_segment_list_index_ = kInvalidSegmentListIndex;
  std::vector<ReplayMessage> loaded_segment_records_;
  std::size_t current_segment_list_index_ = 0;
  std::size_t current_record_index_ = 0;
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
