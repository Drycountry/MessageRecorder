#include "jojo/rec/recorder.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "internal/internal.hpp"

namespace jojo::rec {
namespace {

constexpr std::uint64_t kBytesPerMb = 1000ULL * 1000ULL;

bool ConvertMegabytesToBytes(std::size_t megabytes, std::uint64_t* bytes) {
  if (megabytes > std::numeric_limits<std::uint64_t>::max() / kBytesPerMb) {
    return false;
  }
  *bytes = static_cast<std::uint64_t>(megabytes) * kBytesPerMb;
  return true;
}

std::size_t ComputeQueueSlotCapacity(std::size_t queue_capacity_bytes) {
  const std::size_t min_record_bytes = internal::EstimateRecordBytes(RecordedMessage{});
  if (min_record_bytes == 0) {
    return 2;
  }
  const std::uint64_t max_entries =
      static_cast<std::uint64_t>(queue_capacity_bytes) / min_record_bytes + 2ULL;
  const std::uint64_t clamped =
      std::min<std::uint64_t>(max_entries, std::numeric_limits<std::size_t>::max());
  return static_cast<std::size_t>(std::max<std::uint64_t>(clamped, 2ULL));
}

void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

void PrepareQueuedMessage(ReplayMessage* owned,
                          const RecordedMessage& message,
                          std::uint64_t record_seq,
                          std::uint64_t event_mono_ts_us,
                          std::uint64_t event_utc_ts_us) {
  owned->record_seq = record_seq;
  owned->event_mono_ts_us = event_mono_ts_us;
  owned->event_utc_ts_us = event_utc_ts_us;
  owned->session_id = message.session_id;
  owned->stream_id = message.stream_id;
  owned->message_type = message.message_type;
  owned->message_version = message.message_version;
  owned->payload.clear();
  owned->attributes.clear();
}

void CopyByteView(const ByteView& source, std::vector<std::uint8_t>* destination) {
  destination->resize(source.size);
  if (source.size != 0) {
    std::memcpy(destination->data(), source.data, source.size);
  }
}

}  // 匿名命名空间

class RecorderImpl {
 public:
  /**
   * @brief 生产者线程与写线程共享的队列条目。
   */
  struct QueuedEntry {
    /** @brief 生产者到写线程的状态机。 */
    enum class State {
      /** @brief 生产者已预留队列字节，但尚未完成拷贝。 */
      kPending,
      /** @brief 条目已经拥有自己的字节数据，可以按顺序写盘。 */
      kReady,
      /** @brief 生产者在分配序号后发生失败。 */
      kAborted,
      /** @brief 写线程已消费该条目，并释放了队列容量。 */
      kConsumed,
    };

    /** @brief 当前生产者与写线程之间的协同状态。 */
    std::atomic<State> state{State::kPending};
    /** @brief 计入实际队列字节容量的已预留队列字节数。 */
    std::size_t reserved_bytes = 0;
    /** @brief 由生产者填充的完整拥有消息。 */
    ReplayMessage message;
  };

  /**
   * @brief 由互斥锁保护的一组待处理条目及其预留容量。
   */
  struct QueueBuffer {
    QueueBuffer() = default;

    explicit QueueBuffer(std::size_t capacity) : slots(capacity, nullptr) {}

    bool Empty() const { return size == 0; }

    bool Full() const { return size == slots.size(); }

    QueuedEntry* Front() const { return Empty() ? nullptr : slots[head_index]; }

    void PushBack(QueuedEntry* entry) {
      const std::size_t tail_index = (head_index + size) % slots.size();
      slots[tail_index] = entry;
      ++size;
    }

    void PopFront() {
      slots[head_index] = nullptr;
      head_index = (head_index + 1) % slots.size();
      --size;
      if (size == 0) {
        head_index = 0;
      }
    }

    /** @brief 固定容量 ring buffer 的槽位存储。 */
    std::vector<QueuedEntry*> slots;
    /** @brief 队头所在槽位索引。 */
    std::size_t head_index = 0;
    /** @brief 当前 ring 中有效条目数。 */
    std::size_t size = 0;
    /** @brief 当前缓冲中已预留的总字节数。 */
    std::size_t reserved_bytes = 0;
  };

  /**
   * @brief `Append()` 试图把消息放入当前接收队列后的结果。
   */
  enum class AcceptQueueStatus {
    /** @brief 当前接收队列可直接接收。 */
    kAccepted,
    /** @brief 切换到下一个空闲接收队列后接收成功。 */
    kAcceptedAfterSwitch,
    /** @brief 当前无法接收，调用方需要等待或失败返回。 */
    kBlocked,
  };

  RecorderImpl(const RecorderConfig& config, std::string* error) : config_(config) {
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
    const std::size_t queue_slot_capacity = ComputeQueueSlotCapacity(queue_capacity_bytes_);
    queues_.assign(config_.queue_buffer_count, QueueBuffer(queue_slot_capacity));
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

    // 先写入初始 manifest，再启动写线程，这样即使崩溃也会留下元数据。
    open_ = true;
    writer_thread_ = std::thread([this]() { WriterLoop(); });
  }
  ~RecorderImpl() {
    std::string ignored;
    if (open_) {
      Close(&ignored);
    }
  }

  RecorderImpl(RecorderImpl&& other) noexcept { *this = std::move(other); }

  RecorderImpl& operator=(RecorderImpl&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    this->~RecorderImpl();
    config_ = std::move(other.config_);
    queue_capacity_bytes_ = other.queue_capacity_bytes_;
    segment_max_bytes_ = other.segment_max_bytes_;
    recording_path_ = std::move(other.recording_path_);
    manifest_ = std::move(other.manifest_);
    current_segment_ = std::move(other.current_segment_);
    has_current_segment_ = other.has_current_segment_;
    open_ = other.open_;
    closing_requested_ = other.closing_requested_;
    closed_ = other.closed_;
    fatal_error_ = std::move(other.fatal_error_);
    next_record_seq_ = other.next_record_seq_;
    queues_ = std::move(other.queues_);
    accept_queue_index_ = other.accept_queue_index_;
    drain_queue_index_ = other.drain_queue_index_;
    entry_storage_ = std::move(other.entry_storage_);
    free_entries_ = std::move(other.free_entries_);
    flush_requested_id_ = other.flush_requested_id_;
    flush_completed_id_ = other.flush_completed_id_;
    writer_thread_ = std::move(other.writer_thread_);

    other.queue_capacity_bytes_ = 0;
    other.segment_max_bytes_ = 0;
    other.has_current_segment_ = false;
    other.open_ = false;
    other.closing_requested_ = true;
    other.closed_ = true;
    other.queues_.clear();
    other.accept_queue_index_ = 0;
    other.drain_queue_index_ = 0;
    other.entry_storage_.clear();
    other.free_entries_.clear();
    return *this;
  }

  bool IsOpen() const { return open_; }

  const RecorderConfig& Config() const { return config_; }

  std::filesystem::path RecordingPath() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return recording_path_;
  }

  template <typename PrepareFn>
  AppendResult ReserveAppendEntry(std::size_t reserve_bytes,
                                  PrepareFn&& prepare_entry,
                                  QueuedEntry** entry,
                                  AcceptQueueStatus* accept_status) {
    if (!open_) {
      return AppendResult::kClosed;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    while (!closing_requested_ && fatal_error_.empty()) {
      *accept_status = PrepareAcceptQueueLocked(reserve_bytes);
      if (*accept_status != AcceptQueueStatus::kBlocked) {
        break;
      }
      if (config_.backpressure_policy == BackpressurePolicy::kFailFast) {
        return AppendResult::kBackpressure;
      }
      // 当所有内部接收队列都无法再接收时，阻塞等待写线程释放队列容量。
      producer_cv_.wait(lock);
    }
    if (closing_requested_) {
      return AppendResult::kClosed;
    }
    if (!fatal_error_.empty()) {
      return AppendResult::kInternalError;
    }

    const auto mono_now = std::chrono::steady_clock::now();
    const auto utc_now = std::chrono::system_clock::now();
    *entry = AcquireEntryLocked();
    (*entry)->state.store(QueuedEntry::State::kPending, std::memory_order_relaxed);
    (*entry)->reserved_bytes = reserve_bytes;
    prepare_entry(*entry, next_record_seq_++, internal::ToSteadyMicros(mono_now),
                  internal::ToUnixMicros(utc_now));
    QueueBuffer& accept_queue = queues_[accept_queue_index_];
    accept_queue.PushBack(*entry);
    accept_queue.reserved_bytes += reserve_bytes;
    return AppendResult::kOk;
  }

  void PublishReservedEntry(AcceptQueueStatus accept_status) {
    writer_cv_.notify_one();
    if (accept_status == AcceptQueueStatus::kAcceptedAfterSwitch) {
      producer_cv_.notify_all();
    }
  }

  AppendResult Append(const RecordedMessage& message) {
    QueuedEntry* entry = nullptr;
    AcceptQueueStatus accept_status = AcceptQueueStatus::kBlocked;
    const AppendResult reserve_result = ReserveAppendEntry(
        internal::EstimateRecordBytes(message),
        [&](QueuedEntry* reserved_entry, std::uint64_t record_seq, std::uint64_t mono_ts_us,
            std::uint64_t utc_ts_us) {
          PrepareQueuedMessage(&reserved_entry->message, message, record_seq, mono_ts_us,
                               utc_ts_us);
        },
        &entry, &accept_status);
    if (reserve_result != AppendResult::kOk) {
      return reserve_result;
    }
    PublishReservedEntry(accept_status);

    try {
      // 生产者在预留好有序队列槽位后，再复制负载字节；复用条目自带缓冲容量。
      CopyByteView(message.payload, &entry->message.payload);
      if (message.attributes.has_value()) {
        CopyByteView(*message.attributes, &entry->message.attributes);
      }
    } catch (const std::exception&) {
      entry->state.store(QueuedEntry::State::kAborted, std::memory_order_release);
      writer_cv_.notify_one();
      return AppendResult::kInternalError;
    }

    entry->state.store(QueuedEntry::State::kReady, std::memory_order_release);
    writer_cv_.notify_one();
    return AppendResult::kOk;
  }

  bool Flush(std::string* error) {
    std::size_t target_id = 0;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!fatal_error_.empty()) {
        SetError(fatal_error_, error);
        return false;
      }
      if (!open_ || closed_) {
        SetError("recorder is closed", error);
        return false;
      }
      target_id = ++flush_requested_id_;
      writer_cv_.notify_one();
      flush_cv_.wait(lock, [&]() {
        return flush_completed_id_ >= target_id || !fatal_error_.empty() || closed_;
      });
      if (!fatal_error_.empty()) {
        SetError(fatal_error_, error);
        return false;
      }
      return true;
    }
  }

  bool Close(std::string* error) {
    if (!open_) {
      SetError("recorder is not open", error);
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        return fatal_error_.empty();
      }
      closing_requested_ = true;
    }
    producer_cv_.notify_all();
    writer_cv_.notify_one();
    if (writer_thread_.joinable()) {
      writer_thread_.join();
    }

    if (!fatal_error_.empty()) {
      SetError(fatal_error_, error);
      return false;
    }
    manifest_.stop_utc = internal::FormatUtcCompact(std::chrono::system_clock::now());
    manifest_.incomplete = false;
    const std::filesystem::path finalized_path =
        config_.output_root /
        ("recording-" + manifest_.start_utc + "-to-" + *manifest_.stop_utc);
    if (!internal::WriteManifest(recording_path_, manifest_, error)) {
      return false;
    }
    if (!internal::EnsureDirectory(finalized_path, error)) {
      return false;
    }
    std::error_code move_ec;
    for (const auto& entry : std::filesystem::directory_iterator(recording_path_, move_ec)) {
      if (move_ec) {
        SetError("failed to enumerate recording directory for finalize move: " + move_ec.message(),
                 error);
        return false;
      }
      const auto destination = finalized_path / entry.path().filename();
      std::filesystem::rename(entry.path(), destination, move_ec);
      if (move_ec) {
        SetError("failed to move finalized recording file '" + entry.path().string() + "': " +
                     move_ec.message(),
                 error);
        return false;
      }
    }
    std::filesystem::remove(recording_path_, move_ec);
    if (move_ec) {
      SetError("failed to remove source recording directory: " + move_ec.message(), error);
      return false;
    }

    // 成功关闭后，对外暴露的录制路径会切换为最终目录名。
    {
      std::lock_guard<std::mutex> lock(mutex_);
      recording_path_ = finalized_path;
      closed_ = true;
      open_ = false;
    }
    flush_cv_.notify_all();
    return true;
  }

 private:
  QueuedEntry* AcquireEntryLocked() {
    if (!free_entries_.empty()) {
      QueuedEntry* entry = free_entries_.back();
      free_entries_.pop_back();
      return entry;
    }
    entry_storage_.push_back(std::make_unique<QueuedEntry>());
    return entry_storage_.back().get();
  }

  void RecycleEntryLocked(QueuedEntry* entry) {
    entry->state.store(QueuedEntry::State::kConsumed, std::memory_order_relaxed);
    entry->reserved_bytes = 0;
    entry->message.record_seq = 0;
    entry->message.event_mono_ts_us = 0;
    entry->message.event_utc_ts_us = 0;
    entry->message.session_id = 0;
    entry->message.stream_id.reset();
    entry->message.message_type = 0;
    entry->message.message_version = 0;
    entry->message.payload.clear();
    entry->message.attributes.clear();
    free_entries_.push_back(entry);
  }

  bool CanAcceptInBuffer(const QueueBuffer& buffer, std::size_t reserve_bytes) const {
    if (buffer.Full()) {
      return false;
    }
    const bool oversized_only = buffer.Empty() && reserve_bytes > queue_capacity_bytes_;
    return oversized_only || buffer.reserved_bytes + reserve_bytes <= queue_capacity_bytes_;
  }

  bool AnyQueueHasEntriesLocked() const {
    for (const QueueBuffer& buffer : queues_) {
      if (!buffer.Empty()) {
        return true;
      }
    }
    return false;
  }

  bool SwitchAcceptQueueLocked() {
    for (std::size_t offset = 1; offset < queues_.size(); ++offset) {
      const std::size_t index = (accept_queue_index_ + offset) % queues_.size();
      if (queues_[index].Empty()) {
        accept_queue_index_ = index;
        return true;
      }
    }
    return false;
  }

  bool SelectNextDrainQueueLocked() {
    for (std::size_t offset = 0; offset < queues_.size(); ++offset) {
      const std::size_t index = (drain_queue_index_ + offset) % queues_.size();
      if (!queues_[index].Empty()) {
        drain_queue_index_ = index;
        return true;
      }
    }
    return false;
  }

  AcceptQueueStatus PrepareAcceptQueueLocked(std::size_t reserve_bytes) {
    if (CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes)) {
      return AcceptQueueStatus::kAccepted;
    }
    if (!SwitchAcceptQueueLocked()) {
      return AcceptQueueStatus::kBlocked;
    }
    return CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes)
               ? AcceptQueueStatus::kAcceptedAfterSwitch
               : AcceptQueueStatus::kBlocked;
  }

  bool OpenCurrentSegment(std::string* error) {
    if (has_current_segment_) {
      return true;
    }
    has_current_segment_ = true;
    return internal::OpenSegment(recording_path_,
                                 static_cast<std::uint32_t>(manifest_.segments.size()),
                                 &current_segment_, error);
  }

  void HandleAbortedEntry() {
    manifest_.aborted_entries += 1;
    manifest_.degraded = true;
  }

  bool FlushManifestSnapshot(std::string* error) {
    internal::ManifestData snapshot = manifest_;
    if (has_current_segment_) {
      snapshot.segments.push_back(current_segment_.summary);
    }
    return internal::WriteManifest(recording_path_, snapshot, error);
  }

  bool RotateIfNeeded(const ReplayMessage& message, std::string* error) {
    if (!has_current_segment_) {
      return OpenCurrentSegment(error);
    }
    const RecordedMessage estimated{message.session_id, message.stream_id, message.message_type,
                                    message.message_version,
                                    ByteView{message.payload.data(), message.payload.size()},
                                    ByteView{message.attributes.data(), message.attributes.size()}};
    if (!internal::ShouldRotateSegment(current_segment_, segment_max_bytes_,
                                       internal::EstimateRecordBytes(estimated))) {
      return true;
    }
    if (!internal::FinalizeSegment(&current_segment_, error)) {
      return false;
    }
    manifest_.segments.push_back(current_segment_.summary);
    has_current_segment_ = false;
    if (!FlushManifestSnapshot(error)) {
      return false;
    }
    return OpenCurrentSegment(error);
  }
  void FinishCurrentSegment(std::string* error) {
    if (!has_current_segment_) {
      return;
    }
    if (!internal::FinalizeSegment(&current_segment_, error)) {
      return;
    }
    manifest_.segments.push_back(current_segment_.summary);
    has_current_segment_ = false;
  }

  void WriterLoop() {
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
              return front->state.load(std::memory_order_acquire) !=
                         QueuedEntry::State::kPending ||
                     !fatal_error_.empty();
            });
            continue;
          }
          entry = front;
          entry_aborted =
              entry->state.load(std::memory_order_acquire) == QueuedEntry::State::kAborted;
          drain_queue.PopFront();
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
        producer_cv_.notify_all();
      }

      if (entry) {
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
        manifest_.total_attributes_bytes += entry->message.attributes.size();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          RecycleEntryLocked(entry);
        }
        continue;
      }

      if (should_flush) {
        std::string error;
        if (!FlushManifestSnapshot(&error)) {
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

  /** @brief 构造时复制并冻结的录制器配置。 */
  RecorderConfig config_;
  /** @brief 由 `queue_capacity_mb` 换算得到的每个队列的实际字节容量。 */
  std::size_t queue_capacity_bytes_ = 0;
  /** @brief 由 `segment_max_mb` 换算得到的实际 segment 字节阈值。 */
  std::uint64_t segment_max_bytes_ = 0;
  /** @brief 当前录制目录路径，成功关闭后会被重命名。 */
  std::filesystem::path recording_path_;
  /** @brief 由写线程重建并生成快照的可变 manifest 状态。 */
  internal::ManifestData manifest_;
  /** @brief 当前由写线程持有的打开中的 segment。 */
  internal::SegmentWriteContext current_segment_;
  /** @brief `current_segment_` 包含活动可写 segment 时为 true。 */
  bool has_current_segment_ = false;
  /** @brief 录制器成功完成构造后为 true。 */
  bool open_ = false;
  /** @brief 调用方请求 `Close()` 之后为 true。 */
  bool closing_requested_ = false;
  /** @brief 最终 manifest 写入并重命名完成后为 true。 */
  bool closed_ = false;
  /** @brief 暴露给 append/flush/close 调用方的致命写线程错误。 */
  std::string fatal_error_;
  /** @brief 下一个由录制器分配的记录序号。 */
  std::uint64_t next_record_seq_ = 0;
  /** @brief 由接收端和写线程共享的一组固定容量队列缓冲。 */
  std::vector<QueueBuffer> queues_;
  /** @brief 当前接收新消息的活动队列索引。 */
  std::size_t accept_queue_index_ = 0;
  /** @brief 当前写线程优先排空的队列索引。 */
  std::size_t drain_queue_index_ = 0;
  /** @brief 为复用条目和字节缓冲而持有的对象池存储。 */
  std::vector<std::unique_ptr<QueuedEntry>> entry_storage_;
  /** @brief 当前空闲、可复用的条目。 */
  std::vector<QueuedEntry*> free_entries_;
  /** @brief 最近一次请求的 flush 检查点的单调递增 ID。 */
  std::size_t flush_requested_id_ = 0;
  /** @brief 最近一次完成的 flush 检查点的单调递增 ID。 */
  std::size_t flush_completed_id_ = 0;
  /** @brief append、flush 与写线程共享的队列互斥锁。 */
  mutable std::mutex mutex_;
  /** @brief 仅供生产者等待可接收容量的条件变量。 */
  std::condition_variable producer_cv_;
  /** @brief 仅供写线程等待新条目或状态变化的条件变量。 */
  std::condition_variable writer_cv_;
  /** @brief 仅供 `Flush()` 等待检查点完成的条件变量。 */
  std::condition_variable flush_cv_;
  /** @brief 用于串行化 segment 写入的专用写线程。 */
  std::thread writer_thread_;
};

Recorder::Recorder(const RecorderConfig& config, std::string* error)
    : impl_(std::make_unique<RecorderImpl>(config, error)) {}

Recorder::~Recorder() = default;
Recorder::Recorder(Recorder&&) noexcept = default;
Recorder& Recorder::operator=(Recorder&&) noexcept = default;

bool Recorder::IsOpen() const {
  return impl_ != nullptr && impl_->IsOpen();
}

const RecorderConfig& Recorder::Config() const {
  return impl_->Config();
}

std::filesystem::path Recorder::RecordingPath() const {
  return impl_->RecordingPath();
}

AppendResult Recorder::Append(const RecordedMessage& message) {
  return impl_ == nullptr ? AppendResult::kClosed : impl_->Append(message);
}

bool Recorder::Flush(std::string* error) {
  return impl_ != nullptr && impl_->Flush(error);
}

bool Recorder::Close(std::string* error) {
  return impl_ != nullptr && impl_->Close(error);
}

}  // jojo::rec 命名空间