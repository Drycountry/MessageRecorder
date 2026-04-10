#ifndef JOJO_REC_RECORDER_HPP_
#define JOJO_REC_RECORDER_HPP_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "jojo/rec/detail/internal.hpp"
#include "jojo/rec/types.hpp"

namespace jojo::rec {

/// @brief 带内部多队列和专用写线程的单次录制写入器。
class Recorder {
 private:
  static constexpr std::uint64_t kBytesPerMb = 1000ULL * 1000ULL;  // 十进制 MB 到字节的换算常量。

  /// @brief 生产者线程与写线程共享的队列条目。
  struct QueuedEntry {
    /// @brief 生产者到写线程之间的条目状态。
    enum class State {
      kPending,   // 已预留槽位，但负载仍在拷贝。
      kReady,     // 条目已就绪，可被写线程消费。
      kAborted,   // 生产者在拷贝阶段失败，中止该条目。
      kConsumed,  // 写线程已消费并回收该条目。
    };

    std::atomic<State> state{State::kPending};  // 队列条目状态
    std::size_t reserved_bytes = 0;             // 计入队列容量的预留字节数。
    ReplayMessage message;                      // 消息
  };

  /// @brief 队列缓存，元素类型为QueuedEntry*。环形数组模拟，避免构造后的动态内存分配（非线程安全）
  struct QueueBuffer {
    /// @brief 构造函数
    QueueBuffer();

    /// @brief 构造函数
    /// @param capacity 队列容量
    explicit QueueBuffer(std::size_t capacity);

    /// @brief 队列是否为空。
    bool Empty() const;

    /// @brief 队列是否已满。
    bool Full() const;

    /// @brief 返回队首元素；为空时返回空指针。
    QueuedEntry* Front() const;

    /// @brief 向尾部压入一个元素
    void Push(QueuedEntry* entry);

    /// @brief 弹出队首元素
    void Pop();

    std::vector<QueuedEntry*> slots;  // 固定容量的环形槽位数组
    std::size_t head_index = 0;       // 环形数组头部索引
    std::size_t size = 0;             // 环形数组有效元素数
    std::size_t reserved_bytes = 0;   // 队列预留总字节数，是队列级别的背压配额
  };

  /// @brief 一次追加选择接收队列后的结果。
  enum class AcceptQueueStatus {
    kAccepted,             // 当前接收队列直接接收。
    kAcceptedAfterSwitch,  // 切换到下一个空队列后接收。
    kBlocked,              // 当前所有队列都无法接收。
  };

  // ==========================================================================
 public:
  /// @brief 创建一个以 `config.output_root` 为根目录的录制器。
  /// @param config 在构造时复制并冻结的录制配置。
  /// @param error 构造失败时可选的错误输出字符串。
  explicit Recorder(const RecorderConfig& config, std::string* error = nullptr);

  /// @brief 析构时尽力执行一次关闭流程。
  ~Recorder();

  /// @brief 禁止拷贝构造。
  Recorder(const Recorder&) = delete;

  /// @brief 禁止拷贝赋值。
  Recorder& operator=(const Recorder&) = delete;

  /// @brief 允许移动构造，并在搬运前令源对象静止。
  Recorder(Recorder&& other) noexcept;

  /// @brief 允许移动赋值，并在双对象静止后整体搬运状态。
  Recorder& operator=(Recorder&& other) noexcept;

  /// @brief 返回录制器是否仍可接受新消息。
  /// @return 当录制器可用且没有致命错误时返回 true。
  bool IsOpen() const;

  /// @brief 返回冻结后的录制配置。
  /// @return 不可变的录制配置引用。
  const RecorderConfig& Config() const;

  /// @brief 返回当前活动或最终录制目录路径。
  /// @return 当前对外暴露的录制目录路径。
  std::filesystem::path RecordingPath() const;

  /// @brief 向缓冲追加一条消息。
  /// @param message 由调用方持有、录制器复制的消息视图。
  /// @return 队列接收结果。
  AppendResult Append(const RecordedMessage& message);

  /// @brief 请求写线程执行一次 manifest 刷新或 durability checkpoint。
  /// @param error 失败时可选的错误输出字符串。
  /// @return 成功时返回 true。
  bool Flush(std::string* error = nullptr);

  /// @brief 关闭录制器、完成最终 manifest 并发布最终目录。
  /// @param error 失败时可选的错误输出字符串。
  /// @return 成功时返回 true。
  bool Close(std::string* error = nullptr);

  // ==========================================================================
 private:
  /// @brief 在不抛异常的前提下执行关闭。
  void CloseNoexcept() noexcept;

  /// @brief 将十进制 MB 配置值转换为字节数。
  static bool ConvertMegabytesToBytes(std::size_t megabytes, std::uint64_t* bytes);

  /// @brief 依据字节容量估算 ring buffer 槽位数。
  static std::size_t ComputeQueueSlotCapacity(std::size_t queue_capacity_bytes);

  /// @brief 在提供错误输出缓冲时写入错误消息。
  static void SetError(const std::string& message, std::string* error);

  /// @brief 将 flush 间隔限制到 `milliseconds` 可表示范围。
  static std::chrono::milliseconds ClampFlushInterval(std::uint64_t flush_interval_ms);

  /// @brief 用元数据填充预留好的拥有型消息。
  static void PrepareQueuedMessage(ReplayMessage* owned, const RecordedMessage& message, std::uint64_t record_seq,
                                   std::uint64_t event_mono_ts_us, std::uint64_t event_utc_ts_us);

  /// @brief 将非拥有字节视图复制到拥有型缓冲区。
  static void CopyByteView(const ByteView& source, std::vector<std::uint8_t>* destination);

  /// @brief 为一次追加预留容量、条目和记录序号。
  template <typename PrepareFn>
  AppendResult ReserveAppendEntry(std::size_t reserve_bytes, PrepareFn&& prepare_entry, QueuedEntry** entry,
                                  AcceptQueueStatus* accept_status) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!fatal_error_.empty()) {
      return AppendResult::kInternalError;
    }
    if (!open_) {
      return AppendResult::kClosed;
    }

    for (;;) {
      *accept_status = PrepareAcceptQueueLocked(reserve_bytes);
      if (*accept_status != AcceptQueueStatus::kBlocked) {
        break;
      }
      if (config_.backpressure_policy == BackpressurePolicy::kFailFast) {
        return AppendResult::kBackpressure;
      }
      producer_cv_.wait(lock);
      if (!fatal_error_.empty()) {
        return AppendResult::kInternalError;
      }
      if (!open_) {
        return AppendResult::kClosed;
      }
    }

    *entry = AcquireEntryLocked();
    if (*entry == nullptr) {
      fatal_error_ = "internal entry pool exhausted";
      return AppendResult::kInternalError;
    }
    const auto mono_now = std::chrono::steady_clock::now();
    const auto utc_now = std::chrono::system_clock::now();
    (*entry)->state.store(QueuedEntry::State::kPending, std::memory_order_relaxed);
    (*entry)->reserved_bytes = reserve_bytes;
    prepare_entry(*entry, next_record_seq_++, internal::ToSteadyMicros(mono_now), internal::ToUnixMicros(utc_now));
    QueueBuffer& accept_queue = queues_[accept_queue_index_];
    accept_queue.Push(*entry);
    accept_queue.reserved_bytes += reserve_bytes;
    return AppendResult::kOk;
  }

  /// @brief 在条目预留完成后按需唤醒可能阻塞的生产者。
  void PublishReservedEntry(AcceptQueueStatus accept_status);

  /// @brief 在双对象都已静止且持锁时搬运内部状态。
  void MoveStateFromLocked(Recorder&& other);

  /// @brief 将被搬空对象重置为可析构的静止状态。
  void ResetMovedFromStateLocked();

  /// @brief 从预分配对象池获取一个可复用的队列条目；池耗尽时返回空指针。
  QueuedEntry* AcquireEntryLocked();

  /// @brief 清空条目状态并放回对象池。
  void RecycleEntryLocked(QueuedEntry* entry);

  /// @brief 判断指定队列当前是否还能接收这次预留。
  bool CanAcceptInBuffer(const QueueBuffer& buffer, std::size_t reserve_bytes) const;

  /// @brief 判断任一内部队列是否仍有待消费条目。
  bool AnyQueueHasEntriesLocked() const;

  /// @brief 将接收队列切换到下一个空闲队列。
  bool SwitchAcceptQueueLocked();

  /// @brief 选择下一个需要排空的非空队列。
  bool SelectNextDrainQueueLocked();

  /// @brief 为本次追加挑选可接收的内部队列。
  AcceptQueueStatus PrepareAcceptQueueLocked(std::size_t reserve_bytes);

  /// @brief 确保当前存在一个打开中的可写 segment。
  bool OpenCurrentSegment(std::string* error);

  /// @brief 记录一次生产者在拷贝阶段中止的条目。
  void HandleAbortedEntry();

  /// @brief 生成包含活动 segment 摘要的 manifest 快照。
  internal::ManifestData BuildManifestSnapshot() const;

  /// @brief 将当前 manifest 快照写回录制目录。
  bool FlushManifestSnapshot(std::string* error);

  /// @brief 判断显式 `Flush()` 是否需要执行 durability checkpoint。
  bool ShouldSyncOnExplicitFlush() const;

  /// @brief 判断关闭流程是否需要执行 durability checkpoint。
  bool ShouldSyncOnClose() const;

  /// @brief 刷新当前活动 segment 的用户态流缓冲。
  bool FlushOpenSegmentStream(std::string* error);

  /// @brief 同步自上次 checkpoint 以来新 finalize 的所有 segment 文件。
  bool SyncUnsyncedFinalizedSegments(std::string* error);

  /// @brief 同步仍处于打开状态的当前 segment 文件。
  bool SyncCurrentSegmentFile(std::string* error);

  /// @brief 同步 manifest 文件及其所在目录元数据。
  bool SyncManifestSnapshot(std::string* error);

  /// @brief 在 checkpoint 成功后更新同步状态基线。
  void MarkDurabilityCheckpointComplete();

  /// @brief 执行一次完整 durability checkpoint。
  bool PerformDurabilityCheckpoint(std::string* error);

  /// @brief 必要时 finalize 当前 segment 并轮转到新文件。
  bool RotateIfNeeded(const ReplayMessage& message, std::string* error);

  /// @brief 在关闭前 finalize 当前活动 segment。
  void FinishCurrentSegment(std::string* error);

  /// @brief 专用写线程主循环，串行写入、flush 和收尾。
  void WriterLoop();

  RecorderConfig config_;                          // 构造时复制并冻结的录制器配置。
  std::size_t queue_capacity_bytes_ = 0;           // 由 `queue_capacity_mb` 换算得到的每队列字节容量。
  std::uint64_t segment_max_bytes_ = 0;            // 由 `segment_max_mb` 换算得到的 segment 字节阈值。
  std::filesystem::path recording_path_;           // 当前活动或最终录制目录路径。
  internal::ManifestData manifest_;                // 由写线程维护并写回磁盘的 manifest 状态。
  internal::SegmentWriteContext current_segment_;  // 当前打开中的 segment 写入上下文。
  bool has_current_segment_ = false;               // 当前是否存在活动可写 segment。
  std::size_t synced_segment_count_ = 0;           // 已完成 durability checkpoint 的 finalized segment 前缀数量。
  bool current_segment_dirty_ = false;             // 当前活动 segment 自上次 checkpoint 后是否被写入。
  bool manifest_dirty_ = false;                    // manifest 自上次 checkpoint 后是否发生变化。
  std::chrono::steady_clock::time_point last_sync_time_ =
      std::chrono::steady_clock::time_point::min();          // 最近一次成功 durability checkpoint 的单调时间。
  bool open_ = false;                                        // 录制器仍接受新操作时为 true。
  bool closing_requested_ = false;                           // 已请求写线程排空并退出时为 true。
  bool closed_ = false;                                      // 最终 manifest 写入并重命名完成后为 true。
  std::string fatal_error_;                                  // 暴露给调用方的致命录制错误消息。
  std::uint64_t next_record_seq_ = 0;                        // 下一个分配给新记录的单调序号。
  std::vector<QueueBuffer> queues_;                          // 接收端与写线程共享的固定容量队列集合。
  std::size_t accept_queue_index_ = 0;                       // 当前接收新消息的活动队列索引。
  std::size_t drain_queue_index_ = 0;                        // 当前写线程优先排空的队列索引。
  std::unique_ptr<QueuedEntry[]> entry_storage_;             // 预分配的队列条目对象池。
  std::size_t entry_storage_size_ = 0;                      // `entry_storage_` 中条目总数。
  std::vector<QueuedEntry*> free_entries_;                  // 当前空闲、可复用的条目列表。
  std::size_t flush_requested_id_ = 0;                       // 最近一次请求的 flush 检查点 ID。
  std::size_t flush_completed_id_ = 0;                       // 最近一次完成的 flush 检查点 ID。
  mutable std::mutex mutex_;                                 // 保护状态字段与队列元数据的互斥锁。
  std::condition_variable producer_cv_;                      // 供生产者等待容量释放的条件变量。
  std::condition_variable writer_cv_;                        // 供写线程等待新条目或状态变化的条件变量。
  std::condition_variable flush_cv_;                         // 供 `Flush()` 等待检查点完成的条件变量。
  std::thread writer_thread_;                                // 串行写 segment 的专用写线程。
};

}  // namespace jojo::rec

#endif  // JOJO_REC_RECORDER_HPP_ 头文件保护
