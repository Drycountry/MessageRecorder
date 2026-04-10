#ifndef JOJO_REC_DETAIL_RECORDER_QUEUE_HPP_
#define JOJO_REC_DETAIL_RECORDER_QUEUE_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "jojo/rec/replay_types.hpp"

namespace jojo::rec::internal {

/// @brief 负责维护 Recorder 生产者/写线程之间的队列、对象池和排空顺序。
class RecorderQueue {
 public:
  /// @brief 生产者到写线程之间共享的队列条目。
  struct QueuedEntry {
    /// @brief 条目在生产者和写线程之间流转的状态。
    enum class State {
      kPending,
      kReady,
      kAborted,
      kConsumed,
    };

    std::atomic<State> state{State::kPending};
    std::size_t reserved_bytes = 0;
    ReplayMessage message;
  };

  /// @brief 一次追加选择接收队列后的结果。
  enum class AcceptStatus {
    kAccepted,
    kAcceptedAfterSwitch,
    kBlocked,
  };

  RecorderQueue() = default;
  RecorderQueue(RecorderQueue&&) noexcept = default;
  RecorderQueue& operator=(RecorderQueue&&) noexcept = default;
  RecorderQueue(const RecorderQueue&) = delete;
  RecorderQueue& operator=(const RecorderQueue&) = delete;

  /// @brief 初始化固定容量队列和可复用条目池。
  bool Initialize(std::size_t queue_buffer_count, std::size_t queue_capacity_bytes, std::string* error);
  /// @brief 清空所有内部状态，恢复到默认构造状态。
  void Reset();

  /// @brief 为一次预留挑选当前可接收的内部队列。
  AcceptStatus PrepareAcceptQueueLocked(std::size_t reserve_bytes);
  /// @brief 从对象池中取出一个可复用条目；池耗尽时返回空指针。
  QueuedEntry* AcquireEntryLocked();
  /// @brief 将已经预留好的条目压入当前接收队列。
  void PushReservedEntryLocked(QueuedEntry* entry, std::size_t reserve_bytes);
  /// @brief 判断任一内部队列是否仍有待消费条目。
  bool AnyQueueHasEntriesLocked() const;
  /// @brief 选择下一个需要排空的非空队列。
  bool SelectNextDrainQueueLocked();
  /// @brief 读取当前排空队列的队首条目；若为空则返回空指针。
  QueuedEntry* FrontLocked() const;
  /// @brief 弹出当前排空队列的队首条目。
  void PopFrontLocked();
  /// @brief 清空条目状态并放回对象池。
  void RecycleEntryLocked(QueuedEntry* entry);

 private:
  /// @brief 单个固定容量环形队列。
  struct QueueBuffer {
    QueueBuffer();
    explicit QueueBuffer(std::size_t capacity);

    bool Empty() const;
    bool Full() const;
    QueuedEntry* Front() const;
    void Push(QueuedEntry* entry);
    void Pop();

    std::vector<QueuedEntry*> slots;
    std::size_t head_index = 0;
    std::size_t size = 0;
    std::size_t reserved_bytes = 0;
  };

  /// @brief 依据字节容量估算 ring buffer 槽位数。
  static std::size_t ComputeQueueSlotCapacity(std::size_t queue_capacity_bytes);
  /// @brief 判断指定队列当前是否还能接收这次预留。
  bool CanAcceptInBuffer(const QueueBuffer& buffer, std::size_t reserve_bytes) const;
  /// @brief 将接收队列切换到下一个空闲队列。
  bool SwitchAcceptQueueLocked();

  std::size_t queue_capacity_bytes_ = 0;
  std::vector<QueueBuffer> queues_;
  std::size_t accept_queue_index_ = 0;
  std::size_t drain_queue_index_ = 0;
  std::unique_ptr<QueuedEntry[]> entry_storage_;
  std::size_t entry_storage_size_ = 0;
  std::vector<QueuedEntry*> free_entries_;
};

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_RECORDER_QUEUE_HPP_
