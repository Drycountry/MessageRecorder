#include "jojo/rec/detail/recorder_queue.hpp"

#include <algorithm>
#include <exception>
#include <limits>

#include "jojo/rec/detail/segment.hpp"

namespace jojo::rec::internal {
namespace {

/// @brief 在提供错误输出缓冲时写入错误消息。
void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

}  // namespace

RecorderQueue::QueueBuffer::QueueBuffer() = default;

RecorderQueue::QueueBuffer::QueueBuffer(std::size_t capacity) : slots(capacity, nullptr) {}

bool RecorderQueue::QueueBuffer::Empty() const { return size == 0; }

bool RecorderQueue::QueueBuffer::Full() const { return size == slots.size(); }

RecorderQueue::QueuedEntry* RecorderQueue::QueueBuffer::Front() const { return Empty() ? nullptr : slots[head_index]; }

void RecorderQueue::QueueBuffer::Push(QueuedEntry* entry) {
  const std::size_t tail_index = (head_index + size) % slots.size();
  slots[tail_index] = entry;
  ++size;
}

void RecorderQueue::QueueBuffer::Pop() {
  slots[head_index] = nullptr;
  head_index = (head_index + 1) % slots.size();
  --size;
  if (size == 0) {
    head_index = 0;
  }
}

bool RecorderQueue::Initialize(std::size_t queue_buffer_count, std::size_t queue_capacity_bytes, std::string* error) {
  Reset();
  queue_capacity_bytes_ = queue_capacity_bytes;
  queues_.assign(queue_buffer_count, QueueBuffer(ComputeQueueSlotCapacity(queue_capacity_bytes_)));

  // 写线程可能暂时持有一个已经出队、但尚未回收到空闲池的条目，所以对象池需要比总槽位数多 1。
  std::size_t entry_pool_size = 1;
  for (const QueueBuffer& buffer : queues_) {
    if (buffer.slots.size() > std::numeric_limits<std::size_t>::max() - entry_pool_size) {
      SetError("entry pool is too large", error);
      Reset();
      return false;
    }
    entry_pool_size += buffer.slots.size();
  }

  try {
    entry_storage_ = std::make_unique<QueuedEntry[]>(entry_pool_size);
    entry_storage_size_ = entry_pool_size;
    free_entries_.reserve(entry_pool_size);
  } catch (const std::exception&) {
    SetError("failed to allocate entry pool", error);
    Reset();
    return false;
  }

  for (std::size_t index = 0; index < entry_pool_size; ++index) {
    free_entries_.push_back(&entry_storage_[index]);
  }
  return true;
}

void RecorderQueue::Reset() {
  queue_capacity_bytes_ = 0;
  queues_.clear();
  accept_queue_index_ = 0;
  drain_queue_index_ = 0;
  entry_storage_.reset();
  entry_storage_size_ = 0;
  free_entries_.clear();
}

RecorderQueue::AcceptStatus RecorderQueue::PrepareAcceptQueueLocked(std::size_t reserve_bytes) {
  if (CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes)) {
    return AcceptStatus::kAccepted;
  }
  if (!SwitchAcceptQueueLocked()) {
    return AcceptStatus::kBlocked;
  }
  return CanAcceptInBuffer(queues_[accept_queue_index_], reserve_bytes) ? AcceptStatus::kAcceptedAfterSwitch
                                                                        : AcceptStatus::kBlocked;
}

RecorderQueue::QueuedEntry* RecorderQueue::AcquireEntryLocked() {
  if (free_entries_.empty()) {
    return nullptr;
  }
  QueuedEntry* entry = free_entries_.back();
  free_entries_.pop_back();
  return entry;
}

void RecorderQueue::PushReservedEntryLocked(QueuedEntry* entry, std::size_t reserve_bytes) {
  entry->reserved_bytes = reserve_bytes;
  QueueBuffer& accept_queue = queues_[accept_queue_index_];
  accept_queue.Push(entry);
  accept_queue.reserved_bytes += reserve_bytes;
}

bool RecorderQueue::AnyQueueHasEntriesLocked() const {
  for (const QueueBuffer& buffer : queues_) {
    if (!buffer.Empty()) {
      return true;
    }
  }
  return false;
}

bool RecorderQueue::SelectNextDrainQueueLocked() {
  for (std::size_t offset = 0; offset < queues_.size(); ++offset) {
    const std::size_t index = (drain_queue_index_ + offset) % queues_.size();
    if (!queues_[index].Empty()) {
      drain_queue_index_ = index;
      return true;
    }
  }
  return false;
}

RecorderQueue::QueuedEntry* RecorderQueue::FrontLocked() const { return queues_[drain_queue_index_].Front(); }

void RecorderQueue::PopFrontLocked() {
  QueueBuffer& drain_queue = queues_[drain_queue_index_];
  QueuedEntry* entry = drain_queue.Front();
  if (entry == nullptr) {
    return;
  }
  drain_queue.Pop();
  drain_queue.reserved_bytes -= entry->reserved_bytes;
}

void RecorderQueue::RecycleEntryLocked(QueuedEntry* entry) {
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

std::size_t RecorderQueue::ComputeQueueSlotCapacity(std::size_t queue_capacity_bytes) {
  const std::size_t min_record_bytes = EstimateRecordBytes(RecordedMessage{});
  if (min_record_bytes == 0) {
    return 2;
  }
  const std::uint64_t max_entries = static_cast<std::uint64_t>(queue_capacity_bytes) / min_record_bytes + 2ULL;
  const std::uint64_t clamped = std::min<std::uint64_t>(max_entries, std::numeric_limits<std::size_t>::max());
  return static_cast<std::size_t>(std::max<std::uint64_t>(clamped, 2ULL));
}

bool RecorderQueue::CanAcceptInBuffer(const QueueBuffer& buffer, std::size_t reserve_bytes) const {
  if (buffer.Full()) {
    return false;
  }
  const bool oversized_only = buffer.Empty() && reserve_bytes > queue_capacity_bytes_;
  return oversized_only || buffer.reserved_bytes + reserve_bytes <= queue_capacity_bytes_;
}

bool RecorderQueue::SwitchAcceptQueueLocked() {
  for (std::size_t offset = 1; offset < queues_.size(); ++offset) {
    const std::size_t index = (accept_queue_index_ + offset) % queues_.size();
    if (queues_[index].Empty()) {
      accept_queue_index_ = index;
      return true;
    }
  }
  return false;
}

}  // namespace jojo::rec::internal
