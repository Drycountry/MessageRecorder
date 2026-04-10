#include "jojo/rec/detail/recorder_worker.hpp"

#include <cstring>
#include <exception>
#include <limits>
#include <utility>

namespace jojo::rec::internal {

RecorderWorker::RecorderWorker(RecorderConfig config) : config_(std::move(config)) {}

RecorderWorker::~RecorderWorker() { CloseNoexcept(); }

bool RecorderWorker::Initialize(std::string* error) {
  if (config_.output_root.empty()) {
    SetError("output_root must not be empty", error);
    return false;
  }
  if (config_.queue_capacity_mb == 0 || config_.segment_max_mb == 0 || config_.sparse_index_interval_ms == 0 ||
      config_.sparse_index_max_records == 0 || config_.sparse_index_max_bytes == 0) {
    SetError(
        "queue_capacity_mb, segment_max_mb, sparse_index_interval_ms, sparse_index_max_records and "
        "sparse_index_max_bytes must be non-zero",
        error);
    return false;
  }
  if (config_.queue_buffer_count < 2) {
    SetError("queue_buffer_count must be at least 2", error);
    return false;
  }

  std::uint64_t queue_capacity_bytes = 0;
  if (!ConvertMegabytesToBytes(config_.queue_capacity_mb, &queue_capacity_bytes) ||
      queue_capacity_bytes > std::numeric_limits<std::size_t>::max()) {
    SetError("queue_capacity_mb is too large", error);
    return false;
  }
  queue_capacity_bytes_ = static_cast<std::size_t>(queue_capacity_bytes);
  if (!queue_.Initialize(config_.queue_buffer_count, queue_capacity_bytes_, error)) {
    return false;
  }

  if (!ConvertMegabytesToBytes(config_.segment_max_mb, &segment_max_bytes_)) {
    SetError("segment_max_mb is too large", error);
    return false;
  }
  if (!ConvertMillisecondsToMicros(config_.sparse_index_interval_ms, &sparse_index_interval_us_)) {
    SetError("sparse_index_interval_ms is too large", error);
    return false;
  }
  if (!storage_.Initialize(config_.output_root, config_.recording_label, config_.message_type_names, error)) {
    return false;
  }
  storage_.SetFlushPolicy(config_.fsync_policy, config_.flush_interval_ms);

  open_ = true;
  writer_thread_ = std::thread([this]() { WriterLoop(); });
  return true;
}

bool RecorderWorker::IsOpen() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return open_ && fatal_error_.empty();
}

std::filesystem::path RecorderWorker::RecordingPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return storage_.recording_path();
}

AppendResult RecorderWorker::Append(const RecordedMessage& message) {
  RecorderQueue::QueuedEntry* entry = nullptr;
  RecorderQueue::AcceptStatus accept_status = RecorderQueue::AcceptStatus::kBlocked;
  const AppendResult reserve_result =
      ReserveAppendEntryLocked(message, EstimateRecordBytes(message), &entry, &accept_status);
  if (reserve_result != AppendResult::kOk) {
    return reserve_result;
  }
  PublishReservedEntry(accept_status);

  try {
    // 只有在完整负载复制完成后再唤醒写线程，避免 pending 条目导致额外睡醒抖动。
    CopyByteView(message.payload, &entry->message.payload);
  } catch (const std::exception&) {
    entry->state.store(RecorderQueue::QueuedEntry::State::kAborted, std::memory_order_release);
    writer_cv_.notify_one();
    return AppendResult::kInternalError;
  }

  entry->state.store(RecorderQueue::QueuedEntry::State::kReady, std::memory_order_release);
  writer_cv_.notify_one();
  return AppendResult::kOk;
}

bool RecorderWorker::Flush(std::string* error) {
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

bool RecorderWorker::Close(std::string* error) {
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

  storage_.MarkClosedAt(std::chrono::system_clock::now());
  const std::filesystem::path finalized_path = storage_.MakeFinalizedPath(config_.output_root);
  std::string close_error;
  if (storage_.ShouldSyncOnClose()) {
    if (!storage_.PerformDurabilityCheckpoint(&close_error)) {
      remember_close_failure(close_error);
      SetError(close_error, error);
      return false;
    }
  } else if (!storage_.FlushManifestSnapshot(&close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }
  if (!RenameDirectoryAtomically(storage_.recording_path(), finalized_path, &close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }
  if (storage_.ShouldSyncOnClose() && !SyncDirectory(config_.output_root, &close_error)) {
    remember_close_failure(close_error);
    SetError(close_error, error);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    storage_.SetRecordingPath(finalized_path);
    closed_ = true;
  }
  flush_cv_.notify_all();
  return true;
}

void RecorderWorker::CloseNoexcept() noexcept {
  std::string ignored;
  Close(&ignored);
}

bool RecorderWorker::ConvertMegabytesToBytes(std::size_t megabytes, std::uint64_t* bytes) {
  if (megabytes > std::numeric_limits<std::uint64_t>::max() / kBytesPerMb) {
    return false;
  }
  *bytes = static_cast<std::uint64_t>(megabytes) * kBytesPerMb;
  return true;
}

bool RecorderWorker::ConvertMillisecondsToMicros(std::uint64_t milliseconds, std::uint64_t* micros) {
  constexpr std::uint64_t kMicrosPerMilli = 1000ULL;
  if (milliseconds > std::numeric_limits<std::uint64_t>::max() / kMicrosPerMilli) {
    return false;
  }
  *micros = milliseconds * kMicrosPerMilli;
  return true;
}

void RecorderWorker::SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

void RecorderWorker::PrepareQueuedMessage(ReplayMessage* owned, const RecordedMessage& message, std::uint64_t record_seq,
                                          std::uint64_t event_mono_ts_us, std::uint64_t event_utc_ts_us) {
  owned->record_seq = record_seq;
  owned->event_mono_ts_us = event_mono_ts_us;
  owned->event_utc_ts_us = event_utc_ts_us;
  owned->session_id = message.session_id;
  owned->message_type = message.message_type;
  owned->payload.clear();
}

void RecorderWorker::CopyByteView(const ByteView& source, std::vector<std::uint8_t>* destination) {
  destination->resize(source.size);
  if (source.size != 0) {
    std::memcpy(destination->data(), source.data, source.size);
  }
}

AppendResult RecorderWorker::ReserveAppendEntryLocked(const RecordedMessage& message, std::size_t reserve_bytes,
                                                      RecorderQueue::QueuedEntry** entry,
                                                      RecorderQueue::AcceptStatus* accept_status) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!fatal_error_.empty()) {
    return AppendResult::kInternalError;
  }
  if (!open_) {
    return AppendResult::kClosed;
  }

  while (true) {
    *accept_status = queue_.PrepareAcceptQueueLocked(reserve_bytes);
    if (*accept_status != RecorderQueue::AcceptStatus::kBlocked) {
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

  *entry = queue_.AcquireEntryLocked();
  if (*entry == nullptr) {
    fatal_error_ = "internal entry pool exhausted";
    return AppendResult::kInternalError;
  }

  const auto mono_now = std::chrono::steady_clock::now();
  const auto utc_now = std::chrono::system_clock::now();
  (*entry)->state.store(RecorderQueue::QueuedEntry::State::kPending, std::memory_order_relaxed);
  PrepareQueuedMessage(&(*entry)->message, message, next_record_seq_++, ToSteadyMicros(mono_now), ToUnixMicros(utc_now));
  queue_.PushReservedEntryLocked(*entry, reserve_bytes);
  return AppendResult::kOk;
}

void RecorderWorker::PublishReservedEntry(RecorderQueue::AcceptStatus accept_status) {
  if (accept_status == RecorderQueue::AcceptStatus::kAcceptedAfterSwitch) {
    producer_cv_.notify_one();
  }
}

void RecorderWorker::WriterLoop() {
  for (;;) {
    RecorderQueue::QueuedEntry* entry = nullptr;
    bool should_flush = false;
    bool should_exit = false;
    bool entry_aborted = false;
    bool notify_producers = false;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      writer_cv_.wait(lock, [&]() {
        return closing_requested_ || !fatal_error_.empty() || queue_.AnyQueueHasEntriesLocked() ||
               flush_requested_id_ > flush_completed_id_;
      });

      if (queue_.SelectNextDrainQueueLocked()) {
        RecorderQueue::QueuedEntry* front = queue_.FrontLocked();
        if (front->state.load(std::memory_order_acquire) == RecorderQueue::QueuedEntry::State::kPending) {
          writer_cv_.wait(lock, [&]() {
            return front->state.load(std::memory_order_acquire) != RecorderQueue::QueuedEntry::State::kPending ||
                   !fatal_error_.empty();
          });
          continue;
        }
        entry = front;
        entry_aborted =
            entry->state.load(std::memory_order_acquire) == RecorderQueue::QueuedEntry::State::kAborted;
        queue_.PopFrontLocked();
        entry->state.store(RecorderQueue::QueuedEntry::State::kConsumed, std::memory_order_relaxed);
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
        storage_.HandleAbortedEntry();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          queue_.RecycleEntryLocked(entry);
        }
        continue;
      }

      std::string error;
      if (!storage_.WriteRecord(entry->message, segment_max_bytes_, sparse_index_interval_us_,
                                config_.sparse_index_max_records, config_.sparse_index_max_bytes, &error)) {
        storage_.MarkDegraded();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          fatal_error_ = error;
          queue_.RecycleEntryLocked(entry);
        }
        producer_cv_.notify_all();
        flush_cv_.notify_all();
        writer_cv_.notify_all();
        break;
      }

      {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.RecycleEntryLocked(entry);
      }
      continue;
    }

    if (should_flush) {
      std::string error;
      if (storage_.ShouldSyncOnExplicitFlush() ? !storage_.PerformDurabilityCheckpoint(&error)
                                               : !storage_.FlushManifestSnapshot(&error)) {
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
      storage_.FinishCurrentSegment(&error);
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

}  // namespace jojo::rec::internal
