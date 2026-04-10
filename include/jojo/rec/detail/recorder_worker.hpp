#ifndef JOJO_REC_DETAIL_RECORDER_WORKER_HPP_
#define JOJO_REC_DETAIL_RECORDER_WORKER_HPP_

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "jojo/rec/detail/recorder_queue.hpp"
#include "jojo/rec/detail/recorder_storage.hpp"
#include "jojo/rec/record_types.hpp"

namespace jojo::rec::internal {

/// @brief 驱动录制后台线程、队列与持久化状态的内部执行层。
class RecorderWorker {
 public:
  explicit RecorderWorker(RecorderConfig config);
  ~RecorderWorker();

  RecorderWorker(const RecorderWorker&) = delete;
  RecorderWorker& operator=(const RecorderWorker&) = delete;
  RecorderWorker(RecorderWorker&&) = delete;
  RecorderWorker& operator=(RecorderWorker&&) = delete;

  /// @brief 初始化后台执行层并启动写线程。
  bool Initialize(std::string* error);
  /// @brief 返回录制器是否仍可接受新消息。
  bool IsOpen() const;
  /// @brief 返回当前活动或最终录制目录路径。
  std::filesystem::path RecordingPath() const;
  /// @brief 向后台队列追加一条消息。
  AppendResult Append(const RecordedMessage& message);
  /// @brief 请求后台线程执行一次 flush。
  bool Flush(std::string* error);
  /// @brief 关闭后台线程并完成最终发布。
  bool Close(std::string* error);

 private:
  static constexpr std::uint64_t kBytesPerMb = 1000ULL * 1000ULL;

  /// @brief 在不抛异常的前提下执行关闭。
  void CloseNoexcept() noexcept;
  /// @brief 将十进制 MB 配置值转换为字节数。
  static bool ConvertMegabytesToBytes(std::size_t megabytes, std::uint64_t* bytes);
  /// @brief 将毫秒配置值转换为微秒数。
  static bool ConvertMillisecondsToMicros(std::uint64_t milliseconds, std::uint64_t* micros);
  /// @brief 在提供错误输出缓冲时写入错误消息。
  static void SetError(const std::string& message, std::string* error);
  /// @brief 用元数据填充预留好的拥有型消息。
  static void PrepareQueuedMessage(ReplayMessage* owned, const RecordedMessage& message, std::uint64_t record_seq,
                                   std::uint64_t event_mono_ts_us, std::uint64_t event_utc_ts_us);
  /// @brief 将非拥有字节视图复制到拥有型缓冲区。
  static void CopyByteView(const ByteView& source, std::vector<std::uint8_t>* destination);
  /// @brief 为一次追加预留容量、条目和记录序号。
  AppendResult ReserveAppendEntryLocked(const RecordedMessage& message, std::size_t reserve_bytes,
                                        RecorderQueue::QueuedEntry** entry, RecorderQueue::AcceptStatus* accept_status);
  /// @brief 在条目预留完成后按需唤醒可能阻塞的生产者。
  void PublishReservedEntry(RecorderQueue::AcceptStatus accept_status);
  /// @brief 专用写线程主循环，串行写入、flush 和收尾。
  void WriterLoop();

  RecorderConfig config_;
  std::size_t queue_capacity_bytes_ = 0;
  std::uint64_t segment_max_bytes_ = 0;
  std::uint64_t sparse_index_interval_us_ = 0;
  RecorderStorage storage_;
  bool open_ = false;
  bool closing_requested_ = false;
  bool closed_ = false;
  std::string fatal_error_;
  std::uint64_t next_record_seq_ = 0;
  RecorderQueue queue_;
  std::size_t flush_requested_id_ = 0;
  std::size_t flush_completed_id_ = 0;
  mutable std::mutex mutex_;
  std::condition_variable producer_cv_;
  std::condition_variable writer_cv_;
  std::condition_variable flush_cv_;
  std::thread writer_thread_;
};

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_RECORDER_WORKER_HPP_
