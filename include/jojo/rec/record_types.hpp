#ifndef JOJO_REC_RECORD_TYPES_HPP_
#define JOJO_REC_RECORD_TYPES_HPP_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>

namespace jojo::rec {

/// @brief 非拥有字节视图
struct ByteView {
  const std::uint8_t* data = nullptr;  // 指向首字节的指针；空视图时为 `nullptr`。
  std::size_t size = 0;                // 从 `data` 开始可读取的字节数。
};

/// @brief 内部接收缓冲耗尽时的背压处理策略。
enum class BackpressurePolicy {
  kBlock,     // 阻塞调用方，直到至少一个内部接收队列重新腾出容量。
  kFailFast,  // 立即返回 `AppendResult::kBackpressure`。
};

/// @brief 为保证录制 checkpoint 数据持久性而使用的同步策略。
enum class FsyncPolicy {
  kNever,       // 从不强制执行显式同步调用。
  kInterval,    // 只在定时 `Flush()` 时执行同步。
  kEveryFlush,  // 每次显式调用 `Flush()` 都执行同步。
};

/// @brief `Recorder::Append` 的返回结果。
enum class AppendResult {
  kOk,             // 消息已被 Recorder 队列接受。
  kBackpressure,   // 所有内部接收队列都已满，且配置了 `FailFast`。
  kClosed,         //  Recorder 正在关闭或已经关闭。
  kInternalError,  //  Recorder 遇到了不可恢复的内部错误。
};

/// @brief 待入队消息
struct RecordedMessage {
  std::uint64_t session_id = 0;    // 逻辑会话标识
  std::uint32_t message_type = 0;  // 消息类型
  ByteView payload;                // 非拥有负载视图
};

/// @brief 不可变的 Recorder 配置。
struct RecorderConfig {
  std::string recording_label;                              // 标签（不参控）
  std::map<std::uint32_t, std::string> message_type_names;  // 从消息类型 ID 到可读名称的冻结映射。

  std::filesystem::path output_root;   // 库用于创建录制目录的输出根路径。
  std::size_t queue_capacity_mb = 4;   // 每个内部接收队列的容量，单位 MB。
  std::size_t queue_buffer_count = 2;  //  Recorder 内部接收队列数量，最小为 2。
  std::size_t segment_max_mb = 2048;   // 轮转到下一个文件前允许的最大 segment 大小，单位 MB。
  BackpressurePolicy backpressure_policy = BackpressurePolicy::kBlock;  // 当所有内部接收队列都耗尽时使用的背压策略。
  std::uint64_t sparse_index_interval_ms = 100;  // 相邻稀疏索引点允许跨越的最大单调时间跨度，单位为毫秒。
  std::size_t sparse_index_max_records = 512;    // 未命中时间条件时，强制补一个稀疏索引点的最大记录数跨度。
  std::size_t sparse_index_max_bytes = 1024 * 1024;  // 未命中时间条件时，强制补一个稀疏索引点的最大记录区字节跨度。

  FsyncPolicy fsync_policy = FsyncPolicy::kNever;  // `Flush()` 和 `Close()` 使用的 durability checkpoint 同步策略。
  std::uint64_t flush_interval_ms = 1000;          // 当 `fsync_policy == kInterval` 时使用的毫秒级间隔提示。
};

}  // namespace jojo::rec

#endif  // JOJO_REC_RECORD_TYPES_HPP_
