#ifndef JOJO_REC_TYPES_HPP_
#define JOJO_REC_TYPES_HPP_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

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

  FsyncPolicy fsync_policy = FsyncPolicy::kNever;  // `Flush()` 和 `Close()` 使用的 durability checkpoint 同步策略。
  std::uint64_t flush_interval_ms = 1000;          // 当 `fsync_policy == kInterval` 时使用的毫秒级间隔提示。
};

/// @brief 对外暴露的回放定位类型。
enum class ReplayCursorKind {
  kRecordSequence,     // 定位到第一条满足 `record_seq >= value` 的记录。
  kEventMonoTime,      // 定位到第一条满足 `event_mono_ts_us >= value` 的记录。
  kEventUtcTime,       // 定位到第一条满足 `event_utc_ts_us >= value` 的记录。
  kSegmentCheckpoint,  // 定位到位于或晚于某个 segment checkpoint 的第一条记录。
};

/// @brief 回放游标请求。
struct ReplayCursor {
  ReplayCursorKind kind = ReplayCursorKind::kRecordSequence;  // 游标解释方式。
  std::uint64_t value = 0;                                    // 用于序号或时间戳定位的数值。
  std::optional<std::uint32_t> segment_index;                 // 用于 checkpoint 定位的可选 segment 索引。

  /// @brief 创建一个按记录序号定位的回放游标。
  static ReplayCursor FromRecordSequence(std::uint64_t record_seq) {
    return ReplayCursor{ReplayCursorKind::kRecordSequence, record_seq, std::nullopt};
  }

  /// @brief 创建一个按单调时间戳定位的回放游标。
  static ReplayCursor FromEventMonoTime(std::uint64_t event_mono_ts_us) {
    return ReplayCursor{ReplayCursorKind::kEventMonoTime, event_mono_ts_us, std::nullopt};
  }

  /// @brief 创建一个按 UTC 时间戳定位的回放游标。
  static ReplayCursor FromEventUtcTime(std::uint64_t event_utc_ts_us) {
    return ReplayCursor{ReplayCursorKind::kEventUtcTime, event_utc_ts_us, std::nullopt};
  }

  /// @brief 创建一个按 segment checkpoint 定位的回放游标。
  static ReplayCursor FromSegmentCheckpoint(std::uint32_t index) {
    return ReplayCursor{ReplayCursorKind::kSegmentCheckpoint, 0, index};
  }
};

/// @brief 控制回放节奏的选项。
struct ReplayOptions {
  double speed = 1.0;                // 回放速率倍率，`1.0` 表示实时回放，`0.0` 表示尽快回放。
  bool high_precision_mode = false;  // 启用靠近截止时间时的固定内部忙等窗口。
};

/// @brief 传递给回放目标和导出接口的完整拥有消息。
struct ReplayMessage {
  std::uint64_t record_seq = 0;        // 由 Recorder 分配的序号。
  std::uint64_t event_mono_ts_us = 0;  // 以微秒表示的单调事件时间戳。
  std::uint64_t event_utc_ts_us = 0;   // 以微秒表示的 UTC 事件时间戳。
  std::uint64_t session_id = 0;        // 逻辑会话标识。
  std::uint32_t message_type = 0;      // 稳定的数值型消息类型标识。
  std::vector<std::uint8_t> payload;   // 自有的负载字节。
};

/// @brief 会话和检查接口返回的回放失败信息。
struct ReplayFailure {
  std::optional<std::uint64_t> record_seq;       // 已知时的失败记录序号。
  std::optional<std::uint64_t> event_utc_ts_us;  // 已知时的失败 UTC 事件时间戳。
  std::string reason;                            // 便于阅读的错误描述。
};

/// @brief 异步回放的最终状态。
struct ReplayResult {
  bool completed = false;                // 回放在没有目标端失败的情况下到达流末尾时为 true。
  bool stopped_by_request = false;       // 调用方在完成前请求停止时为 true。
  std::optional<ReplayFailure> failure;  // 目标端或读取端错误的可选失败详情。
};

/// @brief 单个 segment 的摘要信息。
struct SegmentSummary {
  std::uint32_t segment_index = 0;                      // segment 序号（从 0 开始的）
  std::string file_name;                                // 相对录制目录的 segment 文件名。
  std::uint64_t record_count = 0;                       // 该 segment 中的记录数量
  std::optional<std::uint64_t> first_record_seq;        // 可用时，该 segment 内的第一条记录序号。
  std::optional<std::uint64_t> last_record_seq;         // 可用时，该 segment 内的最后一条记录序号。
  std::optional<std::uint64_t> first_event_mono_ts_us;  // 可用时，该 segment 内的首个单调事件时间戳。
  std::optional<std::uint64_t> last_event_mono_ts_us;   // 可用时，该 segment 内的最后一个单调事件时间戳。
  std::optional<std::uint64_t> first_event_utc_ts_us;   // 可用时，该 segment 内的首个 UTC 事件时间戳。
  std::optional<std::uint64_t> last_event_utc_ts_us;    // 可用时，该 segment 内的最后一个 UTC 事件时间戳。
  std::uint64_t file_size_bytes = 0;                    // 磁盘上文件总大小，单位为字节。
  std::uint64_t valid_bytes = 0;                        // 用于读取和修复边界的逻辑有效字节长度。
  bool has_footer = false;                              // 成功写入有效 footer 和索引时为 true。
};

/// @brief CLI 和测试使用的录制级摘要。
struct RecordingSummary {
  std::filesystem::path recording_path;                     // 解析后的录制目录路径。
  std::string start_utc;                                    // 紧凑 UTC 字符串形式的开始时间。
  std::optional<std::string> stop_utc;                      // 紧凑 UTC 字符串形式的可选结束时间。
  bool incomplete = false;                                  // 录制未被干净地完成收尾时为 true。
  bool degraded = false;                                    // 观测到任何降级状态时为 true。
  std::uint64_t aborted_entries = 0;                        // 生产者中止的队列条目数量。
  std::uint64_t total_records = 0;                          // 所有 segment 中有效记录的总数。
  std::uint64_t total_payload_bytes = 0;                    // 所有 segment 中负载字节总数。
  std::string recording_label;                              // 从配置复制而来的可读录制标签。
  std::map<std::uint32_t, std::string> message_type_names;  // 从类型 ID 到可读名称的冻结映射。
  std::vector<SegmentSummary> segments;                     // 按录制顺序排列的 segment 摘要。
};

/// @brief 录制目录的校验或修复结果。
struct VerifyResult {
  bool success = false;                     // 操作在没有结构性错误的情况下完成时为 true。
  bool degraded = false;                    // 录制在结构上已降级但仍可读取时为 true。
  bool changed = false;                     // 操作重写了 manifest 元数据时为 true。
  std::optional<RecordingSummary> summary;  // 校验或修复后的可选摘要。
  std::vector<std::string> issues;          // 详细的可读问题列表。
};

/// @brief 检查接口输出的紧凑导出条目。
struct DumpEntry {
  std::uint64_t record_seq = 0;        // 由 Recorder 分配的序号。
  std::uint64_t event_mono_ts_us = 0;  // 以微秒表示的单调事件时间戳。
  std::uint64_t event_utc_ts_us = 0;   // 以微秒表示的 UTC 事件时间戳。
  std::uint64_t session_id = 0;        // 逻辑会话标识。
  std::uint32_t message_type = 0;      // 稳定的数值型消息类型标识。
  std::uint32_t payload_size = 0;      // 负载大小，单位为字节。
};

}  // namespace jojo::rec

#endif  // JOJO_REC_TYPES_HPP_ 头文件保护
