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

/**
 * @brief 公共 API 使用的非拥有字节视图。
 */
struct ByteView {
  /** @brief 指向首字节的指针；空视图时为 `nullptr`。 */
  const std::uint8_t* data = nullptr;
  /** @brief 从 `data` 开始可读取的字节数。 */
  std::size_t size = 0;
};

/**
 * @brief 内部接收缓冲耗尽时的背压处理策略。
 */
enum class BackpressurePolicy {
  /** @brief 阻塞调用方，直到至少一个内部接收队列重新腾出容量。 */
  kBlock,
  /** @brief 立即返回 `AppendResult::kBackpressure`。 */
  kFailFast,
};

/**
 * @brief 为保证数据持久性而使用的 manifest 刷新策略。
 */
enum class FsyncPolicy {
  /** @brief 从不强制执行显式同步调用。 */
  kNever,
  /** @brief 只在定时 `Flush()` 时执行同步。 */
  kInterval,
  /** @brief 每次显式调用 `Flush()` 都执行同步。 */
  kEveryFlush,
};

/**
 * @brief `Recorder::Append` 的返回结果。
 */
enum class AppendResult {
  /** @brief 消息已被录制器队列接受。 */
  kOk,
  /** @brief 所有内部接收队列都已满，且配置了 `FailFast`。 */
  kBackpressure,
  /** @brief 录制器正在关闭或已经关闭。 */
  kClosed,
  /** @brief 录制器遇到了不可恢复的内部错误。 */
  kInternalError,
};

/**
 * @brief 由调用方提供、可进入队列的消息。
 */
struct RecordedMessage {
  /** @brief 由调用方提供的逻辑会话标识。 */
  std::uint64_t session_id = 0;
  /** @brief 会话内可选的逻辑流标识。 */
  std::optional<std::uint64_t> stream_id;
  /** @brief 稳定的数值型消息类型标识。 */
  std::uint32_t message_type = 0;
  /** @brief 模式或负载版本标识。 */
  std::uint16_t message_version = 0;
  /** @brief 由录制器复制的非拥有负载视图。 */
  ByteView payload;
  /** @brief 由录制器复制的可选非拥有属性视图。 */
  std::optional<ByteView> attributes;
};

/**
 * @brief 不可变的录制器配置。
 */
struct RecorderConfig {
  /** @brief 库用于创建录制目录的输出根路径。 */
  std::filesystem::path output_root;
  /** @brief 每个内部接收队列可占用的最大排队容量，单位为十进制 MB。 */
  std::size_t queue_capacity_mb = 4;
  /** @brief 录制器内部接收队列数量，最小为 2。 */
  std::size_t queue_buffer_count = 2;
  /** @brief 轮转到下一个文件前允许的最大 segment 大小，单位为十进制 MB。 */
  std::size_t segment_max_mb = 32;
  /** @brief 当所有内部接收队列都耗尽时使用的背压策略。 */
  BackpressurePolicy backpressure_policy = BackpressurePolicy::kBlock;
  /** @brief `Flush()` 和 `Close()` 使用的 manifest fsync 策略。 */
  FsyncPolicy fsync_policy = FsyncPolicy::kNever;
  /** @brief 当 `fsync_policy == kInterval` 时使用的毫秒级间隔提示。 */
  std::uint64_t flush_interval_ms = 1000;
  /** @brief 写入 manifest 的可选录制标签。 */
  std::string recording_label;
  /** @brief 从消息类型 ID 到可读名称的冻结映射。 */
  std::map<std::uint32_t, std::string> message_type_names;
};

/**
 * @brief 对外暴露的回放定位类型。
 */
enum class ReplayCursorKind {
  /** @brief 定位到第一条满足 `record_seq >= value` 的记录。 */
  kRecordSequence,
  /** @brief 定位到第一条满足 `event_mono_ts_us >= value` 的记录。 */
  kEventMonoTime,
  /** @brief 定位到第一条满足 `event_utc_ts_us >= value` 的记录。 */
  kEventUtcTime,
  /** @brief 定位到位于或晚于某个 segment checkpoint 的第一条记录。 */
  kSegmentCheckpoint,
};

/**
 * @brief 回放游标请求。
 */
struct ReplayCursor {
  /** @brief 游标解释方式。 */
  ReplayCursorKind kind = ReplayCursorKind::kRecordSequence;
  /** @brief 用于序号或时间戳定位的数值。 */
  std::uint64_t value = 0;
  /** @brief 用于 checkpoint 定位的可选 segment 索引。 */
  std::optional<std::uint32_t> segment_index;

  static ReplayCursor FromRecordSequence(std::uint64_t record_seq) {
    return ReplayCursor{ReplayCursorKind::kRecordSequence, record_seq, std::nullopt};
  }

  static ReplayCursor FromEventMonoTime(std::uint64_t event_mono_ts_us) {
    return ReplayCursor{ReplayCursorKind::kEventMonoTime, event_mono_ts_us, std::nullopt};
  }

  static ReplayCursor FromEventUtcTime(std::uint64_t event_utc_ts_us) {
    return ReplayCursor{ReplayCursorKind::kEventUtcTime, event_utc_ts_us, std::nullopt};
  }

  static ReplayCursor FromSegmentCheckpoint(std::uint32_t index) {
    return ReplayCursor{ReplayCursorKind::kSegmentCheckpoint, 0, index};
  }
};

/**
 * @brief 控制回放节奏的选项。
 */
struct ReplayOptions {
  /** @brief 回放速率倍率，`1.0` 表示实时回放，`0.0` 表示尽快回放。 */
  double speed = 1.0;
  /** @brief 启用靠近截止时间时的固定内部忙等窗口。 */
  bool high_precision_mode = false;
};

/**
 * @brief 传递给回放目标和导出接口的完整拥有消息。
 */
struct ReplayMessage {
  /** @brief 由录制器分配的序号。 */
  std::uint64_t record_seq = 0;
  /** @brief 以微秒表示的单调事件时间戳。 */
  std::uint64_t event_mono_ts_us = 0;
  /** @brief 以微秒表示的 UTC 事件时间戳。 */
  std::uint64_t event_utc_ts_us = 0;
  /** @brief 逻辑会话标识。 */
  std::uint64_t session_id = 0;
  /** @brief 可选的逻辑流标识。 */
  std::optional<std::uint64_t> stream_id;
  /** @brief 稳定的数值型消息类型标识。 */
  std::uint32_t message_type = 0;
  /** @brief 模式或负载版本标识。 */
  std::uint16_t message_version = 0;
  /** @brief 自有的负载字节。 */
  std::vector<std::uint8_t> payload;
  /** @brief 自有的可选属性字节。 */
  std::vector<std::uint8_t> attributes;
};

/**
 * @brief 会话和检查接口返回的回放失败信息。
 */
struct ReplayFailure {
  /** @brief 已知时的失败记录序号。 */
  std::optional<std::uint64_t> record_seq;
  /** @brief 已知时的失败 UTC 事件时间戳。 */
  std::optional<std::uint64_t> event_utc_ts_us;
  /** @brief 便于阅读的错误描述。 */
  std::string reason;
};

/**
 * @brief 异步回放的最终状态。
 */
struct ReplayResult {
  /** @brief 回放在没有目标端失败的情况下到达流末尾时为 true。 */
  bool completed = false;
  /** @brief 调用方在完成前请求停止时为 true。 */
  bool stopped_by_request = false;
  /** @brief 目标端或读取端错误的可选失败详情。 */
  std::optional<ReplayFailure> failure;
};

/**
 * @brief 单个录制 segment 的摘要信息。
 */
struct SegmentSummary {
  /** @brief 从 0 开始的 segment 序号。 */
  std::uint32_t segment_index = 0;
  /** @brief 相对录制目录的 segment 文件名。 */
  std::string file_name;
  /** @brief 该 segment 中有效记录的数量。 */
  std::uint64_t record_count = 0;
  /** @brief 可用时，该 segment 内的第一条记录序号。 */
  std::optional<std::uint64_t> first_record_seq;
  /** @brief 可用时，该 segment 内的最后一条记录序号。 */
  std::optional<std::uint64_t> last_record_seq;
  /** @brief 可用时，该 segment 内的首个单调事件时间戳。 */
  std::optional<std::uint64_t> first_event_mono_ts_us;
  /** @brief 可用时，该 segment 内的最后一个单调事件时间戳。 */
  std::optional<std::uint64_t> last_event_mono_ts_us;
  /** @brief 可用时，该 segment 内的首个 UTC 事件时间戳。 */
  std::optional<std::uint64_t> first_event_utc_ts_us;
  /** @brief 可用时，该 segment 内的最后一个 UTC 事件时间戳。 */
  std::optional<std::uint64_t> last_event_utc_ts_us;
  /** @brief 磁盘上文件总大小，单位为字节。 */
  std::uint64_t file_size_bytes = 0;
  /** @brief 用于读取和修复边界的逻辑有效字节长度。 */
  std::uint64_t valid_bytes = 0;
  /** @brief 成功写入有效 footer 和索引时为 true。 */
  bool has_footer = false;
};

/**
 * @brief CLI 和测试使用的录制级摘要。
 */
struct RecordingSummary {
  /** @brief 解析后的录制目录路径。 */
  std::filesystem::path recording_path;
  /** @brief 紧凑 UTC 字符串形式的开始时间。 */
  std::string start_utc;
  /** @brief 紧凑 UTC 字符串形式的可选结束时间。 */
  std::optional<std::string> stop_utc;
  /** @brief 录制未被干净地完成收尾时为 true。 */
  bool incomplete = false;
  /** @brief 观测到任何降级状态时为 true。 */
  bool degraded = false;
  /** @brief 生产者中止的队列条目数量。 */
  std::uint64_t aborted_entries = 0;
  /** @brief 所有 segment 中有效记录的总数。 */
  std::uint64_t total_records = 0;
  /** @brief 所有 segment 中负载字节总数。 */
  std::uint64_t total_payload_bytes = 0;
  /** @brief 所有 segment 中属性字节总数。 */
  std::uint64_t total_attributes_bytes = 0;
  /** @brief 从配置复制而来的可读录制标签。 */
  std::string recording_label;
  /** @brief 从类型 ID 到可读名称的冻结映射。 */
  std::map<std::uint32_t, std::string> message_type_names;
  /** @brief 按录制顺序排列的 segment 摘要。 */
  std::vector<SegmentSummary> segments;
};

/**
 * @brief 录制目录的校验或修复结果。
 */
struct VerifyResult {
  /** @brief 操作在没有结构性错误的情况下完成时为 true。 */
  bool success = false;
  /** @brief 录制在结构上已降级但仍可读取时为 true。 */
  bool degraded = false;
  /** @brief 操作重写了 manifest 元数据时为 true。 */
  bool changed = false;
  /** @brief 校验或修复后的可选摘要。 */
  std::optional<RecordingSummary> summary;
  /** @brief 详细的可读问题列表。 */
  std::vector<std::string> issues;
};

/**
 * @brief 检查接口输出的紧凑导出条目。
 */
struct DumpEntry {
  /** @brief 由录制器分配的序号。 */
  std::uint64_t record_seq = 0;
  /** @brief 以微秒表示的单调事件时间戳。 */
  std::uint64_t event_mono_ts_us = 0;
  /** @brief 以微秒表示的 UTC 事件时间戳。 */
  std::uint64_t event_utc_ts_us = 0;
  /** @brief 逻辑会话标识。 */
  std::uint64_t session_id = 0;
  /** @brief 可选的逻辑流标识。 */
  std::optional<std::uint64_t> stream_id;
  /** @brief 稳定的数值型消息类型标识。 */
  std::uint32_t message_type = 0;
  /** @brief 模式或负载版本标识。 */
  std::uint16_t message_version = 0;
  /** @brief 负载大小，单位为字节。 */
  std::uint32_t payload_size = 0;
  /** @brief 属性块大小，单位为字节。 */
  std::uint32_t attributes_size = 0;
};

}  // namespace jojo::rec

#endif  // JOJO_REC_TYPES_HPP_ 头文件保护
