#ifndef JOJO_REC_REPLAY_TYPES_HPP_
#define JOJO_REC_REPLAY_TYPES_HPP_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace jojo::rec {

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

}  // namespace jojo::rec

#endif  // JOJO_REC_REPLAY_TYPES_HPP_
