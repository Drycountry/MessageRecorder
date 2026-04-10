#ifndef JOJO_REC_INSPECT_TYPES_HPP_
#define JOJO_REC_INSPECT_TYPES_HPP_

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace jojo::rec {

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

#endif  // JOJO_REC_INSPECT_TYPES_HPP_
