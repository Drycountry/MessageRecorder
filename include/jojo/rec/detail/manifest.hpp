#ifndef JOJO_REC_DETAIL_MANIFEST_HPP_
#define JOJO_REC_DETAIL_MANIFEST_HPP_

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "jojo/rec/inspect_types.hpp"

namespace jojo::rec::internal {

/// @brief 磁盘上的 manifest 模型。
struct ManifestData {
  std::uint32_t format_version = 1;                         // 写入 manifest 的格式版本。
  std::string start_utc;                                    // 紧凑 UTC 形式的录制开始时间戳。
  std::optional<std::string> stop_utc;                      // 紧凑 UTC 形式的可选正常结束时间戳。
  bool incomplete = true;                                   // 录制未被干净地完成收尾时为 true。
  bool degraded = false;                                    // 观测到任何降级状态时为 true。
  std::uint64_t aborted_entries = 0;                        // 生产者中止的条目数量。
  std::uint64_t total_records = 0;                          // 所有 segment 中有效记录的总数。
  std::uint64_t total_payload_bytes = 0;                    // 所有 segment 中负载字节总数。
  std::string recording_label;                              // 从配置复制而来的可读录制标签。
  std::map<std::uint32_t, std::string> message_type_names;  // 从类型 ID 到可读名称的冻结映射。
  std::vector<SegmentSummary> segments;                     // 按写入顺序保存的各 segment 摘要。
};

/// @brief 以原子替换方式写出当前录制目录下的 manifest 文件。
bool WriteManifest(const std::filesystem::path& recording_path, const ManifestData& manifest, std::string* error);

/// @brief 从录制目录加载并解析 manifest 文件。
bool LoadManifest(const std::filesystem::path& recording_path, ManifestData* manifest, std::string* error);

/// @brief 将内部 manifest 模型转换为对外暴露的录制摘要。
RecordingSummary ToRecordingSummary(const std::filesystem::path& recording_path, const ManifestData& manifest);

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_MANIFEST_HPP_
