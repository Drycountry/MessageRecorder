#ifndef JOJO_REC_DETAIL_INTERNAL_HPP_
#define JOJO_REC_DETAIL_INTERNAL_HPP_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "jojo/rec/types.hpp"

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

/// @brief 存储在 footer 索引区中的单条稠密 segment 索引项。
struct SegmentIndexEntry {
  std::uint64_t record_seq = 0;        // 由录制器分配的记录序号。
  std::uint64_t event_mono_ts_us = 0;  // 以微秒表示的单调事件时间戳。
  std::uint64_t event_utc_ts_us = 0;   // 以微秒表示的 UTC 事件时间戳。
  std::uint64_t file_offset = 0;       // 记录头在 segment 文件中的字节偏移。
};

/// @brief 由写线程持有的可写 segment 状态。
struct SegmentWriteContext {
  std::filesystem::path file_path;               // 当前活动 segment 的绝对文件路径。
  std::ofstream stream;                          // 指向 segment 的二进制输出流。
  std::vector<SegmentIndexEntry> index_entries;  // 写记录时同步维护的内存稠密索引。
  SegmentSummary summary;                        // 用于 manifest 快照的累计摘要字段。
  std::uint64_t data_size_bytes = 0;             // 记录区当前累计大小，单位为字节。
  bool finalized = false;                        // footer 已成功写入后为 true。
};

/// @brief 控制 segment 扫描行为的选项。
struct SegmentScanOptions {
  bool load_payloads = false;                   // 是否为每条记录加载完整的负载和属性字节。
  std::optional<std::uint64_t> max_data_bytes;  // 宣告尾部垃圾前允许扫描的最大逻辑数据字节数。
};

/// @brief 扫描磁盘 segment 后得到的结果。
struct SegmentScanResult {
  SegmentSummary summary;                        // 从可读记录区重建出的摘要。
  std::vector<ReplayMessage> records;            // 当 `load_payloads` 启用时得到的完整拥有回放记录。
  bool footer_present = false;                   // 在文件尾发现 footer 尾迹时为 true。
  bool footer_valid = false;                     // footer 尾迹成功解析时为 true。
  std::uint64_t total_payload_bytes = 0;         // 当前 segment 中可读记录的负载字节总数。
  std::vector<std::string> issues;               // 扫描 segment 时遇到的问题列表。
};

/// @brief 打开一个新的可写 segment 并初始化写上下文。
bool OpenSegment(const std::filesystem::path& recording_path, std::uint32_t segment_index, SegmentWriteContext* context,
                 std::string* error);
/// @brief 判断再写入下一条记录后是否需要轮转到新 segment。
bool ShouldRotateSegment(const SegmentWriteContext& context, std::uint64_t segment_max_bytes,
                         std::size_t next_record_size);
/// @brief 将一条完整拥有消息追加写入当前 segment。
bool WriteRecordToSegment(SegmentWriteContext* context, const ReplayMessage& message, std::string* error);
/// @brief 为当前 segment 写出索引和 footer 并完成收尾。
bool FinalizeSegment(SegmentWriteContext* context, std::string* error);
/// @brief 扫描 segment 文件并重建摘要、索引和可选记录内容。
bool ScanSegment(const std::filesystem::path& segment_path, const SegmentScanOptions& options,
                 SegmentScanResult* result, std::string* error);
/// @brief 估算一条待录制消息写入 segment 后占用的总字节数。
std::size_t EstimateRecordBytes(const RecordedMessage& message);

/// @brief 确保目录树存在，不存在时递归创建。
bool EnsureDirectory(const std::filesystem::path& path, std::string* error);
/// @brief 以无 BOM 的 UTF-8 文本格式写出文件。
bool WriteTextFileUtf8NoBom(const std::filesystem::path& path, const std::string& text, std::string* error);
/// @brief 读取整个文本文件内容到字符串。
bool ReadTextFileUtf8(const std::filesystem::path& path, std::string* text, std::string* error);
/// @brief 将单个文件的已写缓冲刷新到稳定存储。
bool SyncFile(const std::filesystem::path& path, std::string* error);
/// @brief 将目录项元数据尽可能刷新到稳定存储。
bool SyncDirectory(const std::filesystem::path& path, std::string* error);
/// @brief 同步指定路径的父目录。
bool SyncParentDirectory(const std::filesystem::path& path, std::string* error);
/// @brief 通过目录级 rename 原子发布最终录制目录。
bool RenameDirectoryAtomically(const std::filesystem::path& from, const std::filesystem::path& to, std::string* error);
/// @brief 先移除目标路径，再执行重命名或兼容性降级替换。
bool RenameWithReplace(const std::filesystem::path& from, const std::filesystem::path& to, std::string* error);
/// @brief 删除已存在的文件或目录，不存在时视为成功。
bool RemoveIfExists(const std::filesystem::path& path, std::string* error);
/// @brief 读取文件大小，失败时通过错误字符串返回原因。
std::uint64_t FileSize(const std::filesystem::path& path, std::string* error);
/// @brief 将系统时钟时间点格式化为紧凑 UTC 字符串。
std::string FormatUtcCompact(std::chrono::system_clock::time_point time_point);
/// @brief 将系统时钟时间点转换为 Unix 微秒时间戳。
std::uint64_t ToUnixMicros(std::chrono::system_clock::time_point time_point);
/// @brief 将单调时钟时间点转换为微秒计数。
std::uint64_t ToSteadyMicros(std::chrono::steady_clock::time_point time_point);
/// @brief 使用固定分隔符拼接问题列表。
std::string JoinIssues(const std::vector<std::string>& issues);

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_INTERNAL_HPP_ 头文件保护
