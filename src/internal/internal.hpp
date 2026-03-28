#ifndef JOJO_REC_INTERNAL_INTERNAL_HPP_
#define JOJO_REC_INTERNAL_INTERNAL_HPP_

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

/**
 * @brief 磁盘上的 manifest 模型。
 */
struct ManifestData {
  /** @brief 写入 manifest 的格式版本。 */
  std::uint32_t format_version = 1;
  /** @brief 紧凑 UTC 形式的录制开始时间戳。 */
  std::string start_utc;
  /** @brief 紧凑 UTC 形式的可选正常结束时间戳。 */
  std::optional<std::string> stop_utc;
  /** @brief 录制未被干净地完成收尾时为 true。 */
  bool incomplete = true;
  /** @brief 观测到任何降级状态时为 true。 */
  bool degraded = false;
  /** @brief 生产者中止的条目数量。 */
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
  /** @brief 按写入顺序保存的各 segment 摘要。 */
  std::vector<SegmentSummary> segments;
};

bool WriteManifest(const std::filesystem::path& recording_path,
                   const ManifestData& manifest,
                   std::string* error);
bool LoadManifest(const std::filesystem::path& recording_path,
                  ManifestData* manifest,
                  std::string* error);
RecordingSummary ToRecordingSummary(const std::filesystem::path& recording_path,
                                    const ManifestData& manifest);

/**
 * @brief 存储在 footer 索引区中的单条稠密 segment 索引项。
 */
struct SegmentIndexEntry {
  /** @brief 由录制器分配的记录序号。 */
  std::uint64_t record_seq = 0;
  /** @brief 以微秒表示的单调事件时间戳。 */
  std::uint64_t event_mono_ts_us = 0;
  /** @brief 以微秒表示的 UTC 事件时间戳。 */
  std::uint64_t event_utc_ts_us = 0;
  /** @brief 记录头在 segment 文件中的字节偏移。 */
  std::uint64_t file_offset = 0;
};

/**
 * @brief 由写线程持有的可写 segment 状态。
 */
struct SegmentWriteContext {
  /** @brief 当前活动 segment 的绝对文件路径。 */
  std::filesystem::path file_path;
  /** @brief 从 0 开始的 segment 序号。 */
  std::uint32_t segment_index = 0;
  /** @brief 指向 segment 的二进制输出流。 */
  std::ofstream stream;
  /** @brief 写记录时同步维护的内存稠密索引。 */
  std::vector<SegmentIndexEntry> index_entries;
  /** @brief 用于 manifest 快照的累计摘要字段。 */
  SegmentSummary summary;
  /** @brief 记录区当前累计大小，单位为字节。 */
  std::uint64_t data_size_bytes = 0;
  /** @brief footer 已成功写入后为 true。 */
  bool finalized = false;
};

/**
 * @brief 控制 segment 扫描行为的选项。
 */
struct SegmentScanOptions {
  /** @brief 是否为每条记录加载完整的负载和属性字节。 */
  bool load_payloads = false;
  /** @brief 宣告尾部垃圾前允许扫描的最大逻辑数据字节数。 */
  std::optional<std::uint64_t> max_data_bytes;
};

/**
 * @brief 扫描磁盘 segment 后得到的结果。
 */
struct SegmentScanResult {
  /** @brief 从可读记录区重建出的摘要。 */
  SegmentSummary summary;
  /** @brief footer 可用时从中加载出的稠密索引项。 */
  std::vector<SegmentIndexEntry> index_entries;
  /** @brief 当 `load_payloads` 启用时得到的完整拥有回放记录。 */
  std::vector<ReplayMessage> records;
  /** @brief 文件总大小，单位为字节。 */
  std::uint64_t file_size_bytes = 0;
  /** @brief 在文件尾发现 footer 尾迹时为 true。 */
  bool footer_present = false;
  /** @brief footer 尾迹成功解析时为 true。 */
  bool footer_valid = false;
  /** @brief 可读区域后的字节被当作垃圾忽略时为 true。 */
  bool truncated_tail = false;
  /** @brief 当前 segment 中可读记录的负载字节总数。 */
  std::uint64_t total_payload_bytes = 0;
  /** @brief 当前 segment 中可读记录的属性字节总数。 */
  std::uint64_t total_attributes_bytes = 0;
  /** @brief 扫描 segment 时遇到的问题列表。 */
  std::vector<std::string> issues;
};

bool OpenSegment(const std::filesystem::path& recording_path,
                 std::uint32_t segment_index,
                 SegmentWriteContext* context,
                 std::string* error);
bool ShouldRotateSegment(const SegmentWriteContext& context,
                         std::uint64_t segment_max_bytes,
                         std::size_t next_record_size);
bool WriteRecordToSegment(SegmentWriteContext* context,
                          const ReplayMessage& message,
                          std::string* error);
bool FinalizeSegment(SegmentWriteContext* context, std::string* error);
bool ScanSegment(const std::filesystem::path& segment_path,
                 const SegmentScanOptions& options,
                 SegmentScanResult* result,
                 std::string* error);
std::size_t EstimateRecordBytes(const RecordedMessage& message);

bool EnsureDirectory(const std::filesystem::path& path, std::string* error);
bool WriteTextFileUtf8NoBom(const std::filesystem::path& path,
                            const std::string& text,
                            std::string* error);
bool ReadTextFileUtf8(const std::filesystem::path& path,
                      std::string* text,
                      std::string* error);
bool RenameWithReplace(const std::filesystem::path& from,
                       const std::filesystem::path& to,
                       std::string* error);
bool RemoveIfExists(const std::filesystem::path& path, std::string* error);
std::uint64_t FileSize(const std::filesystem::path& path, std::string* error);
std::string FormatUtcCompact(std::chrono::system_clock::time_point time_point);
std::uint64_t ToUnixMicros(std::chrono::system_clock::time_point time_point);
std::uint64_t ToSteadyMicros(std::chrono::steady_clock::time_point time_point);
std::string JoinIssues(const std::vector<std::string>& issues);

}  // jojo::rec::internal 命名空间

#endif  // JOJO_REC_INTERNAL_INTERNAL_HPP_ 头文件保护