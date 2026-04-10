#ifndef JOJO_REC_DETAIL_SEGMENT_HPP_
#define JOJO_REC_DETAIL_SEGMENT_HPP_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "jojo/rec/inspect_types.hpp"
#include "jojo/rec/record_types.hpp"
#include "jojo/rec/replay_types.hpp"

namespace jojo::rec::internal {

/// @brief 存储在 footer 索引区中的单条稀疏 segment 索引项。
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
  std::vector<SegmentIndexEntry> index_entries;  // 写记录时按配置阈值维护的内存稀疏索引。
  SegmentSummary summary;                        // 用于 manifest 快照的累计摘要字段。
  std::uint64_t data_size_bytes = 0;             // 记录区当前累计大小，单位为字节。
  std::uint64_t sparse_index_interval_us = 0;    // 相邻索引点允许跨越的最大单调时间跨度。（稀疏索引生成条件）
  std::size_t sparse_index_max_records = 0;      // 未命中时间条件时触发索引点的最大记录数跨度。（稀疏索引生成条件）
  std::size_t sparse_index_max_bytes = 0;        // 未命中时间条件时触发索引点的最大字节跨度。（稀疏索引生成条件）
  std::uint64_t last_sparse_index_event_mono_ts_us = 0;  // 最近一个索引点对应记录的单调时间戳。
  std::size_t records_since_last_index = 0;              // 距离最近一个索引点已写入的记录数量。
  std::size_t bytes_since_last_index = 0;                // 距离最近一个索引点已写入的记录区字节数。
  bool finalized = false;                                // footer 已成功写入后为 true。
};

/// @brief 控制 segment 扫描行为的选项。
struct SegmentScanOptions {
  bool load_payloads = false;                   // 是否为每条记录加载完整的负载和属性字节。
  std::optional<std::uint64_t> max_data_bytes;  // 宣告尾部垃圾前允许扫描的最大逻辑数据字节数。
};

/// @brief 扫描磁盘 segment 后得到的结果。
struct SegmentScanResult {
  SegmentSummary summary;                 // 从可读记录区重建出的摘要。
  std::vector<ReplayMessage> records;     // 当 `load_payloads` 启用时得到的完整拥有回放记录。
  bool footer_present = false;            // 在文件尾发现 footer 尾迹时为 true。
  bool footer_valid = false;              // footer 尾迹成功解析时为 true。
  std::uint64_t total_payload_bytes = 0;  // 当前 segment 中可读记录的负载字节总数。
  std::vector<std::string> issues;        // 扫描 segment 时遇到的问题列表。
};

/// @brief 从磁盘读取 segment 的稀疏索引区。
struct SegmentSparseIndexData {
  bool footer_present = false;             // 在文件尾发现 footer 尾迹时为 true。
  bool footer_valid = false;               // footer 尾迹成功解析且索引区可读时为 true。
  std::uint64_t valid_bytes = 0;           // footer 声明的记录区有效字节长度。
  std::vector<SegmentIndexEntry> entries;  // 解析出的稀疏索引项。
};

/// @brief 用于按偏移顺序流式读取 segment 中的 record。
struct SegmentReadContext {
  std::filesystem::path file_path;  // 当前打开的 segment 文件路径。
  std::ifstream stream;             // 供回放路径复用的二进制输入流。
  std::uint64_t valid_bytes = 0;    // 记录区的逻辑有效字节长度。
  bool open = false;                // 输入流已经打开并可供读取时为 true。
};

/// @brief 打开一个新的可写 segment 并初始化写上下文。
bool OpenSegment(const std::filesystem::path& recording_path, std::uint32_t segment_index,
                 std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                 std::size_t sparse_index_max_bytes, SegmentWriteContext* context, std::string* error);

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

/// @brief 尝试从磁盘加载一个 segment 的稀疏索引区。
bool LoadSegmentSparseIndex(const std::filesystem::path& segment_path, SegmentSparseIndexData* data,
                            std::string* error);

/// @brief 打开一个供流式回放使用的 segment 读取上下文。
bool OpenSegmentReadContext(const std::filesystem::path& segment_path, std::uint64_t valid_bytes,
                            SegmentReadContext* context, std::string* error);

/// @brief 关闭回放使用的 segment 读取上下文。
void CloseSegmentReadContext(SegmentReadContext* context);

/// @brief 从指定偏移读取一条完整 record，并返回下一条记录的偏移。
bool ReadRecordAtOffset(SegmentReadContext* context, std::uint64_t offset, ReplayMessage* record,
                        std::uint64_t* next_offset, std::string* error);

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_SEGMENT_HPP_
