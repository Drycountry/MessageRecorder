#ifndef JOJO_REC_INSPECT_HPP_
#define JOJO_REC_INSPECT_HPP_

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

#include "jojo/rec/types.hpp"

namespace jojo::rec {

/// @brief 从 manifest 元数据加载录制摘要。
/// @param recording_path 要检查的录制目录。
/// @param summary 成功时填充的输出摘要。
/// @param error 失败时可选的错误输出字符串。
/// @return 成功时返回 true。
bool LoadRecordingSummary(const std::filesystem::path& recording_path, RecordingSummary* summary,
                          std::string* error = nullptr);

/// @brief 在不修改任何文件的前提下校验录制目录。
/// @param recording_path 要检查的录制目录。
/// @return 包含问题列表和摘要的校验结果。
VerifyResult VerifyRecording(const std::filesystem::path& recording_path);

/// @brief 在不截断 segment 的前提下修复录制目录的 manifest 元数据。
/// @param recording_path 要修复的录制目录。
/// @return 包含问题列表和更新后摘要的修复结果。
VerifyResult RepairRecording(const std::filesystem::path& recording_path);

/// @brief 从指定回放游标开始导出紧凑的记录头摘要。
/// @param recording_path 要检查的录制目录。
/// @param cursor 起始游标，使用与回放一致的 lower-bound 语义。
/// @param max_records 最多输出的记录条数。
/// @param entries 导出结果输出数组。
/// @param error 失败时可选的错误输出字符串。
/// @return 成功时返回 true。
bool DumpRecording(const std::filesystem::path& recording_path, const ReplayCursor& cursor, std::size_t max_records,
                   std::vector<DumpEntry>* entries, std::string* error = nullptr);

}  // namespace jojo::rec

#endif  // JOJO_REC_INSPECT_HPP_ 头文件保护