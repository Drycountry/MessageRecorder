#ifndef JOJO_REC_DETAIL_FILE_UTIL_HPP_
#define JOJO_REC_DETAIL_FILE_UTIL_HPP_

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace jojo::rec::internal {

/// @brief 确保目录树存在，不存在时递归创建。
bool EnsureDirectory(const std::filesystem::path& path, std::string* error);

/// @brief 以无 BOM 的 UTF-8 文本格式写出文件。
bool WriteTextFile(const std::filesystem::path& path, const std::string& text, std::string* error);

/// @brief 读取整个文本文件内容到字符串。
bool ReadTextFile(const std::filesystem::path& path, std::string* text, std::string* error);

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

#endif  // JOJO_REC_DETAIL_FILE_UTIL_HPP_
