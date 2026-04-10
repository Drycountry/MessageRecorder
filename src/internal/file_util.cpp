#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include "jojo/rec/detail/file_util.hpp"

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace jojo::rec::internal {
namespace {

/// @brief 在提供错误输出缓冲时写入错误消息。
void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

/// @brief 将平台错误码转换为可读字符串。
std::string FormatPlatformErrorMessage(int code) { return std::system_category().message(code); }

/// @brief 在目录整体重命名失败时逐项搬运目录内容。
bool MoveDirectoryContents(const std::filesystem::path& from, const std::filesystem::path& to, std::string* error) {
  std::error_code ec;
  std::filesystem::create_directories(to, ec);
  if (ec) {
    SetError("failed to create fallback directory '" + to.string() + "': " + ec.message(), error);
    return false;
  }

  for (const auto& entry : std::filesystem::recursive_directory_iterator(from, ec)) {
    if (ec) {
      SetError("failed to enumerate directory '" + from.string() + "': " + ec.message(), error);
      return false;
    }
    const auto relative = std::filesystem::relative(entry.path(), from, ec);
    if (ec) {
      SetError("failed to compute relative path during directory move", error);
      return false;
    }
    const auto destination = to / relative;
    if (entry.is_directory()) {
      std::filesystem::create_directories(destination, ec);
      if (ec) {
        SetError("failed to create directory '" + destination.string() + "': " + ec.message(), error);
        return false;
      }
      continue;
    }
    std::filesystem::create_directories(destination.parent_path(), ec);
    ec.clear();
    std::filesystem::rename(entry.path(), destination, ec);
    if (ec) {
      ec.clear();
      std::filesystem::copy_file(entry.path(), destination, std::filesystem::copy_options::overwrite_existing, ec);
      if (ec) {
        SetError("failed to move file '" + entry.path().string() + "': " + ec.message(), error);
        return false;
      }
      ec.clear();
      std::filesystem::remove(entry.path(), ec);
      if (ec) {
        SetError("failed to remove source file '" + entry.path().string() + "': " + ec.message(), error);
        return false;
      }
    }
  }

  std::filesystem::remove_all(from, ec);
  if (ec) {
    SetError("failed to remove source directory '" + from.string() + "': " + ec.message(), error);
    return false;
  }
  return true;
}

}  // namespace

/// @brief 确保目录树存在，不存在时递归创建。
bool EnsureDirectory(const std::filesystem::path& path, std::string* error) {
  std::error_code ec;
  // 在录制器启动前先创建目录树，后续写入即可默认它已经存在。
  std::filesystem::create_directories(path, ec);
  if (ec) {
    SetError("failed to create directory '" + path.string() + "': " + ec.message(), error);
    return false;
  }
  return true;
}

/// @brief 写文件。
bool WriteTextFile(const std::filesystem::path& path, const std::string& text, std::string* error) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  if (!stream.is_open()) {
    SetError("failed to open file for write: " + path.string(), error);
    return false;
  }

  // manifest 重写时始终输出无 BOM 的 UTF-8 文本，以获得稳定的跨平台 diff。
  stream.write(text.data(), static_cast<std::streamsize>(text.size()));
  stream.flush();
  if (!stream.good()) {
    SetError("failed to write file: " + path.string(), error);
    return false;
  }
  return true;
}

/// @brief 读取整个 UTF-8 文本文件内容。
bool ReadTextFile(const std::filesystem::path& path, std::string* text, std::string* error) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream.is_open()) {
    SetError("failed to open file for read: " + path.string(), error);
    return false;
  }

  std::ostringstream buffer;
  buffer << stream.rdbuf();
  if (!stream.good() && !stream.eof()) {
    SetError("failed to read file: " + path.string(), error);
    return false;
  }
  *text = buffer.str();
  return true;
}

/// @brief 刷新单个文件的内核缓冲，尽量保证数据落盘。
bool SyncFile(const std::filesystem::path& path, std::string* error) {
#if defined(_WIN32)
  HANDLE handle = ::CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    SetError("failed to open file for sync '" + path.string() +
                 "': " + FormatPlatformErrorMessage(static_cast<int>(::GetLastError())),
             error);
    return false;
  }
  const bool success = ::FlushFileBuffers(handle) != 0;
  const DWORD flush_error = success ? 0U : ::GetLastError();
  ::CloseHandle(handle);
  if (!success) {
    SetError("failed to flush file buffers for '" + path.string() +
                 "': " + FormatPlatformErrorMessage(static_cast<int>(flush_error)),
             error);
    return false;
  }
  return true;
#else
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    SetError("failed to open file for sync '" + path.string() + "': " + FormatPlatformErrorMessage(errno), error);
    return false;
  }
  const bool success = ::fsync(fd) == 0;
  const int sync_error = success ? 0 : errno;
  const bool close_success = ::close(fd) == 0;
  const int close_error = close_success ? 0 : errno;
  if (!success) {
    SetError("failed to fsync file '" + path.string() + "': " + FormatPlatformErrorMessage(sync_error), error);
    return false;
  }
  if (!close_success) {
    SetError("failed to close synced file '" + path.string() + "': " + FormatPlatformErrorMessage(close_error), error);
    return false;
  }
  return true;
#endif
}

/// @brief 刷新目录元数据，使新增、替换或重命名更持久。
bool SyncDirectory(const std::filesystem::path& path, std::string* error) {
#if defined(_WIN32)
  (void)path;
  (void)error;
  // Windows does not offer a practical directory fsync primitive through the
  // non-privileged file APIs used by this project, so file flushes are the
  // strongest portability point here.
  return true;
#else
  const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    SetError("failed to open directory for sync '" + path.string() + "': " + FormatPlatformErrorMessage(errno), error);
    return false;
  }
  const bool success = ::fsync(fd) == 0;
  const int sync_error = success ? 0 : errno;
  const bool close_success = ::close(fd) == 0;
  const int close_error = close_success ? 0 : errno;
  if (!success) {
    SetError("failed to fsync directory '" + path.string() + "': " + FormatPlatformErrorMessage(sync_error), error);
    return false;
  }
  if (!close_success) {
    SetError("failed to close synced directory '" + path.string() + "': " + FormatPlatformErrorMessage(close_error),
             error);
    return false;
  }
  return true;
#endif
}

/// @brief 刷新给定路径的父目录元数据。
bool SyncParentDirectory(const std::filesystem::path& path, std::string* error) {
  const std::filesystem::path parent = path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path();
  return SyncDirectory(parent, error);
}

/// @brief 使用一次目录级 rename 执行原子发布。
bool RenameDirectoryAtomically(const std::filesystem::path& from, const std::filesystem::path& to, std::string* error) {
  std::error_code ec;
  std::filesystem::rename(from, to, ec);
  if (ec) {
    SetError("failed to atomically rename directory '" + from.string() + "' to '" + to.string() + "': " + ec.message(),
             error);
    return false;
  }
  return true;
}

/// @brief 删除目标路径，不存在时直接视为成功。
bool RemoveIfExists(const std::filesystem::path& path, std::string* error) {
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    return true;
  }

  // 替换式更新会在安装替换内容前先清理已有文件或目录。
  const auto removed = std::filesystem::is_directory(path, ec)
                           ? std::filesystem::remove_all(path, ec)
                           : static_cast<std::uintmax_t>(std::filesystem::remove(path, ec));
  (void)removed;
  if (ec) {
    SetError("failed to remove path '" + path.string() + "': " + ec.message(), error);
    return false;
  }
  return true;
}

/// @brief 以替换语义重命名路径，并兼容目录移动回退。
bool RenameWithReplace(const std::filesystem::path& from, const std::filesystem::path& to, std::string* error) {
  if (!RemoveIfExists(to, error)) {
    return false;
  }

  std::error_code ec;
  std::filesystem::rename(from, to, ec);
  if (!ec) {
    return true;
  }

  const std::string rename_error = ec.message();
  ec.clear();

  // 当子句柄仍带有限制性共享标志时，Windows 可能拒绝目录重命名。
  if (std::filesystem::exists(from, ec) && std::filesystem::is_directory(from, ec) &&
      MoveDirectoryContents(from, to, error)) {
    return true;
  }

  ec.clear();
  if (!std::filesystem::exists(from, ec) && std::filesystem::exists(to, ec)) {
    return true;
  }

  SetError("failed to rename '" + from.string() + "' to '" + to.string() + "': " + rename_error, error);
  return false;
}

/// @brief 返回文件大小，失败时通过错误字符串报告原因。
std::uint64_t FileSize(const std::filesystem::path& path, std::string* error) {
  std::error_code ec;
  const std::uint64_t size = std::filesystem::file_size(path, ec);
  if (ec) {
    SetError("failed to read file size for '" + path.string() + "': " + ec.message(), error);
    return 0;
  }
  return size;
}

/// @brief 将系统时钟时间点格式化为紧凑 UTC 字符串。
std::string FormatUtcCompact(std::chrono::system_clock::time_point time_point) {
  const std::time_t seconds = std::chrono::system_clock::to_time_t(time_point);
  std::tm utc_tm{};
#if defined(_WIN32)
  gmtime_s(&utc_tm, &seconds);
#else
  gmtime_r(&seconds, &utc_tm);
#endif
  char buffer[32] = {};
  // 目录名和 manifest 中的时间有意使用紧凑的 UTC 表示。
  std::strftime(buffer, sizeof(buffer), "%Y%m%dT%H%M%SZ", &utc_tm);
  return std::string(buffer);
}

/// @brief 将系统时钟时间点转换为 Unix 微秒时间戳。
std::uint64_t ToUnixMicros(std::chrono::system_clock::time_point time_point) {
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(time_point.time_since_epoch());
  return static_cast<std::uint64_t>(micros.count());
}

/// @brief 将单调时钟时间点转换为微秒计数。
std::uint64_t ToSteadyMicros(std::chrono::steady_clock::time_point time_point) {
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(time_point.time_since_epoch());
  return static_cast<std::uint64_t>(micros.count());
}

/// @brief 用固定分隔符将问题列表拼接成单行文本。
std::string JoinIssues(const std::vector<std::string>& issues) {
  std::ostringstream stream;
  for (std::size_t index = 0; index < issues.size(); ++index) {
    if (index != 0) {
      stream << " | ";
    }
    stream << issues[index];
  }
  return stream.str();
}

}  // namespace jojo::rec::internal
