#include "internal/internal.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

namespace jojo::rec::internal {
namespace {

void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

bool MoveDirectoryContents(const std::filesystem::path& from,
                           const std::filesystem::path& to,
                           std::string* error) {
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
        SetError("failed to create directory '" + destination.string() + "': " + ec.message(),
                 error);
        return false;
      }
      continue;
    }
    std::filesystem::create_directories(destination.parent_path(), ec);
    ec.clear();
    std::filesystem::rename(entry.path(), destination, ec);
    if (ec) {
      ec.clear();
      std::filesystem::copy_file(entry.path(), destination,
                                 std::filesystem::copy_options::overwrite_existing, ec);
      if (ec) {
        SetError("failed to move file '" + entry.path().string() + "': " + ec.message(), error);
        return false;
      }
      ec.clear();
      std::filesystem::remove(entry.path(), ec);
      if (ec) {
        SetError("failed to remove source file '" + entry.path().string() + "': " + ec.message(),
                 error);
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

}  // 匿名命名空间

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

bool WriteTextFileUtf8NoBom(const std::filesystem::path& path,
                            const std::string& text,
                            std::string* error) {
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

bool ReadTextFileUtf8(const std::filesystem::path& path,
                      std::string* text,
                      std::string* error) {
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

bool RenameWithReplace(const std::filesystem::path& from,
                       const std::filesystem::path& to,
                       std::string* error) {
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

  SetError("failed to rename '" + from.string() + "' to '" + to.string() + "': " +
               rename_error,
           error);
  return false;
}

std::uint64_t FileSize(const std::filesystem::path& path, std::string* error) {
  std::error_code ec;
  const std::uint64_t size = std::filesystem::file_size(path, ec);
  if (ec) {
    SetError("failed to read file size for '" + path.string() + "': " + ec.message(), error);
    return 0;
  }
  return size;
}

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

std::uint64_t ToUnixMicros(std::chrono::system_clock::time_point time_point) {
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      time_point.time_since_epoch());
  return static_cast<std::uint64_t>(micros.count());
}

std::uint64_t ToSteadyMicros(std::chrono::steady_clock::time_point time_point) {
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      time_point.time_since_epoch());
  return static_cast<std::uint64_t>(micros.count());
}

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

}  // jojo::rec::internal 命名空间