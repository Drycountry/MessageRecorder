#ifndef MESSAGE_RECORDER_TEST_HELPERS_HPP_
#define MESSAGE_RECORDER_TEST_HELPERS_HPP_

#include <filesystem>
#include <functional>
#include <string>
#include <vector>

#include "jojo/rec/inspect.hpp"
#include "jojo/rec/recorder.hpp"
#include "jojo/rec/replay.hpp"

namespace test {

/// @brief 为测试创建并在析构时清理临时目录。
class TempDir {
 public:
  /// @brief 创建一个唯一的临时目录。
  TempDir();
  /// @brief 删除此前创建的临时目录及其内容。
  ~TempDir();

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  /// @brief 返回测试临时目录路径。
  const std::filesystem::path& Path() const;

 private:
  /// @brief 测试结束后会被删除的临时目录。
  std::filesystem::path path_;
};

/// @brief 将字符串内容转换为字节数组。
std::vector<std::uint8_t> Bytes(const std::string& text);
/// @brief 创建一份适合测试使用的默认录制配置。
jojo::rec::RecorderConfig MakeConfig(const std::filesystem::path& root);
/// @brief 断言条件为真，否则抛出带消息的异常。
void Require(bool condition, const std::string& message);
/// @brief 断言两个无符号整数相等，否则抛出带消息的异常。
void RequireEqual(std::uint64_t lhs, std::uint64_t rhs, const std::string& message);
/// @brief 断言两个字符串相等，否则抛出带消息的异常。
void RequireString(const std::string& lhs, const std::string& rhs, const std::string& message);
/// @brief 执行单个测试用例并输出 PASS/FAIL。
int RunTest(const std::string& name, const std::function<void()>& body);

}  // namespace test

#endif  // MESSAGE_RECORDER_TEST_HELPERS_HPP_ 头文件保护
