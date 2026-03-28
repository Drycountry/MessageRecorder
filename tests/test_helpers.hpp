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

class TempDir {
 public:
  TempDir();
  ~TempDir();

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::filesystem::path& Path() const;

 private:
  /** @brief 测试结束后会被删除的临时目录。 */
  std::filesystem::path path_;
};

std::vector<std::uint8_t> Bytes(const std::string& text);
jojo::rec::RecorderConfig MakeConfig(const std::filesystem::path& root);
void Require(bool condition, const std::string& message);
void RequireEqual(std::uint64_t lhs, std::uint64_t rhs, const std::string& message);
void RequireString(const std::string& lhs, const std::string& rhs, const std::string& message);
int RunTest(const std::string& name, const std::function<void()>& body);

}  // test 命名空间

#endif  // MESSAGE_RECORDER_TEST_HELPERS_HPP_ 头文件保护