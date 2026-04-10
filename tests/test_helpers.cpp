#include "test_helpers.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace test {

TempDir::TempDir() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  path_ = std::filesystem::temp_directory_path() / ("message_recorder_tests_" + std::to_string(now));
  std::filesystem::create_directories(path_);
}

TempDir::~TempDir() {
  std::error_code ec;
  std::filesystem::remove_all(path_, ec);
}

const std::filesystem::path& TempDir::Path() const { return path_; }

std::vector<std::uint8_t> Bytes(const std::string& text) { return std::vector<std::uint8_t>(text.begin(), text.end()); }

jojo::rec::RecorderConfig MakeConfig(const std::filesystem::path& root) {
  jojo::rec::RecorderConfig config;
  config.output_root = root;
  config.queue_capacity_mb = 1;
  config.segment_max_mb = 1;
  config.backpressure_policy = jojo::rec::BackpressurePolicy::kBlock;
  config.message_type_names = {{1U, "alpha"}};
  return config;
}

void Require(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void RequireEqual(std::uint64_t lhs, std::uint64_t rhs, const std::string& message) {
  if (lhs != rhs) {
    throw std::runtime_error(message + ": expected=" + std::to_string(rhs) + " actual=" + std::to_string(lhs));
  }
}

void RequireString(const std::string& lhs, const std::string& rhs, const std::string& message) {
  if (lhs != rhs) {
    throw std::runtime_error(message + ": expected='" + rhs + "' actual='" + lhs + "'");
  }
}

int RunTest(const std::string& name, const std::function<void()>& body) {
  try {
    body();
    std::cout << "PASS " << name << "\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cout << "FAIL " << name << ": " << ex.what() << "\n";
    return 1;
  }
}

}  // namespace test