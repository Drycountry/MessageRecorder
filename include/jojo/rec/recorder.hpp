#ifndef JOJO_REC_RECORDER_HPP_
#define JOJO_REC_RECORDER_HPP_

#include <filesystem>
#include <memory>
#include <string>

#include "jojo/rec/record_types.hpp"

namespace jojo::rec::internal {
class RecorderWorker;
}

namespace jojo::rec {

/// @brief 带内部多队列和专用写线程的单次录制写入器。
class Recorder {
 public:
  /// @brief 创建一个以 `config.output_root` 为根目录的录制器。
  /// @param config 在构造时复制并冻结的录制配置。
  /// @param error 构造失败时可选的错误输出字符串。
  explicit Recorder(const RecorderConfig& config, std::string* error = nullptr);

  /// @brief 析构时尽力执行一次关闭流程。
  ~Recorder();

  /// @brief 禁止拷贝构造。
  Recorder(const Recorder&) = delete;

  /// @brief 禁止拷贝赋值。
  Recorder& operator=(const Recorder&) = delete;

  /// @brief 允许移动构造。
  Recorder(Recorder&& other) noexcept;

  /// @brief 允许移动赋值。
  Recorder& operator=(Recorder&& other) noexcept;

  /// @brief 返回录制器是否仍可接受新消息。
  /// @return 当录制器可用且没有致命错误时返回 true。
  bool IsOpen() const;

  /// @brief 返回冻结后的录制配置。
  /// @return 不可变的录制配置引用。
  const RecorderConfig& Config() const;

  /// @brief 返回当前活动或最终录制目录路径。
  /// @return 当前对外暴露的录制目录路径。
  std::filesystem::path RecordingPath() const;

  /// @brief 向缓冲追加一条消息。
  /// @param message 由调用方持有、录制器复制的消息视图。
  /// @return 队列接收结果。
  AppendResult Append(const RecordedMessage& message);

  /// @brief 请求写线程执行一次 manifest 刷新或 durability checkpoint。
  /// @param error 失败时可选的错误输出字符串。
  /// @return 成功时返回 true。
  bool Flush(std::string* error = nullptr);

  /// @brief 关闭录制器、完成最终 manifest 并发布最终目录。
  /// @param error 失败时可选的错误输出字符串。
  /// @return 成功时返回 true。
  bool Close(std::string* error = nullptr);

 private:
  RecorderConfig config_;
  std::unique_ptr<internal::RecorderWorker> worker_;
};

}  // namespace jojo::rec

#endif  // JOJO_REC_RECORDER_HPP_
