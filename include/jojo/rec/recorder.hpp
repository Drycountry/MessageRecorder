#ifndef JOJO_REC_RECORDER_HPP_
#define JOJO_REC_RECORDER_HPP_

#include <filesystem>
#include <memory>
#include <string>

#include "jojo/rec/types.hpp"

namespace jojo::rec {

class RecorderImpl;

/**
 * @brief 带内部多队列和专用写线程的单次录制写入器。
 */
class Recorder {
 public:
  /**
   * @brief 创建一个以 `config.output_root` 为根目录的录制器。
   * @param config 在构造时复制并冻结的不可变录制配置。
   * @param error 构造失败时可选的错误输出字符串。
   */
  explicit Recorder(const RecorderConfig& config, std::string* error = nullptr);

  /**
   * @brief 析构录制器，并尽力执行一次 `Close()`。
   */
  ~Recorder();

  Recorder(const Recorder&) = delete;
  Recorder& operator=(const Recorder&) = delete;
  Recorder(Recorder&&) noexcept;
  Recorder& operator=(Recorder&&) noexcept;

  /**
   * @brief 返回构造是否成功。
   * @return 当录制器可以接收消息时返回 true。
   */
  bool IsOpen() const;

  /**
   * @brief 返回冻结后的配置副本。
   * @return 不可变的录制配置。
   */
  const RecorderConfig& Config() const;

  /**
   * @brief 返回当前活动录制目录路径。
   * @return 当前录制目录路径。
   */
  std::filesystem::path RecordingPath() const;

  /**
   * @brief 向内部多队列缓冲追加一条消息。
   * @param message 由调用方持有、并由录制器复制的消息视图。
   * @return 队列接收结果。
   */
  AppendResult Append(const RecordedMessage& message);

  /**
   * @brief 刷新当前 segment 数据和 manifest 元数据。
   * @param error 失败时可选的错误输出字符串。
   * @return 成功时返回 true。
   */
  bool Flush(std::string* error = nullptr);

  /**
   * @brief 关闭录制器并完成录制目录的最终整理。
   * @param error 失败时可选的错误输出字符串。
   * @return 成功时返回 true。
   */
  bool Close(std::string* error = nullptr);

 private:
  /** @brief 持有实现对象的指针，用于隐藏线程与文件格式细节。 */
  std::unique_ptr<RecorderImpl> impl_;
};

}  // jojo::rec 命名空间

#endif  // JOJO_REC_RECORDER_HPP_ 头文件保护



