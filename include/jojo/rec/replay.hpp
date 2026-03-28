#ifndef JOJO_REC_REPLAY_HPP_
#define JOJO_REC_REPLAY_HPP_

#include <filesystem>
#include <memory>
#include <string>

#include "jojo/rec/types.hpp"

namespace jojo::rec {

/**
 * @brief 接收完整拥有消息的回放回调接口。
 */
class IReplayTarget {
 public:
  /**
   * @brief 虚析构函数。
   */
  virtual ~IReplayTarget() = default;

  /**
   * @brief 处理一条回放出来的消息。
   * @param message 完整拥有的回放消息。
   * @param error 目标端自定义失败信息的可选输出字符串。
   * @return 返回 true 继续回放，返回 false 表示失败并停止。
   */
  virtual bool OnMessage(const ReplayMessage& message, std::string* error) = 0;
};

/**
 * @brief 由内部工作线程驱动的异步回放控制器。
 */
class IReplaySession {
 public:
  /**
   * @brief 虚析构函数。
   */
  virtual ~IReplaySession() = default;

  /**
   * @brief 从会话当前游标位置启动回放。
   * @param error 启动失败时可选的错误输出字符串。
   * @return 工作线程启动成功时返回 true。
   */
  virtual bool Start(std::string* error = nullptr) = 0;

  /**
   * @brief 在当前回调返回后暂停回放。
   * @param error 状态切换非法时可选的错误输出字符串。
   * @return 会话进入暂停状态时返回 true。
   */
  virtual bool Pause(std::string* error = nullptr) = 0;

  /**
   * @brief 从暂停状态恢复回放。
   * @param error 状态切换非法时可选的错误输出字符串。
   * @return 恢复成功时返回 true。
   */
  virtual bool Resume(std::string* error = nullptr) = 0;

  /**
   * @brief 在会话运行过程中更新回放速率倍率。
   * @param speed 新的速率倍率；`1.0` 表示实时回放，`0.0` 表示尽快回放。
   * @param error 更新失败时可选的错误输出字符串。
   * @return 新速率被接受时返回 true。
   */
  virtual bool SetSpeed(double speed, std::string* error = nullptr) = 0;

  /**
   * @brief 使用 lower-bound 时间语义跳转到新的回放游标。
   * @param cursor 目标回放游标。
   * @param error 跳转失败时可选的错误输出字符串。
   * @return 新游标被接受时返回 true。
   */
  virtual bool Seek(const ReplayCursor& cursor, std::string* error = nullptr) = 0;

  /**
   * @brief 请求一次平滑停止。
   */
  virtual void Stop() = 0;

  /**
   * @brief 等待回放完成并获取最终结果。
   * @return 最终回放结果。
   */
  virtual ReplayResult Wait() = 0;
};

/**
 * @brief 创建一个异步回放会话。
 * @param recording_path 要回放的录制目录。
 * @param initial_cursor 初始回放位置。
 * @param options 回放节奏选项。
 * @param target 接收回放消息的目标对象。
 * @param error 构造失败时可选的错误输出字符串。
 * @return 成功时返回会话对象，否则返回 `nullptr`。
 */
std::unique_ptr<IReplaySession> CreateReplaySession(
    const std::filesystem::path& recording_path, const ReplayCursor& initial_cursor,
    const ReplayOptions& options, std::unique_ptr<IReplayTarget> target,
    std::string* error = nullptr);

}  // jojo::rec 命名空间

#endif  // JOJO_REC_REPLAY_HPP_ 头文件保护
