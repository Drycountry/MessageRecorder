#ifndef JOJO_REC_DETAIL_RECORDER_STORAGE_HPP_
#define JOJO_REC_DETAIL_RECORDER_STORAGE_HPP_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>

#include "jojo/rec/detail/file_util.hpp"
#include "jojo/rec/detail/manifest.hpp"
#include "jojo/rec/detail/segment.hpp"
#include "jojo/rec/record_types.hpp"

namespace jojo::rec::internal {

/// @brief 负责维护 Recorder 的 segment、manifest 和 durability 状态。
class RecorderStorage {
 public:
  RecorderStorage() = default;
  RecorderStorage(RecorderStorage&&) noexcept = default;
  RecorderStorage& operator=(RecorderStorage&&) noexcept = default;
  RecorderStorage(const RecorderStorage&) = delete;
  RecorderStorage& operator=(const RecorderStorage&) = delete;

  /// @brief 初始化活动录制目录和初始 manifest。
  bool Initialize(const std::filesystem::path& output_root, const std::string& recording_label,
                  const std::map<std::uint32_t, std::string>& message_type_names, std::string* error);
  /// @brief 返回当前活动或最终录制目录路径。
  const std::filesystem::path& recording_path() const;
  /// @brief 更新对外暴露的录制目录路径。
  void SetRecordingPath(std::filesystem::path recording_path);
  /// @brief 返回内部维护的 manifest 状态。
  const ManifestData& manifest() const;
  /// @brief 为录制标记一次降级。
  void MarkDegraded();
  /// @brief 记录一次生产者在拷贝阶段中止的条目。
  void HandleAbortedEntry();
  /// @brief 生成包含活动 segment 摘要的 manifest 快照。
  ManifestData BuildManifestSnapshot() const;
  /// @brief 将当前 manifest 快照写回录制目录。
  bool FlushManifestSnapshot(std::string* error);
  /// @brief 判断显式 `Flush()` 是否需要执行 durability checkpoint。
  bool ShouldSyncOnExplicitFlush() const;
  /// @brief 判断关闭流程是否需要执行 durability checkpoint。
  bool ShouldSyncOnClose() const;
  /// @brief 执行一次完整 durability checkpoint。
  bool PerformDurabilityCheckpoint(std::string* error);
  /// @brief 按当前 segment 规则写入一条完整记录。
  bool WriteRecord(const ReplayMessage& message, std::uint64_t segment_max_bytes,
                   std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                   std::size_t sparse_index_max_bytes, std::string* error);
  /// @brief 在关闭前 finalize 当前活动 segment。
  void FinishCurrentSegment(std::string* error);
  /// @brief 标记录制正常结束时间并准备最终 manifest。
  void MarkClosedAt(std::chrono::system_clock::time_point time_point);
  /// @brief 生成正常关闭后的最终录制目录路径。
  std::filesystem::path MakeFinalizedPath(const std::filesystem::path& output_root) const;
  /// @brief 清空所有内部状态。
  void Reset();
  /// @brief 配置显式 flush 所需的 fsync 策略参数。
  void SetFlushPolicy(FsyncPolicy policy, std::uint64_t flush_interval_ms);

 private:
  /// @brief 将 flush 间隔限制到 `milliseconds` 可表示范围。
  static std::chrono::milliseconds ClampFlushInterval(std::uint64_t flush_interval_ms);
  /// @brief 确保当前存在一个打开中的可写 segment。
  bool OpenCurrentSegment(std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                          std::size_t sparse_index_max_bytes, std::string* error);
  /// @brief 刷新当前活动 segment 的用户态流缓冲。
  bool FlushOpenSegmentStream(std::string* error);
  /// @brief 同步自上次 checkpoint 以来新 finalize 的所有 segment 文件。
  bool SyncUnsyncedFinalizedSegments(std::string* error);
  /// @brief 同步仍处于打开状态的当前 segment 文件。
  bool SyncCurrentSegmentFile(std::string* error);
  /// @brief 同步 manifest 文件及其所在目录元数据。
  bool SyncManifestSnapshot(std::string* error);
  /// @brief 在 checkpoint 成功后更新同步状态基线。
  void MarkDurabilityCheckpointComplete();
  /// @brief 必要时 finalize 当前 segment 并轮转到新文件。
  bool RotateIfNeeded(const ReplayMessage& message, std::uint64_t segment_max_bytes,
                      std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                      std::size_t sparse_index_max_bytes, std::string* error);

  std::filesystem::path recording_path_;
  ManifestData manifest_;
  SegmentWriteContext current_segment_;
  bool has_current_segment_ = false;
  std::size_t synced_segment_count_ = 0;
  bool current_segment_dirty_ = false;
  bool manifest_dirty_ = false;
  std::chrono::steady_clock::time_point last_sync_time_ = std::chrono::steady_clock::time_point::min();
  FsyncPolicy fsync_policy_ = FsyncPolicy::kNever;
  std::uint64_t flush_interval_ms_ = 0;
};

}  // namespace jojo::rec::internal

#endif  // JOJO_REC_DETAIL_RECORDER_STORAGE_HPP_
