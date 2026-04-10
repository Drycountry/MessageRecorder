#include "jojo/rec/inspect.hpp"

#include <algorithm>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "jojo/rec/detail/file_util.hpp"
#include "jojo/rec/detail/manifest.hpp"
#include "jojo/rec/detail/segment.hpp"

namespace jojo::rec {
namespace {

/// @brief 判断字符串是否以前缀开头。
bool StartsWith(const std::string& text, const std::string& prefix) {
  return text.size() >= prefix.size() && text.compare(0, prefix.size(), prefix) == 0;
}

/// @brief 在提供错误输出缓冲时写入错误消息。
void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

/// @brief 列出录制目录中按名称排序的 segment 文件。
bool ListSegmentFiles(const std::filesystem::path& recording_path, std::vector<std::filesystem::path>* files,
                      std::string* error) {
  files->clear();
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(recording_path, ec)) {
    if (ec) {
      SetError("failed to enumerate recording directory: " + ec.message(), error);
      return false;
    }
    if (!entry.is_regular_file()) {
      continue;
    }
    const std::string file_name = entry.path().filename().string();
    if (StartsWith(file_name, "segment-") && entry.path().extension() == ".seg") {
      files->push_back(entry.path());
    }
  }
  std::sort(files->begin(), files->end());
  return true;
}

/// @brief 按文件名建立 manifest 中 segment 摘要的查找表。
std::map<std::string, SegmentSummary> MakeManifestSegmentMap(const internal::ManifestData& manifest) {
  std::map<std::string, SegmentSummary> result;
  for (const SegmentSummary& segment : manifest.segments) {
    result[segment.file_name] = segment;
  }
  return result;
}

/// @brief 基于磁盘上的 segment 文件重建 manifest 统计和摘要。
bool RebuildFromDisk(const std::filesystem::path& recording_path, const internal::ManifestData& base_manifest,
                     internal::ManifestData* rebuilt_manifest, std::vector<std::string>* issues, std::string* error) {
  *rebuilt_manifest = base_manifest;
  rebuilt_manifest->segments.clear();
  rebuilt_manifest->total_records = 0;
  rebuilt_manifest->total_payload_bytes = 0;
  rebuilt_manifest->degraded = base_manifest.degraded;

  std::vector<std::filesystem::path> files;
  if (!ListSegmentFiles(recording_path, &files, error)) {
    return false;
  }

  const auto manifest_segments = MakeManifestSegmentMap(base_manifest);
  for (std::size_t index = 0; index < files.size(); ++index) {
    internal::SegmentScanOptions options;
    auto manifest_it = manifest_segments.find(files[index].filename().string());
    if (manifest_it != manifest_segments.end()) {
      options.max_data_bytes = manifest_it->second.valid_bytes;
    }

    internal::SegmentScanResult scan_result;
    if (!internal::ScanSegment(files[index], options, &scan_result, error)) {
      return false;
    }
    SegmentSummary summary = scan_result.summary;
    summary.segment_index = static_cast<std::uint32_t>(index);
    summary.file_name = files[index].filename().string();
    rebuilt_manifest->segments.push_back(summary);
    rebuilt_manifest->total_records += summary.record_count;
    rebuilt_manifest->total_payload_bytes += scan_result.total_payload_bytes;

    if (!scan_result.issues.empty()) {
      rebuilt_manifest->degraded = true;
      issues->insert(issues->end(), scan_result.issues.begin(), scan_result.issues.end());
    }
  }

  for (const auto& item : manifest_segments) {
    const bool still_exists = std::any_of(files.begin(), files.end(),
                                          [&](const auto& path) { return path.filename().string() == item.first; });
    if (!still_exists) {
      issues->push_back("manifest references missing segment '" + item.first + "'");
      rebuilt_manifest->degraded = true;
    }
  }
  return true;
}

/// @brief 对比 manifest 声明与磁盘重建结果之间的差异。
void CompareManifestToDisk(const internal::ManifestData& manifest, const internal::ManifestData& rebuilt,
                           std::vector<std::string>* issues) {
  if (manifest.segments.size() != rebuilt.segments.size()) {
    issues->push_back("manifest segment count differs from on-disk segments");
  }
  const std::size_t common_size = std::min(manifest.segments.size(), rebuilt.segments.size());
  for (std::size_t index = 0; index < common_size; ++index) {
    const SegmentSummary& expected = manifest.segments[index];
    const SegmentSummary& actual = rebuilt.segments[index];
    if (expected.file_name != actual.file_name) {
      issues->push_back("segment order differs for entry " + std::to_string(index));
    }
    if (expected.valid_bytes != actual.valid_bytes) {
      issues->push_back("segment valid_bytes mismatch for '" + actual.file_name + "'");
    }
    if (expected.record_count != actual.record_count) {
      issues->push_back("segment record_count mismatch for '" + actual.file_name + "'");
    }
    if (expected.has_footer != actual.has_footer) {
      issues->push_back("segment footer state mismatch for '" + actual.file_name + "'");
    }
  }
}

/// @brief 按 manifest 顺序加载所有可回放记录及其 segment 索引。
bool LoadAllRecords(const std::filesystem::path& recording_path, const internal::ManifestData& manifest,
                    std::vector<ReplayMessage>* records, std::vector<std::uint32_t>* segment_indexes,
                    std::string* error) {
  records->clear();
  segment_indexes->clear();
  for (const SegmentSummary& segment : manifest.segments) {
    internal::SegmentScanOptions options;
    options.load_payloads = true;
    options.max_data_bytes = segment.valid_bytes;
    internal::SegmentScanResult scan_result;
    if (!internal::ScanSegment(recording_path / segment.file_name, options, &scan_result, error)) {
      return false;
    }
    for (ReplayMessage& record : scan_result.records) {
      records->push_back(std::move(record));
      segment_indexes->push_back(segment.segment_index);
    }
  }
  return true;
}

}  // namespace

/// @brief 从 manifest 读取录制摘要。
bool LoadRecordingSummary(const std::filesystem::path& recording_path, RecordingSummary* summary, std::string* error) {
  internal::ManifestData manifest;
  if (!internal::LoadManifest(recording_path, &manifest, error)) {
    return false;
  }
  *summary = internal::ToRecordingSummary(recording_path, manifest);
  return true;
}

/// @brief 校验 manifest 与磁盘 segment 之间是否一致。
VerifyResult VerifyRecording(const std::filesystem::path& recording_path) {
  VerifyResult result;
  internal::ManifestData manifest;
  std::string error;
  if (!internal::LoadManifest(recording_path, &manifest, &error)) {
    result.issues.push_back(error);
    return result;
  }

  internal::ManifestData rebuilt;
  if (!RebuildFromDisk(recording_path, manifest, &rebuilt, &result.issues, &error)) {
    result.issues.push_back(error);
    return result;
  }

  CompareManifestToDisk(manifest, rebuilt, &result.issues);
  result.summary = internal::ToRecordingSummary(recording_path, manifest);
  result.degraded = manifest.degraded || manifest.incomplete || !result.issues.empty();
  result.success = true;
  return result;
}

/// @brief 用磁盘扫描结果重写 manifest，以修复元数据漂移。
VerifyResult RepairRecording(const std::filesystem::path& recording_path) {
  VerifyResult result;
  internal::ManifestData manifest;
  std::string error;
  if (!internal::LoadManifest(recording_path, &manifest, &error)) {
    result.issues.push_back(error);
    return result;
  }

  internal::ManifestData rebuilt;
  if (!RebuildFromDisk(recording_path, manifest, &rebuilt, &result.issues, &error)) {
    result.issues.push_back(error);
    return result;
  }
  rebuilt.degraded = rebuilt.degraded || rebuilt.incomplete || !result.issues.empty();

  if (!internal::WriteManifest(recording_path, rebuilt, &error)) {
    result.issues.push_back(error);
    return result;
  }
  if (!internal::SyncFile(recording_path / "manifest.json", &error) ||
      !internal::SyncDirectory(recording_path, &error)) {
    result.issues.push_back(error);
    return result;
  }

  result.summary = internal::ToRecordingSummary(recording_path, rebuilt);
  result.degraded = rebuilt.degraded;
  result.success = true;
  result.changed = true;
  return result;
}

/// @brief 从指定游标开始导出紧凑记录头摘要。
bool DumpRecording(const std::filesystem::path& recording_path, const ReplayCursor& cursor, std::size_t max_records,
                   std::vector<DumpEntry>* entries, std::string* error) {
  internal::ManifestData manifest;
  if (!internal::LoadManifest(recording_path, &manifest, error)) {
    return false;
  }

  std::vector<ReplayMessage> records;
  std::vector<std::uint32_t> segment_indexes;
  if (!LoadAllRecords(recording_path, manifest, &records, &segment_indexes, error)) {
    return false;
  }

  std::size_t start_index = 0;
  if (cursor.kind == ReplayCursorKind::kRecordSequence) {
    while (start_index < records.size() && records[start_index].record_seq < cursor.value) {
      ++start_index;
    }
  } else if (cursor.kind == ReplayCursorKind::kEventMonoTime) {
    while (start_index < records.size() && records[start_index].event_mono_ts_us < cursor.value) {
      ++start_index;
    }
  } else if (cursor.kind == ReplayCursorKind::kEventUtcTime) {
    while (start_index < records.size() && records[start_index].event_utc_ts_us < cursor.value) {
      ++start_index;
    }
  } else if (cursor.kind == ReplayCursorKind::kSegmentCheckpoint && cursor.segment_index.has_value()) {
    while (start_index < segment_indexes.size() && segment_indexes[start_index] < *cursor.segment_index) {
      ++start_index;
    }
  }
  if (start_index >= records.size()) {
    SetError("dump cursor is beyond end of recording", error);
    return false;
  }

  entries->clear();
  const std::size_t end_index = std::min(records.size(), start_index + max_records);
  for (std::size_t index = start_index; index < end_index; ++index) {
    const ReplayMessage& record = records[index];
    entries->push_back(DumpEntry{record.record_seq, record.event_mono_ts_us, record.event_utc_ts_us,
                                 record.session_id, record.message_type,
                                 static_cast<std::uint32_t>(record.payload.size())});
  }
  return true;
}

}  // namespace jojo::rec
