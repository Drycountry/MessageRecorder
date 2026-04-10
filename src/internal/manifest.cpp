#include <cstdint>
#include <nlohmann/json.hpp>
#include <sstream>

#include "internal/internal.hpp"

namespace jojo::rec::internal {
namespace {

using JsonValue = nlohmann::json;

/// @brief 在 JSON 对象中查找指定键对应的值。
const JsonValue* FindObjectItem(const JsonValue& object, const std::string& key) {
  const auto it = object.find(key);
  return it == object.end() ? nullptr : &(*it);
}

/// @brief 从 manifest JSON 对象中读取必填字符串字段。
bool ReadString(const JsonValue& object, const std::string& key, std::string* value, std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr || !item->is_string()) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a string";
    }
    return false;
  }
  *value = item->get<std::string>();
  return true;
}

/// @brief 从 manifest JSON 对象中读取可空字符串字段。
bool ReadOptionalString(const JsonValue& object, const std::string& key, std::optional<std::string>* value,
                        std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr || item->is_null()) {
    *value = std::nullopt;
    return true;
  }
  if (!item->is_string()) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a string or null";
    }
    return false;
  }
  *value = item->get<std::string>();
  return true;
}

/// @brief 从 manifest JSON 对象中读取布尔字段。
bool ReadBool(const JsonValue& object, const std::string& key, bool* value, std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr || !item->is_boolean()) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a boolean";
    }
    return false;
  }
  *value = item->get<bool>();
  return true;
}

/// @brief 将一个 JSON 数值节点解析为非负的 `std::uint64_t`。
bool ReadNumberValue(const JsonValue& item, const std::string& field_name, std::uint64_t* value, std::string* error) {
  if (item.is_number_unsigned()) {
    *value = item.get<std::uint64_t>();
    return true;
  }
  if (item.is_number_integer()) {
    const std::int64_t signed_value = item.get<std::int64_t>();
    if (signed_value < 0) {
      if (error != nullptr) {
        *error = "manifest field '" + field_name + "' must be non-negative";
      }
      return false;
    }
    *value = static_cast<std::uint64_t>(signed_value);
    return true;
  }
  if (error != nullptr) {
    *error = "manifest field '" + field_name + "' must be a number";
  }
  return false;
}

/// @brief 从 manifest JSON 对象中读取必填非负数值字段。
bool ReadNumber(const JsonValue& object, const std::string& key, std::uint64_t* value, std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a number";
    }
    return false;
  }
  return ReadNumberValue(*item, key, value, error);
}

/// @brief 将类型 ID 到名称的映射序列化为 JSON 对象。
JsonValue SerializeTypeMap(const std::map<std::uint32_t, std::string>& values) {
  JsonValue object = JsonValue::object();
  for (const auto& item : values) {
    object[std::to_string(item.first)] = item.second;
  }
  return object;
}

/// @brief 将 manifest 中的类型字典解析为内部映射。
bool ParseTypeMap(const JsonValue& value, std::map<std::uint32_t, std::string>* out, std::string* error) {
  if (!value.is_object()) {
    if (error != nullptr) {
      *error = "manifest dictionary must be an object";
    }
    return false;
  }
  out->clear();
  for (auto it = value.begin(); it != value.end(); ++it) {
    if (!it.value().is_string()) {
      if (error != nullptr) {
        *error = "manifest dictionary values must be strings";
      }
      return false;
    }
    (*out)[static_cast<std::uint32_t>(std::stoul(it.key()))] = it.value().get<std::string>();
  }
  return true;
}

/// @brief 将可选数字序列化为数字或 `null`。
JsonValue SerializeOptionalNumber(const std::optional<std::uint64_t>& value) {
  return value.has_value() ? JsonValue(*value) : JsonValue(nullptr);
}

/// @brief 从 manifest JSON 对象中读取可空非负数值字段。
bool ParseOptionalNumber(const JsonValue& object, const std::string& key, std::optional<std::uint64_t>* value,
                         std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr || item->is_null()) {
    *value = std::nullopt;
    return true;
  }
  if (!item->is_number_integer() && !item->is_number_unsigned()) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a non-negative number or null";
    }
    return false;
  }

  std::uint64_t parsed = 0;
  if (!ReadNumberValue(*item, key, &parsed, error)) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a non-negative number or null";
    }
    return false;
  }
  *value = parsed;
  return true;
}

/// @brief 将单个 segment 摘要序列化为 manifest JSON 条目。
JsonValue SerializeSegment(const SegmentSummary& segment) {
  return JsonValue{{"segment_index", segment.segment_index},
                   {"file_name", segment.file_name},
                   {"record_count", segment.record_count},
                   {"first_record_seq", SerializeOptionalNumber(segment.first_record_seq)},
                   {"last_record_seq", SerializeOptionalNumber(segment.last_record_seq)},
                   {"first_event_mono_ts_us", SerializeOptionalNumber(segment.first_event_mono_ts_us)},
                   {"last_event_mono_ts_us", SerializeOptionalNumber(segment.last_event_mono_ts_us)},
                   {"first_event_utc_ts_us", SerializeOptionalNumber(segment.first_event_utc_ts_us)},
                   {"last_event_utc_ts_us", SerializeOptionalNumber(segment.last_event_utc_ts_us)},
                   {"file_size_bytes", segment.file_size_bytes},
                   {"valid_bytes", segment.valid_bytes},
                   {"has_footer", segment.has_footer}};
}

/// @brief 将一个 manifest segment 条目解析为内部摘要。
bool ParseSegment(const JsonValue& value, SegmentSummary* segment, std::string* error) {
  if (!value.is_object()) {
    if (error != nullptr) {
      *error = "manifest segment entry must be an object";
    }
    return false;
  }
  std::uint64_t number = 0;
  if (!ReadNumber(value, "segment_index", &number, error)) {
    return false;
  }
  segment->segment_index = static_cast<std::uint32_t>(number);
  if (!ReadString(value, "file_name", &segment->file_name, error) ||
      !ReadNumber(value, "record_count", &segment->record_count, error) ||
      !ParseOptionalNumber(value, "first_record_seq", &segment->first_record_seq, error) ||
      !ParseOptionalNumber(value, "last_record_seq", &segment->last_record_seq, error) ||
      !ParseOptionalNumber(value, "first_event_mono_ts_us", &segment->first_event_mono_ts_us, error) ||
      !ParseOptionalNumber(value, "last_event_mono_ts_us", &segment->last_event_mono_ts_us, error) ||
      !ParseOptionalNumber(value, "first_event_utc_ts_us", &segment->first_event_utc_ts_us, error) ||
      !ParseOptionalNumber(value, "last_event_utc_ts_us", &segment->last_event_utc_ts_us, error) ||
      !ReadNumber(value, "file_size_bytes", &segment->file_size_bytes, error) ||
      !ReadNumber(value, "valid_bytes", &segment->valid_bytes, error) ||
      !ReadBool(value, "has_footer", &segment->has_footer, error)) {
    return false;
  }
  return true;
}

/// @brief 将内部 manifest 模型转换为 JSON 树。
JsonValue ManifestToJson(const ManifestData& manifest) {
  JsonValue root = JsonValue::object();
  root["format_version"] = manifest.format_version;
  root["start_utc"] = manifest.start_utc;
  root["stop_utc"] = manifest.stop_utc.has_value() ? JsonValue(*manifest.stop_utc) : JsonValue(nullptr);
  root["incomplete"] = manifest.incomplete;
  root["degraded"] = manifest.degraded;
  root["aborted_entries"] = manifest.aborted_entries;
  root["total_records"] = manifest.total_records;
  root["total_payload_bytes"] = manifest.total_payload_bytes;
  root["recording_label"] = manifest.recording_label;
  root["message_type_names"] = SerializeTypeMap(manifest.message_type_names);

  JsonValue segments = JsonValue::array();
  for (const SegmentSummary& segment : manifest.segments) {
    segments.push_back(SerializeSegment(segment));
  }
  root["segments"] = std::move(segments);
  return root;
}

/// @brief 将 JSON 根节点解析为内部 manifest 模型。
bool ManifestFromJson(const JsonValue& root, ManifestData* manifest, std::string* error) {
  if (!root.is_object()) {
    if (error != nullptr) {
      *error = "manifest root must be a JSON object";
    }
    return false;
  }

  std::uint64_t format_version = 0;
  if (!ReadNumber(root, "format_version", &format_version, error)) {
    return false;
  }
  manifest->format_version = static_cast<std::uint32_t>(format_version);
  if (!ReadString(root, "start_utc", &manifest->start_utc, error) ||
      !ReadOptionalString(root, "stop_utc", &manifest->stop_utc, error) ||
      !ReadBool(root, "incomplete", &manifest->incomplete, error) ||
      !ReadBool(root, "degraded", &manifest->degraded, error) ||
      !ReadNumber(root, "aborted_entries", &manifest->aborted_entries, error) ||
      !ReadNumber(root, "total_records", &manifest->total_records, error) ||
      !ReadNumber(root, "total_payload_bytes", &manifest->total_payload_bytes, error) ||
      !ReadString(root, "recording_label", &manifest->recording_label, error)) {
    return false;
  }

  const JsonValue* types = FindObjectItem(root, "message_type_names");
  const JsonValue* segments = FindObjectItem(root, "segments");
  if (types == nullptr || segments == nullptr) {
    if (error != nullptr) {
      *error = "manifest dictionary or segments field missing";
    }
    return false;
  }
  if (!ParseTypeMap(*types, &manifest->message_type_names, error)) {
    return false;
  }
  if (!segments->is_array()) {
    if (error != nullptr) {
      *error = "manifest segments field must be an array";
    }
    return false;
  }

  manifest->segments.clear();
  for (const JsonValue& item : *segments) {
    SegmentSummary segment;
    if (!ParseSegment(item, &segment, error)) {
      return false;
    }
    manifest->segments.push_back(segment);
  }
  return true;
}

}  // namespace

/// @brief 以临时文件替换的方式写出 manifest。
bool WriteManifest(const std::filesystem::path& recording_path, const ManifestData& manifest, std::string* error) {
  const std::filesystem::path manifest_path = recording_path / "manifest.json";
  const std::filesystem::path temp_path = recording_path / "manifest.json.tmp";
  std::string text = ManifestToJson(manifest).dump(2);
  text.push_back('\n');
  if (!WriteTextFileUtf8NoBom(temp_path, text, error)) {
    return false;
  }

  // manifest 更新在文件级别保持原子性，因此 verify 可以保持只读。
  return RenameWithReplace(temp_path, manifest_path, error);
}

/// @brief 从磁盘读取并解析 manifest 文件。
bool LoadManifest(const std::filesystem::path& recording_path, ManifestData* manifest, std::string* error) {
  std::string text;
  if (!ReadTextFileUtf8(recording_path / "manifest.json", &text, error)) {
    return false;
  }

  JsonValue root;
  try {
    root = JsonValue::parse(text);
  } catch (const std::exception& ex) {
    if (error != nullptr) {
      *error = std::string("failed to parse manifest JSON: ") + ex.what();
    }
    return false;
  }
  return ManifestFromJson(root, manifest, error);
}

/// @brief 将内部 manifest 转换为公共摘要结构。
RecordingSummary ToRecordingSummary(const std::filesystem::path& recording_path, const ManifestData& manifest) {
  RecordingSummary summary;
  summary.recording_path = recording_path;
  summary.start_utc = manifest.start_utc;
  summary.stop_utc = manifest.stop_utc;
  summary.incomplete = manifest.incomplete;
  summary.degraded = manifest.degraded;
  summary.aborted_entries = manifest.aborted_entries;
  summary.total_records = manifest.total_records;
  summary.total_payload_bytes = manifest.total_payload_bytes;
  summary.recording_label = manifest.recording_label;
  summary.message_type_names = manifest.message_type_names;
  summary.segments = manifest.segments;
  return summary;
}

}  // namespace jojo::rec::internal
