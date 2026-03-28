#include "internal/internal.hpp"

#include <nlohmann/json.hpp>

#include <cstdint>
#include <sstream>

namespace jojo::rec::internal {
namespace {

using JsonValue = nlohmann::json;

const JsonValue* FindObjectItem(const JsonValue& object, const std::string& key) {
  const auto it = object.find(key);
  return it == object.end() ? nullptr : &(*it);
}

bool ReadString(const JsonValue& object,
                const std::string& key,
                std::string* value,
                std::string* error) {
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

bool ReadOptionalString(const JsonValue& object,
                        const std::string& key,
                        std::optional<std::string>* value,
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

bool ReadBool(const JsonValue& object,
              const std::string& key,
              bool* value,
              std::string* error) {
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

bool ReadNumberValue(const JsonValue& item,
                     const std::string& field_name,
                     std::uint64_t* value,
                     std::string* error) {
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

bool ReadNumber(const JsonValue& object,
                const std::string& key,
                std::uint64_t* value,
                std::string* error) {
  const JsonValue* item = FindObjectItem(object, key);
  if (item == nullptr) {
    if (error != nullptr) {
      *error = "manifest field '" + key + "' must be a number";
    }
    return false;
  }
  return ReadNumberValue(*item, key, value, error);
}

JsonValue SerializeTypeMap(const std::map<std::uint32_t, std::string>& values) {
  JsonValue object = JsonValue::object();
  for (const auto& item : values) {
    object[std::to_string(item.first)] = item.second;
  }
  return object;
}

bool ParseTypeMap(const JsonValue& value,
                  std::map<std::uint32_t, std::string>* out,
                  std::string* error) {
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

JsonValue SerializeOptionalNumber(const std::optional<std::uint64_t>& value) {
  return value.has_value() ? JsonValue(*value) : JsonValue(nullptr);
}

bool ParseOptionalNumber(const JsonValue& object,
                         const std::string& key,
                         std::optional<std::uint64_t>* value,
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

JsonValue SerializeSegment(const SegmentSummary& segment) {
  return JsonValue{{"segment_index", segment.segment_index},
                   {"file_name", segment.file_name},
                   {"record_count", segment.record_count},
                   {"first_record_seq", SerializeOptionalNumber(segment.first_record_seq)},
                   {"last_record_seq", SerializeOptionalNumber(segment.last_record_seq)},
                   {"first_event_mono_ts_us",
                    SerializeOptionalNumber(segment.first_event_mono_ts_us)},
                   {"last_event_mono_ts_us",
                    SerializeOptionalNumber(segment.last_event_mono_ts_us)},
                   {"first_event_utc_ts_us",
                    SerializeOptionalNumber(segment.first_event_utc_ts_us)},
                   {"last_event_utc_ts_us",
                    SerializeOptionalNumber(segment.last_event_utc_ts_us)},
                   {"file_size_bytes", segment.file_size_bytes},
                   {"valid_bytes", segment.valid_bytes},
                   {"has_footer", segment.has_footer}};
}

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
      !ParseOptionalNumber(value, "first_event_mono_ts_us", &segment->first_event_mono_ts_us,
                           error) ||
      !ParseOptionalNumber(value, "last_event_mono_ts_us", &segment->last_event_mono_ts_us,
                           error) ||
      !ParseOptionalNumber(value, "first_event_utc_ts_us", &segment->first_event_utc_ts_us,
                           error) ||
      !ParseOptionalNumber(value, "last_event_utc_ts_us", &segment->last_event_utc_ts_us,
                           error) ||
      !ReadNumber(value, "file_size_bytes", &segment->file_size_bytes, error) ||
      !ReadNumber(value, "valid_bytes", &segment->valid_bytes, error) ||
      !ReadBool(value, "has_footer", &segment->has_footer, error)) {
    return false;
  }
  return true;
}

JsonValue ManifestToJson(const ManifestData& manifest) {
  JsonValue root = JsonValue::object();
  root["format_version"] = manifest.format_version;
  root["start_utc"] = manifest.start_utc;
  root["stop_utc"] = manifest.stop_utc.has_value() ? JsonValue(*manifest.stop_utc)
                                                     : JsonValue(nullptr);
  root["incomplete"] = manifest.incomplete;
  root["degraded"] = manifest.degraded;
  root["aborted_entries"] = manifest.aborted_entries;
  root["total_records"] = manifest.total_records;
  root["total_payload_bytes"] = manifest.total_payload_bytes;
  root["total_attributes_bytes"] = manifest.total_attributes_bytes;
  root["recording_label"] = manifest.recording_label;
  root["message_type_names"] = SerializeTypeMap(manifest.message_type_names);

  JsonValue segments = JsonValue::array();
  for (const SegmentSummary& segment : manifest.segments) {
    segments.push_back(SerializeSegment(segment));
  }
  root["segments"] = std::move(segments);
  return root;
}

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
      !ReadNumber(root, "total_attributes_bytes", &manifest->total_attributes_bytes, error) ||
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

}  // 匿名命名空间

bool WriteManifest(const std::filesystem::path& recording_path,
                   const ManifestData& manifest,
                   std::string* error) {
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

bool LoadManifest(const std::filesystem::path& recording_path,
                  ManifestData* manifest,
                  std::string* error) {
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

RecordingSummary ToRecordingSummary(const std::filesystem::path& recording_path,
                                    const ManifestData& manifest) {
  RecordingSummary summary;
  summary.recording_path = recording_path;
  summary.start_utc = manifest.start_utc;
  summary.stop_utc = manifest.stop_utc;
  summary.incomplete = manifest.incomplete;
  summary.degraded = manifest.degraded;
  summary.aborted_entries = manifest.aborted_entries;
  summary.total_records = manifest.total_records;
  summary.total_payload_bytes = manifest.total_payload_bytes;
  summary.total_attributes_bytes = manifest.total_attributes_bytes;
  summary.recording_label = manifest.recording_label;
  summary.message_type_names = manifest.message_type_names;
  summary.segments = manifest.segments;
  return summary;
}

}  // jojo::rec::internal 命名空间