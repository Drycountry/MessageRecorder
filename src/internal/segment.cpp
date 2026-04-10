#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>

#include "internal/internal.hpp"

namespace jojo::rec::internal {
namespace {

constexpr std::uint32_t kRecordMagic = 0x31524543U;
constexpr std::uint32_t kFooterMagic = 0x31544653U;
constexpr std::uint16_t kCurrentSegmentVersion = 4;
constexpr std::uint16_t kRecordHeaderSize = 48;
constexpr std::size_t kIndexEntrySize = 32;
constexpr std::size_t kFooterSize = 24;

struct FooterProbeResult {
  bool found_magic = false;
  bool valid = false;
  std::uint64_t index_offset = 0;
  std::uint64_t index_count = 0;
};

/// @brief 以小端字节序向缓冲区追加 16 位无符号整数。
void AppendU16(std::vector<std::uint8_t>* buffer, std::uint16_t value) {
  buffer->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  buffer->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

/// @brief 以小端字节序向缓冲区追加 32 位无符号整数。
void AppendU32(std::vector<std::uint8_t>* buffer, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    buffer->push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFU));
  }
}

/// @brief 以小端字节序向缓冲区追加 64 位无符号整数。
void AppendU64(std::vector<std::uint8_t>* buffer, std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    buffer->push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFU));
  }
}

/// @brief 从字节缓冲区指定偏移按小端读取 16 位无符号整数。
std::uint16_t ReadU16(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  return static_cast<std::uint16_t>(buffer[offset]) | (static_cast<std::uint16_t>(buffer[offset + 1]) << 8U);
}

/// @brief 从字节缓冲区指定偏移按小端读取 32 位无符号整数。
std::uint32_t ReadU32(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(buffer[offset + index]) << (index * 8);
  }
  return value;
}

/// @brief 从字节缓冲区指定偏移按小端读取 64 位无符号整数。
std::uint64_t ReadU64(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(buffer[offset + index]) << (index * 8);
  }
  return value;
}

/// @brief 为给定序号生成稳定的 segment 文件名。
std::string FormatSegmentFileName(std::uint32_t segment_index) {
  std::ostringstream stream;
  stream << "segment-";
  stream.width(6);
  stream.fill('0');
  stream << segment_index << ".seg";
  return stream.str();
}

/// @brief 在文件尾尝试探测并校验当前格式 footer。
FooterProbeResult ProbeFooter(const std::vector<std::uint8_t>& bytes) {
  FooterProbeResult result;
  if (bytes.size() < kFooterSize) {
    return result;
  }

  const std::size_t footer_offset = bytes.size() - kFooterSize;
  if (ReadU32(bytes, footer_offset) != kFooterMagic) {
    return result;
  }

  result.found_magic = true;
  if (ReadU16(bytes, footer_offset + 4) != kCurrentSegmentVersion) {
    return result;
  }

  result.index_offset = ReadU64(bytes, footer_offset + 8);
  result.index_count = ReadU64(bytes, footer_offset + 16);
  if (result.index_count > std::numeric_limits<std::uint64_t>::max() / static_cast<std::uint64_t>(kIndexEntrySize)) {
    return result;
  }

  const std::uint64_t expected_index_bytes = result.index_count * kIndexEntrySize;
  if (result.index_offset > footer_offset || footer_offset - result.index_offset != expected_index_bytes) {
    return result;
  }

  result.valid = true;
  return result;
}

/// @brief 根据回放消息生成当前格式版本的记录头。
std::vector<std::uint8_t> BuildRecordHeader(const ReplayMessage& message) {
  std::vector<std::uint8_t> header;
  header.reserve(kRecordHeaderSize);
  AppendU32(&header, kRecordMagic);
  AppendU16(&header, kRecordHeaderSize);
  AppendU16(&header, 0);
  AppendU64(&header, message.record_seq);
  AppendU64(&header, message.event_mono_ts_us);
  AppendU64(&header, message.event_utc_ts_us);
  AppendU64(&header, message.session_id);
  AppendU32(&header, message.message_type);
  AppendU32(&header, static_cast<std::uint32_t>(message.payload.size()));
  return header;
}

/// @brief 将内存索引项编码为 footer 索引区条目。
std::vector<std::uint8_t> BuildIndexEntry(const SegmentIndexEntry& entry) {
  std::vector<std::uint8_t> bytes;
  bytes.reserve(kIndexEntrySize);
  AppendU64(&bytes, entry.record_seq);
  AppendU64(&bytes, entry.event_mono_ts_us);
  AppendU64(&bytes, entry.event_utc_ts_us);
  AppendU64(&bytes, entry.file_offset);
  return bytes;
}

/// @brief 生成指向稠密索引区的 segment footer。
std::vector<std::uint8_t> BuildFooter(std::uint64_t index_offset, std::uint64_t index_count) {
  std::vector<std::uint8_t> footer;
  footer.reserve(kFooterSize);
  AppendU32(&footer, kFooterMagic);
  AppendU16(&footer, kCurrentSegmentVersion);
  AppendU16(&footer, 0);
  AppendU64(&footer, index_offset);
  AppendU64(&footer, index_count);
  return footer;
}

/// @brief 将整个 segment 文件读入内存。
bool LoadFileBytes(const std::filesystem::path& path, std::vector<std::uint8_t>* bytes, std::string* error) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream.is_open()) {
    if (error != nullptr) {
      *error = "failed to open segment: " + path.string();
    }
    return false;
  }
  stream.seekg(0, std::ios::end);
  const std::streamoff size = stream.tellg();
  stream.seekg(0, std::ios::beg);
  if (size < 0) {
    if (error != nullptr) {
      *error = "failed to measure segment size: " + path.string();
    }
    return false;
  }
  bytes->resize(static_cast<std::size_t>(size));
  if (size > 0) {
    stream.read(reinterpret_cast<char*>(bytes->data()), size);
  }
  if (!stream.good() && !stream.eof()) {
    if (error != nullptr) {
      *error = "failed to read segment: " + path.string();
    }
    return false;
  }
  return true;
}

/// @brief 使用一条记录更新 segment 摘要边界和计数。
void UpdateSummaryForRecord(const ReplayMessage& record, SegmentSummary* summary) {
  if (summary->record_count == 0) {
    summary->first_record_seq = record.record_seq;
    summary->first_event_mono_ts_us = record.event_mono_ts_us;
    summary->first_event_utc_ts_us = record.event_utc_ts_us;
  }
  summary->last_record_seq = record.record_seq;
  summary->last_event_mono_ts_us = record.event_mono_ts_us;
  summary->last_event_utc_ts_us = record.event_utc_ts_us;
  ++summary->record_count;
}

}  // namespace

/// @brief 打开新的 segment 文件并初始化写上下文。
bool OpenSegment(const std::filesystem::path& recording_path, std::uint32_t segment_index, SegmentWriteContext* context,
                 std::string* error) {
  context->file_path = recording_path / FormatSegmentFileName(segment_index);
  context->stream = std::ofstream(context->file_path, std::ios::binary | std::ios::trunc);
  if (!context->stream.is_open()) {
    if (error != nullptr) {
      *error = "failed to open segment for write: " + context->file_path.string();
    }
    return false;
  }

  // 写线程同一时间只持有一个 segment，并在 finalize 前将 footer 和索引状态保留在内存中。
  context->index_entries.clear();
  context->summary = SegmentSummary{};
  context->summary.segment_index = segment_index;
  context->summary.file_name = context->file_path.filename().string();
  context->summary.file_size_bytes = 0;
  context->summary.valid_bytes = 0;
  context->summary.has_footer = false;
  context->data_size_bytes = 0;
  context->finalized = false;
  return true;
}

/// @brief 判断再写入一条记录后是否会超过 segment 上限。
bool ShouldRotateSegment(const SegmentWriteContext& context, std::uint64_t segment_max_bytes,
                         std::size_t next_record_size) {
  if (context.summary.record_count == 0) {
    return false;
  }
  return context.data_size_bytes + next_record_size > segment_max_bytes;
}

/// @brief 将一条消息写入 segment 数据区并维护内存索引。
bool WriteRecordToSegment(SegmentWriteContext* context, const ReplayMessage& message, std::string* error) {
  const std::vector<std::uint8_t> header = BuildRecordHeader(message);

  // 记录字节由专用写线程严格按照 `record_seq` 顺序追加。
  context->stream.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
  if (!message.payload.empty()) {
    context->stream.write(reinterpret_cast<const char*>(message.payload.data()),
                          static_cast<std::streamsize>(message.payload.size()));
  }
  if (!context->stream.good()) {
    if (error != nullptr) {
      *error = "failed to write segment data: " + context->file_path.string();
    }
    return false;
  }

  context->index_entries.push_back(SegmentIndexEntry{message.record_seq, message.event_mono_ts_us,
                                                     message.event_utc_ts_us, context->data_size_bytes});
  context->data_size_bytes += static_cast<std::uint64_t>(header.size() + message.payload.size());
  context->summary.valid_bytes = context->data_size_bytes;
  context->summary.file_size_bytes = context->data_size_bytes;
  UpdateSummaryForRecord(message, &context->summary);
  return true;
}

/// @brief 写出索引和 footer，完成一个 segment 的收尾。
bool FinalizeSegment(SegmentWriteContext* context, std::string* error) {
  if (context->finalized) {
    return true;
  }

  const std::uint64_t index_offset = context->data_size_bytes;
  for (const SegmentIndexEntry& entry : context->index_entries) {
    const std::vector<std::uint8_t> bytes = BuildIndexEntry(entry);
    context->stream.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
  }

  const std::vector<std::uint8_t> footer = BuildFooter(index_offset, context->index_entries.size());
  context->stream.write(reinterpret_cast<const char*>(footer.data()), static_cast<std::streamsize>(footer.size()));
  context->stream.flush();
  if (!context->stream.good()) {
    if (error != nullptr) {
      *error = "failed to finalize segment: " + context->file_path.string();
    }
    return false;
  }

  // 完成收尾的 segment 会暴露稠密 footer 索引，同时让 `valid_bytes` 仍指向记录数据区。
  context->summary.valid_bytes = index_offset;
  context->summary.file_size_bytes = index_offset + context->index_entries.size() * kIndexEntrySize + footer.size();
  context->summary.has_footer = true;
  context->stream.close();
  context->finalized = true;
  return true;
}

/// @brief 估算一条记录编码后的总字节数。
std::size_t EstimateRecordBytes(const RecordedMessage& message) {
  return kRecordHeaderSize + message.payload.size;
}

/// @brief 扫描 segment 文件并尽量恢复可读记录和摘要信息。
bool ScanSegment(const std::filesystem::path& segment_path, const SegmentScanOptions& options,
                 SegmentScanResult* result, std::string* error) {
  std::vector<std::uint8_t> bytes;
  if (!LoadFileBytes(segment_path, &bytes, error)) {
    return false;
  }

  *result = SegmentScanResult{};
  result->summary.file_name = segment_path.filename().string();
  result->summary.file_size_bytes = bytes.size();

  const FooterProbeResult footer = ProbeFooter(bytes);
  result->footer_present = footer.found_magic;
  if (footer.valid) {
    result->footer_valid = true;
    result->summary.has_footer = true;
    result->summary.valid_bytes = footer.index_offset;
  } else if (result->footer_present) {
    result->issues.push_back("segment footer is malformed");
  }

  std::uint64_t scan_limit = 0;
  if (result->footer_valid) {
    scan_limit = result->summary.valid_bytes;
  } else if (options.max_data_bytes.has_value()) {
    scan_limit = std::min<std::uint64_t>(*options.max_data_bytes, bytes.size());
  } else {
    scan_limit = bytes.size();
  }

  const std::uint64_t footer_valid_bytes = result->summary.valid_bytes;
  std::uint64_t offset = 0;
  while (offset < scan_limit) {
    if (scan_limit - offset < kRecordHeaderSize) {
      result->issues.push_back("truncated record header near byte " + std::to_string(offset));
      break;
    }
    const std::size_t header_offset = static_cast<std::size_t>(offset);
    if (ReadU32(bytes, header_offset) != kRecordMagic) {
      result->issues.push_back("record magic mismatch near byte " + std::to_string(offset));
      break;
    }

    const std::uint16_t header_size = ReadU16(bytes, header_offset + 4);
    if (header_size != kRecordHeaderSize) {
      result->issues.push_back("record header size mismatch near byte " + std::to_string(offset));
      break;
    }
    if (scan_limit - offset < header_size) {
      result->issues.push_back("truncated record header near byte " + std::to_string(offset));
      break;
    }

    const std::uint64_t record_seq = ReadU64(bytes, header_offset + 8);
    const std::uint64_t event_mono_ts_us = ReadU64(bytes, header_offset + 16);
    const std::uint64_t event_utc_ts_us = ReadU64(bytes, header_offset + 24);
    const std::uint64_t session_id = ReadU64(bytes, header_offset + 32);
    const std::uint32_t message_type = ReadU32(bytes, header_offset + 40);
    const std::uint32_t payload_size = ReadU32(bytes, header_offset + 44);
    const std::uint64_t total_size = static_cast<std::uint64_t>(header_size) + payload_size;
    if (offset + total_size > scan_limit) {
      result->issues.push_back("truncated record body near byte " + std::to_string(offset));
      break;
    }

    const std::size_t payload_offset = header_offset + header_size;

    ReplayMessage record;
    record.record_seq = record_seq;
    record.event_mono_ts_us = event_mono_ts_us;
    record.event_utc_ts_us = event_utc_ts_us;
    record.session_id = session_id;
    record.message_type = message_type;
    if (options.load_payloads) {
      record.payload.assign(bytes.begin() + payload_offset, bytes.begin() + payload_offset + payload_size);
      result->records.push_back(record);
    }

    result->total_payload_bytes += payload_size;
    UpdateSummaryForRecord(record, &result->summary);
    offset += total_size;
  }

  if (!result->summary.has_footer) {
    result->summary.valid_bytes = offset;
    result->summary.file_size_bytes = bytes.size();
  } else if (offset != footer_valid_bytes) {
    result->summary.valid_bytes = offset;
    result->issues.push_back("segment footer/index covered more bytes than readable records");
  } else {
    result->summary.valid_bytes = footer_valid_bytes;
  }
  return true;
}

}  // namespace jojo::rec::internal
