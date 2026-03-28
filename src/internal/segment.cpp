#include "internal/internal.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>

namespace jojo::rec::internal {
namespace {

constexpr std::uint32_t kRecordMagic = 0x31524543U;
constexpr std::uint32_t kFooterMagic = 0x31544653U;
constexpr std::uint16_t kSegmentVersionV1 = 1;
constexpr std::uint16_t kSegmentVersionV2 = 2;
constexpr std::uint16_t kSegmentVersionV3 = 3;
constexpr std::uint16_t kCurrentSegmentVersion = kSegmentVersionV3;
constexpr std::uint16_t kRecordHeaderSizeV1 = 72;
constexpr std::uint16_t kRecordHeaderSizeV2 = 68;
constexpr std::uint16_t kRecordHeaderSizeV3 = 64;
constexpr std::size_t kIndexEntrySize = 32;
constexpr std::size_t kFooterSizeV1 = 32;
constexpr std::size_t kFooterSizeV2 = 24;
constexpr std::uint32_t kHasStreamIdFlag = 1U;

struct FooterProbeResult {
  bool found_magic = false;
  bool valid = false;
  std::uint16_t version = 0;
  std::uint64_t index_offset = 0;
  std::uint64_t index_count = 0;
  std::size_t footer_size = 0;
};

void AppendU16(std::vector<std::uint8_t>* buffer, std::uint16_t value) {
  buffer->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  buffer->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void AppendU32(std::vector<std::uint8_t>* buffer, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    buffer->push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFU));
  }
}

void AppendU64(std::vector<std::uint8_t>* buffer, std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    buffer->push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFU));
  }
}

std::uint16_t ReadU16(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  return static_cast<std::uint16_t>(buffer[offset]) |
         (static_cast<std::uint16_t>(buffer[offset + 1]) << 8U);
}

std::uint32_t ReadU32(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(buffer[offset + index]) << (index * 8);
  }
  return value;
}

std::uint64_t ReadU64(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(buffer[offset + index]) << (index * 8);
  }
  return value;
}

std::string FormatSegmentFileName(std::uint32_t segment_index) {
  std::ostringstream stream;
  stream << "segment-";
  stream.width(6);
  stream.fill('0');
  stream << segment_index << ".seg";
  return stream.str();
}

std::uint16_t RecordHeaderSizeForVersion(std::uint16_t version) {
  switch (version) {
    case kSegmentVersionV1:
      return kRecordHeaderSizeV1;
    case kSegmentVersionV2:
      return kRecordHeaderSizeV2;
    case kSegmentVersionV3:
      return kRecordHeaderSizeV3;
    default:
      return 0;
  }
}

std::size_t FooterSizeForVersion(std::uint16_t version) {
  switch (version) {
    case kSegmentVersionV1:
      return kFooterSizeV1;
    case kSegmentVersionV2:
      return kFooterSizeV2;
    case kSegmentVersionV3:
      return kFooterSizeV2;
    default:
      return 0;
  }
}

FooterProbeResult ProbeFooter(const std::vector<std::uint8_t>& bytes, std::size_t footer_size) {
  FooterProbeResult result;
  if (bytes.size() < footer_size) {
    return result;
  }

  const std::size_t footer_offset = bytes.size() - footer_size;
  if (ReadU32(bytes, footer_offset) != kFooterMagic) {
    return result;
  }

  result.found_magic = true;
  result.version = ReadU16(bytes, footer_offset + 4);
  result.footer_size = footer_size;
  if (FooterSizeForVersion(result.version) != footer_size) {
    return result;
  }

  result.index_offset = ReadU64(bytes, footer_offset + 8);
  result.index_count = ReadU64(bytes, footer_offset + 16);
  if (result.index_count >
      std::numeric_limits<std::uint64_t>::max() / static_cast<std::uint64_t>(kIndexEntrySize)) {
    return result;
  }

  const std::uint64_t expected_index_bytes = result.index_count * kIndexEntrySize;
  if (result.index_offset > footer_offset ||
      footer_offset - result.index_offset != expected_index_bytes) {
    return result;
  }

  result.valid = true;
  return result;
}

std::vector<std::uint8_t> BuildRecordHeader(const ReplayMessage& message) {
  std::vector<std::uint8_t> header;
  header.reserve(kRecordHeaderSizeV3);
  AppendU32(&header, kRecordMagic);
  AppendU16(&header, kRecordHeaderSizeV3);
  AppendU16(&header, message.message_version);
  AppendU64(&header, message.record_seq);
  AppendU64(&header, message.event_mono_ts_us);
  AppendU64(&header, message.event_utc_ts_us);
  AppendU64(&header, message.session_id);
  AppendU64(&header, message.stream_id.value_or(0));
  AppendU32(&header, message.stream_id.has_value() ? kHasStreamIdFlag : 0U);
  AppendU32(&header, message.message_type);
  AppendU32(&header, static_cast<std::uint32_t>(message.payload.size()));
  AppendU32(&header, static_cast<std::uint32_t>(message.attributes.size()));
  return header;
}

std::vector<std::uint8_t> BuildIndexEntry(const SegmentIndexEntry& entry) {
  std::vector<std::uint8_t> bytes;
  bytes.reserve(kIndexEntrySize);
  AppendU64(&bytes, entry.record_seq);
  AppendU64(&bytes, entry.event_mono_ts_us);
  AppendU64(&bytes, entry.event_utc_ts_us);
  AppendU64(&bytes, entry.file_offset);
  return bytes;
}

std::vector<std::uint8_t> BuildFooter(std::uint64_t index_offset,
                                      std::uint64_t index_count) {
  std::vector<std::uint8_t> footer;
  footer.reserve(kFooterSizeV2);
  AppendU32(&footer, kFooterMagic);
  AppendU16(&footer, kCurrentSegmentVersion);
  AppendU16(&footer, 0);
  AppendU64(&footer, index_offset);
  AppendU64(&footer, index_count);
  return footer;
}

bool LoadFileBytes(const std::filesystem::path& path,
                   std::vector<std::uint8_t>* bytes,
                   std::string* error) {
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

}  // 匿名命名空间

bool OpenSegment(const std::filesystem::path& recording_path,
                 std::uint32_t segment_index,
                 SegmentWriteContext* context,
                 std::string* error) {
  context->file_path = recording_path / FormatSegmentFileName(segment_index);
  context->segment_index = segment_index;
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

bool ShouldRotateSegment(const SegmentWriteContext& context,
                         std::uint64_t segment_max_bytes,
                         std::size_t next_record_size) {
  if (context.summary.record_count == 0) {
    return false;
  }
  return context.data_size_bytes + next_record_size > segment_max_bytes;
}

bool WriteRecordToSegment(SegmentWriteContext* context,
                          const ReplayMessage& message,
                          std::string* error) {
  const std::vector<std::uint8_t> header = BuildRecordHeader(message);

  // 记录字节由专用写线程严格按照 `record_seq` 顺序追加。
  context->stream.write(reinterpret_cast<const char*>(header.data()),
                        static_cast<std::streamsize>(header.size()));
  if (!message.payload.empty()) {
    context->stream.write(reinterpret_cast<const char*>(message.payload.data()),
                          static_cast<std::streamsize>(message.payload.size()));
  }
  if (!message.attributes.empty()) {
    context->stream.write(reinterpret_cast<const char*>(message.attributes.data()),
                          static_cast<std::streamsize>(message.attributes.size()));
  }
  if (!context->stream.good()) {
    if (error != nullptr) {
      *error = "failed to write segment data: " + context->file_path.string();
    }
    return false;
  }

  context->index_entries.push_back(
      SegmentIndexEntry{message.record_seq, message.event_mono_ts_us, message.event_utc_ts_us,
                        context->data_size_bytes});
  context->data_size_bytes +=
      static_cast<std::uint64_t>(header.size() + message.payload.size() + message.attributes.size());
  context->summary.valid_bytes = context->data_size_bytes;
  context->summary.file_size_bytes = context->data_size_bytes;
  UpdateSummaryForRecord(message, &context->summary);
  return true;
}

bool FinalizeSegment(SegmentWriteContext* context, std::string* error) {
  if (context->finalized) {
    return true;
  }

  const std::uint64_t index_offset = context->data_size_bytes;
  for (const SegmentIndexEntry& entry : context->index_entries) {
    const std::vector<std::uint8_t> bytes = BuildIndexEntry(entry);
    context->stream.write(reinterpret_cast<const char*>(bytes.data()),
                          static_cast<std::streamsize>(bytes.size()));
  }

  const std::vector<std::uint8_t> footer = BuildFooter(index_offset, context->index_entries.size());
  context->stream.write(reinterpret_cast<const char*>(footer.data()),
                        static_cast<std::streamsize>(footer.size()));
  context->stream.flush();
  if (!context->stream.good()) {
    if (error != nullptr) {
      *error = "failed to finalize segment: " + context->file_path.string();
    }
    return false;
  }

  // 完成收尾的 segment 会暴露稠密 footer 索引，同时让 `valid_bytes` 仍指向记录数据区。
  context->summary.valid_bytes = index_offset;
  context->summary.file_size_bytes =
      index_offset + context->index_entries.size() * kIndexEntrySize + footer.size();
  context->summary.has_footer = true;
  context->stream.close();
  context->finalized = true;
  return true;
}

std::size_t EstimateRecordBytes(const RecordedMessage& message) {
  const std::size_t attributes_size =
      message.attributes.has_value() ? message.attributes->size : 0U;
  return kRecordHeaderSizeV3 + message.payload.size + attributes_size;
}

bool ScanSegment(const std::filesystem::path& segment_path,
                 const SegmentScanOptions& options,
                 SegmentScanResult* result,
                 std::string* error) {
  std::vector<std::uint8_t> bytes;
  if (!LoadFileBytes(segment_path, &bytes, error)) {
    return false;
  }

  *result = SegmentScanResult{};
  result->summary.file_name = segment_path.filename().string();
  result->file_size_bytes = bytes.size();
  result->summary.file_size_bytes = bytes.size();

  std::uint16_t footer_version = 0;
  const FooterProbeResult footer_v2 = ProbeFooter(bytes, kFooterSizeV2);
  const FooterProbeResult footer_v1 = ProbeFooter(bytes, kFooterSizeV1);
  result->footer_present = footer_v2.found_magic || footer_v1.found_magic;

  const FooterProbeResult* footer = nullptr;
  if (footer_v2.valid) {
    footer = &footer_v2;
  } else if (footer_v1.valid) {
    footer = &footer_v1;
  }

  if (footer != nullptr) {
    result->footer_valid = true;
    result->summary.has_footer = true;
    result->summary.valid_bytes = footer->index_offset;
    footer_version = footer->version;

    for (std::uint64_t entry_index = 0; entry_index < footer->index_count; ++entry_index) {
      const std::size_t entry_offset =
          static_cast<std::size_t>(footer->index_offset + entry_index * kIndexEntrySize);
      result->index_entries.push_back(SegmentIndexEntry{
          ReadU64(bytes, entry_offset), ReadU64(bytes, entry_offset + 8),
          ReadU64(bytes, entry_offset + 16), ReadU64(bytes, entry_offset + 24)});
    }
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

  const std::uint16_t expected_header_size = RecordHeaderSizeForVersion(footer_version);
  const std::uint64_t footer_valid_bytes = result->summary.valid_bytes;
  std::uint64_t offset = 0;
  while (offset < scan_limit) {
    if (scan_limit - offset < kRecordHeaderSizeV3) {
      result->issues.push_back("truncated record header near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }
    const std::size_t header_offset = static_cast<std::size_t>(offset);
    if (ReadU32(bytes, header_offset) != kRecordMagic) {
      result->issues.push_back("record magic mismatch near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }

    const std::uint16_t header_size = ReadU16(bytes, header_offset + 4);
    if (header_size != kRecordHeaderSizeV1 && header_size != kRecordHeaderSizeV2 && header_size != kRecordHeaderSizeV3) {
      result->issues.push_back("record header size mismatch near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }
    if (expected_header_size != 0 && header_size != expected_header_size) {
      result->issues.push_back("record header version mismatch near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }
    if (scan_limit - offset < header_size) {
      result->issues.push_back("truncated record header near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }

    const std::uint64_t record_seq = ReadU64(bytes, header_offset + 8);
    const std::uint64_t event_mono_ts_us = ReadU64(bytes, header_offset + 16);
    const std::uint64_t event_utc_ts_us = ReadU64(bytes, header_offset + 24);
    const std::uint64_t session_id = ReadU64(bytes, header_offset + 32);
    const std::uint64_t stream_id_value = ReadU64(bytes, header_offset + 40);
    const std::uint32_t flags = ReadU32(bytes, header_offset + 48);
    const std::uint32_t message_type = ReadU32(bytes, header_offset + 52);
    const std::uint32_t payload_size =
        header_size == kRecordHeaderSizeV3 ? ReadU32(bytes, header_offset + 56)
                                           : ReadU32(bytes, header_offset + 60);
    const std::uint32_t attributes_size =
        header_size == kRecordHeaderSizeV3 ? ReadU32(bytes, header_offset + 60)
                                           : ReadU32(bytes, header_offset + 64);
    const std::uint64_t total_size =
        static_cast<std::uint64_t>(header_size) + payload_size + attributes_size;
    if (offset + total_size > scan_limit) {
      result->issues.push_back("truncated record body near byte " + std::to_string(offset));
      result->truncated_tail = true;
      break;
    }

    const std::size_t payload_offset = header_offset + header_size;
    const std::size_t attributes_offset = payload_offset + payload_size;

    ReplayMessage record;
    record.record_seq = record_seq;
    record.event_mono_ts_us = event_mono_ts_us;
    record.event_utc_ts_us = event_utc_ts_us;
    record.session_id = session_id;
    record.stream_id = (flags & kHasStreamIdFlag) != 0U ? std::optional<std::uint64_t>(stream_id_value)
                                                        : std::nullopt;
    record.message_type = message_type;
    record.message_version = ReadU16(bytes, header_offset + 6);
    if (options.load_payloads) {
      record.payload.assign(bytes.begin() + payload_offset,
                            bytes.begin() + payload_offset + payload_size);
      record.attributes.assign(bytes.begin() + attributes_offset,
                               bytes.begin() + attributes_offset + attributes_size);
      result->records.push_back(record);
    }

    result->total_payload_bytes += payload_size;
    result->total_attributes_bytes += attributes_size;
    UpdateSummaryForRecord(record, &result->summary);
    offset += total_size;
  }

  if (!result->summary.has_footer) {
    result->summary.valid_bytes = offset;
    result->summary.file_size_bytes = bytes.size();
    if (offset < bytes.size()) {
      result->truncated_tail = true;
    }
  } else if (offset != footer_valid_bytes) {
    result->summary.valid_bytes = offset;
    result->issues.push_back("segment footer/index covered more bytes than readable records");
  } else {
    result->summary.valid_bytes = footer_valid_bytes;
  }
  return true;
}

}  // jojo::rec::internal 命名空间
