#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>

#include "jojo/rec/detail/segment.hpp"

namespace jojo::rec::internal {
namespace {

constexpr std::uint32_t kRecordMagic = 0x31524543U;
constexpr std::uint32_t kFooterMagic = 0x31544653U;
constexpr std::uint16_t kRecordHeaderSize = 48;
constexpr std::size_t kIndexEntrySize = 32;
constexpr std::size_t kFooterSize = 20;

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

/// @brief 从原始字节指针按小端读取 16 位无符号整数。
std::uint16_t ReadU16(const std::uint8_t* bytes) {
  return static_cast<std::uint16_t>(bytes[0]) | (static_cast<std::uint16_t>(bytes[1]) << 8U);
}

/// @brief 从字节缓冲区指定偏移按小端读取 32 位无符号整数。
std::uint32_t ReadU32(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(buffer[offset + index]) << (index * 8);
  }
  return value;
}

/// @brief 从原始字节指针按小端读取 32 位无符号整数。
std::uint32_t ReadU32(const std::uint8_t* bytes) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(bytes[index]) << (index * 8);
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

/// @brief 从原始字节指针按小端读取 64 位无符号整数。
std::uint64_t ReadU64(const std::uint8_t* bytes) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(bytes[index]) << (index * 8);
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
  result.index_offset = ReadU64(bytes, footer_offset + 4);
  result.index_count = ReadU64(bytes, footer_offset + 12);
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

/// @brief 基于 footer 原始字节和文件偏移校验当前格式 footer。
FooterProbeResult ProbeFooter(const std::uint8_t* footer_bytes, std::uint64_t footer_offset) {
  FooterProbeResult result;
  if (ReadU32(footer_bytes) != kFooterMagic) {
    return result;
  }

  result.found_magic = true;
  result.index_offset = ReadU64(footer_bytes + 4);
  result.index_count = ReadU64(footer_bytes + 12);
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

/// @brief 将内存稀疏索引项编码为 footer 索引区条目。
std::vector<std::uint8_t> BuildIndexEntry(const SegmentIndexEntry& entry) {
  std::vector<std::uint8_t> bytes;
  bytes.reserve(kIndexEntrySize);
  AppendU64(&bytes, entry.record_seq);
  AppendU64(&bytes, entry.event_mono_ts_us);
  AppendU64(&bytes, entry.event_utc_ts_us);
  AppendU64(&bytes, entry.file_offset);
  return bytes;
}

/// @brief 生成指向稀疏索引区的 segment footer。
std::vector<std::uint8_t> BuildFooter(std::uint64_t index_offset, std::uint64_t index_count) {
  std::vector<std::uint8_t> footer;
  footer.reserve(kFooterSize);
  AppendU32(&footer, kFooterMagic);
  AppendU64(&footer, index_offset);
  AppendU64(&footer, index_count);
  return footer;
}

/// @brief 从指定偏移读取固定长度字节块。
bool ReadExact(std::ifstream* stream, std::uint64_t offset, std::uint8_t* destination, std::size_t size) {
  stream->clear();
  stream->seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  if (!stream->good()) {
    return false;
  }
  if (size == 0) {
    return true;
  }
  stream->read(reinterpret_cast<char*>(destination), static_cast<std::streamsize>(size));
  return stream->good();
}

/// @brief 从原始索引条目字节中解析单条稀疏索引。
SegmentIndexEntry ParseIndexEntry(const std::uint8_t* bytes) {
  SegmentIndexEntry entry;
  entry.record_seq = ReadU64(bytes);
  entry.event_mono_ts_us = ReadU64(bytes + 8);
  entry.event_utc_ts_us = ReadU64(bytes + 16);
  entry.file_offset = ReadU64(bytes + 24);
  return entry;
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

/// @brief 判断当前记录是否需要成为新的稀疏索引点。
bool ShouldEmitSparseIndex(const SegmentWriteContext& context, const ReplayMessage& message) {
  if (context.index_entries.empty()) {
    return true;
  }

  const bool time_due = message.event_mono_ts_us >= context.last_sparse_index_event_mono_ts_us &&
                        message.event_mono_ts_us - context.last_sparse_index_event_mono_ts_us >=
                            context.sparse_index_interval_us;
  const bool record_due = context.records_since_last_index >= context.sparse_index_max_records;
  const bool bytes_due = context.bytes_since_last_index >= context.sparse_index_max_bytes;
  return time_due || record_due || bytes_due;
}

/// @brief 记录一次稀疏索引命中，并重置跨度统计。
void MarkSparseIndexEmission(SegmentWriteContext* context, const ReplayMessage& message) {
  context->last_sparse_index_event_mono_ts_us = message.event_mono_ts_us;
  context->records_since_last_index = 0;
  context->bytes_since_last_index = 0;
}

}  // namespace

/// @brief 打开新的 segment 文件并初始化写上下文。
bool OpenSegment(const std::filesystem::path& recording_path, std::uint32_t segment_index,
                 std::uint64_t sparse_index_interval_us, std::size_t sparse_index_max_records,
                 std::size_t sparse_index_max_bytes, SegmentWriteContext* context, std::string* error) {
  context->file_path = recording_path / FormatSegmentFileName(segment_index);
  context->stream = std::ofstream(context->file_path, std::ios::binary | std::ios::trunc);
  if (!context->stream.is_open()) {
    if (error != nullptr) {
      *error = "failed to open segment for write: " + context->file_path.string();
    }
    return false;
  }

  // 写线程同一时间只持有一个 segment，并在 finalize 前将 footer 和稀疏索引状态保留在内存中。
  context->index_entries.clear();
  context->summary = SegmentSummary{};
  context->summary.segment_index = segment_index;
  context->summary.file_name = context->file_path.filename().string();
  context->summary.file_size_bytes = 0;
  context->summary.valid_bytes = 0;
  context->summary.has_footer = false;
  context->data_size_bytes = 0;
  context->sparse_index_interval_us = sparse_index_interval_us;
  context->sparse_index_max_records = sparse_index_max_records;
  context->sparse_index_max_bytes = sparse_index_max_bytes;
  context->last_sparse_index_event_mono_ts_us = 0;
  context->records_since_last_index = 0;
  context->bytes_since_last_index = 0;
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

/// @brief 将一条消息写入 segment 数据区并按采样规则维护内存稀疏索引。
bool WriteRecordToSegment(SegmentWriteContext* context, const ReplayMessage& message, std::string* error) {
  const std::vector<std::uint8_t> header = BuildRecordHeader(message);
  const std::uint64_t record_offset = context->data_size_bytes;
  const std::size_t record_size = header.size() + message.payload.size();

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

  context->data_size_bytes += static_cast<std::uint64_t>(record_size);
  context->summary.valid_bytes = context->data_size_bytes;
  context->summary.file_size_bytes = context->data_size_bytes;
  UpdateSummaryForRecord(message, &context->summary);
  context->records_since_last_index += 1;
  context->bytes_since_last_index += record_size;
  if (ShouldEmitSparseIndex(*context, message)) {
    context->index_entries.push_back(
        SegmentIndexEntry{message.record_seq, message.event_mono_ts_us, message.event_utc_ts_us, record_offset});
    MarkSparseIndexEmission(context, message);
  }
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

  // 完成收尾的 segment 会暴露稀疏 footer 索引，同时让 `valid_bytes` 仍指向记录数据区。
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

/// @brief 只读取 footer 和索引区，恢复 segment 的稀疏索引数据。
bool LoadSegmentSparseIndex(const std::filesystem::path& segment_path, SegmentSparseIndexData* data, std::string* error) {
  *data = SegmentSparseIndexData{};

  std::ifstream stream(segment_path, std::ios::binary);
  if (!stream.is_open()) {
    if (error != nullptr) {
      *error = "failed to open segment: " + segment_path.string();
    }
    return false;
  }

  stream.seekg(0, std::ios::end);
  const std::streamoff file_size = stream.tellg();
  if (file_size < 0) {
    if (error != nullptr) {
      *error = "failed to measure segment size: " + segment_path.string();
    }
    return false;
  }
  if (static_cast<std::uint64_t>(file_size) < kFooterSize) {
    return true;
  }

  const std::uint64_t footer_offset = static_cast<std::uint64_t>(file_size) - kFooterSize;
  std::array<std::uint8_t, kFooterSize> footer_bytes{};
  if (!ReadExact(&stream, footer_offset, footer_bytes.data(), footer_bytes.size())) {
    if (error != nullptr) {
      *error = "failed to read segment footer: " + segment_path.string();
    }
    return false;
  }

  const FooterProbeResult footer = ProbeFooter(footer_bytes.data(), footer_offset);
  data->footer_present = footer.found_magic;
  if (!footer.valid) {
    return true;
  }

  data->footer_valid = true;
  data->valid_bytes = footer.index_offset;
  if (footer.index_count == 0) {
    return true;
  }
  if (footer.index_count > std::numeric_limits<std::size_t>::max() / kIndexEntrySize) {
    if (error != nullptr) {
      *error = "segment sparse index is too large: " + segment_path.string();
    }
    return false;
  }

  const std::size_t index_bytes = static_cast<std::size_t>(footer.index_count) * kIndexEntrySize;
  std::vector<std::uint8_t> encoded_index(index_bytes);
  if (!ReadExact(&stream, footer.index_offset, encoded_index.data(), encoded_index.size())) {
    if (error != nullptr) {
      *error = "failed to read segment sparse index: " + segment_path.string();
    }
    return false;
  }

  data->entries.reserve(static_cast<std::size_t>(footer.index_count));
  for (std::size_t offset = 0; offset < encoded_index.size(); offset += kIndexEntrySize) {
    data->entries.push_back(ParseIndexEntry(encoded_index.data() + offset));
  }
  return true;
}

/// @brief 打开一个可复用的 segment 顺序读取上下文。
bool OpenSegmentReadContext(const std::filesystem::path& segment_path, std::uint64_t valid_bytes,
                            SegmentReadContext* context, std::string* error) {
  CloseSegmentReadContext(context);
  context->stream = std::ifstream(segment_path, std::ios::binary);
  if (!context->stream.is_open()) {
    if (error != nullptr) {
      *error = "failed to open segment for read: " + segment_path.string();
    }
    return false;
  }

  context->file_path = segment_path;
  context->valid_bytes = valid_bytes;
  context->open = true;
  return true;
}

/// @brief 关闭复用的 segment 读取上下文。
void CloseSegmentReadContext(SegmentReadContext* context) {
  if (context->stream.is_open()) {
    context->stream.close();
  }
  context->file_path.clear();
  context->valid_bytes = 0;
  context->open = false;
}

/// @brief 按指定偏移读取一条完整记录。
bool ReadRecordAtOffset(SegmentReadContext* context, std::uint64_t offset, ReplayMessage* record,
                        std::uint64_t* next_offset, std::string* error) {
  if (!context->open) {
    if (error != nullptr) {
      *error = "segment read context is not open";
    }
    return false;
  }
  if (offset >= context->valid_bytes) {
    if (error != nullptr) {
      *error = "record offset is beyond segment valid bytes: " + context->file_path.string();
    }
    return false;
  }
  if (context->valid_bytes - offset < kRecordHeaderSize) {
    if (error != nullptr) {
      *error = "truncated record header near byte " + std::to_string(offset) + " in " + context->file_path.string();
    }
    return false;
  }

  std::array<std::uint8_t, kRecordHeaderSize> header{};
  if (!ReadExact(&context->stream, offset, header.data(), header.size())) {
    if (error != nullptr) {
      *error = "failed to read record header: " + context->file_path.string();
    }
    return false;
  }
  if (ReadU32(header.data()) != kRecordMagic) {
    if (error != nullptr) {
      *error = "record magic mismatch near byte " + std::to_string(offset) + " in " + context->file_path.string();
    }
    return false;
  }

  const std::uint16_t header_size = ReadU16(header.data() + 4);
  if (header_size != kRecordHeaderSize) {
    if (error != nullptr) {
      *error =
          "record header size mismatch near byte " + std::to_string(offset) + " in " + context->file_path.string();
    }
    return false;
  }

  const std::uint32_t payload_size = ReadU32(header.data() + 44);
  const std::uint64_t total_size = static_cast<std::uint64_t>(header_size) + payload_size;
  if (offset + total_size > context->valid_bytes) {
    if (error != nullptr) {
      *error = "truncated record body near byte " + std::to_string(offset) + " in " + context->file_path.string();
    }
    return false;
  }

  *record = ReplayMessage{};
  record->record_seq = ReadU64(header.data() + 8);
  record->event_mono_ts_us = ReadU64(header.data() + 16);
  record->event_utc_ts_us = ReadU64(header.data() + 24);
  record->session_id = ReadU64(header.data() + 32);
  record->message_type = ReadU32(header.data() + 40);
  record->payload.resize(payload_size);
  if (payload_size != 0) {
    const std::uint64_t payload_offset = offset + header_size;
    if (!ReadExact(&context->stream, payload_offset, record->payload.data(), record->payload.size())) {
      if (error != nullptr) {
        *error = "failed to read record payload: " + context->file_path.string();
      }
      return false;
    }
  }

  *next_offset = offset + total_size;
  return true;
}

}  // namespace jojo::rec::internal
