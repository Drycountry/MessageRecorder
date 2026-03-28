#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "jojo/rec/inspect.hpp"

namespace {

void PrintUsage() {
  std::cout << "Usage:\n"
            << "  message_recorder_cli summary <recording_path>\n"
            << "  message_recorder_cli verify <recording_path>\n"
            << "  message_recorder_cli repair <recording_path>\n"
            << "  message_recorder_cli dump <recording_path> [cursor_kind] [value] [max_records]\n";
}

jojo::rec::ReplayCursor ParseCursor(const std::string& kind, const std::string& value) {
  if (kind == "seq") {
    return jojo::rec::ReplayCursor::FromRecordSequence(std::stoull(value));
  }
  if (kind == "mono") {
    return jojo::rec::ReplayCursor::FromEventMonoTime(std::stoull(value));
  }
  if (kind == "utc") {
    return jojo::rec::ReplayCursor::FromEventUtcTime(std::stoull(value));
  }
  return jojo::rec::ReplayCursor::FromSegmentCheckpoint(static_cast<std::uint32_t>(std::stoul(value)));
}

void PrintSummary(const jojo::rec::RecordingSummary& summary) {
  std::cout << "path: " << summary.recording_path.string() << "\n"
            << "start_utc: " << summary.start_utc << "\n"
            << "stop_utc: " << (summary.stop_utc.has_value() ? *summary.stop_utc : "<open>") << "\n"
            << "incomplete: " << (summary.incomplete ? "true" : "false") << "\n"
            << "degraded: " << (summary.degraded ? "true" : "false") << "\n"
            << "aborted_entries: " << summary.aborted_entries << "\n"
            << "total_records: " << summary.total_records << "\n"
            << "segments: " << summary.segments.size() << "\n";
  for (const auto& segment : summary.segments) {
    std::cout << "  [" << segment.segment_index << "] " << segment.file_name
              << " records=" << segment.record_count
              << " valid_bytes=" << segment.valid_bytes
              << " file_size_bytes=" << segment.file_size_bytes
              << " has_footer=" << (segment.has_footer ? "true" : "false") << "\n";
  }
}

int PrintVerifyResult(const jojo::rec::VerifyResult& result) {
  std::cout << "success: " << (result.success ? "true" : "false") << "\n"
            << "degraded: " << (result.degraded ? "true" : "false") << "\n"
            << "changed: " << (result.changed ? "true" : "false") << "\n";
  for (const std::string& issue : result.issues) {
    std::cout << "issue: " << issue << "\n";
  }
  if (result.summary.has_value()) {
    PrintSummary(*result.summary);
  }
  return result.success ? 0 : 1;
}

}  // 匿名命名空间

int main(int argc, char** argv) {
  if (argc < 3) {
    PrintUsage();
    return 1;
  }

  const std::string command = argv[1];
  const std::filesystem::path recording_path = argv[2];
  if (command == "summary") {
    jojo::rec::RecordingSummary summary;
    std::string error;
    if (!jojo::rec::LoadRecordingSummary(recording_path, &summary, &error)) {
      std::cerr << error << "\n";
      return 1;
    }
    PrintSummary(summary);
    return 0;
  }
  if (command == "verify") {
    return PrintVerifyResult(jojo::rec::VerifyRecording(recording_path));
  }
  if (command == "repair") {
    return PrintVerifyResult(jojo::rec::RepairRecording(recording_path));
  }
  if (command == "dump") {
    const std::string cursor_kind = argc >= 4 ? argv[3] : "seq";
    const std::string cursor_value = argc >= 5 ? argv[4] : "0";
    const std::size_t max_records = argc >= 6 ? static_cast<std::size_t>(std::stoull(argv[5])) : 10U;
    std::vector<jojo::rec::DumpEntry> entries;
    std::string error;
    if (!jojo::rec::DumpRecording(recording_path, ParseCursor(cursor_kind, cursor_value),
                                  max_records, &entries, &error)) {
      std::cerr << error << "\n";
      return 1;
    }
    for (const auto& entry : entries) {
      std::cout << "record_seq=" << entry.record_seq
                << " mono_us=" << entry.event_mono_ts_us
                << " utc_us=" << entry.event_utc_ts_us
                << " session_id=" << entry.session_id
                << " type=" << entry.message_type                << " payload_size=" << entry.payload_size
                << " attributes_size=" << entry.attributes_size << "\n";
    }
    return 0;
  }

  PrintUsage();
  return 1;
}