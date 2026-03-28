#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "jojo/rec/inspect.hpp"
#include "jojo/rec/recorder.hpp"

namespace {

struct BenchmarkOptions {
  std::filesystem::path output_root = "benchmark-output";
  std::uint64_t message_count = 200000;
  std::size_t payload_bytes = 256;
  std::size_t attributes_bytes = 0;
  std::size_t thread_count = 1;
  std::size_t queue_capacity_mb = 64;
  std::size_t queue_buffer_count = 2;
  std::size_t segment_max_mb = 64;
  bool fail_fast = false;
  bool keep_output = false;
};

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  message_recorder_benchmark [options]\n"
      << "\n"
      << "Options:\n"
      << "  --output-root <path>             Root directory for benchmark recordings\n"
      << "  --messages <count>               Total messages to append (default: 200000)\n"
      << "  --payload-bytes <count>          Payload bytes per message (default: 256)\n"
      << "  --attributes-bytes <count>       Attribute bytes per message (default: 0)\n"
      << "  --threads <count>                Producer threads (default: 1)\n"
      << "  --queue-capacity-mb <count>      Per-queue capacity in MB (default: 64)\n"
      << "  --queue-buffer-count <count>     Internal queue buffer count (default: 2, min: 2)\n"
      << "  --segment-max-mb <count>         Segment rotation threshold in MB (default: 64)\n"
      << "  --fail-fast                      Use fail-fast backpressure instead of blocking\n"
      << "  --keep-output                    Keep the generated recording directory\n"
      << "  --help                           Show this message\n";
}

bool ParseUnsigned(const std::string& text, std::uint64_t* value) {
  try {
    *value = std::stoull(text);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool ParseOptions(int argc, char** argv, BenchmarkOptions* options, std::string* error) {
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    auto require_value = [&](const char* name) -> const char* {
      if (index + 1 >= argc) {
        if (error != nullptr) {
          *error = std::string("missing value for option '") + name + "'";
        }
        return nullptr;
      }
      ++index;
      return argv[index];
    };

    std::uint64_t value = 0;
    if (arg == "--help") {
      PrintUsage();
      return false;
    }
    if (arg == "--output-root") {
      const char* raw = require_value("--output-root");
      if (raw == nullptr) {
        return false;
      }
      options->output_root = raw;
      continue;
    }
    if (arg == "--messages") {
      const char* raw = require_value("--messages");
      if (raw == nullptr || !ParseUnsigned(raw, &value) || value == 0) {
        if (error != nullptr) {
          *error = "invalid value for --messages";
        }
        return false;
      }
      options->message_count = value;
      continue;
    }
    if (arg == "--payload-bytes") {
      const char* raw = require_value("--payload-bytes");
      if (raw == nullptr || !ParseUnsigned(raw, &value)) {
        if (error != nullptr) {
          *error = "invalid value for --payload-bytes";
        }
        return false;
      }
      options->payload_bytes = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--attributes-bytes") {
      const char* raw = require_value("--attributes-bytes");
      if (raw == nullptr || !ParseUnsigned(raw, &value)) {
        if (error != nullptr) {
          *error = "invalid value for --attributes-bytes";
        }
        return false;
      }
      options->attributes_bytes = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--threads") {
      const char* raw = require_value("--threads");
      if (raw == nullptr || !ParseUnsigned(raw, &value) || value == 0) {
        if (error != nullptr) {
          *error = "invalid value for --threads";
        }
        return false;
      }
      options->thread_count = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--queue-capacity-mb") {
      const char* raw = require_value("--queue-capacity-mb");
      if (raw == nullptr || !ParseUnsigned(raw, &value) || value == 0) {
        if (error != nullptr) {
          *error = "invalid value for --queue-capacity-mb";
        }
        return false;
      }
      options->queue_capacity_mb = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--queue-buffer-count") {
      const char* raw = require_value("--queue-buffer-count");
      if (raw == nullptr || !ParseUnsigned(raw, &value) || value < 2) {
        if (error != nullptr) {
          *error = "invalid value for --queue-buffer-count";
        }
        return false;
      }
      options->queue_buffer_count = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--segment-max-mb") {
      const char* raw = require_value("--segment-max-mb");
      if (raw == nullptr || !ParseUnsigned(raw, &value) || value == 0) {
        if (error != nullptr) {
          *error = "invalid value for --segment-max-mb";
        }
        return false;
      }
      options->segment_max_mb = static_cast<std::size_t>(value);
      continue;
    }
    if (arg == "--fail-fast") {
      options->fail_fast = true;
      continue;
    }
    if (arg == "--keep-output") {
      options->keep_output = true;
      continue;
    }

    if (error != nullptr) {
      *error = "unknown option: " + arg;
    }
    return false;
  }
  return true;
}

std::uint64_t SumFileBytes(const jojo::rec::RecordingSummary& summary) {
  std::uint64_t total = 0;
  for (const auto& segment : summary.segments) {
    total += segment.file_size_bytes;
  }
  return total;
}
void PrintResult(const BenchmarkOptions& options,
                 const jojo::rec::RecordingSummary& summary,
                 std::uint64_t attempted_messages,
                 std::uint64_t append_ok,
                 std::uint64_t append_backpressure,
                 std::uint64_t append_closed,
                 std::uint64_t append_internal_error,
                 double append_seconds,
                 double close_seconds) {
  const double total_seconds = append_seconds + close_seconds;
  const std::uint64_t logical_bytes =
      summary.total_payload_bytes + summary.total_attributes_bytes;
  const std::uint64_t disk_bytes = SumFileBytes(summary);
  const std::uint64_t total_queue_capacity_mb =
      static_cast<std::uint64_t>(options.queue_capacity_mb) * options.queue_buffer_count;
  const double append_mps = append_seconds > 0.0 ? append_ok / append_seconds : 0.0;
  const double end_to_end_mps =
      total_seconds > 0.0 ? summary.total_records / total_seconds : 0.0;
  const double logical_mib =
      total_seconds > 0.0 ? logical_bytes / total_seconds / 1024.0 / 1024.0 : 0.0;
  const double disk_mib =
      total_seconds > 0.0 ? disk_bytes / total_seconds / 1024.0 / 1024.0 : 0.0;
  const double logical_mb =
      total_seconds > 0.0 ? logical_bytes / total_seconds / 1000.0 / 1000.0 : 0.0;
  const double disk_mb =
      total_seconds > 0.0 ? disk_bytes / total_seconds / 1000.0 / 1000.0 : 0.0;
  const double append_logical_mb =
      append_seconds > 0.0 ? logical_bytes / append_seconds / 1000.0 / 1000.0 : 0.0;

  std::cout << "benchmark.recording_path=" << summary.recording_path.string() << "\n"
            << "benchmark.threads=" << options.thread_count << "\n"
            << "benchmark.queue_buffer_count=" << options.queue_buffer_count << "\n"
            << "benchmark.queue_capacity_mb_per_buffer=" << options.queue_capacity_mb << "\n"
            << "benchmark.total_queue_capacity_mb=" << total_queue_capacity_mb << "\n"
            << "benchmark.messages_attempted=" << attempted_messages << "\n"
            << "benchmark.messages_append_ok=" << append_ok << "\n"
            << "benchmark.messages_written=" << summary.total_records << "\n"
            << "benchmark.append_backpressure=" << append_backpressure << "\n"
            << "benchmark.append_closed=" << append_closed << "\n"
            << "benchmark.append_internal_error=" << append_internal_error << "\n"
            << "benchmark.aborted_entries=" << summary.aborted_entries << "\n"
            << "benchmark.append_seconds=" << append_seconds << "\n"
            << "benchmark.close_seconds=" << close_seconds << "\n"
            << "benchmark.total_seconds=" << total_seconds << "\n"
            << "benchmark.payload_bytes=" << summary.total_payload_bytes << "\n"
            << "benchmark.attributes_bytes=" << summary.total_attributes_bytes << "\n"
            << "benchmark.logical_bytes=" << logical_bytes << "\n"
            << "benchmark.disk_bytes=" << disk_bytes << "\n"
            << "benchmark.append_messages_per_sec=" << append_mps << "\n"
            << "benchmark.end_to_end_messages_per_sec=" << end_to_end_mps << "\n"
            << "benchmark.logical_mib_per_sec=" << logical_mib << "\n"
            << "benchmark.disk_mib_per_sec=" << disk_mib << "\n"
            << "benchmark.logical_mb_per_sec=" << logical_mb << "\n"
            << "benchmark.disk_mb_per_sec=" << disk_mb << "\n"
            << "benchmark.append_logical_mb_per_sec=" << append_logical_mb << "\n"
            << "benchmark.degraded=" << (summary.degraded ? "true" : "false") << "\n"
            << "benchmark.incomplete=" << (summary.incomplete ? "true" : "false") << "\n";
}

}  // 匿名命名空间

int main(int argc, char** argv) {
  BenchmarkOptions options;
  std::string error;
  if (!ParseOptions(argc, argv, &options, &error)) {
    if (!error.empty()) {
      std::cerr << error << "\n";
      PrintUsage();
      return 1;
    }
    return 0;
  }

  jojo::rec::RecorderConfig config;
  config.output_root = options.output_root;
  config.queue_capacity_mb = options.queue_capacity_mb;
  config.queue_buffer_count = options.queue_buffer_count;
  config.segment_max_mb = options.segment_max_mb;
  config.backpressure_policy = options.fail_fast ? jojo::rec::BackpressurePolicy::kFailFast
                                                 : jojo::rec::BackpressurePolicy::kBlock;
  config.recording_label = "benchmark";
  config.message_type_names = {{1U, "benchmark"}};

  jojo::rec::Recorder recorder(config, &error);
  if (!recorder.IsOpen()) {
    std::cerr << "failed to open recorder: " << error << "\n";
    return 1;
  }

  std::atomic<bool> start_flag{false};
  std::atomic<std::size_t> ready_threads{0};
  std::atomic<std::uint64_t> append_ok{0};
  std::atomic<std::uint64_t> append_backpressure{0};
  std::atomic<std::uint64_t> append_closed{0};
  std::atomic<std::uint64_t> append_internal_error{0};

  const std::uint64_t base_count = options.message_count / options.thread_count;
  const std::uint64_t extra = options.message_count % options.thread_count;
  std::vector<std::thread> workers;
  workers.reserve(options.thread_count);

  for (std::size_t thread_index = 0; thread_index < options.thread_count; ++thread_index) {
    const std::uint64_t thread_messages = base_count + (thread_index < extra ? 1U : 0U);
    workers.emplace_back([&, thread_index, thread_messages]() {
      std::vector<std::uint8_t> payload(options.payload_bytes,
                                        static_cast<std::uint8_t>(0x30 + (thread_index % 64)));
      std::vector<std::uint8_t> attributes(options.attributes_bytes,
                                           static_cast<std::uint8_t>(0x80 + (thread_index % 64)));
      ready_threads.fetch_add(1, std::memory_order_release);
      while (!start_flag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::uint64_t message_index = 0; message_index < thread_messages; ++message_index) {
        jojo::rec::RecordedMessage message;
        message.session_id = static_cast<std::uint64_t>(thread_index + 1);
        message.message_type = 1U;
        message.message_version = 1U;
        message.payload = jojo::rec::ByteView{payload.data(), payload.size()};
        if (!attributes.empty()) {
          message.attributes = jojo::rec::ByteView{attributes.data(), attributes.size()};
        }

        const auto result = recorder.Append(message);
        if (result == jojo::rec::AppendResult::kOk) {
          append_ok.fetch_add(1, std::memory_order_relaxed);
        } else if (result == jojo::rec::AppendResult::kBackpressure) {
          append_backpressure.fetch_add(1, std::memory_order_relaxed);
        } else if (result == jojo::rec::AppendResult::kClosed) {
          append_closed.fetch_add(1, std::memory_order_relaxed);
        } else {
          append_internal_error.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  while (ready_threads.load(std::memory_order_acquire) != options.thread_count) {
    std::this_thread::yield();
  }
  const auto append_begin = std::chrono::steady_clock::now();
  start_flag.store(true, std::memory_order_release);

  for (auto& worker : workers) {
    worker.join();
  }
  const auto append_end = std::chrono::steady_clock::now();

  const auto close_begin = std::chrono::steady_clock::now();
  if (!recorder.Close(&error)) {
    std::cerr << "failed to close recorder: " << error << "\n";
    return 1;
  }
  const auto close_end = std::chrono::steady_clock::now();

  jojo::rec::RecordingSummary summary;
  if (!jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error)) {
    std::cerr << "failed to load recording summary: " << error << "\n";
    return 1;
  }

  const double append_seconds =
      std::chrono::duration<double>(append_end - append_begin).count();
  const double close_seconds =
      std::chrono::duration<double>(close_end - close_begin).count();
  PrintResult(options, summary, options.message_count, append_ok.load(), append_backpressure.load(),
              append_closed.load(), append_internal_error.load(), append_seconds, close_seconds);

  if (!options.keep_output) {
    std::error_code ec;
    std::filesystem::remove_all(summary.recording_path, ec);
    if (ec) {
      std::cerr << "warning: failed to remove benchmark recording '"
                << summary.recording_path.string() << "': " << ec.message() << "\n";
    }
  }
  return 0;
}