#include "test_helpers.hpp"

#include <chrono>
#include <fstream>
#include <memory>
#include <thread>

namespace {

jojo::rec::RecordedMessage MakeMessage(const std::vector<std::uint8_t>& payload) {
  jojo::rec::RecordedMessage message;
  message.session_id = 7;
  message.message_type = 1;
  message.message_version = 1;
  message.payload = jojo::rec::ByteView{payload.data(), payload.size()};
  return message;
}

void TestQueueBufferCountValidation() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.queue_buffer_count = 1;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(!recorder.IsOpen(), "queue_buffer_count < 2 should fail");
  test::Require(error.find("queue_buffer_count") != std::string::npos,
                "validation error should mention queue_buffer_count");
}

void TestDictionarySnapshotAndFinalizePath() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = test::Bytes("hello");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append should succeed");
  config.message_type_names[1] = "mutated";
  test::Require(recorder.Close(&error), error);

  jojo::rec::RecordingSummary summary;
  test::Require(jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error), error);
  test::Require(summary.stop_utc.has_value(), "closed recording should have stop_utc");
  test::Require(summary.recording_path.filename().string().find("-to-") != std::string::npos,
                "recording path should be finalized");
  test::RequireString(summary.message_type_names[1], "alpha", "dictionary must be frozen");
}

void TestFailFastBackpressure() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.backpressure_policy = jojo::rec::BackpressurePolicy::kFailFast;
  config.queue_capacity_mb = 1;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = std::vector<std::uint8_t>(900 * 1000, 0x5A);
  bool saw_backpressure = false;
  for (int attempt = 0; attempt < 64; ++attempt) {
    const auto result = recorder.Append(MakeMessage(payload));
    if (result == jojo::rec::AppendResult::kBackpressure) {
      saw_backpressure = true;
      break;
    }
    test::Require(result == jojo::rec::AppendResult::kOk,
                  "append should either succeed or report backpressure");
  }
  test::Require(saw_backpressure, "fail-fast mode should eventually report backpressure");
  test::Require(recorder.Close(&error), error);
}

void TestTwoQueueAlternatingReceive() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.backpressure_policy = jojo::rec::BackpressurePolicy::kFailFast;
  config.queue_capacity_mb = 1;
  config.queue_buffer_count = 2;
  config.segment_max_mb = 64;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = std::vector<std::uint8_t>(8 * 1000 * 1000, 0x33);
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "first oversized append should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "second oversized append should be accepted by the second queue buffer");
  test::Require(recorder.Close(&error), error);

  jojo::rec::RecordingSummary summary;
  test::Require(jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error), error);
  test::RequireEqual(summary.total_records, 2,
                     "two queue buffers should preserve both oversized messages");
}

void TestAdditionalQueueBuffersAcceptMoreOversizedMessages() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.backpressure_policy = jojo::rec::BackpressurePolicy::kFailFast;
  config.queue_capacity_mb = 1;
  config.queue_buffer_count = 3;
  config.segment_max_mb = 64;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = std::vector<std::uint8_t>(8 * 1000 * 1000, 0x33);
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "first oversized append should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "second oversized append should succeed with a third queue buffer available");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "third oversized append should succeed with three queue buffers");
  test::Require(recorder.Close(&error), error);

  jojo::rec::RecordingSummary summary;
  test::Require(jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error), error);
  test::RequireEqual(summary.total_records, 3,
                     "three queue buffers should preserve three oversized messages");
}

void TestSegmentRotationAndRepairWithoutTruncate() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.segment_max_mb = 1;
  config.queue_capacity_mb = 2;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = std::vector<std::uint8_t>(600 * 1000, 0x7F);
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "first append should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "second append should succeed");
  test::Require(recorder.Close(&error), error);

  jojo::rec::RecordingSummary summary;
  test::Require(jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error), error);
  test::Require(summary.segments.size() >= 2, "size-based rotation should create multiple segments");

  const auto segment_path = recorder.RecordingPath() / summary.segments.back().file_name;
  const auto original_size = std::filesystem::file_size(segment_path);
  std::ofstream stream(segment_path, std::ios::binary | std::ios::app);
  const std::string garbage = "garbage";
  stream.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
  stream.close();

  const jojo::rec::VerifyResult verify = jojo::rec::VerifyRecording(recorder.RecordingPath());
  test::Require(verify.success, "verify should stay readable");
  test::Require(verify.degraded, "verify should report degraded after tail corruption");

  const jojo::rec::VerifyResult repair = jojo::rec::RepairRecording(recorder.RecordingPath());
  test::Require(repair.success, "repair should succeed");
  test::Require(repair.changed, "repair should rewrite metadata");
  test::RequireEqual(std::filesystem::file_size(segment_path), original_size + garbage.size(),
                     "repair must not truncate segment bytes");
}

class CollectingTarget final : public jojo::rec::IReplayTarget {
 public:
  explicit CollectingTarget(std::vector<std::uint64_t>* seen) : seen_(seen) {}

  bool OnMessage(const jojo::rec::ReplayMessage& message, std::string* error) override {
    seen_->push_back(message.record_seq);
    if (fail_after_.has_value() && message.record_seq >= *fail_after_) {
      if (error != nullptr) {
        *error = "intentional target failure";
      }
      return false;
    }
    return true;
  }

  void SetFailAfter(std::uint64_t record_seq) { fail_after_ = record_seq; }

 private:
  std::vector<std::uint64_t>* seen_;
  std::optional<std::uint64_t> fail_after_;
};

class SeekingTarget final : public jojo::rec::IReplayTarget {
 public:
  SeekingTarget(std::vector<std::uint64_t>* seen, jojo::rec::ReplayCursor target_cursor)
      : seen_(seen), target_cursor_(target_cursor) {}

  void SetSession(jojo::rec::IReplaySession* session) { session_ = session; }

  bool OnMessage(const jojo::rec::ReplayMessage& message, std::string* error) override {
    seen_->push_back(message.record_seq);
    if (!seek_requested_ && session_ != nullptr) {
      seek_requested_ = true;
      std::string seek_error;
      if (!session_->Seek(target_cursor_, &seek_error)) {
        if (error != nullptr) {
          *error = seek_error.empty() ? "failed to seek during callback" : seek_error;
        }
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<std::uint64_t>* seen_;
  jojo::rec::ReplayCursor target_cursor_;
  jojo::rec::IReplaySession* session_ = nullptr;
  bool seek_requested_ = false;
};

class SpeedChangingTarget final : public jojo::rec::IReplayTarget {
 public:
  explicit SpeedChangingTarget(
      std::vector<std::chrono::steady_clock::time_point>* dispatch_times,
      double updated_speed)
      : dispatch_times_(dispatch_times), updated_speed_(updated_speed) {}

  void SetSession(jojo::rec::IReplaySession* session) { session_ = session; }

  bool OnMessage(const jojo::rec::ReplayMessage& message, std::string* error) override {
    (void)message;
    dispatch_times_->push_back(std::chrono::steady_clock::now());
    if (!speed_updated_ && session_ != nullptr) {
      speed_updated_ = true;
      std::string speed_error;
      if (!session_->SetSpeed(updated_speed_, &speed_error)) {
        if (error != nullptr) {
          *error = speed_error.empty() ? "failed to change replay speed" : speed_error;
        }
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<std::chrono::steady_clock::time_point>* dispatch_times_;
  jojo::rec::IReplaySession* session_ = nullptr;
  double updated_speed_ = 0.0;
  bool speed_updated_ = false;
};

void TestReplaySeekAndFailureStop() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = test::Bytes("abc");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 1 should succeed");
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 2 should succeed");
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 3 should succeed");
  test::Require(recorder.Close(&error), error);

  std::vector<jojo::rec::DumpEntry> dump_entries;
  test::Require(jojo::rec::DumpRecording(recorder.RecordingPath(),
                                         jojo::rec::ReplayCursor::FromRecordSequence(0), 10,
                                         &dump_entries, &error),
                error);
  const std::uint64_t target_mono =
      (dump_entries[0].event_mono_ts_us + dump_entries[1].event_mono_ts_us) / 2;

  std::vector<std::uint64_t> seen;
  auto target = std::make_unique<CollectingTarget>(&seen);
  auto session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromEventMonoTime(target_mono),
      jojo::rec::ReplayOptions{}, std::move(target), &error);
  test::Require(session != nullptr, error);
  test::Require(session->Start(&error), error);
  const jojo::rec::ReplayResult result = session->Wait();
  test::Require(result.completed, "seeked replay should complete");
  test::RequireEqual(seen.front(), dump_entries[1].record_seq,
                     "time seek should land on first record >= target");

  seen.clear();
  auto failing_target = std::make_unique<CollectingTarget>(&seen);
  failing_target->SetFailAfter(dump_entries[1].record_seq);
  auto failing_session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromRecordSequence(0),
      jojo::rec::ReplayOptions{}, std::move(failing_target), &error);
  test::Require(failing_session != nullptr, error);
  test::Require(failing_session->Start(&error), error);
  const jojo::rec::ReplayResult failing_result = failing_session->Wait();
  test::Require(failing_result.failure.has_value(), "target failure should stop replay");
  test::RequireEqual(*failing_result.failure->record_seq, dump_entries[1].record_seq,
                     "reported failing record should match target stop point");
}

void TestReplaySegmentCheckpointSeek() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  config.segment_max_mb = 1;
  config.queue_capacity_mb = 2;
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = std::vector<std::uint8_t>(600 * 1000, 0x44);
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 1 should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 2 should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 3 should succeed");
  test::Require(recorder.Close(&error), error);

  jojo::rec::RecordingSummary summary;
  test::Require(jojo::rec::LoadRecordingSummary(recorder.RecordingPath(), &summary, &error), error);
  test::Require(summary.segments.size() >= 2, "rotation should create a later segment");
  test::Require(summary.segments[1].first_record_seq.has_value(),
                "later segment should expose a first record");

  std::vector<std::uint64_t> seen;
  auto target = std::make_unique<CollectingTarget>(&seen);
  auto session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromSegmentCheckpoint(1),
      jojo::rec::ReplayOptions{}, std::move(target), &error);
  test::Require(session != nullptr, error);
  test::Require(session->Start(&error), error);
  const jojo::rec::ReplayResult result = session->Wait();
  test::Require(result.completed, "checkpoint seeked replay should complete");
  test::RequireEqual(seen.front(), *summary.segments[1].first_record_seq,
                     "checkpoint seek should land on the first record in the requested segment");
}

void TestReplaySeekDuringCallback() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = test::Bytes("seek");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 1 should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 2 should succeed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 3 should succeed");
  test::Require(recorder.Close(&error), error);

  std::vector<jojo::rec::DumpEntry> dump_entries;
  test::Require(jojo::rec::DumpRecording(recorder.RecordingPath(),
                                         jojo::rec::ReplayCursor::FromRecordSequence(0), 10,
                                         &dump_entries, &error),
                error);
  test::Require(dump_entries.size() >= 3, "dump should expose three replayable records");

  std::vector<std::uint64_t> seen;
  auto target = std::make_unique<SeekingTarget>(
      &seen, jojo::rec::ReplayCursor::FromRecordSequence(dump_entries[2].record_seq));
  SeekingTarget* target_ptr = target.get();
  auto session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromRecordSequence(0),
      jojo::rec::ReplayOptions{}, std::move(target), &error);
  test::Require(session != nullptr, error);
  target_ptr->SetSession(session.get());
  test::Require(session->Start(&error), error);
  const jojo::rec::ReplayResult result = session->Wait();
  test::Require(result.completed, "callback seek replay should complete");
  test::RequireEqual(static_cast<std::uint64_t>(seen.size()), 2,
                     "callback-triggered seek should keep only the original and target records");
  test::RequireEqual(seen[0], dump_entries[0].record_seq,
                     "first callback should observe the initial record");
  test::RequireEqual(seen[1], dump_entries[2].record_seq,
                     "seek during callback should continue from the requested record");
}

void TestReplaySetSpeedValidation() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = test::Bytes("speed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append should succeed");
  test::Require(recorder.Close(&error), error);

  std::vector<std::uint64_t> seen;
  auto session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromRecordSequence(0),
      jojo::rec::ReplayOptions{}, std::make_unique<CollectingTarget>(&seen), &error);
  test::Require(session != nullptr, error);

  test::Require(!session->SetSpeed(-1.0, &error), "negative replay speed should be rejected");
  test::Require(error.find("speed") != std::string::npos,
                "validation error should mention speed");
}

void TestReplaySetSpeedDuringPlayback() {
  test::TempDir temp_dir;
  auto config = test::MakeConfig(temp_dir.Path());
  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  test::Require(recorder.IsOpen(), error);

  auto payload = test::Bytes("speed");
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 1 should succeed");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  test::Require(recorder.Append(MakeMessage(payload)) == jojo::rec::AppendResult::kOk,
                "append 2 should succeed");
  test::Require(recorder.Close(&error), error);

  std::vector<std::chrono::steady_clock::time_point> dispatch_times;
  auto target = std::make_unique<SpeedChangingTarget>(&dispatch_times, 10.0);
  SpeedChangingTarget* target_ptr = target.get();
  jojo::rec::ReplayOptions options;
  options.speed = 0.25;
  auto session = jojo::rec::CreateReplaySession(
      recorder.RecordingPath(), jojo::rec::ReplayCursor::FromRecordSequence(0), options,
      std::move(target), &error);
  test::Require(session != nullptr, error);
  target_ptr->SetSession(session.get());
  test::Require(session->Start(&error), error);
  const jojo::rec::ReplayResult result = session->Wait();
  test::Require(result.completed, "speed-adjusted replay should complete");
  test::RequireEqual(static_cast<std::uint64_t>(dispatch_times.size()), 2,
                     "speed-adjusted replay should deliver both records");

  const auto dispatch_gap =
      std::chrono::duration_cast<std::chrono::milliseconds>(dispatch_times[1] -
                                                            dispatch_times[0]);
  test::Require(dispatch_gap < std::chrono::milliseconds(250),
                "SetSpeed during replay should shorten the next inter-message delay");
}

}  // 匿名命名空间

int RunTests() {
  int failures = 0;
  failures += test::RunTest("queue_buffer_count_validation", TestQueueBufferCountValidation);
  failures += test::RunTest("dictionary_snapshot_and_finalize_path",
                            TestDictionarySnapshotAndFinalizePath);
  failures += test::RunTest("fail_fast_backpressure", TestFailFastBackpressure);
  failures += test::RunTest("two_queue_alternating_receive", TestTwoQueueAlternatingReceive);
  failures += test::RunTest("additional_queue_buffers_accept_more_oversized_messages",
                            TestAdditionalQueueBuffersAcceptMoreOversizedMessages);
  failures += test::RunTest("segment_rotation_and_repair_without_truncate",
                            TestSegmentRotationAndRepairWithoutTruncate);
  failures += test::RunTest("replay_segment_checkpoint_seek", TestReplaySegmentCheckpointSeek);
  failures += test::RunTest("replay_seek_and_failure_stop", TestReplaySeekAndFailureStop);
  failures += test::RunTest("replay_seek_during_callback", TestReplaySeekDuringCallback);
  failures += test::RunTest("replay_set_speed_validation", TestReplaySetSpeedValidation);
  failures += test::RunTest("replay_set_speed_during_playback",
                            TestReplaySetSpeedDuringPlayback);
  return failures;
}
