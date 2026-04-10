#include "jojo/rec/recorder.hpp"

#include "jojo/rec/detail/recorder_worker.hpp"

namespace jojo::rec {

Recorder::Recorder(const RecorderConfig& config, std::string* error) : config_(config) {
  worker_ = std::make_unique<internal::RecorderWorker>(config_);
  if (!worker_->Initialize(error)) {
    worker_.reset();
  }
}

Recorder::~Recorder() = default;

Recorder::Recorder(Recorder&& other) noexcept = default;

Recorder& Recorder::operator=(Recorder&& other) noexcept = default;

bool Recorder::IsOpen() const { return worker_ != nullptr && worker_->IsOpen(); }

const RecorderConfig& Recorder::Config() const { return config_; }

std::filesystem::path Recorder::RecordingPath() const {
  return worker_ == nullptr ? std::filesystem::path{} : worker_->RecordingPath();
}

AppendResult Recorder::Append(const RecordedMessage& message) {
  return worker_ == nullptr ? AppendResult::kClosed : worker_->Append(message);
}

bool Recorder::Flush(std::string* error) {
  if (worker_ == nullptr) {
    if (error != nullptr) {
      *error = "recorder is not open";
    }
    return false;
  }
  return worker_->Flush(error);
}

bool Recorder::Close(std::string* error) {
  if (worker_ == nullptr) {
    return true;
  }
  return worker_->Close(error);
}

}  // namespace jojo::rec
