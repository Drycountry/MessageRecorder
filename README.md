# Message Recorder

`Message Recorder` 是一个基于 C++17 和 CMake 的本地消息录制/回放组件，当前仓库提供四部分能力：

- `message_recorder`：核心库，负责把消息写入录制目录并回放。
- `message_recorder_cli`：检查工具，支持摘要、校验、修复和头部转储。
- `message_recorder_benchmark`：录制吞吐量压测工具。
- `message_recorder_tests`：自动化测试。

项目当前 `CMakeLists.txt` 版本号为 `0.1.0`，核心实现依赖 C++17 标准库，不依赖第三方运行时库。

## 功能概览

- 录制消息到目录包，目录内包含 `manifest.json` 和多个 `segment-*.seg` 文件。
- `Append()` 按记录序号顺序写入；录制器内部使用可配置数量的接收队列缓冲，支持阻塞式背压和快速失败两种模式。
- 录制结束后，目录会从 `recording-<start_utc>` 重命名为 `recording-<start_utc>-to-<stop_utc>`。
- 支持按记录序号、单调时间、UTC 时间或 segment checkpoint 开始回放。
- 支持 `summary`、`verify`、`repair`、`dump` 四个 CLI 命令。
- `repair` 只重建 `manifest.json` 元数据，不截断或改写已有 `segment` 正文。

## 仓库结构

```text
.
├─ include/jojo/rec/
│  ├─ recorder.hpp
│  ├─ replay.hpp
│  ├─ inspect.hpp
│  └─ types.hpp
├─ src/
├─ cli/main.cpp
├─ benchmarks/
├─ tests/
├─ docs/PLAN.md
└─ CMakeLists.txt
```

## 构建要求

- CMake 3.20+
- 支持 C++17 的编译器
- Windows 或 Linux

## 构建方法

### Windows

```powershell
cmake -S . -B build
cmake --build build --config Debug
```

如果你想生成发布版本：

```powershell
cmake --build build --config Release
```

常见产物路径：

- `build\Debug\message_recorder_cli.exe`
- `build\Debug\message_recorder_benchmark.exe`
- `build\Debug\message_recorder_tests.exe`
- `build\Debug\message_recorder.lib`

### Linux

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

常见产物路径：

- `build/message_recorder_cli`
- `build/message_recorder_benchmark`
- `build/message_recorder_tests`
- `build/libmessage_recorder.a`

## 运行测试

Windows 多配置生成器：

```powershell
ctest --test-dir build -C Debug --output-on-failure
```

Linux 或单配置生成器：

```bash
ctest --test-dir build --output-on-failure
```

## 运行 Benchmark

Windows:

```powershell
.\build\Debug\message_recorder_benchmark.exe --messages 500000 --payload-bytes 512 --threads 4
```

Linux:

```bash
./build/message_recorder_benchmark --messages 500000 --payload-bytes 512 --threads 4
```

常用参数：

- `--messages <count>`：总消息数。
- `--payload-bytes <count>`：每条消息的 payload 字节数。
- `--attributes-bytes <count>`：每条消息的 attributes 字节数。
- `--threads <count>`：并发生产者线程数。
- `--queue-capacity-mb <count>`：每个内部接收队列的容量，单位为 MB。
- `--queue-buffer-count <count>`：内部接收队列数量，最小为 `2`。
- `--segment-max-mb <count>`：segment 轮转阈值，单位为 MB。
- `--fail-fast`：背压时立即返回，不阻塞。
- `--keep-output`：保留 benchmark 生成的录制目录。

输出会包含：

- `benchmark.append_messages_per_sec`
- `benchmark.end_to_end_messages_per_sec`
- `benchmark.logical_mib_per_sec`
- `benchmark.disk_mib_per_sec`
- `benchmark.logical_mb_per_sec`
- `benchmark.disk_mb_per_sec`
- `benchmark.append_logical_mb_per_sec`

### 队列数量与吞吐量

在当前实现中，吞吐量同时受 `总队列空间` 和 `队列数量` 影响，但实测上通常是 `总队列空间` 更重要，`队列数量` 更像二级调优项。

下面这组数据使用统一负载：

- `1,000,000` 条消息
- `payload=512B`
- `threads=4`
- `segment-max-mb=256`
- 每组运行 `3` 次并取平均

按总队列空间横向对齐后的端到端吞吐量如下：

| 总队列空间 | `2` 队列 | `4` 队列 | `8` 队列 |
| --- | --- | --- | --- |
| `8 MB` | `2 x 4 MB` = `426.02 Kop/s`, `218.12 MB/s` | - | - |
| `16 MB` | `2 x 8 MB` = `426.55 Kop/s`, `218.39 MB/s` | `4 x 4 MB` = `425.38 Kop/s`, `217.79 MB/s` | - |
| `32 MB` | `2 x 16 MB` = `428.50 Kop/s`, `219.39 MB/s` | `4 x 8 MB` = `421.39 Kop/s`, `215.75 MB/s` | `8 x 4 MB` = `428.58 Kop/s`, `219.43 MB/s` |
| `64 MB` | - | `4 x 16 MB` = `438.88 Kop/s`, `224.71 MB/s` | `8 x 8 MB` = `443.28 Kop/s`, `226.96 MB/s` |
| `128 MB` | - | - | `8 x 16 MB` = `475.03 Kop/s`, `243.22 MB/s` |

从这组结果可以得到两个直接结论：

- 在相同 `总队列空间` 下，`2/4/8` 队列之间的吞吐量差异通常很小，多数情况下在 `0%` 到 `2%` 量级。
- 随着 `总队列空间` 从 `8/16/32 MB` 提升到 `64/128 MB`，吞吐量提升更明显，因此如果内存预算允许，优先增大 `总队列空间` 往往比反复微调 `队列数量` 更有效。

## 录制目录格式

录制开始后，库会在 `output_root` 下创建一个目录：

```text
recording-<start_utc>/
├─ manifest.json
├─ segment-000000.seg
├─ segment-000001.seg
└─ ...
```

正常调用 `Close()` 后，目录会被整理为：

```text
recording-<start_utc>-to-<stop_utc>/
```

时间格式使用紧凑 UTC 字符串：

```text
YYYYMMDDTHHMMSSZ
```

## CLI 使用方法

CLI 入口：

```text
message_recorder_cli summary <recording_path>
message_recorder_cli verify <recording_path>
message_recorder_cli repair <recording_path>
message_recorder_cli dump <recording_path> [cursor_kind] [value] [max_records]
```

### `summary`

读取 `manifest.json`，输出录制概览。

Windows:

```powershell
.\build\Debug\message_recorder_cli.exe summary D:\data\recording-20260326T142800Z-to-20260326T142805Z
```

Linux:

```bash
./build/message_recorder_cli summary /data/recording-20260326T142800Z-to-20260326T142805Z
```

### `verify`

只读校验录制目录与磁盘上的 `segment` 是否一致，不修改任何文件。

```powershell
.\build\Debug\message_recorder_cli.exe verify D:\data\recording-20260326T142800Z-to-20260326T142805Z
```

### `repair`

重新扫描 `segment`，重建 `manifest.json` 的统计和边界信息。该命令不会截断 `segment` 文件。

```powershell
.\build\Debug\message_recorder_cli.exe repair D:\data\recording-20260326T142800Z-to-20260326T142805Z
```

### `dump`

输出记录头摘要，默认从 `seq 0` 开始，默认最多输出 `10` 条。

可用 `cursor_kind`：

- `seq`：从第一条 `record_seq >= value` 的记录开始。
- `mono`：从第一条 `event_mono_ts_us >= value` 的记录开始。
- `utc`：从第一条 `event_utc_ts_us >= value` 的记录开始。
- `checkpoint`：从 `segment_index >= value` 的位置开始。

示例：

```powershell
.\build\Debug\message_recorder_cli.exe dump D:\data\recording-20260326T142800Z-to-20260326T142805Z seq 0 20
.\build\Debug\message_recorder_cli.exe dump D:\data\recording-20260326T142800Z-to-20260326T142805Z mono 1234567890 20
.\build\Debug\message_recorder_cli.exe dump D:\data\recording-20260326T142800Z-to-20260326T142805Z utc 1743000000000000 20
.\build\Debug\message_recorder_cli.exe dump D:\data\recording-20260326T142800Z-to-20260326T142805Z checkpoint 1 20
```

## 库使用方法

### 1. 最小录制示例

```cpp
#include <cstdint>
#include <iostream>
#include <vector>

#include "jojo/rec/recorder.hpp"

int main() {
  jojo::rec::RecorderConfig config;
  config.output_root = "recordings";
  config.queue_capacity_mb = 4;
  config.queue_buffer_count = 2;
  config.segment_max_mb = 32;
  config.backpressure_policy = jojo::rec::BackpressurePolicy::kBlock;
  config.recording_label = "demo";
  config.message_type_names = {{1U, "alpha"}};

  std::string error;
  jojo::rec::Recorder recorder(config, &error);
  if (!recorder.IsOpen()) {
    std::cerr << "failed to open recorder: " << error << "\n";
    return 1;
  }

  const std::vector<std::uint8_t> payload = {'h', 'e', 'l', 'l', 'o'};
  jojo::rec::RecordedMessage message;
  message.session_id = 7;
  message.message_type = 1;
  message.message_version = 1;
  message.payload = jojo::rec::ByteView{payload.data(), payload.size()};

  const auto result = recorder.Append(message);
  if (result != jojo::rec::AppendResult::kOk) {
    std::cerr << "append failed\n";
    return 1;
  }

  if (!recorder.Flush(&error)) {
    std::cerr << "flush failed: " << error << "\n";
    return 1;
  }

  if (!recorder.Close(&error)) {
    std::cerr << "close failed: " << error << "\n";
    return 1;
  }

  std::cout << "recording written to: " << recorder.RecordingPath() << "\n";
  return 0;
}
```

说明：

- `Append()` 返回前会复制 `payload` 和 `attributes`，所以调用方只需保证原始缓冲区在本次调用结束前有效。
- `queue_capacity_mb` 表示单个内部队列的容量；瞬时排队内存上限通常约为 `queue_buffer_count x queue_capacity_mb`，再加一条正在写入的消息。
- `BackpressurePolicy::kBlock` 会在所有内部队列都耗尽时阻塞调用线程。
- `BackpressurePolicy::kFailFast` 会在所有内部队列都耗尽时直接返回 `AppendResult::kBackpressure`。

### 2. 最小回放示例

```cpp
#include <iostream>
#include <memory>
#include <string>

#include "jojo/rec/replay.hpp"

class PrintingTarget final : public jojo::rec::IReplayTarget {
 public:
  bool OnMessage(const jojo::rec::ReplayMessage& message, std::string* error) override {
    (void)error;
    std::cout << "record_seq=" << message.record_seq
              << " type=" << message.message_type
              << " payload_size=" << message.payload.size() << "\n";
    return true;
  }
};

int main() {
  std::string error;
  jojo::rec::ReplayOptions options;
  options.speed = 1.0;
  options.high_precision_mode = false;

  auto session = jojo::rec::CreateReplaySession(
      "recordings/recording-20260326T142800Z-to-20260326T142805Z",
      jojo::rec::ReplayCursor::FromRecordSequence(0),
      options,
      std::make_unique<PrintingTarget>(),
      &error);

  if (session == nullptr) {
    std::cerr << "failed to create replay session: " << error << "\n";
    return 1;
  }

  if (!session->Start(&error)) {
    std::cerr << "failed to start replay: " << error << "\n";
    return 1;
  }

  if (!session->SetSpeed(2.0, &error)) {
    std::cerr << "failed to update replay speed: " << error << "\n";
    return 1;
  }

  const jojo::rec::ReplayResult result = session->Wait();
  if (!result.completed) {
    std::cerr << "replay did not complete cleanly\n";
    return 1;
  }

  return 0;
}
```

回放定位方式：

- `ReplayCursor::FromRecordSequence(n)`
- `ReplayCursor::FromEventMonoTime(us)`
- `ReplayCursor::FromEventUtcTime(us)`
- `ReplayCursor::FromSegmentCheckpoint(index)`

时间定位采用 lower-bound 语义，也就是从第一条 `timestamp >= target` 的记录开始。

回放速度控制：

- `ReplayOptions::speed` 用于设置初始回放倍率，`1.0` 为实时，`0.0` 为尽快回放。
- `IReplaySession::SetSpeed(x)` 可在回放运行过程中动态调整倍率。

### 3. 读取摘要和校验结果

如果你不想走 CLI，也可以直接调用检查接口：

```cpp
#include <iostream>
#include <string>

#include "jojo/rec/inspect.hpp"

int main() {
  jojo::rec::RecordingSummary summary;
  std::string error;
  if (!jojo::rec::LoadRecordingSummary("recordings/recording-20260326T142800Z-to-20260326T142805Z",
                                       &summary, &error)) {
    std::cerr << error << "\n";
    return 1;
  }

  std::cout << "total_records=" << summary.total_records << "\n";

  const jojo::rec::VerifyResult verify =
      jojo::rec::VerifyRecording("recordings/recording-20260326T142800Z-to-20260326T142805Z");
  std::cout << "verify.success=" << verify.success << "\n";
  return 0;
}
```

## 集成到你的 CMake 工程

如果你的工程把本仓库作为子目录引入，可以直接链接目标：

```cmake
add_subdirectory(path/to/MessageRecorder)
target_link_libraries(your_target PRIVATE message_recorder)
```

头文件包含路径：

```cpp
#include "jojo/rec/recorder.hpp"
#include "jojo/rec/replay.hpp"
#include "jojo/rec/inspect.hpp"
```

## 当前已验证内容

当前仓库中，以下测试目标已经可运行：

```powershell
ctest --test-dir build -C Debug --output-on-failure
```

验证覆盖了：

- 字典快照与最终目录命名
- 快速失败背压与多队列交替接收
- segment 轮转与 `repair` 不截断文件
- 按时间 seek 回放、运行中动态调速与目标失败停止





