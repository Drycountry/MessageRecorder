# Benchmark

`message_recorder_benchmark` 用于压测录制吞吐量。

默认输出：

- append 阶段消息吞吐量
- 端到端消息吞吐量
- 逻辑字节吞吐量
- 磁盘字节吞吐量

示例：

```powershell
.\build\Debug\message_recorder_benchmark.exe --messages 500000 --payload-bytes 512 --threads 4
```
