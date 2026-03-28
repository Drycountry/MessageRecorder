# Message Recorder v1 实现计划（合并版）

## Summary
- 以绿地项目方式搭建 `C++17 + CMake` 跨平台实现，正式目标平台为 Windows 和 Linux；项目交付 `录制/回放核心库 + 检查 CLI + 自动化测试`，核心模块尽量只依赖 C++17 标准库。
- 文件格式从首版起按稳定格式实现；物理布局固定为“录制目录包”：单个 `manifest.json` + 多个顺序命名的二进制 `segment` 文件。
- 录制目录由库自动管理：`Recorder` 创建时只接收输出 root 目录，启动时创建 `recording-<start_utc>`，正常 `Close()` 后原子重命名为 `recording-<start_utc>-to-<stop_utc>`；异常退出时保留开始时间目录，并在 manifest 标记 `incomplete/degraded`。
- 配置面走最小配置，只暴露录制与回放必要参数；`segment` 只按大小轮转，不提供按时间轮转；`high_precision_mode` 的 busy-wait 窗口使用库内固定默认值，不进入配置面。
- `Recorder::Append()` 在返回前完成消息副本写入内部队列；背压支持 `Block` 和 `FailFast`，默认 `Block`；队列容量按字节数建模，而不是按消息条数建模。
- 回放模型采用异步 `IReplaySession`；session 自带后台线程，回放调度以 `event_mono_ts` 为权威时钟，普通模式用 `sleep/yield`，高精度模式在固定短窗口内 busy-wait；按时间 `Seek()` 时落到第一条 `timestamp >= target` 的消息，并允许通过 `SetSpeed()` 在运行中动态调整倍率。
- 检查 CLI 至少提供 `summary`、`verify`、`repair`、`dump`；`verify` 严格只读，`repair` 为独立命令，且只重建元数据，不物理截断或改写任何 `segment`。
- 工程约束固定：统一使用 Doxygen 注释、Google C++ Coding Style、`jojo::rec` 命名空间、`.hpp/.cpp` 文件后缀；所有源码和配置文件统一使用 UTF-8 编码且不带 BOM；所有类数据成员必须写注释，函数体内关键语句和关键逻辑块必须写注释。

## Key Changes
- 建立三个目标：核心库、检查 CLI、自动化测试；项目结构按公共头文件、核心实现、CLI 分层组织，避免录制、格式、回放逻辑互相耦合。
- 所有库代码、内部实现和公共接口统一声明在 `namespace jojo::rec` 中；允许使用 `jojo::rec::internal` 等子命名空间承载内部细节，但对外 API 根命名空间固定不变。
- 录制侧实现 `Ingress Sequencer + 单写线程 Writer`。`Recorder::Append()` 在入口分配 `record_seq` 并采集 `steady_clock` / `system_clock` 时间戳，然后把消息副本写入内部队列；写线程仅按 `record_seq` 顺序落盘。
- 队列容量使用字节计量，至少覆盖 `payload`、`attributes` 和固定记录开销；槽位或条目状态仍显式区分 `ready`、`aborted`、`consumed`。生产者拿到 `record_seq` 后若未完成填充，必须把条目标记为 `aborted`，manifest 统计异常计数并把录制标记为 `degraded`。
- segment 采用 little-endian 二进制记录区、周期索引区和 footer；轮转条件只有 `segment_max_mb`，单位为 MB。manifest 维护版本、最小配置快照、开始/停止 UTC、段列表、类型字典、统计摘要、质量状态，以及每段逻辑有效边界等恢复元数据。
- manifest 在 rotate、`Flush()` 检查点和 `Close()` 时原子重写。异常退出或执行 `repair` 时，通过 segment 顺序扫描恢复最后一段，再重建 manifest 统计、质量状态和每段逻辑有效末尾；不补写目录停止时间，不物理截断 segment，不要求回写 segment 内部索引/footer。若尾段索引缺失，回放允许退化为段内顺扫。
- 回放引擎实现异步 session 模型：`Start()/Pause()/Resume()/SetSpeed()/Seek()/Stop()` 作为控制接口，后台线程负责调度和投递，完成态与错误通过 `Wait()` 返回。`IReplayTarget::OnMessage(...)` 返回失败即停止回放并上报错误。
- `seek` 统一先命中索引，再做段内顺扫；按 `record_seq` 精确定位，按 `event_mono_ts` 或 `event_utc_ts_us` 使用 lower_bound 语义，落到第一条 `timestamp >= target` 的消息；若 target 晚于末条记录则返回 EOF / not found。
- CLI 命令职责固定：`summary` 查看统计与质量状态，`verify` 只读校验，`repair` 重建 manifest 级恢复元数据，`dump` 按 cursor 输出头字段和长度摘要。CLI 必须同时识别“开始时间目录”和“start-to-stop 目录”。
- 工程风格固化为可执行约束：仓库级格式化配置默认按 Google 风格排版；公共头文件、类定义、格式结构、线程/状态机相关实现补充 Doxygen 注释；类定义中的每个数据成员都说明语义、单位/时间基、所有权、线程可见性或状态含义；函数实现中的关键语句重点注释时间戳采集、序号分配、内存拷贝、同步原语、状态转换、错误分支、恢复和 seek 定位逻辑；所有新增代码文件、构建脚本和文档默认以 UTF-8 无 BOM 写入。

## Public APIs / Interfaces
- `AppendResult` 固定为 `Ok | Backpressure | Closed | InternalError`；`Backpressure` 仅允许出现在 `FailFast` 模式。
- `Recorder` 创建接口只接受“输出 root 目录”语义，不接受调用方直接指定最终 recording 目录名。
- `IReplayTarget::OnMessage` 需要返回显式成功/失败状态；失败时 `IReplaySession` 立即停止，并把失败 `record_seq`、`event_utc_ts_us` 和错误原因返回调用方。
- `ReplayCursor` 支持 `record_seq`、`event_mono_ts`、`event_utc_ts_us`、`segment/checkpoint` 四类入口；内部统一解析为“目标段 + 段内起始偏移”，其中时间 seek 明确采用“第一条 `>= target`”语义。
- `IReplaySession` 的默认公共模型固定为“每个 session 一个后台线程”的异步控制对象；除 `Start()/Pause()/Resume()/Seek()/Stop()/Wait()` 外，还支持 `SetSpeed()` 在运行时动态调整倍率；`high_precision_mode` 只保留开关语义，不新增 busy-wait 窗口配置字段。
- 所有公共类型、公共函数、类数据成员都使用 Doxygen 注释，至少说明 `@brief`、参数/返回值、线程安全、所有权、阻塞行为、时间语义、错误条件和成员字段语义；对外头文件统一为 `.hpp`，并位于 `jojo::rec` 对应 include 层级下。

## Test Plan
- 并发录制测试覆盖多线程 `Recorder::Append()`、入口时间戳采集点、`aborted` 条目、字节容量背压、`Block`/`FailFast` 两种模式，以及 `Close()` 期间并发 `Append()` 的边界。
- 格式测试覆盖 manifest JSON 兼容性、segment 编解码、类型字典快照、little-endian 读写、按大小 rotate 后多段读取，以及崩溃后最后一段扫描恢复。
- 目录命名测试覆盖：录制开始时自动创建 `recording-<start_utc>`，正常 `Close()` 后重命名为 `recording-<start_utc>-to-<stop_utc>`，异常退出后保持开始时间目录名不变，CLI/恢复工具对两种目录名都能正确识别。
- 轮转与队列测试覆盖：segment 仅在达到 `segment_max_mb` 指定的 MB 阈值时轮转，长时间低流量不会因时间推进而切段；队列背压由字节容量而不是消息条数触发。
- 索引与 seek 测试覆盖按 `record_seq`、`event_mono_ts`、`event_utc_ts_us` 的定位；验证精确命中、落在两条消息之间、早于首条、晚于末条四类场景都满足“第一条 `>= target`”语义，同时验证时间窗口索引和记录数保护阈值在低流量与突发流量下都生效。
- 回放测试覆盖 1x、快放、慢放、运行中动态调速、暂停/恢复、Seek() 后继续播放、高精度模式固定默认窗口下的抖动上限，以及 `IReplayTarget` 返回失败时的立即停止语义。
- CLI 测试覆盖 `summary`、`verify`、`repair`、`dump` 的正常路径和 degraded/损坏文件路径；`verify` 前后 manifest 与 segment 的内容和长度必须完全不变；`repair` 能重建 manifest 统计、质量状态和逻辑有效末尾，但 segment 文件长度保持不变。
- 恢复测试覆盖损坏尾段经 `repair` 后只回放到 manifest 标记的逻辑有效末尾，不读取保留的脏尾部字节；尾段缺失索引时能够回退为顺扫。
- 工程检查增加风格与注释验证：格式化结果需稳定符合 Google 风格；公共 API、类数据成员和关键实现路径需具备完整注释，并能被 Doxygen 正确解析；同时验证命名空间和文件后缀约定只使用 `jojo::rec`、`.hpp`、`.cpp`；新增和修改文件需验证为 UTF-8 且无 BOM。

## Assumptions And Defaults
- 当前仓库不继承既有 ABI、文件格式或外部依赖约束，按新模块独立设计。
- UTC 时间字符串统一使用无分隔紧凑格式：`YYYYMMDDTHHMMSSZ`，用于目录名和 manifest 中的可读时间字段。
- segment 记录头默认使用固定宽度整数字段：`record_seq` / 时间戳用 `uint64`，长度字段用 `uint32`，flags 用 `uint32`；这是 v1 稳定格式的一部分。
- 校验默认基于顺序扫描和 footer 索引边界，不再依赖额外校验码。
- `Flush()` 和 `Close()` 都按阻塞语义实现；`Close()` 必须等待写线程排空已接受消息、完成 manifest 最终落盘，并在成功后执行目录重命名。
- `repair` 的写入范围只包括 manifest 及其派生恢复元数据；不改写、不截断任何 segment 正文。
- 回放异步 session 的 v1 默认模型就是内置线程会话；后续若扩展外部执行器，也不改变本版既定行为。
- `high_precision_mode` 的 busy-wait 窗口先使用库内固定默认值；该默认值属于实现细节，不纳入公共配置面。
- 代码排版默认使用以 Google 为基线的 `clang-format` 配置；命名、头文件组织、注释习惯和接口可读性以 Google C++ Coding Style 为准。
- 所有仓库内源码、构建脚本、配置和文档默认使用 UTF-8 编码且不带 BOM，避免跨平台编译、工具链和 diff 行为不一致。

