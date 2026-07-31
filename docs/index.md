# MiniDist Tutorial / MiniDist 教程

[Chinese edition / 中文版](zh/index.md)

MiniDist compares four replication protocol families in one deterministic,
in-process laboratory. Read the chapters in order: the first half establishes
the comparison method, simulator, shared vocabulary, and asynchronous-primary
mechanism; the second half follows failover fencing into Raft, compares seven
experiments, then opens the WAL-shipping and ISR/HW mechanisms behind the two
middle columns.

MiniDist 在同一个确定性进程内实验室中比较四种复制协议家族。请按顺序阅读：
前半部建立比较方法、仿真器、共享词汇与异步主从机制；后半部从换主 fencing
进入 Raft，用七个实验对照全谱系，再展开两列中间协议的 WAL shipping 与
ISR/HW 机制。

## Chapters / 全书目录

1. [Why a Protocol Spectrum? / 为什么是协议谱系](tutorial/01-why-a-spectrum.md) — why this is not
   the 101st mini-Raft, and which trade-off questions the spectrum exposes. /
   为什么它不是第 101 个 mini-Raft，以及协议谱系要回答哪些取舍问题。
2. [The Deterministic Simulation Foundation / 确定性仿真基座](tutorial/02-deterministic-simulation.md)
   — `SimNet`, `SimClock`, `Scheduler`, `FailureInjector`, and event-for-event
   `Trace` replay. / `SimNet`、`SimClock`、`Scheduler`、`FailureInjector`
   与逐事件 `Trace` 重放。
3. [The Replication Group Abstraction / 复制组抽象](tutorial/03-replication-group.md) —
   `ReplicationGroup`, acknowledgement/read levels, and explicit refusal of
   unsupported guarantees. / `ReplicationGroup`、确认/读取级别，以及显式拒绝
   不支持的保证。
4. [Asynchronous Primary–Replica Replication / 异步主从复制](tutorial/04-async-primary.md) —
   replica sinks, offsets, bounded backlog, partial resync, and full fallback. /
   replica sink、offset、有界 backlog、部分重同步和全量回退。
5. [Failover and Generation Fencing / 换主与世代栅栏](tutorial/05-failover-fencing.md) —
   promotion, generation/lineage fences, active convergence, and stale
   full-sync rejection. / promotion、generation/lineage fence、主动收敛与旧
   full-sync 拒绝。
6. [Raft I: Election / Raft I：选举](tutorial/06-raft-election.md) — terms, randomized
   timeouts, voting rules, and election safety. / 任期、随机超时、投票规则与选举安全。
7. [Raft II: Log Replication and Commit / Raft II：日志与提交](tutorial/07-raft-replication.md) —
   AppendEntries consistency, current-term commit, and crash recovery. /
   AppendEntries 一致性、当前任期提交与崩溃恢复。
8. [Seven Experiments Across Four Protocols / 七个实验对照四种协议](tutorial/08-experiments.md) —
   acknowledgement loss, slow replicas, reconnect windows, split-brain
   authority, and the closing read-consistency matrix. / 确认写丢失、慢副本、
   重连窗口、脑裂权威与收官的读一致性矩阵。
9. [WAL Shipping, Flush Boundaries, and Timelines / WAL 运输、Flush 边界与 Timeline](tutorial/09-wal-shipping.md)
   — logical WAL bytes, asynchronous/synchronous commit, retention, base
   backup, and promotion timeline fencing. / 逻辑 WAL 字节、异步/同步 commit、
   retention、base backup 与 promotion timeline fencing。
10. [ISR, High Watermark, and Controller Epochs / ISR、High Watermark 与 Controller Epoch](tutorial/10-isr.md) — dynamic
    ISR membership, `min.insync.replicas`, HW, catch-up, clean election,
    leader-epoch fencing, and guarded reads. / 动态 ISR membership、
    `min.insync.replicas`、HW、追赶、clean election、leader-epoch fencing
    与 guarded read。

## How to use the book / 使用方式

Run commands from the repository root after `uv sync --dev`. Every pasted
output is tied to a command and a source path. A simulation tick records event
ordering, never milliseconds or performance. Exercises that propose code
changes keep their reference patches inside folded notes; apply them only in a
scratch branch or temporary copy, never as a prerequisite for later chapters.

执行 `uv sync --dev` 后，从仓库根目录运行命令。每段实测输出都绑定具体命令与
源码路径。simulation tick 只记录事件顺序，绝不表示毫秒或性能。练习若要求改
代码，参考 patch 只放在折叠 note 内；请在临时副本或练习分支操作，后续章节
不依赖这些修改。

The [mechanism mapping](mapping.md), [lab guide](labs-guide.md), and
[behavior/experiment matrix](experiments.md) are reference material. Use the
matrix as the support boundary: planned rows are not implementation evidence.
The [project README](https://github.com/system-in-miniature/mini-dist#readme)
records repository-wide scope and installation requirements.

[机制映射](mapping.md)、[实验指南](labs-guide.md)与
[行为/实验矩阵](experiments.md)是参考资料。请把矩阵视为支持边界：规划中的行
不是实现证据。[项目中文 README](https://github.com/system-in-miniature/mini-dist/blob/main/README.zh-CN.md)
记录全仓范围与安装要求。
