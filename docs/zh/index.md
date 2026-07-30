# MiniDist 教程

[English](../index.md)

MiniDist 在同一个确定性进程内实验室中比较复制协议家族。请按顺序阅读：前半部建立比较方法、仿真器、共享词汇与异步主从机制；后半部从换主 fencing 进入 Raft，最后用并排实验收束。

## 全书目录

1. [为什么是协议谱系](tutorial/01-why-a-spectrum.md)——为什么它不是第 101 个 mini-Raft，以及协议谱系要回答哪些取舍问题。
2. [确定性仿真基座](tutorial/02-deterministic-simulation.md)——`SimNet`、`SimClock`、`Scheduler`、`FailureInjector` 与逐事件 `Trace` 重放。
3. [复制组抽象](tutorial/03-replication-group.md)——`ReplicationGroup`、确认/读取级别，以及显式拒绝不支持的保证。
4. [异步主从](tutorial/04-async-primary.md)——replica sink、offset、有界 backlog、部分重同步和全量回退。
5. [换主与世代栅栏](tutorial/05-failover-fencing.md)——promotion、generation/lineage fence、主动收敛与旧 full-sync 拒绝。
6. [Raft I：选举](tutorial/06-raft-election.md)——任期、随机超时、投票规则与选举安全。
7. [Raft II：日志与提交](tutorial/07-raft-replication.md)——AppendEntries 一致性、当前任期提交与 crash 恢复。
8. [对照实验课](tutorial/08-experiments.md)——完整解释实验 1–3：确认写丢失、双主与任期 fencing、读取一致性。

## 使用方式

执行 `uv sync --dev` 后，从仓库根目录运行命令。每段实测输出都绑定具体命令与源码路径。simulation tick 只记录事件顺序，绝不表示毫秒或性能。练习若要求改代码，参考 patch 只放在折叠 note 内；请在临时副本或练习分支操作，后续章节不依赖这些修改。

[机制映射](mapping.md)、[实验指南](labs-guide.md)与[行为/实验矩阵](experiments.md)是参考资料。请把矩阵视为支持边界：规划中的行不是实现证据。[项目 README](https://github.com/system-in-miniature/mini-dist/blob/main/README.zh-CN.md)记录全仓范围与安装要求。
