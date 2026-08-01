> **Language**: [English](README.md) | 简体中文

# MiniDist

[![CI](https://github.com/system-in-miniature/mini-dist/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/mini-dist/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

MiniDist 是 System-in-Miniature 系列的第六个教学项目：在同一个确定性
仿真环境里比较分布式复制协议的谱系，而不是把某一个协议包装成生产库。
M3 已完成四协议谱系：Redis 式异步主从、PostgreSQL 式 WAL 运输、
Kafka 式 ISR + HW 与教科书 Raft，并在同一套七组实验中对照。

## 为什么不是第 101 个 mini-Raft

Raft 的选举与日志复制已有成熟教材。真实系统仍没有统一采用教科书 Raft：
Kafka 用同步副本集合（in-sync replicas, ISR）+ 高水位（high watermark, HW）
换取更少副本和高吞吐，Redis 接受异步复制可能丢失已确认写，Postgres 运输
预写日志（write-ahead log, WAL）字节并允许同步级别可调，Qdrant 一类系统
则只让元数据（metadata）走共识。MiniDist 要回答的是这些取舍为何不同。

因此本项目采用：

- 同一个最简 KV 状态机，让协议差异而非业务代码成为变量；
- 同一套故障与逻辑时钟，让实验脚本可逐事件重放；
- 不强行抹平语义：协议不支持 `QUORUM` 或 `LINEARIZABLE` 时明确报错；
- 全进程内仿真，不把真实网络、性能数字或生产可靠性混入教学结论。

**M3 已完成。** 四个协议都实现共同 replication-group 接口，实验 1–7
具有四列证据矩阵；FailureInjector 也提供显式“已 ack 但未 fsync”断电丢失
窗口。两个新协议均有同 seed Trace 回放测试。

## 学习模式

- **机制教程**——按概念与机制阅读协议谱系，入口见[双语教材](docs/zh/index.md)。
- **自主重建**——通过十个可独立浏览的 Stage 重建项目，每节包含测试证据与按机制
  分组的 Diff，入口见[重建旅程](docs/zh/journey/index.md)。
- **Agent 带教**——让 Codex 在终端中互动讲解、实现并验收一个 Stage，使用方式见
  [CLI 教程](docs/zh/agent-guided.md)。

## 快速开始

要求 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync --dev
uv run pytest -q
uv run python labs/exp01_normal_replication.py
uv run python labs/exp02_acked_write_loss.py
uv run python labs/exp03_partition_old_leader.py
uv run python labs/exp04_slow_replica.py
uv run python labs/exp05_replica_reconnect.py
uv run python labs/exp06_read_consistency.py
uv run python labs/exp07_split_brain_lease.py
```

实验 2 会先得到成功 ack，然后在复制消息送达前 crash primary 并 promote
旧副本，最后展示新 primary 上读不到该写。

实验 3 并排输出两种协议：被隔离的异步旧 primary 与手动提升的新 primary
都会接受本地脏写；Raft 则通过任期 fencing 拒绝旧 leader 的 quorum 写。

## 目录导览

```text
src/minidist/
├── sim/                         # 逻辑时钟、调度、网络故障、生命周期、Trace
└── protocols/
    ├── types.py                 # ReplicationGroup 的共同实验词汇
    ├── async_primary/           # replica sink、offset、backlog、手动 promote
    ├── wal_shipping/            # 逻辑 WAL 字节、同步 standby、timeline
    ├── isr/                     # controller、ISR、HW、min.insync、leader epoch
    └── raft/                    # 任期选举、日志复制、提交规则、崩溃恢复
labs/                            # 可直接运行的实验 1–7
tests/                           # 仿真、协议和实验断言
docs/mapping.md                  # 协议机制 ↔ 真实系统对照
docs/experiments.md              # 四协议 × 七实验对照表
```

## 确定性边界

MiniDist 不使用墙上时钟、真实线程或全局随机状态。时间只由 `tick()` 推进；
网络随机选择只来自构造时传入的 seed；同 seed、同脚本应生成逐事件相同的
`Trace`。仿真 tick 只表示事件顺序，不是毫秒或性能指标。
