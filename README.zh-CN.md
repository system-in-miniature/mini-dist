> **Language**: [English](README.md) | 简体中文

# MiniDist

[![CI](https://github.com/system-in-miniature/MiniDist/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/MiniDist/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

MiniDist 是 System-in-Miniature 系列的第六个教学项目：在同一个确定性
仿真环境里比较分布式复制协议的谱系，而不是把某一个协议包装成生产库。
当前里程碑交付了仿真基座和谱系两端：协议 1（Redis 式异步主从复制）与
协议 4（教科书 Raft），并在 labs 中对照演示。

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

当前已实现 M1 与 M2：协议 1（异步主从）与协议 4（Raft）。协议 2
（Postgres 式 WAL 运输）与协议 3（Kafka 式 ISR + HW）规划在 M3，尚未实现。

## 快速开始

要求 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync --dev
uv run pytest -q
uv run python labs/exp01_normal_replication.py
uv run python labs/exp02_acked_write_loss.py
```

实验 2 会先得到成功 ack，然后在复制消息送达前 crash primary 并 promote
旧副本，最后展示新 primary 上读不到该写。

## 目录导览

```text
src/minidist/
├── sim/                         # 逻辑时钟、调度、网络故障、生命周期、Trace
└── protocols/
    ├── types.py                 # ReplicationGroup 的共同实验词汇
    ├── async_primary/           # replica sink、offset、backlog、手动 promote
    └── raft/                    # 任期选举、日志复制、提交规则、崩溃恢复
labs/                            # 可直接运行的实验 1/2/3
tests/                           # 仿真、协议和实验断言
docs/mapping.md                  # 协议机制 ↔ 真实系统对照
docs/experiments.md              # 跨协议实验对照表（协议 1 与 4）
```

## 确定性边界

MiniDist 不使用墙上时钟、真实线程或全局随机状态。时间只由 `tick()` 推进；
网络随机选择只来自构造时传入的 seed；同 seed、同脚本应生成逐事件相同的
`Trace`。仿真 tick 只表示事件顺序，不是毫秒或性能指标。
