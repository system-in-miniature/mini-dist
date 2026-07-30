> **Language**: English | [简体中文](README.zh-CN.md)

# MiniDist

[![CI](https://github.com/system-in-miniature/mini-dist/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/mini-dist/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

MiniDist is the sixth educational project in the System-in-Miniature series. It
compares families of distributed replication protocols in the same deterministic
simulation environment instead of packaging any single protocol as a production
library. M3 completes the four-protocol spectrum: Redis-style asynchronous
primary-replica, PostgreSQL-style WAL shipping, Kafka-style ISR + HW, and
textbook Raft, compared under the same seven experiments.

## Why This Is Not the 101st mini-Raft

Raft election and log replication already have mature teaching materials. Yet
real systems have not uniformly adopted textbook Raft: Kafka uses ISR + HW to
trade fewer replicas for high throughput, Redis accepts that asynchronous
replication can lose acknowledged writes, Postgres transports WAL bytes and
allows configurable synchronization levels, and systems such as Qdrant put only
metadata through consensus. MiniDist asks why these tradeoffs differ.

The project therefore uses:

- the same minimal KV state machine, so protocol differences rather than
  application code are the variable;
- the same fault model and logical clock, so experiment scripts can be replayed
  event by event;
- no forced semantic flattening: when a protocol does not support `QUORUM` or
  `LINEARIZABLE`, it reports an explicit error;
- an entirely in-process simulation, so real networks, performance numbers, and
  production reliability are not mixed into the educational conclusions.

**M3 is complete.** All four protocols implement the shared replication-group
surface, experiments 1–7 have a four-column evidence matrix, and the failure
injector exposes an explicit acknowledged-but-unfsynced power-loss window.
Same-seed replay covers both new protocols.

## Quick Start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

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

Experiment 2 first receives a successful ack, then crashes the primary before
the replication message is delivered and promotes the stale replica. It finally
shows that the write cannot be read from the new primary.

Experiment 3 prints the protocols side by side: an isolated asynchronous old
primary and a manually promoted new primary both accept dirty local writes,
while Raft rejects the isolated old leader's quorum write through term fencing.

## Repository Layout

```text
src/minidist/
├── sim/                         # logical clock, scheduler, network faults, lifecycle, Trace
└── protocols/
    ├── types.py                 # shared experiment vocabulary (ReplicationGroup)
    ├── async_primary/           # replica sink, offset, backlog, manual promote
    ├── wal_shipping/            # logical WAL bytes, sync standbys, timeline
    ├── isr/                     # controller, ISR, HW, min.insync, leader epoch
    └── raft/                    # term election, log replication, commit rule, crash recovery
labs/                            # runnable experiments 1–7
tests/                           # simulation, protocol, and experiment assertions
docs/mapping.md                  # protocol mechanisms ↔ real-system counterparts
docs/experiments.md              # four-protocol × seven-experiment matrix
```

## Determinism Boundary

MiniDist does not use wall-clock time, real threads, or global random state. Time
advances only through `tick()`; random network choices come only from the seed
passed to the constructor; the same seed and script should produce an
event-for-event identical `Trace`. A simulation tick represents only event
ordering, not milliseconds or a performance metric.
