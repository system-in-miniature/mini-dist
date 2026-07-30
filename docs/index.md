# MiniDist Tutorial

[中文版](zh/index.md)

MiniDist compares replication protocol families inside one deterministic,
in-process simulation. The current implementation places Redis-style
asynchronous primary-replica replication beside textbook Raft so that
acknowledgement, failover, partition, and fencing semantics can be observed
under the same fault model.

## Install

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/system-in-miniature/MiniDist.git
cd MiniDist
uv sync --dev
```

## First experiment

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

Both runs converge on `b'MiniDist'`, but their acknowledgement boundaries
differ: the asynchronous primary acknowledges before delivery, while Raft
acknowledges only after majority replication.

## Reading path

Start with the repository layout in Chapter 2, then read the mechanism mapping.
Run all three experiments before using the experiment matrix to compare
supported semantics and explicit non-goals.

See the [English README](https://github.com/system-in-miniature/MiniDist#readme)
for the full milestone boundary. This repository currently has no
`docs/superpowers/` design-history archive; the README, mapping, experiment
matrix, and executable tests are its available sources of truth.
