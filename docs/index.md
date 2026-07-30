# MiniDist Tutorial

[中文版](zh/index.md)

MiniDist compares replication protocol families in one deterministic,
in-process laboratory. Read the chapters in order: the first half establishes
the comparison method, simulator, shared vocabulary, and asynchronous-primary
mechanism; the second half follows failover fencing into Raft and closes with
side-by-side experiments.

## Chapters

1. [Why a Protocol Spectrum?](tutorial/01-why-a-spectrum.md) — why this is not
   the 101st mini-Raft, and which trade-off questions the spectrum exposes.
2. [The Deterministic Simulation Foundation](tutorial/02-deterministic-simulation.md)
   — `SimNet`, `SimClock`, `Scheduler`, `FailureInjector`, and event-for-event
   `Trace` replay.
3. [The Replication Group Abstraction](tutorial/03-replication-group.md) —
   `ReplicationGroup`, acknowledgement/read levels, and explicit refusal of
   unsupported guarantees.
4. [Asynchronous Primary–Replica Replication](tutorial/04-async-primary.md) —
   replica sinks, offsets, bounded backlog, partial resync, and full fallback.
5. [Failover and Generation Fencing](tutorial/05-failover-fencing.md) —
   promotion, generation/lineage fences, active convergence, and stale
   full-sync rejection.
6. [Raft I: Election](tutorial/06-raft-election.md) — terms, randomized
   timeouts, voting rules, and election safety.
7. [Raft II: Log Replication and Commit](tutorial/07-raft-replication.md) —
   AppendEntries consistency, current-term commit, and crash recovery.
8. [Comparative Experiment Lab](tutorial/08-experiments.md) — complete
   interpretation of labs 1–3: acknowledgement loss, split brain versus term
   fencing, and read consistency.

## How to use the book

Run commands from the repository root after `uv sync --dev`. Every pasted
output is tied to a command and a source path. A simulation tick records event
ordering, never milliseconds or performance. Exercises that propose code
changes keep their reference patches inside folded notes; apply them only in a
scratch branch or temporary copy, never as a prerequisite for later chapters.

The [mechanism mapping](mapping.md), [lab guide](labs-guide.md), and
[behavior/experiment matrix](experiments.md) are reference material. Use the
matrix as the support boundary: planned rows are not implementation evidence.
The [project README](https://github.com/system-in-miniature/mini-dist#readme)
records repository-wide scope and installation requirements.
