> **Language**: English | [简体中文](zh/experiments.md)

# Replication Protocol Experiment Matrix

A `tick` is a logical time step, not wall-clock time or a performance number.
The conclusions for protocols 1 and 4 come from this repository's labs +
pytest; protocols 2/3 remain placeholders for later milestones.

| Experiment | Protocol 1: asynchronous primary-replica | Protocol 2: WAL transport | Protocol 3: ISR + HW | Protocol 4: Raft |
|---|---|---|---|---|
| 1. Normal replication | **Asserted**: the primary acks at tick 0; the replica's initial read is empty, and its offset and value converge after the replication message is delivered | _Not implemented_ | _Not implemented_ | **Asserted**: elect a leader first; the write returns a QUORUM ack only after majority replication; leaderCommit then propagates, and the three-node state machines and commit indexes converge |
| 2. Kill the leader after ack | **Asserted**: crash the primary without advancing a tick, then promote a stale replica; the new primary cannot read the acknowledged write | _Not implemented_ | _Not implemented_ | **Asserted**: crash the leader immediately after the QUORUM ack; the majority that holds the log prefix elects a new leader, and a read-index read still returns `confirmed` |
| 3. Network partition containing the old leader in the minority | **Asserted**: isolate the old primary, let it acknowledge a local dirty write, then explicitly promote a replica; the new primary also acknowledges a different local dirty write. During the partition each side sees only its own write; after healing, the old node full-syncs to the new generation and all replicas converge to the new primary's history. Separate protocol regressions prove that queued old-generation delivery is rejected | _Not implemented_ | _Not implemented_ | **Asserted**: the old leader's local append cannot receive a QUORUM ack; the majority elects a leader in a higher term and continues committing; after healing, the old leader steps down, its conflicting suffix is rolled back, and it converges with the new leader |
| 4. Slow replica | _Future experiment_ | _Not implemented_ | _Not implemented_ | _Not implemented_ |
| 5. Lagging replica reconnects | _Future experiment_ | _Not implemented_ | _Not implemented_ | _Not implemented_ |
| 6. Read-consistency matrix | _Future experiment_ | _Not implemented_ | _Not implemented_ | _Not implemented_ |
| 7. Split-brain read | _Future experiment_ | _Not implemented_ | _Not implemented_ | _Not implemented_ |

## How to Run

```bash
uv run python labs/exp01_normal_replication.py
uv run python labs/exp01_normal_replication.py --protocol raft
uv run python labs/exp02_acked_write_loss.py
uv run python labs/exp02_acked_write_loss.py --protocol raft
uv run python labs/exp03_partition_old_leader.py
uv run pytest -q tests/labs/test_experiments.py
```

The three labs call only the replication group's public APIs. The asynchronous
path uses write, read, `tick`, `run_until_idle`, `crash`, `isolate`, `heal`,
`promote`, and `probe`;
the Raft path uses write, read, `run_until_leader`, `run_until_converged`,
`crash`, `isolate`, `heal`, and `probe`. They do not directly call the network,
scheduler, RPC handlers, or internal delivery hooks to manufacture their
conclusions.

## Determinism Boundary

Raft's network choices and election timeouts are derived only from the `seed`
injected through the constructor. The replay test performs one initial election,
one partition of the old leader, one majority-side primary change, and one
healing convergence, then asserts that the complete `Trace.as_dicts()` from two
runs with the same seed are identical item by item. Logical ticks, message IDs,
timeout samples, and state-transition order are all part of the replay contract.
