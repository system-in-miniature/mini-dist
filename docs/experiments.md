> **Language**: English | [简体中文](zh/experiments.md)

# Replication Protocol Experiment Matrix

A `tick` is a deterministic ordering step, not wall-clock time. Every filled
cell below is backed by a protocol pytest or a public-API lab assertion.

| Experiment | Protocol 1: asynchronous primary-replica | Protocol 2: WAL shipping | Protocol 3: ISR + HW | Protocol 4: Raft |
|---|---|---|---|---|
| 1. Normal replication | `LEADER` returns before the replica applies; the replica converges after stream delivery. | `LEADER` returns after local WAL flush but before an asynchronous standby applies the ordered WAL bytes; `QUORUM` waits for every configured synchronous standby to flush. | `LEADER` may expose a follower gap; `ALL_ISR` returns only after every current ISR member has the offset and HW reaches it. | `QUORUM` returns after majority replication; commit propagation then converges every live state machine. |
| 2. Kill leader after ack | An immediate promotion of a stale replica loses the acknowledged write. | Async (`LEADER`) commit may be absent from the promoted standby; sync (`QUORUM`) commit is present on every configured synchronous promotion candidate. | An `ALL_ISR` write is below HW on all ISR members, so a clean controller election preserves it. | A QUORUM-committed prefix survives and is read after the majority elects a new leader. |
| 3. Old leader in minority | Both manually designated primaries can accept local dirty writes; the new generation wins on heal. | Promotion increments timeline; queued WAL from the old timeline is rejected before apply and the standby follows the new branch. | The controller increments leader epoch; old-epoch appends are fenced and clean election starts from HW. | The old leader cannot commit; a higher-term majority leader overwrites the uncommitted suffix after heal. |
| 4. Slow replica | The primary remains available because the replica is not a voter; the slow replica is stale. | With an asynchronous standby, local commit remains available; a configured synchronous standby would gate `QUORUM`. | Controller removes the lagging follower from ISR; writes continue while the remaining ISR still satisfies `min.insync.replicas`. | Membership stays fixed, but one slow follower does not prevent a three-node majority from committing. |
| 5. Lagging replica reconnects | Within backlog: partial/incremental resync. Outside backlog or wrong lineage: full snapshot replacement. | Within retained WAL: incremental record shipping. Before the oldest retained LSN or on a new timeline: base backup/full catch-up. | Within retained leader log: incremental fetch. Before the retained start: full snapshot catch-up; rejoin ISR only after reaching HW. | The unbounded M3 log always repairs through `nextIndex` fallback and incremental log replay; snapshot/full transfer is v2. |
| 6. Read-consistency matrix | `LOCAL`: stale on a lagging replica. `LEADER`: fresh but no authority proof. `LINEARIZABLE`: unsupported. | `LOCAL`: stale on a lagging standby. `LEADER`: fresh but no lease proof. `LINEARIZABLE`: unsupported. | `LOCAL`: stale. `LEADER`: may expose an uncommitted leader append. `LINEARIZABLE`: blocked until HW/lease/min.insync prove authority, then fresh. | `LOCAL`: stale on a lagging follower. `LEADER`: fresh but may be a partitioned old leader. `LINEARIZABLE`: fresh after a current-term read-index quorum. |
| 7. Split-brain read and lease | Old primary `LOCAL` returns the pre-promotion value; no linearizable/lease guard is offered. | Old timeline primary `LOCAL` returns the pre-promotion value; timeline fences writes, not standalone stale reads, and no lease read is offered. | Old leader `LOCAL` is stale; controller ownership plus the logical leader lease rejects the displaced leader. | Old leader `LOCAL` is stale; the current leader's read-index succeeds, while a partitioned old leader cannot obtain a quorum barrier. |

## Read-Level Result Matrix

Experiment 6 records status rather than flattening all failures into “not
stale”:

| Protocol | LOCAL | LEADER | LINEARIZABLE |
|---|---|---|---|
| Async primary | stale | fresh | unsupported |
| WAL shipping | stale | fresh | unsupported |
| ISR + HW | stale | fresh but uncommitted | blocked without HW lease |
| Raft | stale | fresh | fresh after read-index |

## How to Run

```bash
uv run python labs/exp04_slow_replica.py
uv run python labs/exp05_replica_reconnect.py
uv run python labs/exp06_read_consistency.py
uv run python labs/exp07_split_brain_lease.py
uv run pytest -q tests/labs/test_experiments.py
```

Experiments 4, 5, and 7 accept `--protocol async|wal|isr|raft|all`. The default
prints all four columns. Experiment 6 always prints the complete 3 × 4 matrix.
The scripts call only replication-group public APIs; they do not invoke RPC
handlers or mutate scheduler/network internals.

## Determinism and Durability Boundaries

Both new protocols have same-seed, event-for-event trace replay tests. The
explicit `lose_unfsynced=True` crash is a power-loss injection: it rolls a node
back to its last simulated fsync. Ordinary `crash(node)` retains accepted
persistent state. Raft persists term/vote/log changes before dependent RPC
responses; WAL `LEADER` flushes the primary, WAL `QUORUM` also flushes every
synchronous standby; Kafka-style replication exposes page-cache-like unfsynced
copies but protects `ALL_ISR` through multiple replicas; the asynchronous
primary can acknowledge an unfsynced local dataset.
