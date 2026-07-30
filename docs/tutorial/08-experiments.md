# Chapter 8: Seven Experiments Across Four Protocols

[中文版](../zh/tutorial/08-experiments.md)

MiniDist now places four replication families over one deterministic simulator:
an asynchronous primary, PostgreSQL-shaped WAL shipping, Kafka-shaped ISR/HW,
and Raft. This chapter is the comparison desk. Seven labs vary one fault or
consistency boundary at a time, then interpret why similar final values can
come from different acknowledgement, membership, history, and authority rules.

## Learning objectives

By the end of this chapter, you will be able to:

1. identify the controlled boundary and observable in all seven experiments;
2. compare all four protocols without treating shared enum names as equal
   guarantees;
3. explain slow-replica availability and reconnect behavior from membership
   and retention rules;
4. read the complete `ReadLevel × protocol` matrix, including unsupported and
   blocked outcomes; and
5. distinguish a stale local read from an authority-checked read after
   leadership changes.

## 1. Evidence rules for the comparison

The shared vocabulary lives in `src/minidist/protocols/types.py`; concrete
mechanisms live under `async_primary/group.py`, `wal_shipping/group.py`,
`isr/group.py`, and `raft/group.py`. Every lab uses public group methods and
returns dataclasses that `tests/labs/test_experiments.py` asserts. `SimClock`,
`Scheduler`, `SimNet`, and `Trace` make one chosen event history repeatable.

A tick is ordering, never milliseconds. These labs do not benchmark latency,
throughput, disks, or real sockets. Keep four moments separate: state before a
fault, the requested acknowledgement boundary, state while authority is
ambiguous, and state after convergence. A final equal dictionary alone does
not reveal whether the client previously received an unsafe success.

Experiments 1–3 predate protocols 2/3, so their runnable scripts directly expose
async and Raft columns. The WAL/ISR cells in the full matrix are backed by
focused protocol tests and the same public transitions, not by pretending
those scripts accept nonexistent `--protocol wal|isr` flags. Experiments 4–7
directly run all four columns.

## 2. Experiments 1–3: acknowledgement and authority

### Experiment 1 — normal replication

Run:

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

Measured output:

```text
实验 1：正常复制（异步主从）
1) 写入在 offset=1 返回 ack；primary 立即 ack，复制仍在途中。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=1, follower=1；副本已收敛。
实验 1：正常复制（Raft）
1) 写入在 offset=2 返回 ack；多数派复制后才 ack，随后 leaderCommit 传播到全部 follower。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=2, follower=2；副本已收敛。
```

`AsyncPrimaryGroup.client_write()` returns after local apply and message
scheduling. `RaftGroup.client_write()` waits for majority commit, although the
observed third node may not yet have learned `leaderCommit`. WAL `LEADER`
instead returns after the primary's simulated WAL flush; WAL `QUORUM` waits for
every configured synchronous standby. ISR `LEADER` returns after local append;
`ALL_ISR` waits for every current ISR member and HW. Four systems can all
eventually converge while crossing four different success boundaries.

### Experiment 2 — crash immediately after acknowledgement

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

Measured result:

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
实验 2：ack 后立即 crash leader（Raft）
1) client 写入 order:42=confirmed；leader 在 offset=2 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash node-1，随后完成 failover。
4) 新 leader=node-2，读取 order:42：b'confirmed'。
结论：多数派确认的写在换主后仍在。
```

No delivery tick is inserted before the async crash. A stale replica becomes
authoritative and the local-only acknowledgement disappears. Raft's committed
entry is held by a majority, and its voting freshness rule prevents a candidate
without that prefix from winning.

For WAL shipping, the result depends on the requested mode and promotion
candidate: a `LEADER` write is locally flushed but may not yet exist on an
asynchronous standby; a successful `QUORUM` write has been flushed by every
configured synchronous standby. For ISR, successful `ALL_ISR` means the entry
is at or below HW on all current ISR members; clean controller election chooses
one of those members and truncates only above HW. Preconditions—not protocol
brand names—explain survival.

### Experiment 3 — old leader in a minority

```bash
uv run python labs/exp03_partition_old_leader.py --protocol both
```

Measured conclusions:

```text
实验 3：异步主从的双主脏写
1) 隔离旧 primary primary；其本地写 ack=True。
2) 显式 promote replica-1；新 primary 本地写 ack=True。
4) 愈合后按新 primary 世代全量收敛：converged=True, old=None, new=b'new-primary'。
结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。

实验 3：旧 leader 位于少数派的 Raft 网络分区
1) term=1 的旧 leader node-1 被双向隔离；其新写 QUORUM ack=False。
2) 多数派在 term=3 选出 node-3；继续写入的 QUORUM ack=True。
3) 分区愈合后旧 leader 角色=follower；日志收敛=True。
4) 最终状态：minority-only=None, majority=b'continues'。
```

Async manual promotion creates a new generation and replication ID; delivery
fences select that history, but both local primaries could acknowledge dirty
writes during the partition. WAL promotion similarly increments a timeline and
rejects queued old-timeline bytes; this protects branch application but does
not make an async commit present on the promoted standby. ISR's controller
increments leader epoch, chooses an ISR member, and truncates to HW. Raft's
majority elects a higher-term leader, then AppendEntries conflict repair
overwrites the uncommitted suffix. A fence selects authority; a separate
catch-up mechanism converges data.

## 3. Experiment 4: one slow replica

Run:

```bash
uv run python labs/exp04_slow_replica.py
```

Measured output:

```text
实验 4 [async]：write_available=True
slow_replica_stale=True; membership=replica is not a voter
实验 4 [wal]：write_available=True
slow_replica_stale=True; membership=async standby does not gate commit
实验 4 [isr]：write_available=True
slow_replica_stale=True; membership=removed from ISR
实验 4 [raft]：write_available=True
slow_replica_stale=True; membership=fixed voter; majority unaffected
```

The Boolean column is intentionally insufficient. Async replicas never vote.
The WAL lab configures the slow standby as asynchronous, so local-flush
`LEADER` commit does not wait; configuring it as synchronous would change the
result for `QUORUM`. ISR detects lag and changes its dynamic membership, while
remaining writable only because two members still meet
`min.insync.replicas=2`. Raft membership stays fixed; two of three nodes still
form a majority. “Available with one slow copy” comes from four mechanisms.

## 4. Experiment 5: reconnect inside and outside retention

```bash
uv run python labs/exp05_replica_reconnect.py
```

Measured output:

```text
实验 5 [async]：inside_window=incremental; outside_window=full
实验 5 [wal]：inside_window=incremental; outside_window=full
实验 5 [isr]：inside_window=incremental; outside_window=full
实验 5 [raft]：inside_window=log replay; outside_window=log replay
```

Async checks replication ID plus bounded backlog coverage. WAL checks timeline
plus retained LSN coverage. ISR checks whether the follower's next offset still
exists in the retained leader log and only re-adds it after catch-up. Each uses
atomic whole-state replacement outside its teaching window. Raft is the
deliberate exception: M3 implements no snapshot or compaction, so `nextIndex`
fallback replays its unbounded log for both sizes. “Full” is not a universal
protocol operation, and absence of full transfer is not automatically better;
the log is allowed to grow without bound.

## 5. Experiment 6: the closing read-consistency matrix

```bash
uv run python labs/exp06_read_consistency.py
```

Measured output:

```text
实验 6：ReadLevel × 协议
async: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
wal: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
isr: LOCAL=stale, LEADER=fresh, LINEARIZABLE=blocked
raft: LOCAL=stale, LEADER=fresh, LINEARIZABLE=fresh
```

| Protocol | `LOCAL` observation | `LEADER` observation | Authority-checked result |
|---|---|---|---|
| Async primary | lagging replica is stale | current primary has `v` | `LINEARIZABLE` unsupported |
| WAL shipping | lagging standby is stale | primary has locally flushed `v` | `LINEARIZABLE` unsupported |
| ISR + HW | lagging follower is stale | leader exposes append above HW | blocked because HW/lease proof is absent |
| Raft | isolated follower is stale | current leader has committed `v` | fresh after read-index quorum |

This is the book's closing diagram because it prevents two common collapses.
First, `LEADER=fresh` describes this schedule's observed value, not a general
authority proof. A partitioned old leader can still return an old local value.
Second, unsupported and blocked differ: async/WAL implement no such guarantee;
ISR has a guard and refuses because this particular entry is above HW; Raft
obtains the current-term quorum evidence.

## 6. Experiment 7: stale old side and authority guard

```bash
uv run python labs/exp07_split_brain_lease.py
```

Measured output:

```text
实验 7 [async]：old_LOCAL=b'before'; current=b'after'; authority_guard=unsupported
实验 7 [wal]：old_LOCAL=b'before'; current=b'after'; authority_guard=unsupported
实验 7 [isr]：old_LOCAL=b'before'; current=b'after'; authority_guard=lease fenced
实验 7 [raft]：old_LOCAL=b'before'; current=b'after'; authority_guard=read-index fenced
```

All old sides retain a readable pre-change value, proving that write fencing
does not erase stale local state. Async generation and WAL timeline checks
fence incoming replication traffic, but neither protocol supplies a
linearizable/lease read guard. ISR `lease_read()` requires controller ownership,
an unexpired logical lease, sufficient ISR, and no append above HW. Raft's
linearizable path requires a current-term read-index majority. The latter two
both fail closed, but one is a pedagogical controller/HW lease and the other a
quorum protocol operation.

## 7. Verification and support boundary

Run the asserted lab suite:

```bash
uv run pytest -q tests/labs/test_experiments.py
```

Measured output:

```text
...........                                                              [100%]
11 passed in 0.07s
```

The assertions cover six original async/Raft runs plus all four columns for
experiments 4–7. The [experiment matrix](../experiments.md) records every
cell, while Chapters [9](09-wal-shipping.md) and [10](10-isr.md) provide the
new protocol mechanisms.

These experiments prove deterministic in-process state transitions. They do
not prove network interoperability, wall-clock availability, disk behavior,
Byzantine safety, Redis/PostgreSQL/Kafka wire compatibility, or production
operations. The explicit `lose_unfsynced=True` paths model one durability
boundary, not real storage.

## 8. Exercises

### Understanding

1. In experiment 4, why is `write_available=True` not a common guarantee?
2. Why does ISR `LINEARIZABLE=blocked` convey more information than
   `unsupported`?
3. Which two independent mechanisms repair an old Raft leader after a
   partition?

??? note "Reference answers"

    1. The setup requests different supported levels and changes membership
       differently: no vote, async standby, dynamic ISR, or fixed majority.
    2. ISR implements an HW/lease/min-insync guard and the current state fails
       it. Unsupported protocols offer no such path at all.
    3. A higher term establishes authority and forces step-down; log prefix
       checks, suffix deletion, and `nextIndex` retry repair data.

### Hands-on

4. Write a temporary pytest that calls experiment 6 twice and asserts the two
   complete matrices are equal. Then assert the four `LINEARIZABLE` statuses
   in protocol order.

??? note "Reference answer"

    ```python
    from labs.exp06_read_consistency import run_experiment

    def test_read_matrix_replays():
        first = run_experiment(verbose=False)
        assert first == run_experiment(verbose=False)
        assert [first[p]["LINEARIZABLE"].status for p in
                ("async", "wal", "isr", "raft")] == [
            "unsupported", "unsupported", "blocked", "fresh"
        ]
    ```

    Save it under `/tmp` and run
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch08.py`.

## Summary

Seven experiments turn the protocol spectrum into observable boundaries.
Normal convergence hides different acknowledgement rules; failover exposes
which successful writes reached a future authority; partitions separate
fencing from data repair; slow and reconnecting replicas reveal static,
configured, and dynamic membership; and the final read matrix separates
location from authority evidence. Shared APIs make the questions comparable,
while explicit refusal, HW blocking, timeline/epoch checks, and read-index keep
the answers honestly different.
