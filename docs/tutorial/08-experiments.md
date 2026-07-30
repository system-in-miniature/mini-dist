# Chapter 8: Comparative Experiment Lab

MiniDist is valuable because it runs different replication semantics over the
same state machine, logical clock, network, and fault vocabulary. This final
chapter reads experiments 1–3 as controlled comparisons: normal convergence,
acknowledged-write loss, and minority partition. It closes with read
consistency, where similar method names deliberately do not imply equal
guarantees.

## Learning objectives

By the end of this chapter, you will be able to:

- identify the independent variable and observed boundary in each lab;
- explain why asynchronous and quorum acknowledgements diverge after failure;
- distinguish split-brain dirty writes from Raft term fencing;
- compare local, leader-routed, and linearizable reads without flattening them;
- reproduce all six protocol runs and connect output to source and tests.

## 1. How to read a deterministic experiment

All three labs use public `ReplicationGroup` operations defined by the shared
vocabulary in `src/minidist/protocols/types.py`: write, read, tick, crash,
restart, isolate, heal, and probe. The implementations are
`AsyncPrimaryGroup` in
`src/minidist/protocols/async_primary/group.py` and `RaftGroup` in
`src/minidist/protocols/raft/group.py`.

The common substrate controls nuisance variables. Both groups use `SimClock`,
`Scheduler`, `SimNet`, and `Trace`; the same seed and script produce the same
event ordering. A tick is not elapsed real time. These labs compare semantics,
not throughput, latency, durability, or behavior on real sockets.

Each printed result is backed by assertions in
`tests/labs/test_experiments.py`. The prose output helps a human interpret the
run; the returned dataclasses give tests exact facts. A conclusion should be no
broader than the state transition that the script controls.

Keep three moments separate while reading a run: the state before the fault,
the operation's acknowledgement boundary, and the state after recovery or
convergence. Comparing only the final dictionaries would hide whether a client
was told that an unsafe write had succeeded. Comparing only the acknowledgement
would hide the authoritative state eventually selected. The experiment is the
relationship among all three observations.

## 2. Experiment 1: normal replication

`labs/exp01_normal_replication.py:run_experiment()` writes
`course=MiniDist`, observes a follower at the acknowledgement boundary, drives
the system to convergence, and compares the final value and offset.

### Asynchronous primary

Run:

```bash
uv run python labs/exp01_normal_replication.py --protocol async
```

Measured output:

```text
实验 1：正常复制（异步主从）
1) 写入在 offset=1 返回 ack；primary 立即 ack，复制仍在途中。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=1, follower=1；副本已收敛。
```

In `AsyncPrimaryGroup.client_write()`, the primary changes its dictionary and
returns after scheduling replica messages. `AsyncPrimaryGroup.tick()` is what
advances delivery. Therefore the empty follower at the ack boundary is expected
behavior, not a race in the lab. Later convergence proves eventual delivery in
this no-fault schedule.

### Raft

Run:

```bash
uv run python labs/exp01_normal_replication.py --protocol raft
```

Measured output:

```text
实验 1：正常复制（Raft）
1) 写入在 offset=2 返回 ack；多数派复制后才 ack，随后 leaderCommit 传播到全部 follower。
2) ack 边界处，观察 follower 状态机：None。
3) 推进至收敛后，观察 follower 状态机：b'MiniDist'。
4) 最终 offset：leader=2, follower=2；副本已收敛。
```

`RaftGroup.client_write()` calls `_wait_for_commit()`, so its success boundary
requires majority commit. Yet one selected follower may still show `None`
immediately. The majority can be leader plus the *other* follower, and
commit-index propagation to every node happens on later AppendEntries. Quorum
acknowledgement means a majority-backed committed log entry, not synchronous
application on all nodes. Offset 2 includes the election no-op at index 1.

The controlled result is not “both protocols are equally safe because both
converge.” It is that both converge without faults while exposing different ack
boundaries.

## 3. Experiment 2: crash immediately after ack

`labs/exp02_acked_write_loss.py:run_experiment()` writes
`order:42=confirmed`, reads it from the current leader, crashes that leader
without adding an unrelated delivery tick, performs failover, and reads from
the new leader.

### Asynchronous primary: acknowledged write lost

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
```

Measured output:

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
```

The write result is true because `AsyncPrimaryGroup.client_write()` defines
`LEADER` as local application. `AsyncPrimaryGroup.crash()` marks the node dead
and partitions its links, so the queued packet cannot leak across the intended
fault boundary. `AsyncPrimaryGroup.promote()` makes the stale replica
authoritative. The missing read is the expected counterexample to interpreting
this ack as failover durability.

### Raft: quorum-confirmed write survives

```bash
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

Measured output:

```text
实验 2：ack 后立即 crash leader（Raft）
1) client 写入 order:42=confirmed；leader 在 offset=2 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash node-1，随后完成 failover。
4) 新 leader=node-2，读取 order:42：b'confirmed'。
结论：多数派确认的写在换主后仍在。
```

Here `RaftGroup._advance_commit()` required majority storage of a current-term
entry before the client succeeded. `RaftGroup._receive_request_vote()` permits
votes only for a candidate whose log is at least as up to date. The new
majority leader therefore contains the committed prefix. The final
`ReadLevel.LINEARIZABLE` call additionally obtains a read-index quorum before
returning `confirmed`.

This result assumes MiniDist's persistent fields survive the modeled crash. It
does not test disks, correlated failures, or loss of a majority.

## 4. Experiment 3: old leader in a minority

`labs/exp03_partition_old_leader.py` isolates the old authority
bidirectionally, permits the majority side to continue, heals the partition,
and checks convergence.

### Asynchronous protocol: two accepted dirty histories

```bash
uv run python labs/exp03_partition_old_leader.py --protocol async
```

Measured output:

```text
实验 3：异步主从的双主脏写
1) 隔离旧 primary primary；其本地写 ack=True。
2) 显式 promote replica-1；新 primary 本地写 ack=True。
3) 分区中两侧状态：旧侧 old=b'old-primary', new=None；新侧 old=None, new=b'new-primary'。
4) 愈合后按新 primary 世代全量收敛：converged=True, old=None, new=b'new-primary'。
结论：协议 1 没有任期 fencing；分区中两侧都可确认本地脏写。
```

The old side accepts because `AsyncPrimaryGroup.client_write()` waits for no
peer. The lab manually calls `promote()` on the other side, which also accepts
locally. This is genuine split-brain dirty state in the experiment, not two
committed consensus histories. On healing, generation/lineage fencing and full
synchronization choose the new primary's history; the old-side write is
discarded rather than merged.

### Raft: minority rejection and log repair

```bash
uv run python labs/exp03_partition_old_leader.py --protocol raft
```

Measured output:

```text
实验 3：旧 leader 位于少数派的 Raft 网络分区
1) term=1 的旧 leader node-1 被双向隔离；其新写 QUORUM ack=False。
2) 多数派在 term=3 选出 node-3；继续写入的 QUORUM ack=True。
3) 分区愈合后旧 leader 角色=follower；日志收敛=True。
4) 最终状态：minority-only=None, majority=b'continues'。
结论：高任期完成 fencing，少数派未提交分叉被新 leader 日志覆盖。
```

The isolated leader can append locally, but
`RaftGroup._wait_for_commit()` cannot observe a quorum and returns false. Two
connected nodes elect a higher-term leader and commit. After healing,
`RaftGroup._receive_append_entries()` makes the old leader step down;
`prevLogIndex`/`prevLogTerm` rejection and
`_receive_append_response()` retry move `nextIndex` backward until the new
leader can overwrite the uncommitted fork.

The new leader appears in term 3 for this seed and schedule. The important
claim is monotonic higher-term authority and quorum behavior, not that every
execution elects in exactly term 3.

## 5. Read consistency is another independent choice

The shared enum does not promise that both protocols implement every level.
`AsyncPrimaryGroup.client_read()` supports:

- `LOCAL`: read a selected node, possibly stale;
- `LEADER`: route to the manually current primary, without a quorum proof.

It rejects `LINEARIZABLE`. `RaftGroup.client_read()` rejects `LOCAL`, provides a
direct `LEADER` read, and implements a simplified `LINEARIZABLE` barrier. For
the barrier it creates a read context, broadcasts empty AppendEntries in the
current term, and waits for successful responses from a majority before
reading the leader's applied dictionary.

A direct Raft `LEADER` read is intentionally weaker: the locally known leader
may be partitioned and unaware of a newer term. Experiment 3's old leader
illustrates that risk. The read-index round proves current-term majority
reachability in this model. It does not append the read command to the log.

The [experiment behavior
matrix](../experiments.md) and [DIFFERENCES
mapping](../mapping.md#differences-between-this-implementation-and-textbook-raft)
state the boundary: this is a single-round educational barrier, without leases,
batching, cross-thread apply waits, or full client-session machinery.

## 6. What these experiments do and do not prove

The repository's [full experiment matrix](../experiments.md) marks protocol 1
and protocol 4 conclusions as asserted and protocols 2/3 as not implemented.
Do not fill those blank columns by analogy.

The labs prove deterministic state and fault semantics for two in-process
implementations. They do not prove:

- real network interoperability or wire compatibility;
- performance, latency, or availability percentages;
- disk durability, fsync ordering, or power-loss recovery;
- Redis Sentinel/Cluster correctness;
- production Raft membership changes, snapshots, storage, or client sessions;
- safety under Byzantine nodes.

This honesty does not weaken the lab. It identifies the controlled variable:
acknowledgement, authority, log, and read rules under the same crash and
partition schedule.

## 7. Test the experiment claims

Run:

```bash
uv run pytest -q tests/labs/test_experiments.py
```

Measured output:

```text
......                                                                   [100%]
6 passed in 0.03s
```

These are the six experiment assertions: normal async, normal Raft, failover
loss, failover survival, async split brain, and Raft fencing.

For protocol mechanisms as well as lab outputs:

```bash
uv run pytest -q tests/protocols/test_async_primary.py \
  tests/protocols/test_raft.py tests/labs/test_experiments.py
```

Measured output:

```text
............................                                             [100%]
28 passed in 0.06s
```

The exact duration may vary by machine; the pass count and zero failures are
the acceptance facts.

## 8. Exercises

### Understanding

1. In experiment 1, why may the observed Raft follower still be empty when the
   QUORUM write has succeeded?
2. In experiment 3, what separate jobs are performed by a higher term and by
   AppendEntries conflict repair?

??? note "Reference answers"

    1. A three-node majority needs only the leader and one follower. The
       selected observer may be the other follower, and it may not yet have
       received the commit-index propagation that makes it apply the entry.
    2. The higher term establishes and fences authority, forcing the stale
       leader to step down. Prefix checking, suffix deletion, and `nextIndex`
       retry replace its uncommitted branch with the authoritative log.

### Hands-on

3. Run all six commands in this chapter and save only their stdout in a
   temporary comparison note. Acceptance: identify one ack-boundary observation
   and one final-state observation for each experiment; do not treat tick
   numbers as milliseconds.
4. Create a temporary script that elects a Raft leader, isolates it, and
   attempts both `ReadLevel.LEADER` and `ReadLevel.LINEARIZABLE`. Acceptance:
   record the direct leader-read result and the linearizable read's
   `RuntimeError` containing `read-index quorum`. Do not edit `src/`.

??? note "Reference answers"

    3. Experiment 1 pairs the immediate follower value with final convergence;
       experiment 2 pairs `ack=True` with the value after failover; experiment
       3 pairs writes accepted/rejected during partition with the converged
       post-heal state. The six measured blocks above are the reference.
    4. Use `RaftGroup.run_until_leader()`, write a setup value with
       `AckLevel.QUORUM`, call `isolate(leader)`, then issue the two reads.
       `LEADER` reads the locally applied value because it performs no barrier;
       `LINEARIZABLE` drives ticks and raises when a majority does not
       acknowledge. This mirrors
       `test_linearizable_read_fails_without_a_current_term_quorum()` in
       `tests/protocols/test_raft.py`.

## Summary

The three controlled experiments expose a protocol spectrum. Under normal
delivery both implementations converge, but their ack boundaries differ.
After an immediate crash, a local asynchronous ack can disappear while a
Raft quorum commit survives. During partition, two manually designated
asynchronous primaries can accept incompatible dirty writes, whereas an
isolated Raft leader cannot commit and is later fenced and repaired. Read
levels add a second axis: routing to a leader is not the same as proving
current majority authority. Together, the labs show why distributed-system
APIs must name their actual guarantees instead of flattening unlike mechanisms
behind one successful return value.

## M3 protocol update

Protocols 2/3 have been added; see the comparison table in `docs/experiments.md`.
