# Chapter 10: ISR, High Watermark, and Controller Epochs

[中文版](../zh/tutorial/10-isr.md)

Protocol 3 models a Kafka-shaped partition. A controller owns the current
leader, leader epoch, and in-sync replica set (ISR). The leader may append
ahead, followers copy its ordered log, and the high watermark (HW) identifies
the prefix shared by the current ISR. This creates a dynamic acknowledgement
boundary unlike both fixed Raft majority and configured WAL standbys.

## Learning objectives

By the end of this chapter, you will be able to:

1. trace `LEADER` and `ALL_ISR` writes from leader append to HW advancement;
2. explain ISR shrink, rejoin, and `min.insync.replicas` as three distinct
   decisions;
3. distinguish log-end data from HW-committed data on reads;
4. follow a controller election through leader-epoch fencing and clean-prefix
   truncation; and
5. compare MiniDist's pedagogical lease with real Kafka's replication
   mechanisms without claiming an API equivalence.

## 1. Controller state defines the partition

`IsrGroup.__init__()` in `src/minidist/protocols/isr/group.py` creates the same
`SimClock`, `Scheduler`, `SimNet`, and `Trace` used elsewhere. It also creates
four controller-owned values: `_leader`, `_leader_epoch`,
`_controller_epoch`, and `_isr`. Initially every node is in the ISR, HW is
zero, and the selected leader starts at epoch 1.

The controller is an in-process authority, not another simulated replicated
service. `controller_elect()` is a public lab control operation. Real Kafka
stores metadata through a controller/metadata quorum and handles failures of
that control plane. MiniDist assumes that decision is available so the data
plane—ISR, HW, leader epochs, and replica catch-up—can be isolated.

Each `LogEntry` has an offset, the epoch in which it originated, and byte key
and value. A node's `data` contains everything locally appended, while
`committed_data` contains only the prefix at or below HW. `probe()` exposes
both maps, `log_end_offset`, `applied_hw`, durable offset, and catch-up mode.
That split is essential: “present on the leader” and “committed for an
authority-checked read” are different facts.

## 2. `LEADER` and `ALL_ISR` acknowledge different boundaries

`IsrGroup.client_write()` supports exactly two levels:

- `AckLevel.LEADER` appends locally, schedules follower messages, and returns
  without waiting for ISR or HW.
- `AckLevel.ALL_ISR` waits until every member of the **current ISR** reaches
  the entry and HW advances through it.

`NONE` and `QUORUM` raise `UnsupportedLevelError`. Again, enum names do not
make protocol semantics portable. `ALL_ISR` is not “all configured replicas
forever.” The set can shrink before or during the bounded wait.

Before appending an `ALL_ISR` write, `_refresh_isr_and_hw()` runs and the group
checks `len(_isr) >= min_insync_replicas`. If that condition is already false,
the write returns `accepted=False` without adding an entry. During the wait,
the same condition is checked again; losing too many members fails the request.
If enough members remain and every current ISR log reaches the offset, HW can
advance and the write succeeds.

HW is computed in `_refresh_isr_and_hw()` as the minimum log-end offset among
alive ISR nodes. It only moves forward. `_apply_high_watermark()` then copies
entries through that position into each node's `committed_data` and advances
`applied_hw`.

The write message “all current ISR replicas acknowledged and HW advanced”
therefore names two facts: membership still meets the minimum, and the
committed prefix covers the entry. It does not mean a fixed majority voted or
that all configured replicas contain it.

## 3. ISR shrink preserves availability conditionally

MiniDist models a slow follower with `set_replica_slow(node, True)`.
`_send_entry()` then records `isr_replication_delayed` and sends nothing to
that node. Every tick calls `_refresh_isr_and_hw()`. If a follower remains
behind for `replica_lag_timeout` logical ticks, it is removed and
`isr_member_removed` is recorded.

Removal can let the remaining ISR advance HW. `min.insync.replicas` does not
prevent that set from shrinking, even below the configured value; it controls
admission of `ALL_ISR` writes only. With three nodes and a minimum of two, one
lagging follower can leave and writes continue. If both followers leave, the
leader remains in the ISR but the next `ALL_ISR` write is rejected.

The mechanism therefore has three steps that are easy to collapse incorrectly:

1. lag detection decides whether a follower remains in ISR;
2. `min.insync.replicas` decides whether a strong write is admitted;
3. the minimum log-end offset of the resulting ISR decides HW.

A recovering follower is not re-added just because its network link heals.
`set_replica_slow(node, False)` starts `_synchronize()`. Only after its
log-end offset catches the leader and it is no longer marked slow does
`_refresh_isr_and_hw()` record `isr_member_added`.

Experiment 4 exposes the membership contrast:

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

All four writes are available in this particular setup, but for four different
reasons. The ISR row is the only one that changes membership.

## 4. Retained log and rejoin

Like WAL shipping, ISR keeps a bounded `_retained` deque. `_synchronize()`
selects `INCREMENTAL` when the follower is caught up or its next missing offset
is still retained. Otherwise it sends an `isr_snapshot` containing the leader
log, data, and HW, and marks `FULL`.

Incremental append messages carry both the packet's current `leader_epoch` and
the entry's original `entry_leader_epoch`. The distinction lets a new leader
replay a committed entry created in an older epoch while still fencing a packet
sent by an obsolete leader. Snapshot installation similarly rebuilds
`committed_data` only through the advertised HW.

Real Kafka catches replicas up through fetch requests and segment files.
MiniDist's full path atomically replaces Python state. There are no batches,
segment indexes, fetch sessions, throttling, or transfer cost. The experiment
preserves the retention decision and the “catch up before rejoin” rule.

## 5. Controller election fences and truncates

`controller_elect(node)` accepts only a live ISR member. It increments both
controller and leader epochs, selects the new leader, and truncates the
candidate log to entries at or below HW:

```python
candidate.log = [
    entry for entry in candidate.log
    if entry.offset <= self._high_watermark
]
```

The candidate rebuilds its local data from that clean prefix. The old leader is
removed from ISR, a new logical lease expiry is set, and all live followers
start synchronization.

Every append or snapshot message carries the current packet leader epoch and
source leader. `_receive()` rejects a mismatched epoch as
`stale_leader_epoch`, then rejects a non-current source. These checks happen
before appending or replacing state. A committed old-epoch entry remains
replayable because its `entry_leader_epoch` is data history; packet authority
comes from the new current epoch.

This is controller-directed clean election, not Raft election. Nodes do not
campaign or exchange votes. The controller may choose only an ISR member, and
HW defines the safe prefix. Real Kafka also has unclean-election policy and
many metadata failure modes that MiniDist does not model.

## 6. Read levels expose HW and lease boundaries

`client_read()` deliberately separates three observations:

- `LOCAL` reads a named node's full local `data`; it may be stale.
- `LEADER` reads the current leader's full local `data`; it may include an
  append above HW.
- `LINEARIZABLE` reads `committed_data` only if the leader has no entry above
  HW, the logical lease has not expired, and ISR still satisfies the minimum.
  Otherwise it raises `RuntimeError`.

The method name is shared course vocabulary. MiniDist's predicate is a
pedagogical authority guard, not a proof that Kafka exposes a linearizable
read API. `lease_read(node, key)` additionally requires the named node to be
the controller's current leader and the lease to be live, then delegates to
that HW-checked read. A displaced old leader therefore fails closed.

Experiment 6 records the full matrix:

```text
实验 6：ReadLevel × 协议
async: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
wal: LOCAL=stale, LEADER=fresh, LINEARIZABLE=unsupported
isr: LOCAL=stale, LEADER=fresh, LINEARIZABLE=blocked
raft: LOCAL=stale, LEADER=fresh, LINEARIZABLE=fresh
```

“Blocked” is meaningful: the ISR leader has locally appended `k=v`, but HW has
not reached it, so the authority read refuses to expose the uncommitted value.

## 7. Hands-on experiment: shrink, rejoin, and inspect HW

Run:

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.isr import IsrGroup

g = IsrGroup(replica_lag_timeout=2, min_insync_replicas=2)
slow = next(n for n in g.probe().nodes if n != g.leader)
g.set_replica_slow(slow, True)
for i in range(3):
    result = g.client_write(f"k{i}".encode(), str(i).encode(),
                            AckLevel.ALL_ISR)
state = g.probe()
print("write:", result.accepted)
print("ISR after lag:", sorted(state.isr))
print("slow LEO / HW:", state.nodes[slow].log_end_offset,
      state.high_watermark)
g.set_replica_slow(slow, False)
g.run_until_idle()
state = g.probe()
print("ISR after catch-up:", sorted(state.isr))
print("slow LEO / HW:", state.nodes[slow].log_end_offset,
      state.high_watermark)
PY
```

Measured output:

```text
write: True
ISR after lag: ['node-1', 'node-3']
slow LEO / HW: 0 3
ISR after catch-up: ['node-1', 'node-2', 'node-3']
slow LEO / HW: 3 3
```

The follower is not declared healthy in advance. It first receives the retained
entries; reaching the leader at offset 3 permits ISR rejoin.

Run the focused suite:

```bash
uv run pytest -q tests/protocols/test_isr.py
```

Measured output:

```text
...........                                                              [100%]
11 passed in 0.03s
```

## 8. Against real Kafka

The [protocol mapping](../mapping.md#mapping-protocol-3-to-kafka-isr-replication)
classifies ISR shrink/rejoin, HW, `acks=all` plus
`min.insync.replicas`, leader epochs, and clean-prefix truncation as the
mechanisms preserved for this laboratory.

MiniDist omits brokers and clients, record batches, producer IDs and epochs,
idempotence, transactions, log segments, retention bytes/time, fetch sessions,
replica selector policies, KRaft metadata replication, reassignment, rack
awareness, and real durability. Its in-process controller never fails.
`lease_read()` is a teaching operation; it is not a Kafka client API.
`fsync()` and `lose_unfsynced=True` expose a simulated local durability image,
not Kafka's page-cache and flush configuration in production.

The honest correspondence is that a dynamic synchronized set defines a
committed prefix and write-admission boundary. It is not “Raft with a different
name,” and dynamic `ALL_ISR` may cover fewer nodes than a fixed majority.

## 9. Exercises

### Understanding

1. Why can removing a slow follower increase progress without weakening
   `min.insync.replicas` automatically?
2. What is the difference between leader `data` and `committed_data`?
3. Why are `leader_epoch` and `entry_leader_epoch` separate fields?

??? note "Reference answers"

    1. Removal changes ISR, but the remaining size is checked independently
       against the configured minimum before `ALL_ISR` admission.
    2. `data` includes every local append; `committed_data` includes only the
       prefix through HW. A `LEADER` read may see more than an authority read.
    3. Packet epoch proves current sending authority. Entry epoch records where
       historical data originated, so a new leader can replay a committed old
       prefix without accepting old-leader packets.

### Hands-on

4. Write a temporary test that marks both followers slow, advances two ticks,
   and asserts an `ALL_ISR` write is rejected without changing the leader's log
   end offset.

??? note "Reference answer"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.isr import IsrGroup

    def test_minimum_rejects_before_append():
        g = IsrGroup(replica_lag_timeout=1, min_insync_replicas=2)
        followers = [n for n in g.probe().nodes if n != g.leader]
        for node in followers:
            g.set_replica_slow(node, True)
        g.tick(); g.tick()
        before = g.probe().nodes[g.leader].log_end_offset
        result = g.client_write(b"k", b"v", AckLevel.ALL_ISR)
        assert not result.accepted
        assert g.probe().nodes[g.leader].log_end_offset == before
    ```

    Save it as `/tmp/test_isr_ch10.py`, run
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_isr_ch10.py`, and leave
    `src/` unchanged.

## Summary

ISR replication makes membership part of the write boundary. A lagging replica
can leave even when the ISR falls below `min.insync.replicas`; that setting
rejects subsequent `ALL_ISR` writes rather than preventing ISR shrink. HW is the
minimum log end across the surviving ISR, and only that prefix is committed. A
caught-up replica can rejoin, while a controller election truncates to HW and
increments the leader epoch that fences old packets. The final comparison is
therefore not one ladder from weak to strong: configured WAL standbys, dynamic
ISR, fixed Raft majority, and asynchronous sinks answer different operational
questions. Return to Chapter 8's seven-lab matrix whenever a shared enum name
tempts you to collapse those answers.
