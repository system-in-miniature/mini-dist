# Chapter 9: WAL Shipping, Flush Boundaries, and Timelines

[中文版](../zh/tutorial/09-wal-shipping.md)

Raft replicates commands as entries in a consensus log. Protocol 2 asks a
different question: what if a primary first turns each mutation into an ordered
write-ahead-log record, flushes that record locally, and then transports the
same bytes to standbys? MiniDist's `WalShippingGroup` preserves that
PostgreSQL-shaped path while keeping the shared state machine small enough to
inspect.

## Learning objectives

By the end of this chapter, you will be able to:

1. trace a SET from logical WAL encoding through local flush, transport,
   standby replay, and flush acknowledgement;
2. distinguish MiniDist's asynchronous `LEADER` boundary from its synchronous
   `QUORUM` boundary;
3. predict incremental catch-up versus base-backup replacement from timeline,
   LSN, and retention;
4. explain why promotion creates a new timeline and why queued old-timeline WAL
   must be rejected; and
5. state precisely which PostgreSQL mechanisms this teaching model preserves
   and which it omits.

## 1. The replicated object is an ordered byte record

`WalShippingGroup.client_write()` in
`src/minidist/protocols/wal_shipping/group.py` validates byte-string keys and
values, chooses `lsn = primary.applied_lsn + 1`, and calls `_encode_set()`.
The encoder is deliberately tiny:

```python
return (
    len(key).to_bytes(4, "big") + key
    + len(value).to_bytes(4, "big") + value
)
```

The two lengths make the record binary-safe. `_decode_set()` reverses the
layout and rejects a payload whose declared end does not equal its actual
length. `WalRecord` attaches the current `timeline` and monotonic `lsn` to
those bytes. `_append_record()` appends the exact payload to `wal_bytes`,
stores the decoded value, advances `applied_lsn`, and records `wal_applied`.

This is more than sending a Python `(key, value)` tuple, but it is not
PostgreSQL's physical WAL. Real PostgreSQL records page and transaction effects
needed for crash recovery. MiniDist transports a logical SET record so all four
protocols can mutate the same KV state machine. The preserved lesson is that a
standby consumes one ordered byte history identified by positions; the
simplification is what those bytes describe.

The receive path enforces `record.lsn == target.applied_lsn + 1`. A gap never
gets applied over an unknown prefix. Instead, `_receive()` invokes
`_synchronize()`, which decides whether retained WAL covers the missing suffix
or a full base backup is required.

## 2. Local flush and remote waiting are separate choices

After appending locally, `client_write()` puts the record in the bounded
`_retained` deque and calls `fsync(self._primary)`. `fsync()` copies live WAL,
records, data, and LSN into the node's durable image and sets `flushed_lsn`.
Only then does the group send the record to every standby.

The requested acknowledgement level decides whether the caller waits after
that point:

- `AckLevel.LEADER` returns after the primary's simulated WAL flush. Standby
  delivery remains asynchronous.
- `AckLevel.QUORUM` waits until **every configured synchronous standby** has
  replayed and flushed the record, or until `ack_timeout` expires.
- `NONE` and `ALL_ISR` raise `UnsupportedLevelError`.

The name `QUORUM` is shared experimental vocabulary, not a claim that this path
uses a Raft majority. In `client_write()`, the required set is literally
`set(self._synchronous)`. A synchronous standby calls `fsync(target_id)` after
applying a record, then returns a `flush_ack`. An asynchronous standby also
sends an acknowledgement in this model, but it does not belong to the required
set.

An empty synchronous set makes the condition vacuously true. That is a useful
reason to inspect configuration rather than infer durability from the enum
alone. Conversely, one isolated configured synchronous standby makes
`QUORUM` return `accepted=False`, even if other copies exist. The write is
already locally flushed; the result says the requested remote acknowledgement
condition was not satisfied before the deterministic timeout.

`tests/protocols/test_wal_shipping.py` fixes all three boundaries:
`test_leader_ack_exposes_ordered_logical_wal_bytes_before_standby_apply`,
`test_quorum_means_every_configured_synchronous_standby_flushed`, and
`test_quorum_write_fails_when_a_synchronous_standby_is_unreachable`.

## 3. Live, flushed, and durable positions are observable

Each `_Node` separates `applied_lsn`, `flushed_lsn`, and `durable_lsn`.
Ordinary record replay advances the live applied position. A synchronous
standby additionally flushes it. `probe()` exposes all three positions without
allowing the caller to mutate protocol state.

The distinction becomes visible only under the explicit power-loss injection.
`crash(node)` normally retains the live in-process state. Calling
`crash(node, lose_unfsynced=True)` restores `wal_bytes`, records, data, and LSN
from the last durable image and records `wal_fsync_loss`. On restart, a standby
synchronizes from its restored position and can receive the lost suffix again.

This is a simulated fsync boundary, not a disk implementation. There are no
files, sectors, checksums, torn records, WAL writer process, or recovery startup
time. The model lets an experiment distinguish “received” from “flushed”
without claiming storage-engine realism.

Reads remain a separate axis. `client_read()` supports `LOCAL` and `LEADER`.
A local standby may lag; a leader-routed read sees the current primary's
applied data. `LINEARIZABLE` is rejected because synchronous WAL commit does
not by itself prove that a primary still owns current read authority.

## 4. Retention decides incremental or full catch-up

`_retained` is a bounded deque of recent `WalRecord` objects. When a standby
restarts or heals, `_synchronize()` permits incremental catch-up only when:

1. the standby timeline equals the group's current timeline; and
2. its LSN already equals the primary's, or it is at least one position before
   the oldest retained record.

In that case, records above the standby LSN are sent in order and the standby's
`SyncMode` becomes `INCREMENTAL`. If the missing prefix has fallen outside
retention, or the timeline differs, the primary sends a `base_backup` carrying
its WAL bytes, records, data, LSN, and timeline. `_receive()` replaces the
standby's state in one simulated event and marks `FULL`.

The boundary parallels PostgreSQL WAL retention and the need for a new base
backup when the required segment is unavailable. MiniDist does not implement
archive recovery, replication slots, segment files, checkpoints, or transfer
cost. “Base backup” here is an atomic snapshot payload, not a directory copied
while a database continues to write.

Experiment 5 makes the retention rule comparable across all protocols:

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

The matching words do not imply identical artifacts. Async replays a structured
backlog, WAL shipping transports retained byte records, ISR copies retained
leader-log entries, and M3 Raft has no compaction boundary at all.

## 5. Promotion forks a timeline

`WalShippingGroup.promote()` accepts only a live standby. It increments
`_timeline`, selects the new primary, assigns the new timeline to that node,
clears retained old-branch WAL, flushes the promoted node, records
`wal_standby_promoted`, and starts synchronization for other live standbys.

Messages already queued in `Scheduler` do not disappear. Every `wal_record` and
`base_backup` therefore carries `timeline` and `source_primary`.
`_rejection_reason()` runs at delivery and rejects either a stale timeline or a
non-current physical/logical source before state can change.

This ordering matters most for full replacement: applying an old base backup
and validating later would already have destroyed new-branch state. Timeline
fencing chooses a branch; it does not merge two divergent branches or recover
an async write that never reached the promotion candidate.

## 6. Hands-on experiment: observe flush and timeline fencing

Run:

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.wal_shipping import WalShippingGroup

g = WalShippingGroup(standby_ids=("s1", "s2"), replication_delay=2)
w = g.client_write(b"course", b"MiniDist", AckLevel.LEADER)
before = g.probe()
print("ack:", w.accepted, "lsn:", w.offset)
print("primary durable:", before.nodes["primary"].durable_lsn)
print("s1 before delivery:", before.nodes["s1"].applied_lsn)
g.tick()
old_timeline = g.probe().timeline
g.promote("s1")
g.run_until_idle()
after = g.probe()
print("timeline:", old_timeline, "->", after.timeline)
print("primary:", after.primary)
print("stale timeline rejected:", any(
    e.kind == "wal_rejected" and e.details["reason"] == "stale_timeline"
    for e in g.trace.events
))
print("leader read:", g.client_read(b"course", ReadLevel.LEADER).value)
PY
```

Measured output:

```text
ack: True lsn: 1
primary durable: 1
s1 before delivery: 0
timeline: 1 -> 2
primary: s1
stale timeline rejected: True
leader read: None
```

The final `None` is not a decoder failure. Promotion happened before `s1`
received LSN 1, so the newly authoritative branch never contained that
asynchronously committed value. The old packet then failed the timeline fence.

Run the focused tests:

```bash
uv run pytest -q tests/protocols/test_wal_shipping.py
```

Measured output:

```text
.........                                                                [100%]
9 passed in 0.03s
```

## 7. Against real PostgreSQL

The [protocol mapping](../mapping.md#mapping-protocol-2-to-postgresql-wal-shipping)
classifies the exact boundary. Ordered WAL, LSN replay, local versus
synchronous remote flush, retention-limited catch-up, base-backup fallback, and
timeline branch fencing point in PostgreSQL's direction.

MiniDist omits physical WAL records, transactions, checkpoints, WAL segments,
archive commands, replication slots, WAL sender/receiver processes, recovery
conflicts, cascading standbys, `synchronous_standby_names` selection grammar,
and real storage. Its `QUORUM` waits for all configured synchronous standbys;
PostgreSQL supports richer `FIRST` and `ANY` selection. Its integer timeline
does not implement timeline-history files or common-ancestor recovery.

The honest claim is therefore mechanism-shaped, not wire-compatible:
MiniDist makes commit waiting, byte-stream position, catch-up coverage, and
post-promotion branch authority executable in one deterministic laboratory.

## 8. Exercises

### Understanding

1. Why is WAL `QUORUM` not interchangeable with Raft `QUORUM`?
2. Why can a standby have `applied_lsn == 1` but `durable_lsn == 0`?
3. Why does a timeline mismatch force full catch-up even if numeric LSNs match?

??? note "Reference answers"

    1. WAL shipping waits for every member of a configured synchronous-standby
       set; Raft commits through a majority under term and election rules.
    2. Asynchronous replay advances live state without calling `fsync()` on that
       standby. Only the durable image survives an explicit fsync-loss crash.
    3. Equal numbers on different branches do not identify equal history.
       Replaying a suffix would join unrelated prefixes.

### Hands-on

4. In a temporary test, configure `s1` and `s2` as synchronous, isolate `s2`,
   and request `AckLevel.QUORUM`. Assert `accepted is False`, while the primary
   `durable_lsn` equals the attempted offset. Do not edit `src/`.

??? note "Reference answer"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.wal_shipping import WalShippingGroup

    def test_remote_wait_can_fail_after_local_flush():
        g = WalShippingGroup(
            standby_ids=("s1", "s2"),
            synchronous_standby_ids=("s1", "s2"),
            ack_timeout=4,
        )
        g.isolate("s2")
        result = g.client_write(b"k", b"v", AckLevel.QUORUM)
        assert not result.accepted
        assert g.probe().nodes[g.primary].durable_lsn == result.offset
    ```

    Save this as `/tmp/test_wal_ch09.py` and run
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_wal_ch09.py`.

## Summary

WAL shipping separates an ordered byte history from the policy that waits for
remote flush. MiniDist's primary always crosses its simulated local durability
boundary; asynchronous `LEADER` returns there, while `QUORUM` additionally
waits for every configured synchronous standby. Retained LSNs enable
incremental replay, a missing prefix requires a base backup, and promotion
creates a timeline that fences old-branch traffic. Chapter 10 moves to a
different dynamic boundary: Kafka-style ISR membership, high watermark,
`min.insync.replicas`, controller epochs, and leases.
