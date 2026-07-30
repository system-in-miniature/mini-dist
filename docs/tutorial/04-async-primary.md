# Chapter 4 — Asynchronous Primary–Replica Replication

[中文版](../zh/tutorial/04-async-primary.md)

Protocol 1 is intentionally simple: one designated primary accepts writes,
replicas consume its ordered stream, and no replica votes before the client sees
success. Simplicity makes the central trade-off unusually clear. Replication
can converge in normal operation while still losing an acknowledged write
after failover.

## Learning objectives

By the end of this chapter, you will be able to:

1. trace a write from local primary application through backlog insertion,
   scheduled delivery, and replica application;
2. explain why replication ID and offset together identify a position in one
   history;
3. predict partial versus full resynchronization from backlog coverage and
   lineage;
4. show how promotion creates a new generation and fences queued old-primary
   traffic; and
5. reproduce acknowledged-write loss without confusing it with disk failure.

## The primary acknowledges before replication

`AsyncPrimaryGroup.__init__` in
`src/minidist/protocols/async_primary/group.py` creates a `SimClock`, `Trace`,
`Scheduler`, and fixed-delay `SimNet`. It chooses generation 1, derives
replication ID `repl-1`, assigns that ID to every node, creates a bounded
`deque` backlog, and registers one receive handler per node.

`AsyncPrimaryGroup.client_write` is the write path:

1. reject acknowledgement levels other than `NONE` and `LEADER`;
2. require byte-string key and value and a live primary;
3. increment the primary's offset and mutate its local dictionary;
4. create an `_Entry` containing generation, replication ID, offset, key, and
   value;
5. append it to the bounded backlog;
6. call `_send_entry` for every non-primary node; and
7. record `client_acknowledged` and return `accepted=True`.

No `tick` or `run_until_idle` occurs between steps 6 and 7. `SimNet.send`
schedules delivery for a future logical tick. The replicas therefore do not
participate in the acknowledgement decision.

`AsyncPrimaryGroup._apply_entry` protects stream order. The payload's
replication ID must match both current primary and target replica, and the
incoming offset must equal `target.offset + 1`. A valid entry mutates replica
data, advances its offset, and records `replica_applied`. A gap triggers
`_synchronize` rather than applying an entry over an unknown prefix.

## Replica sink means no vote

Each `_Node` has data, replication ID, offset, liveness, volatile state, and
`SyncMode`; it has no vote or acknowledgement state. The only write API routes
through `self._primary`. Replicas receive `entry` or `full_sync` messages and
apply them if the lineage checks pass.

This differs from quorum replication at its decision boundary. Adding two
replicas does not make `AckLevel.LEADER` safer: the primary still returns before
either receives the message. More replicas create more potential copies after
progress, not a majority precondition for success.

`AsyncPrimaryGroup.client_read` exposes the resulting staleness. `LOCAL` with a
replica node may return an older value and offset; `LEADER` routes to the current
primary; `LINEARIZABLE` is rejected.

## Replication position is `(ID, offset)`

An offset alone says “how many entries into some stream,” not which history the
stream belongs to. MiniDist therefore carries both `replication_id` and
monotonic `offset`.

Within one history, each write increments the offset and the backlog stores
ordered `_Entry` values. On promotion, `AsyncPrimaryGroup.promote` increments
`_generation`, gives the candidate a new ID through `_new_replication_id`,
clears the backlog, and starts synchronization for every other live node. The
candidate retains its data and numeric offset, but `(repl-2, 1)` is not the same
position as `(repl-1, 1)`.

Every delivery also carries `source_primary` and generation.
`AsyncPrimaryGroup._replication_rejection_reason` rejects a message if:

- its envelope or payload source is not the current primary;
- its generation is stale; or
- its replication ID differs from the current primary's ID.

This fencing is essential because scheduled callbacks can outlive a role
change. Tests
`test_queued_old_primary_full_sync_is_rejected_after_promotion` and
`test_queued_old_primary_backlog_entry_is_rejected_after_promotion` assert that
old work cannot overwrite the new lineage.

The receiver validates authority at delivery time, not merely when the sender
originally queued the message, because authority may change while it waits.

It is still not automatic leader election. A caller explicitly invokes
`promote`; MiniDist's generation records that manual topology epoch.

## Bounded backlog: partial when the prefix is known

`AsyncPrimaryGroup._synchronize` selects a repair mode. Partial resync is
allowed when the replica and primary have the same replication ID and either:

- the offsets are already equal, or
- the replica offset is at least one before the oldest retained backlog entry.

The method filters backlog entries with offsets greater than the replica's and
sends them in `partial` mode. If the lineage differs or the missing prefix has
fallen out of the deque, it sends a `full_sync` payload containing a copy of the
primary's complete data, current ID, and offset.

`_apply_full_sync` atomically replaces the target dictionary in one simulated
event. Real data transfer and loading take time; the atomic event preserves only
the semantic fact that the old state is replaced.

`SyncMode` exposes the last join/catch-up path:

- `NEVER`: no replica synchronization applied yet, or the promoted node;
- `STREAMING`: normal ordered entry delivery;
- `PARTIAL`: backlog catch-up was selected;
- `FULL`: a whole-state replacement was selected and applied.

## Crash, restart, heal, and promote

`AsyncPrimaryGroup.crash` clears volatile state, marks the node dead, and
partitions all incident links. It deliberately retains the node's dataset. A
replica restart heals those links and calls `_synchronize`. Healing an isolated
live replica does the same.

Promotion is allowed only for a live non-primary. It does not ask a quorum or
choose the most advanced replica. That is the experimenter's responsibility.
Promoting a stale replica can therefore discard an acknowledged write that
exists only on the crashed old primary.

After promotion, every live replica is proactively synchronized. A healed old
primary is now a replica; because its replication ID differs, it receives a
full sync and loses dirty data outside the new history. This creates convergence
but does not retroactively make both partition-side acknowledgements safe.

## Hands-on experiment 1: normal stream delivery

Run:

```bash
uv run python -c 'from minidist.protocols import AckLevel,ReadLevel,UnsupportedLevelError; from minidist.protocols.async_primary import AsyncPrimaryGroup; g=AsyncPrimaryGroup(replica_ids=("r1",),backlog_size=2,replication_delay=1,seed=7); w=g.client_write(b"k",b"v",AckLevel.LEADER); print(w); print("before",g.client_read(b"k",ReadLevel.LOCAL,node="r1")); g.tick(); print("after",g.client_read(b"k",ReadLevel.LOCAL,node="r1")); print("backlog",g.probe().backlog_offsets);
try: g.client_write(b"x",b"y",AckLevel.QUORUM)
except UnsupportedLevelError as e: print(type(e).__name__+":",e)'
```

Observed output:

```text
WriteResult(accepted=True, offset=1, message='primary accepted; replica delivery is asynchronous')
before ReadResult(value=None, node='r1', offset=0)
after ReadResult(value=b'v', node='r1', offset=1)
backlog (1,)
UnsupportedLevelError: async primary does not support AckLevel.QUORUM
```

The same object can expose delayed convergence and honest refusal of a stronger
ack level.

## Hands-on experiment 2: partial versus full resync

Run:

```bash
uv run python -c 'from minidist.protocols import AckLevel; from minidist.protocols.async_primary import AsyncPrimaryGroup
def demo(backlog,writes):
 g=AsyncPrimaryGroup(replica_ids=("r1",),backlog_size=backlog,replication_delay=1); g.crash("r1");
 for i in range(writes):g.client_write(f"k{i}".encode(),str(i).encode(),AckLevel.LEADER)
 g.restart("r1"); g.run_until_idle(); s=g.probe().nodes["r1"]; print("backlog",backlog,"writes",writes,"mode",s.sync_mode.name,"offset",s.offset,"data",dict(s.data))
demo(3,2); demo(2,3)'
```

Observed output:

```text
backlog 3 writes 2 mode PARTIAL offset 2 data {b'k0': b'0', b'k1': b'1'}
backlog 2 writes 3 mode FULL offset 3 data {b'k0': b'0', b'k1': b'1', b'k2': b'2'}
```

In the first run, the replica starts at offset 0 and the backlog still begins
at 1. In the second, a two-entry deque retains only offsets 2 and 3; offset 0
cannot prove possession of offset 1, so full replacement is required.

## Hands-on experiment 3: lose an acknowledged write

Run:

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
```

Observed output:

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
```

No tick occurs between acknowledgement and crash in
`labs/exp02_acked_write_loss.py::run_experiment`. The loss is a replication
guarantee counterexample, not a simulated disk corruption.

## Against real Redis

The
[real-system mapping](../mapping.md#mapping-protocols-to-real-system-replication-mechanisms)
classifies the preserved ideas precisely:

- ordinary primary acknowledgement without replica participation;
- replicas as ordered-stream consumers;
- replication ID plus offset;
- bounded backlog partial resynchronization;
- full replacement when history or coverage differs; and
- asynchronous failover's acknowledged-write loss window.

MiniDist simplifies or omits important Redis machinery. Real Redis replication
ships a byte stream and uses `PSYNC`; MiniDist sends structured SET entries.
Real full sync creates/transfers/loads an RDB and buffers concurrent changes;
MiniDist replaces a dict atomically. Real promotion is coordinated through
operators, Sentinel, or Cluster; MiniDist exposes manual `promote`. Redis can
retain primary and secondary replication IDs across failover to improve partial
resync; MiniDist retains one current ID and normally full-syncs across a new
generation.

The dataset-retaining crash is not Redis persistence. There is no RDB, AOF,
fsync, process, socket, authentication, `WAIT`, read-only replica policy,
reconnect timing, or Sentinel election. The generation fence is an educational
manual-promotion epoch, not a complete Sentinel/Cluster configuration-epoch
protocol.

These boundaries also appear in the
[DIFFERENCES list](../mapping.md#differences-list) and the
[experiment matrix](../experiments.md#replication-protocol-experiment-matrix).
In particular, future slow-replica and reconnect rows are not proved merely
because unit tests cover bounded resynchronization.

## Exercises

### Understanding

1. Why is offset 7 insufficient to decide whether two nodes share history?
2. Exactly when can a replica at offset `r` partially resynchronize from a
   backlog whose oldest offset is `o`?

??? note "Reference answers"

    1. The same number can occur in different replication lineages. Compare
       replication ID/generation as well as offset.
    2. IDs must match, and either `r` equals the primary offset or
       `r >= o - 1`. The latter proves every missing entry is still retained.

### Hands-on

3. Write a temporary test that proves a replica one entry beyond backlog
   coverage uses `SyncMode.FULL`.

   **Acceptance:** use a backlog of 2, crash the replica at offset 0, perform
   three writes, restart, and assert `FULL`, offset 3, and equality with primary
   data. Run it from `/tmp` and obtain `1 passed`; do not edit `src/`.

??? note "Reference answer"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.async_primary import AsyncPrimaryGroup, SyncMode

    def test_outside_backlog_full_syncs():
        group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=2)
        group.crash("r1")
        for i in range(3):
            group.client_write(str(i).encode(), b"v", AckLevel.LEADER)
        group.restart("r1")
        group.run_until_idle()
        state = group.probe()
        assert state.nodes["r1"].sync_mode is SyncMode.FULL
        assert state.nodes["r1"].offset == 3
        assert state.nodes["r1"].data == state.nodes[state.primary].data
    ```

4. Write a temporary regression test that queues an entry with delay 2,
   promotes the stale replica before delivery, runs until idle, and asserts the
   old entry is absent plus a `replication_message_rejected` event has reason
   `non_current_primary`.

   **Acceptance:** the test passes and inspects only public `probe`, `trace`,
   and group methods.

??? note "Reference answer"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.async_primary import AsyncPrimaryGroup

    def test_promotion_fences_queued_old_entry():
        group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
        group.client_write(b"old", b"dirty", AckLevel.LEADER)
        group.promote("r1")
        group.run_until_idle()
        assert b"old" not in group.probe().nodes["r1"].data
        assert any(
            event.kind == "replication_message_rejected"
            and event.details["reason"] == "non_current_primary"
            for event in group.trace.events
        )
    ```

## Summary

The async-primary protocol obtains a simple write path by separating local
acknowledgement from replica progress. Ordered entries, `(replication ID,
offset)`, and a bounded backlog make normal streaming and catch-up explicit;
full sync repairs an unknown prefix; generation checks fence obsolete queued
traffic after manual promotion. None of that turns the write into a quorum
commit. Chapter 5 stays on the failover boundary: it isolates generation and
lineage fencing, and explains why a queued old full sync must be rejected after
promotion.
