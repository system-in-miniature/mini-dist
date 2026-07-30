# Chapter 5: Failover and Generation Fencing

Failover is not merely changing a label from “replica” to “primary.” It changes
which history is authoritative. This chapter follows one promotion through
MiniDist and shows why a delayed packet from the old history must never be
allowed to overwrite the new one.

## Learning objectives

By the end of this chapter, you will be able to:

- explain why an acknowledged asynchronous write may disappear after failover;
- trace `promote()` from topology change to generation and lineage change;
- distinguish generation fencing from replication-offset ordering;
- predict why a queued old full sync or stream entry is rejected;
- verify active convergence after promotion with the public probe and trace.

## 1. The failover problem is a history problem

An asynchronous primary acknowledges after applying a write locally. In
`src/minidist/protocols/async_primary/group.py`,
`AsyncPrimaryGroup.client_write()` increments the primary's offset, mutates its
dictionary, puts an `_Entry` in the bounded backlog, and schedules one message
per replica. It then returns without advancing the scheduler:

```python
primary.offset += 1
primary.data[key] = value
self._backlog.append(entry)
for node_id in self._nodes:
    if node_id != self._primary:
        self._send_entry(self._primary, node_id, entry, "stream")
```

The important boundary is not Python statement order alone. Delivery belongs to
the deterministic scheduler and occurs on a later tick. Therefore a successful
`AckLevel.LEADER` result proves “this primary applied the value,” not “another
node has it,” “the value is durable,” or “a future primary must contain it.”
This is the mechanism behind the acknowledged-write-loss experiment.

Suppose the primary has offset 5 and a replica has offset 4. If the primary
fails before entry 5 arrives, promoting the replica creates an authoritative
history that does not contain entry 5. Merely comparing numeric offsets cannot
make entry 5 safe: offset 5 in one history can describe a different command
from offset 5 in another. A replication position is meaningful only inside a
lineage.

MiniDist represents that identity with two fields on every replication
delivery:

- `generation`: the group's manual-promotion epoch;
- `replication_id`: the identifier of the current primary's stream.

Offset orders entries *within* that identity. Generation and replication ID
decide whether the identity is still authoritative.

## 2. Promotion starts a new lineage

Read `AsyncPrimaryGroup.promote()` in
`src/minidist/protocols/async_primary/group.py`. Its state transition is small
enough to enumerate precisely:

1. validate that the candidate exists, is alive, and is not already primary;
2. assign `_primary` to the candidate;
3. increment `_generation`;
4. give the candidate a fresh ID through `_new_replication_id()`;
5. clear the backlog;
6. start `_synchronize()` for every other alive node.

Clearing the backlog is not housekeeping. The backlog's entries belong to the
old `(generation, replication_id)` pair. Advertising them under the candidate's
new ID would falsely claim that two histories are continuous. MiniDist instead
chooses a conservative full resynchronization when the IDs differ.

The new primary retains its existing data and numeric offset. This makes the
loss window visible: promotion does not invent a missing write. The call also
does not elect the candidate. It is an explicit control-plane action supplied
by the lab. The data plane only enforces the consequences of that decision.

Finally, promotion is *active*. It calls `_synchronize()` immediately for all
alive replicas, even if no client performs another write. In
`AsyncPrimaryGroup._synchronize()`, partial resynchronization is allowed only
when the replica and primary IDs match and the replica's offset is covered by
the backlog. Immediately after promotion the IDs differ, so the normal result
is a `full_sync` message containing the new primary's complete dictionary,
offset, generation, and ID.

## 3. Fencing is checked when a message arrives

Changing the current primary is not sufficient because messages already in the
scheduler still exist. The decisive guard is
`AsyncPrimaryGroup._replication_rejection_reason()` in
`src/minidist/protocols/async_primary/group.py`. Before `_receive()` applies
either an entry or a full sync, it checks:

```python
if message.source != self._primary or ...:
    return "non_current_primary"
if payload.get("generation") != self._generation:
    return "stale_generation"
if payload.get("replication_id") != primary.replication_id:
    return "stale_replication_id"
```

These checks defend different failure shapes.

- **Current owner:** a packet physically sent by the old primary is rejected,
  even if its payload claims something else.
- **Generation:** an old packet from the same node identity cannot cross a
  later promotion.
- **Replication ID:** a packet from an obsolete stream cannot be mixed into the
  current stream.

Only after all three checks pass does `_receive()` call `_apply_entry()` or
`_apply_full_sync()`. `_apply_entry()` then enforces the within-lineage rule:
the received offset must be exactly `target.offset + 1`. A gap triggers
`_synchronize()` rather than applying a suffix over an unknown prefix.

This ordering is why a queued old full sync is especially dangerous and why it
must be fenced. Full sync replaces the entire target dictionary in
`AsyncPrimaryGroup._apply_full_sync()`. If the code only checked offsets, a
late snapshot from the old primary could erase writes accepted by the new
primary. Checking authority before dispatch prevents that state replacement.

The rejection is observable rather than silent. `_receive()` records a
`replication_message_rejected` trace event with the message type, source,
generation, replication ID, and reason. Tests and labs can therefore verify
which fence fired.

## 4. Active convergence after healing

`AsyncPrimaryGroup.heal()` repairs all bidirectional links for one node. If that
node is now a replica, it immediately invokes `_synchronize()`. This matters
after a split-brain lab:

1. isolate the old primary;
2. accept a dirty local write on the old side;
3. manually promote a replica and accept another write there;
4. heal the old primary.

The healed old node is a replica of the new generation. Its ID does not match,
so it receives a full sync and replaces its dirty old-side state with the new
primary's state. Convergence is deterministic, but it does **not** merge both
sides. The old dirty write disappears. Fencing protects the chosen authority;
it does not reconcile application-level conflicts.

`AsyncPrimaryGroup.probe()` exposes immutable copies of node data, roles,
offsets, lineages, and sync modes. This is an experiment interface, not an
administrative protocol. Use it to assert the outcome without mutating private
state.

## 5. Comparison with real Redis

The repository's [behavior mapping and DIFFERENCES
list](../mapping.md#differences-list) classifies this boundary explicitly.
Redis uses replication IDs and offsets for partial synchronization. Promotion
can be requested with `REPLICAOF NO ONE` or coordinated by Sentinel/Cluster.
Sentinel and Redis Cluster also have control-plane epochs that help fence stale
ownership.

MiniDist deliberately compresses those layers:

- `promote()` is manual; there is no failure detector, quorum of Sentinels,
  replica selection policy, or automatic election;
- only one current replication ID is retained, whereas Redis can retain a
  secondary replication ID to help replicas continue partial synchronization
  across promotion;
- full sync atomically copies a Python dictionary in one simulation event;
  real Redis creates/transfers/loads an RDB and continues streaming buffered
  commands;
- node crash preserves the teaching dataset and clears process-local control
  state; real restart data depends on RDB/AOF configuration;
- there is no `WAIT`, disk fsync model, RESP transport, Cluster slot ownership,
  or Sentinel configuration exchange.

The equivalent claim is narrow but useful: asynchronous acknowledgement can
precede replica delivery, lineage plus offset identifies a replication
position, and stale replication traffic must not cross a change of authority.
MiniDist's generation is a teaching epoch, not a complete Redis Sentinel or
Cluster protocol.

## 6. Hands-on experiment: catch an old packet

Run this command from the repository root:

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup

g = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
g.client_write(b"old", b"dirty", AckLevel.LEADER)
g.promote("r1")
g.run_until_idle()
s = g.probe()
rejected = [e.details["reason"] for e in g.trace.events
            if e.kind == "replication_message_rejected"]
print("primary:", s.primary)
print("r1 lineage:", s.nodes["r1"].replication_id)
print("old data on r1:", s.nodes["r1"].data.get(b"old"))
print("rejections:", rejected)
PY
```

Measured output:

```text
primary: r1
r1 lineage: repl-2
old data on r1: None
rejections: ['non_current_primary']
```

The old primary applied `old=dirty`, but the delivery was still queued when
`r1` became primary. Promotion created generation 2 and `repl-2`. At delivery,
the physical source was no longer current, so the first fence rejected it.
Nothing was merged into the new history.

The focused regression suite is also executable:

```bash
uv run pytest -q tests/protocols/test_async_primary.py
```

Measured output:

```text
............                                                             [100%]
12 passed in 0.04s
```

This covers queued entry and queued full-sync rejection, generation/ID checks,
and promotion without a new write. The duration may vary; the pass count and
zero failures are the acceptance facts.

## 7. Exercises

### Understanding

1. Why is `(offset=5)` alone insufficient to decide whether a replication
   message belongs to the current history?
2. Why must a full-sync message pass the authority fence *before*
   `_apply_full_sync()` runs?

??? note "Reference answers"

    1. Offsets are only positions inside one history. Different primaries can
       produce different commands at the same numeric offset, so the position
       needs generation/replication lineage.
    2. Full sync replaces the whole dictionary. Applying first and validating
       later would already have allowed stale authority to destroy current
       state.

### Hands-on

3. Without changing `src/`, copy the experiment above into a temporary file.
   Add a second replica, isolate it before promotion, then heal it afterward.
   Acceptance: print that the healed node's `replication_id`, offset, and data
   equal the promoted primary's values.
4. Design a regression test that changes the queued message's generation while
   keeping its source and replication ID current. Acceptance: the trace must
   contain reason `stale_generation`, and the target dictionary must remain
   empty.

??? note "Reference answers"

    3. Construct with `replica_ids=("r1", "r2")`, call `isolate("r2")`,
       promote `r1`, then call `heal("r2")` and `run_until_idle()`. Compare the
       two snapshots returned by `probe()`. Do not compare mutable private
       dictionaries.
    4. Follow
       `test_current_primary_delivery_requires_current_generation_and_replication_id`
       in `tests/protocols/test_async_primary.py`: send through `g.network` with
       `source_primary="primary"`, `generation=0`, and the current
       `replication_id`. Assert the rejection reason and `{}` data. A proposed
       production change would be shown as a diff in an answer, but this
       exercise needs no repository source edit.

## Summary

Promotion selects a new authority and therefore starts a new history. MiniDist
turns that idea into three arrival-time checks—current primary, generation, and
replication ID—then uses offsets only to order entries inside the accepted
lineage. Active synchronization makes replicas converge to the selected
history, but cannot recover writes that asynchronous acknowledgement left only
on the old primary. Chapter 6 replaces manual promotion with Raft's election:
nodes will advance terms, exchange votes, and choose authority through a
majority.
