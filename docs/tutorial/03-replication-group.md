# Chapter 3 — The Replication Group Abstraction

[中文版](../zh/tutorial/03-replication-group.md)

Comparisons become misleading when two protocols are asked different
questions. MiniDist avoids that problem with a small `ReplicationGroup`
vocabulary: write, read, progress, fail, restart, and inspect. The abstraction
does not force every protocol to claim every guarantee. Its most important
behavior is often refusal.

## Learning objectives

By the end of this chapter, you will be able to:

1. read the `ReplicationGroup` protocol and classify each operation as client,
   lifecycle, progress, or observation;
2. explain every `AckLevel` and `ReadLevel` as an experimental request rather
   than a universal promise;
3. predict which levels the async-primary and Raft implementations accept;
4. use `WriteResult`, `ReadResult`, and immutable probe snapshots without
   depending on private protocol fields; and
5. defend explicit `UnsupportedLevelError` as a correctness feature.

## One driver surface, several meanings

`ReplicationGroup` is a structural `Protocol` in
`src/minidist/protocols/types.py`. A conforming object provides
`client_write`, `client_read`, `tick`, `crash`, `restart`, and `probe`.
Experiments can therefore drive different implementations with the same kinds
of action.

These methods fall into four roles:

| Role | Public operations | Question |
|---|---|---|
| Client | `client_write`, `client_read` | What result does this guarantee request produce? |
| Progress | `tick` | What becomes possible after one logical step? |
| Lifecycle | `crash`, `restart` | What survives a node transition? |
| Observation | `probe` | What read-only facts can a lab assert? |

Concrete groups expose additional public harness operations when a mechanism
needs them. `AsyncPrimaryGroup` adds `promote`, `isolate`, `heal`, and
`run_until_idle`. `RaftGroup` adds election- and convergence-oriented helpers.
Those methods are not hidden wire protocol. They are an experiment control
surface.

The structural protocol intentionally has no base-class implementation. The
async group and Raft group need not inherit shared semantics that would blur
their differences. A lab depends on behavior and explicit result objects, not
an inheritance hierarchy.

## Acknowledgement levels ask “what happened before success?”

`AckLevel` in `src/minidist/protocols/types.py` has four members:

- `NONE`: do not request a replication wait;
- `LEADER`: ask for the current leader/primary boundary;
- `QUORUM`: ask for a majority replication boundary;
- `ALL_ISR`: ask for all members of an in-sync replica set.

The names alone are insufficient. For the async protocol,
`AsyncPrimaryGroup.client_write` accepts `NONE` and `LEADER`. Both paths apply
the key/value to the primary's in-memory dictionary, increment its stream
offset, enqueue messages, and return without waiting for any replica. The
source comment says exactly what `LEADER` means there: “applied by this
primary,” not consensus or durability.

Raft makes a different choice.
`RaftGroup.client_write` in `src/minidist/protocols/raft/group.py` rejects
`NONE`, `LEADER`, and `ALL_ISR`; it accepts only `QUORUM`. It appends a log entry
and does not return `accepted=True` until the entry reaches the Raft commit
boundary. Therefore `LEADER` is not a portable unit of safety, and `ALL_ISR`
does not even describe Raft's membership model.

This is why enum support must be inspected per implementation. A caller cannot
compare two successful writes unless it also compares the requested and
implemented acknowledgement boundaries.

## Read levels ask both “where?” and “with what proof?”

`ReadLevel` has three members:

- `LOCAL`: read a named node, or the implementation's default local node;
- `LEADER`: route to the currently selected leader or primary;
- `LINEARIZABLE`: require evidence that the serving leader is still authorized
  strongly enough for a linearizable read.

`AsyncPrimaryGroup.client_read` accepts `LOCAL` and `LEADER`.
`ReadLevel.LOCAL` can intentionally observe a lagging replica. `LEADER` routes
to `self._primary`, but it does not contact replicas or establish a quorum.
`LINEARIZABLE` raises `UnsupportedLevelError`.

`RaftGroup.client_read` accepts all three levels. `LOCAL` reads a selected
node specifically so experiment 6 can expose a stale follower; it makes no
authority claim. Its leader read is merely routed to the known leader and is
not itself a quorum proof. Its linearizable path performs a simplified
current-term heartbeat barrier and requires successful responses from a
majority. The
[Raft mapping](../mapping.md#mapping-protocol-4-to-the-raft-paper) labels direct
leader reads “semantically opposite” to a guarantee one might infer and the
read-index barrier “intentionally simplified.”

The vocabulary thus prevents an especially common mistake: treating “read from
the leader” as synonymous with “linearizable read.” Location and consistency
evidence are different dimensions.

## Results carry the replication point

`WriteResult` is an immutable dataclass containing `accepted`, `offset`, and an
optional message. `ReadResult` contains `value`, the serving `node`, and its
`offset`. Both live in `src/minidist/protocols/types.py`.

The offset must be interpreted in context:

- async uses a monotonic replication stream offset;
- Raft uses a log/commit index;
- a failed Raft quorum write can return `accepted=False` with the local log
  index that was attempted.

The human-readable async message—`primary accepted; replica delivery is
asynchronous`—is a reminder, not the proof. The proof is the ordering inside
`AsyncPrimaryGroup.client_write`: apply locally, send entries, record
acknowledgement, return without advancing the scheduler.

`probe` provides richer snapshots for assertions.
`AsyncPrimaryGroup.probe` copies each mutable data dictionary, wraps it with
`MappingProxyType`, and returns `NodeState`/`GroupState` values including role,
replication ID, offset, sync mode, current tick, and backlog offsets. A lab can
inspect the protocol without mutating its internal state. Raft similarly
returns value snapshots of terms, roles, logs, commit indexes, and applied data.

Observation is deliberately white-box but read-only. It makes mechanisms
testable without letting a lab manufacture a result by changing private fields.

## “Unsupported” is an experimental result

Imagine a generic benchmark that requests `AckLevel.QUORUM` from every group and
quietly maps failures to each implementation's strongest mode. The async group
would then report success at its local-primary boundary under a label that
means majority. The table would look uniform and be false.

`UnsupportedLevelError`, defined as a `ValueError` subclass, blocks this
failure. `AsyncPrimaryGroup.client_write` checks the requested level before
mutating state. `AsyncPrimaryGroup.client_read` likewise rejects
`LINEARIZABLE` before selecting a node. The Raft implementation performs its
own support checks.

Tests pin the contract:

- `tests/protocols/test_async_primary.py::test_async_primary_rejects_stronger_ack_levels`;
- `tests/protocols/test_async_primary.py::test_async_primary_rejects_linearizable_reads`;
- `tests/protocols/test_raft.py::test_raft_rejects_non_quorum_ack_levels`; and
- `tests/test_public_types.py::test_replication_api_levels_and_results_are_explicit`.

An exception here is not missing polish. It is the data point that prevents
semantic flattening.

## Hands-on experiment: build a support matrix

Run:

```bash
uv run python -c 'from minidist.protocols import *; from minidist.protocols.async_primary import AsyncPrimaryGroup
g=AsyncPrimaryGroup()
for ack in (AckLevel.QUORUM,AckLevel.ALL_ISR):
 try:g.client_write(b"k",b"v",ack)
 except UnsupportedLevelError as e:print(e)
try:g.client_read(b"k",ReadLevel.LINEARIZABLE)
except UnsupportedLevelError as e:print(e)'
```

Observed output:

```text
async primary does not support AckLevel.QUORUM
async primary does not support AckLevel.ALL_ISR
async primary does not support ReadLevel.LINEARIZABLE
```

The rejected writes do not mutate the primary because validation precedes the
offset increment. You can verify that with `group.probe()`.

Now run the contract tests:

```bash
uv run pytest -q tests/test_public_types.py tests/protocols/test_async_primary.py
```

Observed for the repository state used to write this chapter:

```text
.............                                                            [100%]
13 passed in 0.02s
```

Test timing varies by machine and is not a protocol measurement. The useful
output is the test count and zero failures.

## Against real system APIs

The shared levels are teaching vocabulary, not a standard API implemented by
Redis, PostgreSQL, Kafka, and Raft.

Redis ordinary writes normally return after the primary processes the command.
The `WAIT` command can wait for replica acknowledgements, but, as the
[Redis behavior mapping](../mapping.md#mapping-protocols-to-real-system-replication-mechanisms)
states, that does not transform Redis into a strongly consistent quorum log.
MiniDist's async group omits `WAIT` entirely and rejects `QUORUM`.

Kafka's `acks=all` is evaluated against the in-sync replica set and interacts
with `min.insync.replicas`; MiniDist's ISR model implements that bounded
teaching path in Chapter 10. PostgreSQL exposes synchronous commit modes with
WAL durability and standby choices; Chapter 9 explains why MiniDist's
`QUORUM` mapping means all configured synchronous standbys rather than a Raft
majority. Raft commit remains majority-log replication under term rules; it
has no ISR.

Read controls differ just as much. Redis replica reads can be stale. Raft
linearizable reads require a leadership/quorum argument. Production systems add
sessions, retries, duplicate suppression, timeouts, topology discovery,
authentication, and wire errors. MiniDist result dataclasses model none of
those.

Consult the
[experiment matrix](../experiments.md#replication-protocol-experiment-matrix)
before interpreting a level. All four implementations and experiments 1–7
have asserted cells, but interface vocabulary alone is still not
implementation evidence.

## Exercises

### Understanding

1. Why would automatically downgrading `QUORUM` to `LEADER` make a comparison
   invalid?
2. What is the difference between `ReadLevel.LEADER` and
   `ReadLevel.LINEARIZABLE`?

??? note "Reference answers"

    1. The caller asked whether a majority boundary preceded success. A local
       primary application cannot answer that question, so relabeling it would
       report a stronger guarantee than the mechanism supplied.
    2. `LEADER` selects a location. `LINEARIZABLE` also requires current
       authority evidence, implemented here for Raft as a simplified quorum
       heartbeat barrier.

### Hands-on

3. Write a temporary test proving that a rejected async `QUORUM` write leaves
   the primary offset and data unchanged.

   **Acceptance:** run
   `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch03.py`; it must report
   `1 passed`, and no repository source file may change.

??? note "Reference answer"

    ```python
    import pytest
    from minidist.protocols import AckLevel, UnsupportedLevelError
    from minidist.protocols.async_primary import AsyncPrimaryGroup

    def test_rejected_level_has_no_side_effect():
        group = AsyncPrimaryGroup()
        before = group.probe().nodes[group.primary]
        with pytest.raises(UnsupportedLevelError):
            group.client_write(b"k", b"v", AckLevel.QUORUM)
        after = group.probe().nodes[group.primary]
        assert after.offset == before.offset == 0
        assert dict(after.data) == dict(before.data) == {}
    ```

4. Produce a Markdown table listing async-primary and Raft support for all four
   acknowledgement levels and all three read levels. Derive every cell from a
   source branch or test, not from enum names.

   **Acceptance:** each supported cell names the concrete method; each rejected
   cell names `UnsupportedLevelError`. Your async row should allow
   `NONE/LEADER` writes and `LOCAL/LEADER` reads; your Raft row should allow
   `QUORUM` writes and `LEADER/LINEARIZABLE` reads.

??? note "Reference answer"

    | Protocol | Write levels | Read levels |
    |---|---|---|
    | Async primary | `NONE`, `LEADER`; rejects `QUORUM`, `ALL_ISR` | `LOCAL`, `LEADER`; rejects `LINEARIZABLE` |
    | Raft | `QUORUM`; rejects `NONE`, `LEADER`, `ALL_ISR` | `LOCAL`, `LEADER`, `LINEARIZABLE`; `LOCAL` is observation-only |

## Summary

`ReplicationGroup` standardizes the questions, not the answers. Explicit levels
make acknowledgement and read requests comparable; immutable results and probes
make observations inspectable; deliberate exceptions prevent unsupported
guarantees from being smuggled into a chart. With that vocabulary fixed,
Chapter 4 can examine the first complete protocol: an asynchronous primary,
replica sinks, stream offsets, bounded partial resynchronization, and full
fallback.
