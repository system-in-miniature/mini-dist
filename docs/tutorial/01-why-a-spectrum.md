# Chapter 1 — Why a Protocol Spectrum?

[中文版](../zh/tutorial/01-why-a-spectrum.md)

MiniDist is not the 101st miniature Raft implementation. It is a controlled
comparison of replication families. Every family must run the same tiny
key/value workload, use the same deterministic fault laboratory, and expose its
guarantees through the same vocabulary. That makes the protocol—not a different
application, clock, or test harness—the variable we study.

## Learning objectives

By the end of this chapter, you will be able to:

1. explain why Redis-style asynchronous replication and Raft answer different
   acknowledgement and failover questions;
2. locate the simulation, shared API, asynchronous-primary, Raft, lab, and test
   layers in this repository;
3. run both implementations of the normal-replication experiment and identify
   the exact point at which each acknowledges a write;
4. distinguish a logical simulation tick from elapsed time or throughput; and
5. state which planned protocol families and production features MiniDist does
   **not** implement.

## The question is not “Which protocol wins?”

Distributed systems do not all choose textbook consensus because they do not
all optimize the same contract. A Redis primary normally applies a write
locally and sends an asynchronous stream to replicas. This makes the ordinary
write path simple and fast, but a failover can lose a write that the old
primary already acknowledged. Raft waits for a majority before calling an entry
committed. That stronger boundary brings elections, terms, quorum availability,
log reconciliation, and more coordination.

Other systems occupy other points. PostgreSQL transports WAL and lets operators
choose how much synchronous acknowledgement to require. Kafka's ISR and
high-watermark model reasons about a dynamic in-sync set. Some systems use
consensus for metadata but replicate bulk data another way. Those are not
failed attempts at one universal algorithm; they are different answers to
“what must be true when the client sees success?”

MiniDist keeps this distinction visible. The shared API in
`src/minidist/protocols/types.py` defines `AckLevel`, `ReadLevel`,
`WriteResult`, `ReadResult`, and the `ReplicationGroup` protocol. It does not
promise that every implementation supports every enum member. The module
docstring and `UnsupportedLevelError` explicitly establish the rule: a protocol
must reject a requested guarantee it cannot supply instead of silently
weakening it.

That is why a common interface is useful here. It is an experimental
vocabulary, not a lowest-common-denominator abstraction. In Chapter 3 we will
make the unsupported combinations fail on purpose.

## Read the repository as a set of experimental layers

Start with the layout documented in `README.md` under “Repository Layout,” then
follow one dependency at a time:

```text
labs/ and tests/
        |
        v
src/minidist/protocols/types.py       shared experimental vocabulary
        |
        +--> protocols/async_primary/group.py
        |
        +--> protocols/raft/group.py
                    |
                    v
src/minidist/sim/                    clock, scheduler, network, failure, trace
```

The bottom layer controls time and faults. `SimClock.advance` in
`src/minidist/sim/clock.py` changes an integer only when the harness asks it to.
`Scheduler.tick` in `src/minidist/sim/scheduler.py` advances that clock once and
runs every event now due. `SimNet.send` in
`src/minidist/sim/network.py` obtains delay, drop, and reorder choices from a
private `random.Random(seed)`. `Trace.record` in
`src/minidist/sim/trace.py` appends a sequence-numbered observation.

The protocol layer owns replication semantics. In
`src/minidist/protocols/async_primary/group.py`,
`AsyncPrimaryGroup.client_write` mutates the primary, enqueues an entry for
each replica, and immediately returns. In
`src/minidist/protocols/raft/group.py`, `RaftGroup.client_write` accepts only
`AckLevel.QUORUM` and drives the simulated cluster until a majority has
replicated the entry or the bounded attempt fails. The same method name does
not erase the different acknowledgement contracts.

The top layer contains executable claims. `labs/exp01_normal_replication.py`
uses only public group operations, while
`tests/labs/test_experiments.py::test_experiment_1_observes_delayed_convergence`
turns its observations into regression assertions. A tutorial sentence is most
trustworthy when you can trace it from prose to lab, public method, internal
mechanism, and test.

### A source-reading discipline

Use the repository in that order instead of opening the largest implementation
first. Begin with a lab to identify the observable question. Read the shared
type to learn the requested guarantee. Enter the concrete group's public method
and note where it returns relative to network progress. Only then follow private
handlers and scheduler callbacks. Finish at the regression test that fixes the
observation.

For example, the sentence “the asynchronous primary acknowledges before
replica delivery” is not inferred from the class name. The lab reads a replica
before `tick`; `AsyncPrimaryGroup.client_write` returns without ticking;
`SimNet.send` only schedules `_deliver`; and
`test_write_acks_before_replica_converges` observes the replica remaining empty
until the configured delay elapses. Four independent pieces line up.

Also separate mechanism from harness. `probe`, `isolate`, and
`run_until_converged` make experiments convenient, but they are not client wire
commands. Conversely, private methods such as `_apply_entry` implement a
mechanism but are not permission for a lab to bypass the public path. This
discipline will matter when Chapter 8 asks whether an output was produced by a
real protocol path or manufactured by test setup.

### Four questions to carry through the book

For every protocol, write down four boundaries before judging it. First, what
must happen before a write returns success? Second, which node may serve a read,
and what evidence authorizes that node? Third, after a disconnection, what
position and history identifiers permit incremental catch-up? Fourth, who is
allowed to choose a successor, and how is obsolete authority fenced?

The async group answers with local-primary acknowledgement, local-or-primary
reads, replication ID plus offset, and manual promotion plus a generation
check. Raft answers with majority commit, leader or read-index reads, term/index
log matching, and majority election with higher-term fencing. Later chapters
unpack those answers. Keeping the questions fixed prevents a feature checklist
from replacing semantic comparison.

Failure outcomes must also be attached to their preconditions. “An acknowledged
write can be lost” is true for the async `LEADER` boundary demonstrated in lab
2; it does not mean every replicated async write disappears on every crash.
“Raft preserves an acknowledged write” depends on the lab's `QUORUM` success
and surviving majority; it does not mean Raft remains writable without a
majority. Precision about acknowledgement, surviving topology, and selected
read level is what turns an anecdote into a protocol claim.

Finally, separate safety, liveness, durability, and performance. A protocol can
refuse writes during a partition to preserve safety; that refusal is not data
loss. A value retained in MiniDist's in-process durable mapping is not proof of
disk durability. A run that completes in fewer ticks is not faster in
milliseconds. The common laboratory lets us compare selected safety and
progress boundaries, but it does not collapse these properties into one score.

## First conversation: one key, two protocols

The normal-replication lab writes `project=MiniDist`. Its asynchronous branch
constructs an `AsyncPrimaryGroup`, calls
`AsyncPrimaryGroup.client_write(..., AckLevel.LEADER)`, reads the replica before
progress, calls `AsyncPrimaryGroup.tick`, and reads again. The Raft branch first
uses `RaftGroup.run_until_leader`, writes at `AckLevel.QUORUM`, and then calls
`RaftGroup.run_until_converged`.

The byte strings matter. `ReplicationGroup.client_write` in
`src/minidist/protocols/types.py` specifies `bytes` keys and values, and both
implementations validate or consume that representation. MiniDist is not
teaching text encoding, client sessions, or a wire protocol. It keeps the state
machine small enough that the replication decision remains visible.

The offset printed by the two branches is also protocol-specific. The async
group's offset counts its ordered write stream. The Raft result reports a log
index; the first elected leader appends a no-op, so the user write commonly
appears at index 2. Equal-looking integers across protocol probes are not a
universal durability unit.

## Hands-on experiment: observe both acknowledgement boundaries

Run from the repository root:

```bash
uv sync --dev
uv run python --version
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

Observed in this repository:

```text
Python 3.12.2
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

Do not conclude that Raft “also lost” the value because one follower initially
shows `None`. `RaftGroup.client_write` needed only a majority, not every node,
before returning. The selected follower can still be behind while the committed
entry is already held by a majority. By contrast, the async primary returned
before *any* replica participated. Chapter 8 interprets the full side-by-side
experiment set.

## Against real systems

The async path deliberately resembles default Redis replication: ordinary
writes do not wait for replicas, replicas consume an ordered stream, and
offsets describe stream progress. The Raft path follows the state and RPC rules
of Figure 2 of the Raft paper, including terms, voting, AppendEntries, and
majority commit.

The resemblance has hard boundaries:

- MiniDist is entirely in-process. `SimNet.send` transports Python values, not
  TCP packets or serialized Redis/Raft frames.
- A tick denotes ordering, not milliseconds. No throughput or latency claim
  follows from a tick count.
- Protocols 2 (WAL transport) and 3 (ISR + high watermark) are not implemented.
- There is no Redis Sentinel, Redis Cluster, `WAIT`, RDB/AOF policy, socket
  client, Raft membership change, or disk/fsync model.
- The async crash model preserves the educational node's data to isolate
  replication loss; it is not a claim about Redis recovery without persistence.

The exact Redis correspondences and simplifications are listed in
[Mapping Protocols to Real-System Replication Mechanisms](../mapping.md), and
the implemented-versus-future status is explicit in the
[replication protocol experiment matrix](../experiments.md#replication-protocol-experiment-matrix).
Treat those pages as a contract. Passing tests cannot imply support for a row
marked “Not implemented” or “Future experiment.”

## Exercises

### Understanding

1. Why is `AckLevel` an enum shared by both protocols if the protocols reject
   different members?
2. In experiment 1, why can the selected Raft follower still read `None` after
   `client_write` returns successfully?
3. Which source function proves that a simulation tick is advanced explicitly,
   and why does that forbid interpreting ticks as milliseconds?

??? note "Reference answers"

    1. It supplies a common question—how much replication work must precede
       success—without pretending every protocol has every answer.
       `UnsupportedLevelError` preserves the semantic difference.
    2. QUORUM requires a majority, not all three nodes. The entry is committed
       once two nodes cover it; leaderCommit propagation to the remaining
       follower can occur later.
    3. `src/minidist/sim/clock.py::SimClock.advance` changes only a logical
       integer at harness request. It never reads wall-clock time, so the number
       records order rather than duration.

### Hands-on

4. Without editing `src/`, add a temporary test that runs experiment 1 twice
   with the async protocol and asserts equality of the two
   `ExperimentResult` values.

   **Acceptance:** save it under `/tmp/test_minidist_ch01.py` and run
   `uv run pytest -q /tmp/test_minidist_ch01.py`; the result must be `1 passed`.

??? note "Reference answer"

    ```python
    from labs.exp01_normal_replication import run_experiment

    def test_normal_experiment_is_repeatable():
        assert run_experiment(protocol="async", verbose=False) == run_experiment(
            protocol="async", verbose=False
        )
    ```

    This changes no repository source. If your pytest environment does not add
    the repository root to imports for `/tmp`, run
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch01.py`.

## Summary

MiniDist turns protocol choice into a controlled experiment: one state machine,
one fault laboratory, one explicit vocabulary, and multiple honest guarantee
sets. The first lab already separates “the primary accepted my write” from “a
majority replicated my log entry.” To trust later failure experiments, however,
we must first understand why the laboratory itself repeats exactly. Chapter 2
opens the deterministic simulation foundation event by event.
