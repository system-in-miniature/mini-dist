# Chapter 2 — The Deterministic Simulation Foundation

[中文版](../zh/tutorial/02-deterministic-simulation.md)

A distributed protocol is mostly interesting at awkward boundaries: a timeout
fires as a message arrives, a node crashes after acknowledgement but before
delivery, or a delayed packet crosses a role change. Reproducing such schedules
with wall clocks and threads is difficult. MiniDist therefore makes schedule,
randomness, failure, and observation explicit values inside one process.

## Learning objectives

After this chapter, you will be able to:

1. trace an event from `SimNet.send` through `Scheduler.schedule` to a
   registered message handler;
2. explain how `SimClock`, scheduler insertion order, and a private seeded RNG
   eliminate host-timing nondeterminism;
3. distinguish send-time drops from delivery-time drops and lifecycle crashes
   from network partitions;
4. inspect a `Trace` and compare two complete runs event for event; and
5. design a deterministic fault experiment without using sleep, threads, or
   real sockets.

## Logical time is controlled state

`SimClock` in `src/minidist/sim/clock.py` owns one integer, `_now`.
`SimClock.advance` accepts only positive steps, increments the integer, and
returns it. There is no `time.time`, `time.monotonic`, or background timer.
Consequently, an idle simulation does not age while the host is busy.

This is stronger than replacing seconds with small numbers. The harness owns
when causality progresses. If a lab calls `client_write` and crashes the
primary before calling `tick`, no scheduled replication delivery can sneak in
because the operating system happened to run another thread.

`Scheduler.schedule` in `src/minidist/sim/scheduler.py` converts a relative
delay to `clock.now + delay`. Its heap key is the pair `(due_tick, order)`.
`order` is a monotonic insertion counter. Two callbacks due at tick 5 therefore
run in scheduling order, independent of callback identity or heap internals.

`Scheduler.tick` first advances the clock once, then repeatedly pops all events
whose `due_tick <= clock.now`. Each callback runs to completion in the same
thread. `Scheduler.run_until_idle` repeats that operation until the heap is
empty, with `max_ticks=10_000` as a guard against a lesson that schedules work
forever.

The consequence is worth stating precisely: deterministic simulation does not
enumerate every possible schedule. It makes a chosen schedule reproducible.
Coverage still requires different seeds and fault scripts.

A trace that repeats under one seed establishes replay, not correctness by
itself. A protocol bug can be perfectly deterministic. Tests must still state
the intended invariant—for example, that a stale-generation message is
rejected—and then use the replayable schedule as evidence. Determinism makes a
failure explainable and regression-friendly; it does not decide whether the
observed behavior is safe.

### Designing a deterministic fault script

A useful script names the boundary before it injects the fault. “Crash the
leader sometime” is vague. “Call `client_write`, do not call `tick`, then call
`crash`” fixes the boundary between acknowledgement and scheduled delivery.
Likewise, “partition after send but before delivery” requires scheduling a
message, adding the relevant directed partition, and only then advancing the
clock.

Keep protocol actions and observations distinct. Actions such as write,
isolate, tick, heal, or promote change the simulated world. `probe` and trace
filtering observe it. If a script edits a private node dictionary to create the
desired state, it has stopped testing the public protocol path. Tests in
`tests/labs/test_experiments.py` deliberately call the same public group API as
the runnable labs for this reason.

A good deterministic test records three things: the fixed setup (node IDs,
seed, and delay ranges), the ordered action script, and the invariant at the
chosen observation point. When it fails, preserve the seed and full trace.
Changing several fault parameters at once may discover a bug, but shrinking the
case to the smallest action sequence makes the causal boundary teachable.

Do not call `run_until_idle` blindly when the intermediate state is the lesson.
It intentionally erases the distinction between “scheduled” and “delivered” by
advancing until no work remains. Experiment 1 uses one explicit tick to expose
that distinction. Conversely, use `run_until_idle` when the assertion is about
eventual convergence and every finite queued event should finish.

## A message has two fault boundaries

`SimNet` in `src/minidist/sim/network.py` combines an immutable `Message`, a
node-to-handler registry, directed partition state, and a private
`random.Random(seed)`. `SimNet.send` performs these steps:

1. allocate a monotonically increasing message ID;
2. append a `message_sent` trace event;
3. drop immediately if the directed link is partitioned;
4. otherwise make a seeded drop decision;
5. sample a seeded delay and optional reorder jitter; and
6. schedule a callback that will later call `SimNet._deliver`.

The callback does not assume the link stayed healthy. `SimNet._deliver` checks
the partition again. A message accepted before a partition can therefore be
dropped as `partition_at_delivery`. If the destination has no registered
handler, the reason is `unknown_destination`. Otherwise the network records
`message_delivered` before invoking the handler.

The two checks make fault placement teachable. “Partition before send” and
“partition while in flight” become different trace evidence rather than one
timing-dependent symptom. Partitions are directed by default:
`SimNet.partition("a", "b")` blocks only a→b. Passing
`bidirectional=True` also inserts b→a. `heal` removes the same selected edges.

The network transports Python payloads. That deliberately removes framing,
serialization, kernel buffers, reconnect behavior, and TCP backpressure. It is
the right boundary for protocol-ordering lessons, not a network-performance
test.

## Lifecycle failure owns different state

`FailureInjector` and `SimNode` in `src/minidist/sim/failure.py` show the generic
crash model. `FailureInjector.crash` calls `SimNode.on_crash`, which clears the
`volatile` mapping, then marks the node dead and records `node_crashed`. The
`persistent` mapping remains. `restart` calls the hook, marks the node alive,
and records the reverse transition.

Protocol implementations may wrap this boundary with topology behavior.
`AsyncPrimaryGroup.crash` in
`src/minidist/protocols/async_primary/group.py`, for example, clears the
protocol node's `volatile` state, marks it dead, and partitions every incident
link bidirectionally so already queued traffic cannot be applied after the
crash.

Neither model simulates a disk. “Persistent” means retained by this in-process
educational model. It says nothing about fsync, torn sectors, process restart
latency, or what a production Redis instance reloads.

## Trace is the replay oracle

`Trace.record` in `src/minidist/sim/trace.py` assigns
`sequence=len(self._events)`, then stores tick, kind, and structured details in
an immutable `TraceEvent`. Sequence disambiguates multiple events at the same
logical tick. The `events` property returns a tuple, preventing callers from
appending to the internal list. `Trace.as_dicts` uses `dataclasses.asdict` to
produce value snapshots convenient for equality checks.

This gives tests a stronger assertion than “both runs end with the same
dictionary.” Two executions might converge accidentally after taking different
election or message paths. Comparing every trace item checks message IDs,
logical ticks, timeout choices, drops, deliveries, and transitions.

`tests/sim/test_simulation.py::test_same_seed_and_script_replay_event_for_event`
constructs two networks with seed 1729 and one with 1730. The first two traces
must be equal; the third must differ. The test also fixes the exact dropped and
delivered message IDs. At the protocol level,
`tests/protocols/test_raft.py::test_same_seed_replays_election_and_partition_heal_exactly`
repeats election, write, isolation, failover, healing, and convergence, then
compares the full traces.

## Hands-on experiment 1: schedule without sleeping

Run:

```bash
uv run python -c 'from minidist.sim import SimClock,Scheduler,Trace; c=SimClock(); t=Trace(); s=Scheduler(c,t); out=[]; s.schedule(2,lambda:out.append("later"),label="later"); s.schedule(1,lambda:out.append("first"),label="first"); print("start",c.now,s.pending,out); print("tick1",s.tick(),c.now,out); print("tick2",s.tick(),c.now,out); print([(e.sequence,e.tick,e.kind) for e in t.events])'
```

Observed output:

```text
start 0 2 []
tick1 1 1 ['first']
tick2 1 2 ['first', 'later']
[(0, 0, 'event_scheduled'), (1, 0, 'event_scheduled'), (2, 1, 'event_started'), (3, 2, 'event_started')]
```

The first number after each `tick` is the count of callbacks run, not the new
time; the next number is `clock.now`. Both schedule records occur at tick 0.

## Hands-on experiment 2: replay seeded network choices

Run the exact script used to expose its observations:

```bash
uv run python -c 'from minidist.sim import SimClock,Scheduler,SimNet,Trace
def run(seed):
 c=SimClock(); t=Trace(); s=Scheduler(c,t); n=SimNet(seed=seed,clock=c,scheduler=s,trace=t,min_delay=1,max_delay=3,drop_rate=.2,reorder_rate=.8); n.register("b",lambda m:None); [n.send("a","b",{"number":i}) for i in range(8)]; s.run_until_idle(); return t.as_dicts()
a=run(1729); b=run(1729); c=run(1730); print("same-seed",a==b); print("different-seed",a==c); print("dropped",[e["details"]["message_id"] for e in a if e["kind"]=="message_dropped"]); print("delivered",[e["details"]["message_id"] for e in a if e["kind"]=="message_delivered"]); print("ticks",[e["tick"] for e in a if e["kind"]=="message_delivered"])'
```

Observed output:

```text
same-seed True
different-seed False
dropped [1, 2]
delivered [4, 5, 7, 0, 3, 6]
ticks [1, 3, 3, 4, 5, 6]
```

Message 0 arriving after messages 4, 5, and 7 demonstrates seeded reordering.
Changing the seed changes the chosen schedule; keeping it reproduces all trace
details.

## Against real deterministic simulation systems

Production-grade deterministic simulation frameworks often control tasks,
timers, storage, RNG sources, process restarts, and sometimes the runtime
itself. They explore many seeds in CI and minimize a failing history. Real
distributed-system test frameworks may also run actual processes or VMs behind
a fault-controlled network.

MiniDist preserves only the mechanisms needed for these lessons:

- explicit logical time via `SimClock.advance`;
- stable event ordering via `Scheduler.schedule`;
- seeded delay/drop/reorder via `SimNet.send`;
- directed and bidirectional link partitions;
- visible volatile-versus-retained lifecycle state; and
- append-only structured observations via `Trace.record`.

It does **not** model CPU concurrency, real sockets, retransmission, disks,
clock drift, bandwidth, message serialization, or exhaustive schedule search.
See the [experiment matrix's determinism boundary](../experiments.md#determinism-boundary)
and [README determinism boundary](https://github.com/system-in-miniature/mini-dist#determinism-boundary).
Ticks are never latency measurements.

## Exercises

### Understanding

1. Why does `Scheduler` need an insertion counter when it already has a due
   tick?
2. What different claims are established by `message_dropped` reasons
   `partition` and `partition_at_delivery`?

??? note "Reference answers"

    1. Multiple events may share a due tick. The counter creates an explicit,
       stable tie-breaker instead of relying on callback comparison or host
       scheduling.
    2. `partition` means the edge was blocked when `send` ran;
       `partition_at_delivery` means the message was scheduled but the edge was
       blocked before its callback ran.

### Hands-on

3. Write a temporary pytest that schedules two delay-1 callbacks in the order
   `"a"`, `"b"` and asserts both run on tick 1 in insertion order.

   **Acceptance:** the test must live outside `src/`; run
   `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch02.py` and obtain
   `1 passed`.

??? note "Reference answer"

    ```python
    from minidist.sim import SimClock, Scheduler, Trace

    def test_same_tick_uses_insertion_order():
        clock, trace = SimClock(), Trace()
        scheduler = Scheduler(clock, trace)
        seen = []
        scheduler.schedule(1, lambda: seen.append("a"), label="a")
        scheduler.schedule(1, lambda: seen.append("b"), label="b")
        assert scheduler.tick() == 2
        assert clock.now == 1
        assert seen == ["a", "b"]
    ```

4. Change only the second call's delay from 1 to 2. State the expected `seen`
   list after each of two ticks before running the test.

   **Acceptance:** add assertions for `["a"]` after tick 1 and `["a", "b"]`
   after tick 2; the temporary test must pass.

??? note "Reference answer"

    The callback due at tick 1 runs first. The other remains queued until the
    second explicit tick. No sleep is needed, and host load cannot change that
    ordering.

## Summary

MiniDist turns nondeterministic-looking races into explicit inputs: logical
ticks, heap order, seeds, directed partitions, lifecycle transitions, and a
complete structured trace. A seed does not prove every possible execution, but
it makes one execution replayable enough to debug and teach. Chapter 3 now uses
this stable laboratory to compare protocols through one vocabulary without
pretending their guarantees are interchangeable.
