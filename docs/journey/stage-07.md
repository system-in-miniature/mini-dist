# Stage 07 · Shared durability and observation boundaries

### Goal

Make process crash, unflushed power loss, local stale reads, and valid timeout geometry explicit across the simulator, async primary, and Raft.

??? note "Deliverable files"
    - `src/minidist/protocols/async_primary/group.py`
    - `src/minidist/protocols/raft/group.py`
    - `src/minidist/sim/failure.py`
    - `tests/protocols/test_async_primary.py`
    - `tests/protocols/test_raft.py`
    - `tests/sim/test_simulation.py`

### The problem at this point

Earlier crash semantics retain all persistent-looking data, which conflates a process restart with loss of unflushed storage. The protocol comparison also needs the same local-read observation and a guard against election timeouts shorter than a round-trip message budget.

### Test contract

#### See the failure first

The simulator contract stages offset 5, fsyncs it, stages offset 6, and then injects a crash with `lose_unfsynced=True`. Restart must recover 5, not 4 or 6. A crash implementation that simply keeps or clears all persistent data cannot express this boundary.

??? note "File diff: tests/sim/test_simulation.py"
    ```diff
    diff --git a/tests/sim/test_simulation.py b/tests/sim/test_simulation.py
    index a80c7f40ba68d4b6a8ee20fc32765633ba66601f..9946c262b4f9eb5edadea825067802ba5ac76f11 100644
    --- a/tests/sim/test_simulation.py
    +++ b/tests/sim/test_simulation.py
    @@ -123,3 +123,21 @@ def test_crash_restart_clears_volatile_and_keeps_persistent_state() -> None:
         failures.restart("n1")
         assert node.alive
         assert node.persistent == {"offset": 4}
    +
    +
    +def test_explicit_fsync_loss_crash_discards_only_unflushed_updates() -> None:
    +    trace = Trace()
    +    clock = SimClock()
    +    node = SimNode(node_id="n1", persistent={"offset": 4})
    +    failures = FailureInjector(clock=clock, trace=trace)
    +    failures.register(node)
    +
    +    failures.stage_persistent("n1", "offset", 5)
    +    failures.fsync("n1")
    +    failures.stage_persistent("n1", "offset", 6)
    +    assert node.persistent == {"offset": 6}
    +
    +    failures.crash("n1", lose_unfsynced=True)
    +
    +    assert node.persistent == {"offset": 5}
    +    assert trace.events[-1].details["lost_unfsynced"] is True
    ```

**What this test locks**

It locks last-complete-fsync restoration while keeping ordinary process crash semantics unchanged.

**How it constructs the counterexample**

The test creates two staged persistent updates separated by exactly one fsync, then powers off and restarts the node.

**Key test statement**

```python
failures.crash("n1", lose_unfsynced=True)
failures.restart("n1")
```

**What a failure means**

The failure model cannot distinguish accepted page-cache state from the last durable image.

??? note "File diff: tests/protocols/test_async_primary.py"
    ```diff
    diff --git a/tests/protocols/test_async_primary.py b/tests/protocols/test_async_primary.py
    index 07dbe73933b2cab132f4835f47c57c8939e62204..0e0dbf5a131004940af1a3cd194025a6ab71be2d 100644
    --- a/tests/protocols/test_async_primary.py
    +++ b/tests/protocols/test_async_primary.py
    @@ -173,3 +173,17 @@ def test_current_primary_delivery_requires_current_generation_and_replication_id
             and event.details["reason"] == reason
             for event in group.trace.events
         )
    +
    +
    +def test_power_loss_discards_acked_but_unfsynced_primary_state() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    +    group.client_write(b"durable", b"yes", AckLevel.LEADER)
    +    group.fsync("primary")
    +    group.client_write(b"window", b"lost", AckLevel.LEADER)
    +
    +    group.crash("primary", lose_unfsynced=True)
    +    group.restart("primary")
    +
    +    assert group.client_read(b"durable", ReadLevel.LEADER).value == b"yes"
    +    assert group.client_read(b"window", ReadLevel.LEADER).value is None
    +    assert group.probe().nodes["primary"].durable_offset == 1
    ```

**What this test locks**

Leader acknowledgement remains independent of local fsync: a flushed value survives power loss while a later acknowledged but unflushed value disappears.

**How it constructs the counterexample**

It writes, fsyncs, writes again, loses unflushed state, restarts the same primary, and reads both keys.

**Key test statement**

```python
assert group.client_read(b"window", ReadLevel.LEADER).value is None
```

**What a failure means**

The async model is treating acknowledgement as implicit durability or rolling back beyond the named fsync boundary.

??? note "File diff: tests/protocols/test_raft.py"
    ```diff
    diff --git a/tests/protocols/test_raft.py b/tests/protocols/test_raft.py
    index 56957b59ea2752d541d167fed3f09173f950e7ee..16c1e6c277e47c3f05dfc768f10f0b3d1abf8e00 100644
    --- a/tests/protocols/test_raft.py
    +++ b/tests/protocols/test_raft.py
    @@ -17,6 +17,15 @@ def elected_group(*, seed: int = 7) -> RaftGroup:
         return group


    +def test_election_timeout_rejects_round_trip_network_delay_budget() -> None:
    +    with pytest.raises(ValueError, match="network delay"):
    +        RaftGroup(
    +            election_timeout_range=(4, 7),
    +            heartbeat_interval=1,
    +            network_delay=8,
    +        )
    +
    +
     def test_seeded_timeouts_elect_exactly_one_leader() -> None:
         group = elected_group(seed=11)

    @@ -139,3 +148,28 @@ def _election_partition_heal_trace(seed: int) -> list[dict[str, object]]:
     def test_same_seed_replays_election_and_partition_heal_exactly() -> None:
         assert _election_partition_heal_trace(37) == _election_partition_heal_trace(37)

    +
    +def test_local_read_can_observe_a_lagging_raft_follower() -> None:
    +    group = elected_group(seed=43)
    +    state = group.probe()
    +    follower = next(node for node in state.nodes if node != state.leader)
    +    group.isolate(follower)
    +    group.client_write(b"k", b"v", AckLevel.QUORUM)
    +
    +    result = group.client_read(b"k", ReadLevel.LOCAL, node=follower)
    +
    +    assert result.value is None
    +    assert result.node == follower
    +
    +
    +def test_quorum_ack_survives_explicit_unfsynced_loss_injection() -> None:
    +    group = elected_group(seed=47)
    +    old_leader = group.probe().leader
    +    result = group.client_write(b"stable", b"raft", AckLevel.QUORUM)
    +
    +    group.crash(old_leader, lose_unfsynced=True)
    +    new_leader = group.run_until_leader(exclude=old_leader)
    +
    +    assert result.accepted
    +    assert group.client_read(b"stable", ReadLevel.LINEARIZABLE).value == b"raft"
    +    assert group.probe().nodes[new_leader].durable_index >= result.offset
    ```

**What this test locks**

Three cases lock timeout/network compatibility, stale local follower reads, and survival of a quorum acknowledgement after explicit unflushed-loss injection.

**How it constructs the counterexample**

It rejects an election timeout below network round-trip delay, isolates a follower for a local read, and crashes the leader after a quorum write.

**Key test statement**

```python
assert group.probe().nodes[new_leader].durable_index >= result.offset
```

**What a failure means**

The model either permits impossible liveness timing, hides local staleness, or acknowledges before Raft's stable-storage obligation.

### Basic concepts

Staged persistent state is visible to the process but not yet in the durable image. `fsync` advances that image. An ordinary process crash clears volatile memory without inventing disk loss; a power-loss fault may restore the last fsync image. Network-delay budget is part of liveness configuration. Local reads describe one node's state and therefore may be stale even in a consensus protocol.

### Why this mechanism is necessary

Cross-protocol experiments need the same fault vocabulary. If “crash” means different hidden things, durability comparisons are invalid and timing failures may be blamed on consensus logic rather than impossible configuration.

### Runtime mental model

The failure injector keeps a current persistent mapping and a separate durable snapshot. Protocol groups expose compatible `fsync` and `crash(..., lose_unfsynced=...)` operations. Async copies its durable data/offset/lineage back on power loss; Raft already persists term, vote, and log before dependent protocol actions and reports that boundary through its probe.

### Mechanism blocks

#### Explicit staged and durable state

Extend the common failure model so process crash and power-loss rollback are separate, observable faults.

??? note "File diff: src/minidist/sim/failure.py"
    ```diff
    diff --git a/src/minidist/sim/failure.py b/src/minidist/sim/failure.py
    index 05011571c8a88b86ff7ed7e7da69b646ff1013c7..61f7f1e45dd46115096a0386a898e6131b8a7d78 100644
    --- a/src/minidist/sim/failure.py
    +++ b/src/minidist/sim/failure.py
    @@ -38,21 +38,57 @@ class FailureInjector:
             self.clock = clock
             self.trace = trace
             self._nodes: dict[str, SimNode] = {}
    +        self._durable: dict[str, dict[str, Any]] = {}

         def register(self, node: SimNode) -> None:
             if node.node_id in self._nodes:
                 raise ValueError(f"node already registered: {node.node_id}")
             self._nodes[node.node_id] = node
    +        self._durable[node.node_id] = dict(node.persistent)

    -    def crash(self, node_id: str) -> None:
    +    def stage_persistent(self, node_id: str, key: str, value: Any) -> None:
    +        """Expose a write that the process has accepted but not fsynced."""
    +
    +        node = self._node(node_id)
    +        if not node.alive:
    +            raise RuntimeError(f"node is crashed: {node_id}")
    +        node.persistent[key] = value
    +        self.trace.record(
    +            self.clock.now,
    +            "persistent_write_staged",
    +            node=node_id,
    +            key=key,
    +        )
    +
    +    def fsync(self, node_id: str) -> None:
    +        """Advance the simulated disk boundary to the node's current state."""
    +
    +        node = self._node(node_id)
    +        if not node.alive:
    +            raise RuntimeError(f"node is crashed: {node_id}")
    +        self._durable[node_id] = dict(node.persistent)
    +        self.trace.record(self.clock.now, "node_fsynced", node=node_id)
    +
    +    def crash(self, node_id: str, *, lose_unfsynced: bool = False) -> None:
             node = self._node(node_id)
             if not node.alive:
                 raise ValueError(f"node already crashed: {node_id}")
    +        if lose_unfsynced:
    +            # A power-loss fault restores the last complete fsync image.  It is
    +            # explicit because an ordinary process crash does not imply that
    +            # the kernel forgot acknowledged page-cache writes.
    +            node.persistent.clear()
    +            node.persistent.update(self._durable[node_id])
             # Clear volatile data before publishing the down state so observers can
             # never see a crashed node that still owns in-flight process memory.
             node.on_crash()
             node.alive = False
    -        self.trace.record(self.clock.now, "node_crashed", node=node_id)
    +        self.trace.record(
    +            self.clock.now,
    +            "node_crashed",
    +            node=node_id,
    +            lost_unfsynced=lose_unfsynced,
    +        )

         def restart(self, node_id: str) -> None:
             node = self._node(node_id)
    ```

**What it is and why it appears**

The common node lifecycle now owns a last-fsynced snapshot in addition to current process-visible persistent state.

**Runtime role**

`stage_persistent` changes the current mapping, `fsync` copies it into the durable image, and only an explicitly requested loss restores that image before crash callbacks run.

**Key code**

```python
node.persistent.clear()
node.persistent.update(self._durable[node_id])
```

**Statement understanding**

Rollback happens before publishing the node-down state, so observers never see a crashed node owning a newer, non-durable value.

#### Async acknowledgement versus local fsync

Expose which primary state survives an explicit loss of unflushed writes.

??? note "File diff: src/minidist/protocols/async_primary/group.py"
    ```diff
    diff --git a/src/minidist/protocols/async_primary/group.py b/src/minidist/protocols/async_primary/group.py
    index 2e69a2d37569df295c0f82500ed0515fced1322f..8c857a047a36f00ce86008fd06a326d9b24dc651 100644
    --- a/src/minidist/protocols/async_primary/group.py
    +++ b/src/minidist/protocols/async_primary/group.py
    @@ -57,6 +57,9 @@ class _Node:
         alive: bool = True
         volatile: dict[str, Any] = field(default_factory=dict)
         sync_mode: SyncMode = SyncMode.NEVER
    +    durable_data: dict[bytes, bytes] = field(default_factory=dict)
    +    durable_offset: int = 0
    +    durable_replication_id: str = ""


     @dataclass(frozen=True, slots=True)
    @@ -70,6 +73,7 @@ class NodeState:
         offset: int
         data: Mapping[bytes, bytes]
         sync_mode: SyncMode
    +    durable_offset: int


     @dataclass(frozen=True, slots=True)
    @@ -120,6 +124,8 @@ class AsyncPrimaryGroup:
                 node_id: _Node(node_id=node_id, replication_id=replication_id)
                 for node_id in node_ids
             }
    +        for node in self._nodes.values():
    +            node.durable_replication_id = replication_id
             self._backlog: deque[_Entry] = deque(maxlen=backlog_size)
             for node_id in node_ids:
                 self.network.register(
    @@ -234,11 +240,39 @@ class AsyncPrimaryGroup:

             return self.scheduler.run_until_idle(max_ticks=max_ticks)

    -    def crash(self, node: NodeId) -> None:
    +    def fsync(self, node: NodeId) -> None:
    +        """Persist the node's current dataset and replication position."""
    +
    +        target = self._node(node)
    +        self._require_alive(target)
    +        target.durable_data = dict(target.data)
    +        target.durable_offset = target.offset
    +        target.durable_replication_id = target.replication_id
    +        self.trace.record(
    +            self.clock.now,
    +            "protocol_node_fsynced",
    +            node=node,
    +            offset=target.offset,
    +        )
    +
    +    def crash(self, node: NodeId, *, lose_unfsynced: bool = False) -> None:
             """Crash a node, clearing process-local state but retaining its dataset."""

             target = self._node(node)
             self._require_alive(target)
    +        if lose_unfsynced:
    +            target.data = dict(target.durable_data)
    +            target.offset = target.durable_offset
    +            target.replication_id = target.durable_replication_id
    +            if node == self._primary:
    +                self._backlog = deque(
    +                    (
    +                        entry
    +                        for entry in self._backlog
    +                        if entry.offset <= target.durable_offset
    +                    ),
    +                    maxlen=self._backlog.maxlen,
    +                )
             target.volatile.clear()
             target.alive = False
             # A queued packet from the old primary must not be delivered after its
    @@ -246,7 +280,12 @@ class AsyncPrimaryGroup:
             for peer in self._nodes:
                 if peer != node:
                     self.network.partition(node, peer, bidirectional=True)
    -        self.trace.record(self.clock.now, "protocol_node_crashed", node=node)
    +        self.trace.record(
    +            self.clock.now,
    +            "protocol_node_crashed",
    +            node=node,
    +            lost_unfsynced=lose_unfsynced,
    +        )

         def restart(self, node: NodeId) -> None:
             """Restart a node and reconnect a replica through Redis-style resync."""
    @@ -323,6 +362,7 @@ class AsyncPrimaryGroup:
                     offset=node.offset,
                     data=MappingProxyType(dict(node.data)),
                     sync_mode=node.sync_mode,
    +                durable_offset=node.durable_offset,
                 )
                 for node_id, node in self._nodes.items()
             }
    ```

**What it is and why it appears**

Each async node now tracks durable data, durable offset, and durable replication identity separately from live state.

**Runtime role**

Explicit fsync snapshots the live primary; a power-loss crash restores exactly that tuple before restart and future catch-up.

**Key code**

```python
if lose_unfsynced:
    target.data = dict(target.durable_data)
    target.offset = target.durable_offset
```

**Statement understanding**

Data and position roll back together. Rolling back only bytes or only offset would make subsequent replication continuity dishonest.

#### Raft stable storage, timing budget, and local reads

Align Raft with the shared durability and read-observation surface while rejecting impossible timeout geometry.

??? note "File diff: src/minidist/protocols/raft/group.py"
    ```diff
    diff --git a/src/minidist/protocols/raft/group.py b/src/minidist/protocols/raft/group.py
    index 632998bf9d79d18a69c437808dfeafbed65e11ab..a6544d17bca036357f71f4beda41a51b96d119a2 100644
    --- a/src/minidist/protocols/raft/group.py
    +++ b/src/minidist/protocols/raft/group.py
    @@ -88,6 +88,7 @@ class NodeState:
         log: tuple[LogEntry, ...]
         commit_index: int
         last_applied: int
    +    durable_index: int
         data: Mapping[bytes, bytes]


    @@ -121,6 +122,12 @@ class RaftGroup:
                 )
             if heartbeat_interval <= 0 or network_delay <= 0:
                 raise ValueError("heartbeat interval and network delay must be positive")
    +        minimum_timeout = 2 * network_delay + heartbeat_interval
    +        if low <= minimum_timeout:
    +            raise ValueError(
    +                "lowest election timeout must exceed twice the maximum network "
    +                "delay plus the heartbeat interval"
    +            )

             self.clock = SimClock()
             self.trace = Trace()
    @@ -212,7 +219,15 @@ class RaftGroup:

             self._validate_bytes(key, "key")
             if level is ReadLevel.LOCAL:
    -            raise UnsupportedLevelError("Raft does not support ReadLevel.LOCAL")
    +            selected = self._leader().node_id if node is None else node
    +            target = self._node(selected)
    +            if not target.alive:
    +                raise RuntimeError(f"node is crashed: {selected}")
    +            return ReadResult(
    +                value=target.data.get(key),
    +                node=selected,
    +                offset=target.commit_index,
    +            )
             if node is not None:
                 raise ValueError("Raft leader reads do not accept an explicit node")
             leader = self._leader()
    @@ -315,7 +330,26 @@ class RaftGroup:
                 self.tick()
             raise RuntimeError("Raft group did not converge")

    -    def crash(self, node: NodeId) -> None:
    +    def fsync(self, node: NodeId) -> None:
    +        """Expose Raft's stable-storage boundary for cross-protocol labs.
    +
    +        Raft persists term/vote/log before the RPC or client-visible action
    +        that depends on them, so the in-process representation is already at
    +        this boundary when this method is called.
    +        """
    +
    +        target = self._node(node)
    +        if not target.alive:
    +            raise RuntimeError(f"node is crashed: {node}")
    +        self.trace.record(
    +            self.clock.now,
    +            "raft_node_fsynced",
    +            node=node,
    +            durable_index=len(target.log) - 1,
    +            current_term=target.current_term,
    +        )
    +
    +    def crash(self, node: NodeId, *, lose_unfsynced: bool = False) -> None:
             """Crash a server, preserving only term, vote, and log."""

             target = self._node(node)
    @@ -329,6 +363,7 @@ class RaftGroup:
                 node=node,
                 current_term=target.current_term,
                 log_length=len(target.log) - 1,
    +            lost_unfsynced=lose_unfsynced,
             )

         def restart(self, node: NodeId) -> None:
    @@ -379,6 +414,7 @@ class RaftGroup:
                     log=tuple(node.log),
                     commit_index=node.commit_index,
                     last_applied=node.last_applied,
    +                durable_index=len(node.log) - 1,
                     data=MappingProxyType(dict(node.data)),
                 )
                 for node_id, node in self._nodes.items()
    @@ -863,4 +899,3 @@ class RaftGroup:
         def _validate_bytes(value: bytes, field_name: str) -> None:
             if not isinstance(value, bytes):
                 raise TypeError(f"{field_name} must be bytes")
    -
    ```

**What it is and why it appears**

Raft gains the shared fsync/crash surface, explicit durable indices in probes, local read routing, and constructor validation for message-delay geometry.

**Runtime role**

The group exposes its already-stable log boundary and prevents a timeout configuration that cannot complete a request/response before elections repeat.

**Key code**

```python
minimum_timeout = 2 * network_delay + heartbeat_interval
if low <= minimum_timeout:
    raise ValueError(
        "lowest election timeout must exceed twice the maximum network "
        "delay plus the heartbeat interval"
    )
```

**Statement understanding**

The minimum election timeout must leave room for a full round trip; deterministic seeds cannot rescue a configuration with no feasible communication window.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/07-m3-shared-boundaries/tests.txt)`. Five added contracts plus prior suites separate durability, local visibility, and liveness geometry.

### Durable takeaways

Process crash is not power loss. Fsync advances a named durable image, local reads can be stale, and timeout configuration must respect transport delay.

### Explain it in your own words

Explain the three states of an acknowledged async write—primary-visible, locally durable, and replica-visible—and which injected fault can remove each.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/02-deterministic-simulation.md) · [Chapter 7](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/07-raft-replication.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-dist/compare/stage-06...stage-07)

After finishing, use `git checkout stage-07` to compare your result.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/07-m3-shared-boundaries/stage.patch)
