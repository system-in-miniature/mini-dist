# Stage 07 · 公共持久性与观察边界

### 目标

在 Simulator、异步主从与 Raft 之间显式表达进程崩溃、未刷盘掉电丢失、本地陈旧读取与合法 Timeout 几何。

??? note "交付文件"
    - `src/minidist/protocols/async_primary/group.py`
    - `src/minidist/protocols/raft/group.py`
    - `src/minidist/sim/failure.py`
    - `tests/protocols/test_async_primary.py`
    - `tests/protocols/test_raft.py`
    - `tests/sim/test_simulation.py`

### 当前遇到的问题

早期 Crash 语义保留所有看似 Persistent 的数据，把进程重启与未刷盘存储丢失混为一谈。协议比较还需要相同的 Local Read 观察，以及对短于消息往返预算的 Election Timeout 的拒绝。

### 测试契约

#### 先看会坏在哪里

Simulator 契约先暂存 Offset 5、Fsync，再暂存 Offset 6，然后注入 `lose_unfsynced=True` 的 Crash。Restart 必须恢复 5，而不是 4 或 6。简单保留或清空全部 Persistent Data 的实现都无法表达该边界。

??? note "文件差异：tests/sim/test_simulation.py"
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

**测试锁定什么**

锁定恢复到最后一次完整 Fsync，同时保持普通进程 Crash 语义不变。

**如何构造反例**

测试创建两个被一次 Fsync 分隔的暂存持久更新，再掉电并重启节点。

**关键测试语句**

```python
failures.crash("n1", lose_unfsynced=True)
failures.restart("n1")
```

**失败意味着什么**

故障模型无法区分进程已接受的 Page-cache 状态与最后持久镜像。

??? note "文件差异：tests/protocols/test_async_primary.py"
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

**测试锁定什么**

Leader Ack 仍独立于本地 Fsync：已刷盘值能在掉电后保留，随后已确认但未刷盘值消失。

**如何构造反例**

测试写入、Fsync、再次写入、丢失未刷盘状态、重启同一 Primary，再读取两个 Key。

**关键测试语句**

```python
assert group.client_read(b"window", ReadLevel.LEADER).value is None
```

**失败意味着什么**

异步模型把确认当成隐式持久化，或者回滚越过了命名 Fsync 边界。

??? note "文件差异：tests/protocols/test_raft.py"
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

**测试锁定什么**

三个场景锁定 Timeout/Network 兼容性、落后 Follower 本地读，以及 Quorum Ack 在显式未刷盘丢失注入后的存活。

**如何构造反例**

它拒绝小于网络往返延迟的 Election Timeout，隔离一个 Follower 做本地读，并在 Quorum Write 后 Crash Leader。

**关键测试语句**

```python
assert group.probe().nodes[new_leader].durable_index >= result.offset
```

**失败意味着什么**

模型允许不可能的活性时间配置、隐藏本地陈旧性，或在 Raft 稳定存储义务前确认。

### 基本概念

Staged Persistent State 对进程可见，但尚不属于 Durable Image。`fsync` 推进该镜像。普通 Process Crash 清理易失内存，不凭空制造磁盘丢失；Power-loss Fault 可以恢复最后 Fsync 镜像。Network-delay Budget 属于活性配置。Local Read 描述单节点状态，因此即使在共识协议里也可能陈旧。

### 为什么需要这个机制

跨协议实验需要相同故障词汇。如果“Crash”暗中表示不同事情，持久性比较就无效，时间故障也可能被错怪为共识逻辑而非不可能配置。

### 运行时心智模型

Failure Injector 同时保存当前 Persistent Mapping 与独立 Durable Snapshot。协议 Group 暴露兼容的 `fsync` 和 `crash(..., lose_unfsynced=...)`。异步协议在掉电时恢复 Durable Data/Offset/Lineage；Raft 在依赖它们的协议动作前已持久化 Term、Vote 与 Log，并通过 Probe 报告该边界。

### 机制板块

#### 显式暂存态与持久态

扩展公共故障模型，把进程崩溃与掉电回滚分成独立、可观察的故障。

??? note "文件差异：src/minidist/sim/failure.py"
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

**是什么，为什么现在需要**

公共节点生命周期现在除当前进程可见 Persistent State 外，还拥有 Last-fsynced Snapshot。

**在运行时做什么**

`stage_persistent` 改变当前 Mapping，`fsync` 把它复制进 Durable Image；只有显式请求的丢失才在 Crash Callback 前恢复该镜像。

**关键代码**

```python
node.persistent.clear()
node.persistent.update(self._durable[node_id])
```

**关键语句理解**

回滚发生在发布 Node-down 状态以前，因此观察者不会看到已崩溃节点仍拥有更新但未持久值。

#### 异步确认与本地 fsync

显式展示 Primary 的哪些状态能在未刷盘写入丢失后存活。

??? note "文件差异：src/minidist/protocols/async_primary/group.py"
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

**是什么，为什么现在需要**

每个异步节点现在分别跟踪 Durable Data、Durable Offset 与 Durable Replication Identity，而不是与 Live State 混用。

**在运行时做什么**

显式 Fsync 快照 Live Primary；Power-loss Crash 在 Restart 与后续 Catch-up 前精确恢复该 Tuple。

**关键代码**

```python
if lose_unfsynced:
    target.data = dict(target.durable_data)
    target.offset = target.durable_offset
```

**关键语句理解**

Data 与 Position 必须一起回滚。只回滚字节或只回滚 Offset 都会让后续复制连续性失真。

#### Raft 稳定存储、时间预算与本地读取

让 Raft 对齐公共持久性与读取观察面，并拒绝不可能的 Timeout 几何。

??? note "文件差异：src/minidist/protocols/raft/group.py"
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

**是什么，为什么现在需要**

Raft 获得公共 Fsync/Crash Surface、Probe 中的显式 Durable Index、本地读路由，以及消息延迟几何的构造校验。

**在运行时做什么**

Group 暴露其已有 Stable Log Boundary，并阻止无法在重复选举前完成请求/响应的 Timeout 配置。

**关键代码**

```python
minimum_timeout = 2 * network_delay + heartbeat_interval
if low <= minimum_timeout:
    raise ValueError(
        "lowest election timeout must exceed twice the maximum network "
        "delay plus the heartbeat interval"
    )
```

**关键语句理解**

最小 Election Timeout 必须容纳完整往返；确定性 Seed 也无法挽救不存在可行通信窗口的配置。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-m3-shared-boundaries/tests.txt)`。五条新增契约与既有套件共同区分持久性、本地可见性与活性时间几何。

### 需要真正记住的内容

Process Crash 不等于 Power Loss；Fsync 推进命名 Durable Image；Local Read 可以陈旧；Timeout 配置必须尊重 Transport Delay。

### 用自己的话讲清楚

说明一个已确认异步写入的三个状态——Primary 可见、本地持久、副本可见——以及哪种注入故障能移除哪一层。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/02-deterministic-simulation.md) · [第 7 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/07-raft-replication.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-06...stage-07)

完成后可运行 `git checkout stage-07` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/07-m3-shared-boundaries/stage.patch)
