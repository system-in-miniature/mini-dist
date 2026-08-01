# Stage 09 · ISR 成员与 High Watermark

### 目标

建立一个让动态成员、`min.insync`、High-watermark 可见性、Controller Epoch、保留追赶与 Lease Read 组成统一权威模型的 ISR Group。

??? note "交付文件"
    - `src/minidist/protocols/isr/__init__.py`
    - `src/minidist/protocols/isr/group.py`
    - `tests/protocols/test_isr.py`

### 当前遇到的问题

固定 Raft Voter 与配置 WAL Standby 都无法表示 Ack 集合随观测 Lag 变化的 Broker Leader。我们需要区分 Leader Log End 与已提交 High Watermark，并让 Controller 驱动的权威变化 Fencing 旧 Epoch 流量。

### 测试契约

#### 先看会坏在哪里

慢副本契约让一个 Follower 持续落后直到离开 ISR，再证明两个当前成员仍可写。静态成员实现要么永远等待慢节点，要么错误地让 High Watermark 越过仍在同步集合中的成员。

??? note "文件差异：tests/protocols/test_isr.py"
    ```diff
    diff --git a/tests/protocols/test_isr.py b/tests/protocols/test_isr.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4f65b5fe3f4e9117793ec837195862c02d49a895
    --- /dev/null
    +++ b/tests/protocols/test_isr.py
    @@ -0,0 +1,168 @@
    +from __future__ import annotations
    +
    +import pytest
    +
    +from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
    +from minidist.protocols.isr import CatchUpMode, IsrGroup
    +
    +
    +def test_all_isr_ack_advances_hw_after_every_current_isr_member() -> None:
    +    group = IsrGroup(node_ids=("n1", "n2", "n3"), min_insync_replicas=2)
    +
    +    result = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    +    state = group.probe()
    +
    +    assert result.accepted
    +    assert state.high_watermark >= result.offset
    +    assert all(state.nodes[node].log_end_offset >= result.offset for node in state.isr)
    +
    +
    +def test_slow_replica_leaves_isr_while_two_member_writes_remain_available() -> None:
    +    group = IsrGroup(
    +        node_ids=("n1", "n2", "n3"),
    +        min_insync_replicas=2,
    +        replica_lag_timeout=2,
    +    )
    +    slow = next(node for node in group.probe().nodes if node != group.probe().leader)
    +    group.set_replica_slow(slow, True)
    +    for number in range(3):
    +        assert group.client_write(
    +            f"k{number}".encode(), str(number).encode(), AckLevel.ALL_ISR
    +        ).accepted
    +
    +    state = group.probe()
    +    assert slow not in state.isr
    +    assert len(state.isr) == 2
    +
    +
    +def test_min_insync_rejects_all_isr_write_after_isr_shrinks_too_far() -> None:
    +    group = IsrGroup(
    +        node_ids=("n1", "n2", "n3"),
    +        min_insync_replicas=2,
    +        replica_lag_timeout=1,
    +    )
    +    followers = tuple(node for node in group.probe().nodes if node != group.probe().leader)
    +    for follower in followers:
    +        group.set_replica_slow(follower, True)
    +    group.tick()
    +    group.tick()
    +
    +    result = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    +
    +    assert not result.accepted
    +    assert "min.insync" in result.message
    +
    +
    +def test_recovered_replica_rejoins_isr_only_after_catching_up_to_hw() -> None:
    +    group = IsrGroup(replica_lag_timeout=1)
    +    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    +    group.set_replica_slow(follower, True)
    +    group.client_write(b"k", b"v", AckLevel.ALL_ISR)
    +    group.tick()
    +    assert follower not in group.probe().isr
    +
    +    group.set_replica_slow(follower, False)
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert follower in state.isr
    +    assert state.nodes[follower].log_end_offset >= state.high_watermark
    +
    +
    +def test_controller_election_increments_epoch_and_fences_old_epoch_append() -> None:
    +    group = IsrGroup(replication_delay=2)
    +    old_leader = group.probe().leader
    +    new_leader = next(node for node in group.probe().isr if node != old_leader)
    +    old_epoch = group.probe().leader_epoch
    +
    +    group.client_write(b"old", b"queued", AckLevel.LEADER)
    +    group.controller_elect(new_leader)
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.leader == new_leader
    +    assert state.leader_epoch == old_epoch + 1
    +    assert any(
    +        event.kind == "isr_append_rejected"
    +        and event.details["reason"] == "stale_leader_epoch"
    +        for event in group.trace.events
    +    )
    +
    +
    +def test_new_epoch_can_replay_committed_old_epoch_prefix_after_heal() -> None:
    +    group = IsrGroup(
    +        node_ids=("node-1", "node-2", "node-3", "node-4"),
    +        replication_delay=1,
    +    )
    +    lagging = "node-4"
    +    group.crash(lagging)
    +    assert group.client_write(b"before", b"kept", AckLevel.ALL_ISR).accepted
    +    old_leader = group.probe().leader
    +    group.isolate(old_leader)
    +    new_leader = next(node for node in group.probe().isr if node != old_leader)
    +    group.controller_elect(new_leader)
    +    assert group.client_write(b"after", b"kept", AckLevel.ALL_ISR).accepted
    +
    +    group.restart(lagging)
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.nodes[lagging].data == state.nodes[new_leader].data
    +    assert state.nodes[lagging].data == {b"before": b"kept", b"after": b"kept"}
    +
    +
    +def test_reconnect_is_incremental_with_retained_log_and_full_after_compaction() -> None:
    +    incremental = IsrGroup(log_retention=3)
    +    follower = next(
    +        node for node in incremental.probe().nodes if node != incremental.probe().leader
    +    )
    +    incremental.crash(follower)
    +    incremental.client_write(b"a", b"1", AckLevel.ALL_ISR)
    +    incremental.client_write(b"b", b"2", AckLevel.ALL_ISR)
    +    incremental.restart(follower)
    +    incremental.run_until_idle()
    +    assert incremental.probe().nodes[follower].catch_up_mode is CatchUpMode.INCREMENTAL
    +
    +    full = IsrGroup(log_retention=2)
    +    follower = next(node for node in full.probe().nodes if node != full.probe().leader)
    +    full.crash(follower)
    +    for number in range(3):
    +        full.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.ALL_ISR)
    +    full.restart(follower)
    +    full.run_until_idle()
    +    state = full.probe()
    +    assert state.nodes[follower].catch_up_mode is CatchUpMode.FULL
    +    assert state.nodes[follower].data == state.nodes[state.leader].data
    +
    +
    +def test_local_read_can_be_stale_but_hw_read_returns_committed_value() -> None:
    +    group = IsrGroup(replication_delay=2)
    +    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    +    group.client_write(b"k", b"v", AckLevel.LEADER)
    +
    +    assert group.client_read(b"k", ReadLevel.LOCAL, node=follower).value is None
    +    assert group.client_read(b"k", ReadLevel.LEADER).value == b"v"
    +    with pytest.raises(RuntimeError, match="high watermark"):
    +        group.client_read(b"k", ReadLevel.LINEARIZABLE)
    +
    +
    +@pytest.mark.parametrize("level", [AckLevel.NONE, AckLevel.QUORUM])
    +def test_isr_rejects_non_kafka_ack_levels(level: AckLevel) -> None:
    +    group = IsrGroup()
    +    with pytest.raises(UnsupportedLevelError, match=level.name):
    +        group.client_write(b"k", b"v", level)
    +
    +
    +def _trace(seed: int) -> list[dict[str, object]]:
    +    group = IsrGroup(seed=seed, replica_lag_timeout=1)
    +    follower = next(node for node in group.probe().nodes if node != group.probe().leader)
    +    group.set_replica_slow(follower, True)
    +    group.client_write(b"a", b"1", AckLevel.ALL_ISR)
    +    group.tick()
    +    group.set_replica_slow(follower, False)
    +    group.run_until_idle()
    +    return group.trace.as_dicts()
    +
    +
    +def test_same_seed_replays_isr_event_for_event() -> None:
    +    assert _trace(73) == _trace(73)
    ```

**测试锁定什么**

十条契约锁定 `ALL_ISR`、成员收缩与 `min.insync`、安全 Rejoin、Controller/Leader Epoch Fencing、已提交前缀重放、Catch-up Mode、本地与 HW 读取、支持边界与 Trace 重放。

**如何构造反例**

套件标记慢副本、推进 Lag 时间、只从 ISR 选举、排队旧 Epoch Append，并让 Replica 在 Retention 两侧重连。

**关键测试语句**

```python
assert state.high_watermark >= result.offset
assert all(state.nodes[node].log_end_offset >= result.offset for node in state.isr)
```

**失败意味着什么**

Group 在当前 ISR 到达 Record 前确认、暴露未提交数据，或允许过期 Controller 权威修改 Log。

### 基本概念

ISR 是当前足够接近 Leader、可参与 `ALL_ISR` 的 Replica 集合。`min.insync.replicas` 是刷新成员后的准入规则。High Watermark 是 Alive ISR 最小 Log End，限制已提交可见性。Controller 与 Leader Epoch Fencing 旧权威。Clean Election 选择 ISR 成员并截断 HW 之后后缀。Replica 只有追到已提交边界后才能 Rejoin。

### 为什么需要这个机制

动态成员通过降低部分冗余换取 Lag 下可用性，但必须显式保持 Commit Boundary 与最小集合。没有 HW，本地 Log Presence 很容易被误作已提交数据；没有 Epoch，旧 Leader 会在重新分配后继续 Append。

### 运行时心智模型

写入前与 Tick 时，Group 刷新 Lag Timer、移除或加入 ISR Member、推导单调 HW，并应用到该 Offset 的已提交 Entry。`ALL_ISR` 在当前集合仍满足 `min.insync` 时等待它。Controller Election 增加 Epoch、选择 ISR Replica、截断 HW 之后内容并同步 Follower。Lease Read 需要当前权威。

### 机制板块

#### ISR 成员、High Watermark、Epoch 与读取权威

把动态同步成员连接到可用性、已提交可见性、Clean Election、追赶与 Lease 权威。

??? note "文件差异：src/minidist/protocols/isr/group.py"
    ```diff
    diff --git a/src/minidist/protocols/isr/group.py b/src/minidist/protocols/isr/group.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..7bc4dada155b1e3064ed91ec403e97b65161f3f8
    --- /dev/null
    +++ b/src/minidist/protocols/isr/group.py
    @@ -0,0 +1,685 @@
    +"""Kafka-style ISR, high watermark, and controller in miniature.
    +
    +An in-process controller is the sole owner of leader identity, leader epoch,
    +and ISR membership.  The leader appends locally, followers copy the ordered
    +log, and the high watermark advances to the minimum log-end offset in the
    +current ISR.  ``ALL_ISR`` therefore means all *currently in-sync* replicas,
    +subject to ``min.insync.replicas``; it is intentionally not a Raft majority.
    +
    +Every append carries a leader epoch.  This fences packets from a displaced
    +leader before they can mutate a replica.  Real Kafka persists controller state
    +in its metadata quorum, has fetch sessions, batches, segment files, producer
    +epochs, and many timing knobs; this module keeps only the replication-group
    +semantics needed for deterministic comparison.
    +"""
    +
    +from __future__ import annotations
    +
    +from collections import deque
    +from dataclasses import dataclass, field
    +from enum import Enum
    +from types import MappingProxyType
    +from typing import Any, Mapping
    +
    +from minidist.protocols.types import (
    +    AckLevel,
    +    NodeId,
    +    ReadLevel,
    +    ReadResult,
    +    UnsupportedLevelError,
    +    WriteResult,
    +)
    +from minidist.sim import Message, Scheduler, SimClock, SimNet, Trace
    +
    +
    +class CatchUpMode(Enum):
    +    NEVER = "never"
    +    STREAMING = "streaming"
    +    INCREMENTAL = "incremental"
    +    FULL = "full"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class LogEntry:
    +    offset: int
    +    leader_epoch: int
    +    key: bytes
    +    value: bytes
    +
    +
    +@dataclass(slots=True)
    +class _Node:
    +    node_id: NodeId
    +    alive: bool = True
    +    log: list[LogEntry] = field(default_factory=list)
    +    data: dict[bytes, bytes] = field(default_factory=dict)
    +    committed_data: dict[bytes, bytes] = field(default_factory=dict)
    +    applied_hw: int = 0
    +    durable_log: list[LogEntry] = field(default_factory=list)
    +    durable_data: dict[bytes, bytes] = field(default_factory=dict)
    +    durable_offset: int = 0
    +    catch_up_mode: CatchUpMode = CatchUpMode.NEVER
    +
    +    @property
    +    def log_end_offset(self) -> int:
    +        return self.log[-1].offset if self.log else 0
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class NodeState:
    +    node_id: NodeId
    +    alive: bool
    +    role: str
    +    log: tuple[LogEntry, ...]
    +    log_end_offset: int
    +    applied_hw: int
    +    durable_offset: int
    +    data: Mapping[bytes, bytes]
    +    committed_data: Mapping[bytes, bytes]
    +    catch_up_mode: CatchUpMode
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class GroupState:
    +    tick: int
    +    leader: NodeId
    +    leader_epoch: int
    +    controller_epoch: int
    +    isr: frozenset[NodeId]
    +    high_watermark: int
    +    min_insync_replicas: int
    +    lease_expiry: int
    +    retained_offsets: tuple[int, ...]
    +    nodes: Mapping[NodeId, NodeState]
    +
    +
    +class IsrGroup:
    +    """A deterministic Kafka-like replication group with one controller."""
    +
    +    def __init__(
    +        self,
    +        *,
    +        node_ids: tuple[NodeId, ...] = ("node-1", "node-2", "node-3"),
    +        leader_id: NodeId | None = None,
    +        min_insync_replicas: int = 2,
    +        replica_lag_timeout: int = 3,
    +        log_retention: int = 8,
    +        replication_delay: int = 1,
    +        ack_timeout: int = 32,
    +        lease_duration: int = 4,
    +        seed: int = 0,
    +    ) -> None:
    +        if len(node_ids) < 2 or len(set(node_ids)) != len(node_ids):
    +            raise ValueError("ISR requires at least two unique node IDs")
    +        selected_leader = node_ids[0] if leader_id is None else leader_id
    +        if selected_leader not in node_ids:
    +            raise ValueError("leader_id must be a member")
    +        if not 1 <= min_insync_replicas <= len(node_ids):
    +            raise ValueError("min.insync.replicas must fit the group")
    +        if min(replica_lag_timeout, log_retention, replication_delay, ack_timeout) <= 0:
    +            raise ValueError("timeouts, retention, and delay must be positive")
    +
    +        self.clock = SimClock()
    +        self.trace = Trace()
    +        self.scheduler = Scheduler(self.clock, self.trace)
    +        self.network = SimNet(
    +            seed=seed,
    +            clock=self.clock,
    +            scheduler=self.scheduler,
    +            trace=self.trace,
    +            min_delay=replication_delay,
    +            max_delay=replication_delay,
    +        )
    +        self._node_ids = node_ids
    +        self._nodes = {node_id: _Node(node_id=node_id) for node_id in node_ids}
    +        self._leader = selected_leader
    +        self._leader_epoch = 1
    +        self._controller_epoch = 1
    +        self._isr = set(node_ids)
    +        self._high_watermark = 0
    +        self._min_insync = min_insync_replicas
    +        self._lag_timeout = replica_lag_timeout
    +        self._ack_timeout = ack_timeout
    +        self._lease_duration = lease_duration
    +        self._lease_expiry = lease_duration
    +        self._slow: set[NodeId] = set()
    +        self._last_caught_up = {node_id: 0 for node_id in node_ids}
    +        self._retained: deque[LogEntry] = deque(maxlen=log_retention)
    +        for node_id in node_ids:
    +            self.network.register(
    +                node_id,
    +                lambda message, target=node_id: self._receive(target, message),
    +            )
    +        self.trace.record(
    +            0,
    +            "isr_group_created",
    +            nodes=list(node_ids),
    +            leader=self._leader,
    +            leader_epoch=self._leader_epoch,
    +            isr=sorted(self._isr),
    +            min_insync_replicas=self._min_insync,
    +            seed=seed,
    +        )
    +
    +    @property
    +    def leader(self) -> NodeId:
    +        return self._leader
    +
    +    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult:
    +        if ack not in (AckLevel.LEADER, AckLevel.ALL_ISR):
    +            raise UnsupportedLevelError(f"ISR does not support AckLevel.{ack.name}")
    +        self._validate_bytes(key, "key")
    +        self._validate_bytes(value, "value")
    +        leader = self._node(self._leader)
    +        self._require_alive(leader)
    +        self._refresh_isr_and_hw()
    +        if ack is AckLevel.ALL_ISR and len(self._isr) < self._min_insync:
    +            return WriteResult(
    +                accepted=False,
    +                offset=leader.log_end_offset,
    +                message="current ISR is below min.insync.replicas",
    +            )
    +
    +        entry = LogEntry(
    +            offset=leader.log_end_offset + 1,
    +            leader_epoch=self._leader_epoch,
    +            key=key,
    +            value=value,
    +        )
    +        self._append(leader, entry, CatchUpMode.STREAMING)
    +        self._retained.append(entry)
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_leader_appended",
    +            leader=self._leader,
    +            leader_epoch=self._leader_epoch,
    +            offset=entry.offset,
    +        )
    +        for follower in self._followers():
    +            self._send_entry(follower, entry)
    +
    +        accepted = True
    +        if ack is AckLevel.ALL_ISR:
    +            for _ in range(self._ack_timeout):
    +                self._refresh_isr_and_hw()
    +                if len(self._isr) < self._min_insync:
    +                    accepted = False
    +                    break
    +                if all(
    +                    self._nodes[node].log_end_offset >= entry.offset
    +                    for node in self._isr
    +                ):
    +                    self._refresh_isr_and_hw()
    +                    accepted = self._high_watermark >= entry.offset
    +                    break
    +                self.tick()
    +            else:
    +                accepted = False
    +        message = (
    +            "leader appended without waiting for ISR"
    +            if ack is AckLevel.LEADER
    +            else (
    +                "all current ISR replicas acknowledged and HW advanced"
    +                if accepted
    +                else "ALL_ISR could not satisfy min.insync or HW before timeout"
    +            )
    +        )
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_client_write_result",
    +            ack_level=ack.name,
    +            accepted=accepted,
    +            offset=entry.offset,
    +            isr=sorted(self._isr),
    +            high_watermark=self._high_watermark,
    +        )
    +        return WriteResult(accepted=accepted, offset=entry.offset, message=message)
    +
    +    def client_read(
    +        self,
    +        key: bytes,
    +        level: ReadLevel,
    +        node: NodeId | None = None,
    +    ) -> ReadResult:
    +        self._validate_bytes(key, "key")
    +        if level is ReadLevel.LOCAL:
    +            selected = self._leader if node is None else node
    +            target = self._node(selected)
    +            self._require_alive(target)
    +            value = target.data.get(key)
    +            offset = target.log_end_offset
    +        elif level is ReadLevel.LEADER:
    +            if node is not None:
    +                raise ValueError("leader read does not accept an explicit node")
    +            selected = self._leader
    +            target = self._node(selected)
    +            self._require_alive(target)
    +            value = target.data.get(key)
    +            offset = target.log_end_offset
    +        elif level is ReadLevel.LINEARIZABLE:
    +            if node is not None:
    +                raise ValueError("linearizable read does not accept an explicit node")
    +            selected = self._leader
    +            target = self._node(selected)
    +            self._require_alive(target)
    +            if (
    +                target.log_end_offset > self._high_watermark
    +                or self.clock.now > self._lease_expiry
    +                or len(self._isr) < self._min_insync
    +            ):
    +                raise RuntimeError("leader has no committed high watermark lease")
    +            value = target.committed_data.get(key)
    +            offset = self._high_watermark
    +        else:
    +            raise UnsupportedLevelError(f"ISR does not support ReadLevel.{level.name}")
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_client_read",
    +            node=selected,
    +            level=level.name,
    +            offset=offset,
    +            value=value,
    +        )
    +        return ReadResult(value=value, node=selected, offset=offset)
    +
    +    def lease_read(self, node: NodeId, key: bytes) -> ReadResult:
    +        """Read only while controller ownership and its logical lease hold."""
    +
    +        self._validate_bytes(key, "key")
    +        if node != self._leader or self.clock.now > self._lease_expiry:
    +            raise RuntimeError("leader lease expired or node was fenced")
    +        return self.client_read(key, ReadLevel.LINEARIZABLE)
    +
    +    def tick(self) -> None:
    +        self.scheduler.tick()
    +        self._refresh_isr_and_hw()
    +
    +    def run_until_idle(self, *, max_ticks: int = 10_000) -> int:
    +        ticks = 0
    +        while self.scheduler.pending:
    +            if ticks >= max_ticks:
    +                raise RuntimeError("ISR group did not become idle")
    +            self.tick()
    +            ticks += 1
    +        self._refresh_isr_and_hw()
    +        return ticks
    +
    +    def set_replica_slow(self, node: NodeId, slow: bool = True) -> None:
    +        if node == self._leader:
    +            raise ValueError("the leader cannot be marked as a slow replica")
    +        target = self._node(node)
    +        self._require_alive(target)
    +        if slow:
    +            self._slow.add(node)
    +        else:
    +            self._slow.discard(node)
    +            self._synchronize(node)
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_replica_speed_changed",
    +            node=node,
    +            slow=slow,
    +        )
    +
    +    def fsync(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        target.durable_log = list(target.log)
    +        target.durable_data = dict(target.data)
    +        target.durable_offset = target.log_end_offset
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_node_fsynced",
    +            node=node,
    +            offset=target.durable_offset,
    +        )
    +
    +    def crash(self, node: NodeId, *, lose_unfsynced: bool = False) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        if lose_unfsynced:
    +            target.log = list(target.durable_log)
    +            target.data = dict(target.durable_data)
    +            target.committed_data = self._data_through(target, self._high_watermark)
    +            target.applied_hw = min(self._high_watermark, target.log_end_offset)
    +            self.trace.record(
    +                self.clock.now,
    +                "isr_fsync_loss",
    +                node=node,
    +                restored_offset=target.durable_offset,
    +            )
    +        target.alive = False
    +        self._isr.discard(node)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self._refresh_isr_and_hw()
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_node_crashed",
    +            node=node,
    +            lost_unfsynced=lose_unfsynced,
    +        )
    +
    +    def restart(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        if target.alive:
    +            raise ValueError(f"node already running: {node}")
    +        target.alive = True
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "isr_node_restarted", node=node)
    +        if node != self._leader:
    +            self._synchronize(node)
    +
    +    def isolate(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self._isr.discard(node)
    +        self._refresh_isr_and_hw()
    +        self.trace.record(self.clock.now, "isr_node_isolated", node=node)
    +
    +    def heal(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "isr_node_healed", node=node)
    +        if node != self._leader:
    +            self._synchronize(node)
    +
    +    def controller_elect(self, node: NodeId) -> None:
    +        candidate = self._node(node)
    +        self._require_alive(candidate)
    +        if node not in self._isr:
    +            raise RuntimeError("controller may elect only an ISR member")
    +        if node == self._leader:
    +            raise ValueError(f"node is already leader: {node}")
    +        old_leader = self._leader
    +        self._controller_epoch += 1
    +        self._leader_epoch += 1
    +        self._leader = node
    +        # Kafka's clean election chooses an ISR replica and discards any suffix
    +        # beyond HW; only the committed prefix may seed the new epoch.
    +        candidate.log = [
    +            entry for entry in candidate.log if entry.offset <= self._high_watermark
    +        ]
    +        candidate.data = self._data_through(candidate, candidate.log_end_offset)
    +        self._isr.discard(old_leader)
    +        self._lease_expiry = self.clock.now + self._lease_duration
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_controller_elected",
    +            old_leader=old_leader,
    +            new_leader=node,
    +            controller_epoch=self._controller_epoch,
    +            leader_epoch=self._leader_epoch,
    +            high_watermark=self._high_watermark,
    +        )
    +        for follower in self._followers():
    +            if self._nodes[follower].alive:
    +                self._synchronize(follower)
    +
    +    def probe(self) -> GroupState:
    +        nodes = {
    +            node_id: NodeState(
    +                node_id=node_id,
    +                alive=node.alive,
    +                role="leader" if node_id == self._leader else "follower",
    +                log=tuple(node.log),
    +                log_end_offset=node.log_end_offset,
    +                applied_hw=node.applied_hw,
    +                durable_offset=node.durable_offset,
    +                data=MappingProxyType(dict(node.data)),
    +                committed_data=MappingProxyType(dict(node.committed_data)),
    +                catch_up_mode=node.catch_up_mode,
    +            )
    +            for node_id, node in self._nodes.items()
    +        }
    +        return GroupState(
    +            tick=self.clock.now,
    +            leader=self._leader,
    +            leader_epoch=self._leader_epoch,
    +            controller_epoch=self._controller_epoch,
    +            isr=frozenset(self._isr),
    +            high_watermark=self._high_watermark,
    +            min_insync_replicas=self._min_insync,
    +            lease_expiry=self._lease_expiry,
    +            retained_offsets=tuple(entry.offset for entry in self._retained),
    +            nodes=MappingProxyType(nodes),
    +        )
    +
    +    def _refresh_isr_and_hw(self) -> None:
    +        leader = self._node(self._leader)
    +        if not leader.alive:
    +            return
    +        for node_id in self._followers():
    +            node = self._node(node_id)
    +            if not node.alive:
    +                self._isr.discard(node_id)
    +                continue
    +            if node.log_end_offset >= leader.log_end_offset:
    +                self._last_caught_up[node_id] = self.clock.now
    +                if node_id not in self._slow:
    +                    if node_id not in self._isr:
    +                        self.trace.record(
    +                            self.clock.now,
    +                            "isr_member_added",
    +                            node=node_id,
    +                            log_end_offset=node.log_end_offset,
    +                        )
    +                    self._isr.add(node_id)
    +            elif self.clock.now - self._last_caught_up[node_id] >= self._lag_timeout:
    +                if node_id in self._isr:
    +                    self.trace.record(
    +                        self.clock.now,
    +                        "isr_member_removed",
    +                        node=node_id,
    +                        lag=leader.log_end_offset - node.log_end_offset,
    +                    )
    +                self._isr.discard(node_id)
    +        self._isr.add(self._leader)
    +        alive_isr = [self._nodes[node] for node in self._isr if self._nodes[node].alive]
    +        if alive_isr:
    +            candidate_hw = min(node.log_end_offset for node in alive_isr)
    +            if candidate_hw > self._high_watermark:
    +                self._high_watermark = candidate_hw
    +                self.trace.record(
    +                    self.clock.now,
    +                    "isr_high_watermark_advanced",
    +                    high_watermark=self._high_watermark,
    +                    isr=sorted(self._isr),
    +                )
    +        for node in self._nodes.values():
    +            self._apply_high_watermark(node)
    +        if len(self._isr) >= self._min_insync:
    +            self._lease_expiry = self.clock.now + self._lease_duration
    +
    +    def _apply_high_watermark(self, node: _Node) -> None:
    +        target = min(self._high_watermark, node.log_end_offset)
    +        if target <= node.applied_hw:
    +            return
    +        for entry in node.log:
    +            if node.applied_hw < entry.offset <= target:
    +                node.committed_data[entry.key] = entry.value
    +        node.applied_hw = target
    +
    +    def _synchronize(self, follower_id: NodeId) -> None:
    +        leader = self._node(self._leader)
    +        follower = self._node(follower_id)
    +        self._require_alive(leader)
    +        self._require_alive(follower)
    +        oldest = self._retained[0].offset if self._retained else None
    +        can_incremental = (
    +            follower.log_end_offset == leader.log_end_offset
    +            or (
    +                oldest is not None
    +                and follower.log_end_offset >= oldest - 1
    +            )
    +        )
    +        if can_incremental:
    +            follower.catch_up_mode = CatchUpMode.INCREMENTAL
    +            for entry in self._retained:
    +                if entry.offset > follower.log_end_offset:
    +                    self._send_entry(follower_id, entry)
    +            self.trace.record(
    +                self.clock.now,
    +                "isr_incremental_catchup_started",
    +                follower=follower_id,
    +                from_offset=follower.log_end_offset,
    +            )
    +            return
    +        follower.catch_up_mode = CatchUpMode.FULL
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_full_catchup_started",
    +            follower=follower_id,
    +            high_watermark=self._high_watermark,
    +        )
    +        self.network.send(
    +            self._leader,
    +            follower_id,
    +            {
    +                "type": "isr_snapshot",
    +                "leader_epoch": self._leader_epoch,
    +                "source_leader": self._leader,
    +                "log": tuple(leader.log),
    +                "data": dict(leader.data),
    +                "high_watermark": self._high_watermark,
    +            },
    +        )
    +
    +    def _send_entry(self, follower: NodeId, entry: LogEntry) -> None:
    +        if follower in self._slow:
    +            self.trace.record(
    +                self.clock.now,
    +                "isr_replication_delayed",
    +                follower=follower,
    +                offset=entry.offset,
    +            )
    +            return
    +        self.network.send(
    +            self._leader,
    +            follower,
    +            {
    +                "type": "isr_append",
    +                "source_leader": self._leader,
    +                # Packet authority belongs to the current leader.  The record
    +                # may legitimately originate in an older committed epoch and
    +                # must remain replayable after a clean leader change.
    +                "leader_epoch": self._leader_epoch,
    +                "entry_leader_epoch": entry.leader_epoch,
    +                "offset": entry.offset,
    +                "key": entry.key,
    +                "value": entry.value,
    +            },
    +        )
    +
    +    def _receive(self, target_id: NodeId, message: Message) -> None:
    +        target = self._node(target_id)
    +        source = self._nodes.get(message.source)
    +        if not target.alive or source is None or not source.alive:
    +            return
    +        payload = message.payload
    +        if not isinstance(payload, dict):
    +            raise TypeError("ISR message must be a dict")
    +        if payload.get("leader_epoch") != self._leader_epoch:
    +            self.trace.record(
    +                self.clock.now,
    +                "isr_append_rejected",
    +                node=target_id,
    +                reason="stale_leader_epoch",
    +                leader_epoch=payload.get("leader_epoch"),
    +            )
    +            return
    +        if (
    +            message.source != self._leader
    +            or payload.get("source_leader") != self._leader
    +        ):
    +            self.trace.record(
    +                self.clock.now,
    +                "isr_append_rejected",
    +                node=target_id,
    +                reason="non_current_leader",
    +                leader_epoch=payload.get("leader_epoch"),
    +            )
    +            return
    +        if payload.get("type") == "isr_snapshot":
    +            target.log = list(payload["log"])
    +            target.data = dict(payload["data"])
    +            target.committed_data = self._data_through(
    +                target, int(payload["high_watermark"])
    +            )
    +            target.applied_hw = min(
    +                int(payload["high_watermark"]), target.log_end_offset
    +            )
    +            target.catch_up_mode = CatchUpMode.FULL
    +        elif payload.get("type") == "isr_append":
    +            entry = LogEntry(
    +                offset=int(payload["offset"]),
    +                leader_epoch=int(payload["entry_leader_epoch"]),
    +                key=self._require_bytes(payload["key"], "key"),
    +                value=self._require_bytes(payload["value"], "value"),
    +            )
    +            if entry.offset != target.log_end_offset + 1:
    +                self._synchronize(target_id)
    +                return
    +            mode = (
    +                CatchUpMode.INCREMENTAL
    +                if target.catch_up_mode is CatchUpMode.INCREMENTAL
    +                else CatchUpMode.STREAMING
    +            )
    +            self._append(target, entry, mode)
    +        else:
    +            raise ValueError(f"unknown ISR message: {payload.get('type')}")
    +        self._refresh_isr_and_hw()
    +
    +    def _append(self, node: _Node, entry: LogEntry, mode: CatchUpMode) -> None:
    +        node.log.append(entry)
    +        node.data[entry.key] = entry.value
    +        node.catch_up_mode = mode
    +        self.trace.record(
    +            self.clock.now,
    +            "isr_entry_appended",
    +            node=node.node_id,
    +            leader_epoch=entry.leader_epoch,
    +            offset=entry.offset,
    +        )
    +
    +    @staticmethod
    +    def _data_through(node: _Node, offset: int) -> dict[bytes, bytes]:
    +        data: dict[bytes, bytes] = {}
    +        for entry in node.log:
    +            if entry.offset <= offset:
    +                data[entry.key] = entry.value
    +        return data
    +
    +    def _followers(self) -> tuple[NodeId, ...]:
    +        return tuple(node for node in self._node_ids if node != self._leader)
    +
    +    def _node(self, node_id: NodeId) -> _Node:
    +        try:
    +            return self._nodes[node_id]
    +        except KeyError as error:
    +            raise KeyError(f"unknown node: {node_id}") from error
    +
    +    @staticmethod
    +    def _require_alive(node: _Node) -> None:
    +        if not node.alive:
    +            raise RuntimeError(f"node is crashed: {node.node_id}")
    +
    +    @staticmethod
    +    def _validate_bytes(value: object, name: str) -> None:
    +        if not isinstance(value, bytes):
    +            raise TypeError(f"{name} must be bytes")
    +
    +    @staticmethod
    +    def _require_bytes(value: object, name: str) -> bytes:
    +        if not isinstance(value, bytes):
    +            raise TypeError(f"{name} must be bytes")
    +        return value
    ```

**是什么，为什么现在需要**

该 Group 拥有 Broker Log、动态 ISR、HW Apply、Lag Tracking、Clean Election、Epoch Fencing、Retained Catch-up、Fsync State 与 Lease Read。

**在运行时做什么**

成员刷新先于 Ack Decision，并从存活当前 ISR 推导 HW；Apply 永不越过每个节点的本地 Log End。

**关键代码**

```python
candidate_hw = min(node.log_end_offset for node in alive_isr)
if candidate_hw > self._high_watermark:
    self._high_watermark = candidate_hw
```

**关键语句理解**

取最小值让 HW 成为每个当前同步成员都已到达的前缀。单调推进防止后续成员变化把已可见 Record “取消提交”。

#### ISR 包边界

把 Group、Log Entry、Catch-up Mode 与 Probe Value 作为支撑接线导出。

??? note "支撑文件差异（1 个文件）"
    **`src/minidist/protocols/isr/__init__.py`**

    ```diff
    diff --git a/src/minidist/protocols/isr/__init__.py b/src/minidist/protocols/isr/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..36b14fdf5070b00badc3f72bc8b420cbbe5c2af0
    --- /dev/null
    +++ b/src/minidist/protocols/isr/__init__.py
    @@ -0,0 +1,5 @@
    +"""Kafka-style ISR/HW replication protocol."""
    +
    +from .group import CatchUpMode, GroupState, IsrGroup, NodeState
    +
    +__all__ = ["CatchUpMode", "GroupState", "IsrGroup", "NodeState"]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/09-isr-high-watermark/tests.txt)`。十条契约覆盖可用性、提交、权威、恢复、观察与确定性重放。

### 需要真正记住的内容

ISR 是动态成员，HW 是已提交可见性，`min.insync` 是写入准入阈值。Epoch Fencing 权威；Clean Election 只保留 HW 前缀。

### 用自己的话讲清楚

说明移除慢 Replica 为何能保持写可用性，同时不会让 HW 包含当前 ISR 成员尚未收到的数据。

### 教材

[第 10 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/10-isr.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-08...stage-09)

完成后可运行 `git checkout stage-09` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/09-isr-high-watermark/stage.patch)
