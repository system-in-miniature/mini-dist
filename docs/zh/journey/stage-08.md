# Stage 08 · WAL Shipping 与同步刷盘

### 目标

建立一个可独立观察本地 WAL 持久、配置的同步 Standby 刷盘、保留追赶窗口与 Promotion Timeline 的 WAL Shipping Group。

??? note "交付文件"
    - `src/minidist/protocols/wal_shipping/__init__.py`
    - `src/minidist/protocols/wal_shipping/group.py`
    - `tests/protocols/test_wal_shipping.py`

### 当前遇到的问题

异步主从复制逻辑 Key/Value Entry，却无法表达数据库式字节日志位置或配置化远端刷盘策略。Raft 把复制与投票绑定；WAL Shipping 需要另一种权威模型：选定 Standby 可以阻塞 Commit，但不会因此成为共识 Voter。

### 测试契约

#### 先看会坏在哪里

同步 Standby 契约配置两个同步和一个异步 Standby，使用 `AckLevel.QUORUM` 写入，并要求两个已配置 Standby 与 Primary 都已刷过返回 LSN。若计算任意数值多数派，就会实现成 Raft 风格语义，而不是声明的 WAL 策略。

??? note "文件差异：tests/protocols/test_wal_shipping.py"
    ```diff
    diff --git a/tests/protocols/test_wal_shipping.py b/tests/protocols/test_wal_shipping.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cdd434e7ad858b5b320c8a1159f9eb38803c7606
    --- /dev/null
    +++ b/tests/protocols/test_wal_shipping.py
    @@ -0,0 +1,129 @@
    +from __future__ import annotations
    +
    +import pytest
    +
    +from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
    +from minidist.protocols.wal_shipping import SyncMode, WalShippingGroup
    +
    +
    +def test_leader_ack_exposes_ordered_logical_wal_bytes_before_standby_apply() -> None:
    +    group = WalShippingGroup(
    +        standby_ids=("s1",), replication_delay=2, synchronous_standby_ids=()
    +    )
    +
    +    result = group.client_write(b"course", b"MiniDist", AckLevel.LEADER)
    +    before = group.probe()
    +
    +    assert result.accepted
    +    assert before.nodes["primary"].wal_bytes
    +    assert before.nodes["primary"].applied_lsn == result.offset
    +    assert before.nodes["s1"].applied_lsn == 0
    +    group.run_until_idle()
    +    assert group.client_read(b"course", ReadLevel.LOCAL, node="s1").value == b"MiniDist"
    +
    +
    +def test_quorum_means_every_configured_synchronous_standby_flushed() -> None:
    +    group = WalShippingGroup(
    +        standby_ids=("sync-1", "sync-2", "async-1"),
    +        synchronous_standby_ids=("sync-1", "sync-2"),
    +    )
    +
    +    result = group.client_write(b"k", b"v", AckLevel.QUORUM)
    +    state = group.probe()
    +
    +    assert result.accepted
    +    assert all(state.nodes[node].flushed_lsn >= result.offset for node in ("sync-1", "sync-2"))
    +    assert state.nodes["primary"].durable_lsn >= result.offset
    +
    +
    +def test_quorum_write_fails_when_a_synchronous_standby_is_unreachable() -> None:
    +    group = WalShippingGroup(
    +        standby_ids=("s1", "s2"),
    +        synchronous_standby_ids=("s1", "s2"),
    +        ack_timeout=4,
    +    )
    +    group.isolate("s2")
    +
    +    result = group.client_write(b"k", b"v", AckLevel.QUORUM)
    +
    +    assert not result.accepted
    +    assert "synchronous standby" in result.message
    +
    +
    +@pytest.mark.parametrize("level", [AckLevel.NONE, AckLevel.ALL_ISR])
    +def test_wal_shipping_rejects_unrelated_ack_levels(level: AckLevel) -> None:
    +    group = WalShippingGroup()
    +    with pytest.raises(UnsupportedLevelError, match=level.name):
    +        group.client_write(b"k", b"v", level)
    +
    +
    +def test_promotion_increments_timeline_and_rejects_queued_old_timeline_wal() -> None:
    +    group = WalShippingGroup(standby_ids=("s1", "s2"), replication_delay=2)
    +    group.client_write(b"old", b"history", AckLevel.LEADER)
    +    group.tick()
    +    old_timeline = group.probe().timeline
    +
    +    group.promote("s1")
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.timeline == old_timeline + 1
    +    assert state.primary == "s1"
    +    assert any(
    +        event.kind == "wal_rejected"
    +        and event.details["reason"] == "stale_timeline"
    +        for event in group.trace.events
    +    )
    +
    +
    +def test_reconnect_uses_incremental_wal_inside_retention_and_full_outside() -> None:
    +    partial = WalShippingGroup(standby_ids=("s1",), wal_retention=3)
    +    partial.crash("s1")
    +    partial.client_write(b"a", b"1", AckLevel.LEADER)
    +    partial.client_write(b"b", b"2", AckLevel.LEADER)
    +    partial.restart("s1")
    +    partial.run_until_idle()
    +    assert partial.probe().nodes["s1"].sync_mode is SyncMode.INCREMENTAL
    +
    +    full = WalShippingGroup(standby_ids=("s1",), wal_retention=2)
    +    full.crash("s1")
    +    for number in range(3):
    +        full.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.LEADER)
    +    full.restart("s1")
    +    full.run_until_idle()
    +    state = full.probe()
    +    assert state.nodes["s1"].sync_mode is SyncMode.FULL
    +    assert state.nodes["s1"].data == state.nodes[state.primary].data
    +
    +
    +def test_fsync_loss_rolls_back_only_unflushed_standby_wal() -> None:
    +    group = WalShippingGroup(standby_ids=("s1",), replication_delay=1)
    +    group.client_write(b"k", b"v", AckLevel.LEADER)
    +    group.run_until_idle()
    +    assert group.probe().nodes["s1"].applied_lsn == 1
    +    assert group.probe().nodes["s1"].durable_lsn == 0
    +
    +    group.crash("s1", lose_unfsynced=True)
    +    group.restart("s1")
    +    group.run_until_idle()
    +
    +    assert group.client_read(b"k", ReadLevel.LOCAL, node="s1").value == b"v"
    +    assert any(event.kind == "wal_fsync_loss" for event in group.trace.events)
    +
    +
    +def _trace(seed: int) -> list[dict[str, object]]:
    +    group = WalShippingGroup(
    +        standby_ids=("s1", "s2"),
    +        synchronous_standby_ids=("s1",),
    +        seed=seed,
    +    )
    +    group.client_write(b"a", b"1", AckLevel.QUORUM)
    +    group.isolate("s2")
    +    group.client_write(b"b", b"2", AckLevel.LEADER)
    +    group.heal("s2")
    +    group.run_until_idle()
    +    return group.trace.as_dicts()
    +
    +
    +def test_same_seed_replays_wal_shipping_event_for_event() -> None:
    +    assert _trace(41) == _trace(41)
    ```

**测试锁定什么**

八条契约锁定有序 WAL 字节、确认前本地 Flush、所有配置同步 Standby、同步 Standby 不可达时 Timeout、Timeline Fencing、保留窗口内外追赶、Fsync Loss 与确定性重放。

**如何构造反例**

套件独立于集群规模改变同步集合，让旧 Timeline Record 跨 Promotion 保持排队，并让 Standby 在 Retention Window 内外断开。

**关键测试语句**

```python
assert all(state.nodes[node].flushed_lsn >= result.offset for node in ("sync-1", "sync-2"))
assert state.nodes["primary"].durable_lsn >= result.offset
```

**失败意味着什么**

确认不再命名真实 Flush Boundary，或者 Promotion/Catch-up 接受了错误历史的 WAL。

### 基本概念

WAL Record 是由 `(timeline, LSN)` 标识的有序字节 Payload。`applied_lsn`、`flushed_lsn` 与 `durable_lsn` 表示不同进度。`LEADER` 等待本地 WAL 持久；本项目的 `QUORUM` 等待每个显式配置的同步 Standby，而不是数值多数。Retention 决定增量重放或全量复制。Promotion 增加 Timeline，使排队旧历史 WAL 可被 Fencing。

### 为什么需要这个机制

数据库复制取舍取决于什么在何处 Flush，而不只是 Replica 是否收到值。分离本地持久与远端等待策略，让同步复制可配置，又不伪装成共识。

### 运行时心智模型

一次写入编码逻辑 Record，在 Primary Append/Apply、本地 Fsync、安排投递，并可选地 Tick 到所有配置同步 Standby 报告 Flush。Restart 在可证明连续时使用 Retained WAL，否则复制全量状态。Promotion 先增加 Timeline，再同步 Peer。

### 机制板块

#### WAL 位置、同步刷盘、保留窗口与 Timeline

把复制对象建模为有序逻辑 WAL 字节，并区分本地持久、远端刷盘等待、追赶与提升 Fencing。

??? note "文件差异：src/minidist/protocols/wal_shipping/group.py"
    ```diff
    diff --git a/src/minidist/protocols/wal_shipping/group.py b/src/minidist/protocols/wal_shipping/group.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..31eb3aa6e90b31975b7e593be8bad90afe1420b4
    --- /dev/null
    +++ b/src/minidist/protocols/wal_shipping/group.py
    @@ -0,0 +1,567 @@
    +"""PostgreSQL-style logical WAL shipping in miniature.
    +
    +The primary encodes each KV mutation into an ordered byte record and standbys
    +decode and apply that exact stream.  ``LEADER`` is asynchronous commit after
    +the primary flushes locally; ``QUORUM`` deliberately means every configured
    +synchronous standby has flushed, matching the design's comparison vocabulary
    +rather than pretending PostgreSQL uses a Raft majority.
    +
    +Promotion creates a new timeline.  Every WAL message carries its timeline and
    +source primary, so bytes queued on the old branch cannot cross the promotion
    +boundary.  Real PostgreSQL WAL is physical and has far richer record/recovery
    +machinery; MiniDist uses logical SET records to keep the shared KV state
    +machine inspectable while preserving byte transport, LSN order, flush points,
    +retention, and timeline-fork semantics.
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
    +class SyncMode(Enum):
    +    """How a standby most recently joined the current timeline."""
    +
    +    NEVER = "never"
    +    STREAMING = "streaming"
    +    INCREMENTAL = "incremental"
    +    FULL = "full"
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class WalRecord:
    +    timeline: int
    +    lsn: int
    +    payload: bytes
    +
    +
    +@dataclass(slots=True)
    +class _Node:
    +    node_id: NodeId
    +    alive: bool = True
    +    timeline: int = 1
    +    wal_bytes: bytes = b""
    +    records: list[WalRecord] = field(default_factory=list)
    +    data: dict[bytes, bytes] = field(default_factory=dict)
    +    applied_lsn: int = 0
    +    flushed_lsn: int = 0
    +    durable_wal_bytes: bytes = b""
    +    durable_records: list[WalRecord] = field(default_factory=list)
    +    durable_data: dict[bytes, bytes] = field(default_factory=dict)
    +    durable_lsn: int = 0
    +    sync_mode: SyncMode = SyncMode.NEVER
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class NodeState:
    +    node_id: NodeId
    +    alive: bool
    +    role: str
    +    timeline: int
    +    wal_bytes: bytes
    +    applied_lsn: int
    +    flushed_lsn: int
    +    durable_lsn: int
    +    data: Mapping[bytes, bytes]
    +    sync_mode: SyncMode
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class GroupState:
    +    tick: int
    +    primary: NodeId
    +    timeline: int
    +    synchronous_standbys: tuple[NodeId, ...]
    +    retained_lsns: tuple[int, ...]
    +    nodes: Mapping[NodeId, NodeState]
    +
    +
    +class WalShippingGroup:
    +    """A deterministic primary/standby WAL transport group."""
    +
    +    def __init__(
    +        self,
    +        *,
    +        primary_id: NodeId = "primary",
    +        standby_ids: tuple[NodeId, ...] = ("standby-1", "standby-2"),
    +        synchronous_standby_ids: tuple[NodeId, ...] = (),
    +        wal_retention: int = 8,
    +        replication_delay: int = 1,
    +        ack_timeout: int = 32,
    +        seed: int = 0,
    +    ) -> None:
    +        node_ids = (primary_id, *standby_ids)
    +        if len(set(node_ids)) != len(node_ids):
    +            raise ValueError("node IDs must be unique")
    +        if not set(synchronous_standby_ids) <= set(standby_ids):
    +            raise ValueError("synchronous standbys must be standby_ids")
    +        if wal_retention <= 0 or replication_delay <= 0 or ack_timeout <= 0:
    +            raise ValueError("retention, delay, and timeout must be positive")
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
    +        self._primary = primary_id
    +        self._timeline = 1
    +        self._nodes = {node_id: _Node(node_id=node_id) for node_id in node_ids}
    +        self._synchronous = tuple(synchronous_standby_ids)
    +        self._retained: deque[WalRecord] = deque(maxlen=wal_retention)
    +        self._ack_timeout = ack_timeout
    +        self._flush_acks: dict[int, set[NodeId]] = {}
    +        for node_id in node_ids:
    +            self.network.register(
    +                node_id,
    +                lambda message, target=node_id: self._receive(target, message),
    +            )
    +        self.trace.record(
    +            0,
    +            "wal_group_created",
    +            primary=primary_id,
    +            standbys=list(standby_ids),
    +            synchronous_standbys=list(self._synchronous),
    +            timeline=self._timeline,
    +            seed=seed,
    +        )
    +
    +    @property
    +    def primary(self) -> NodeId:
    +        return self._primary
    +
    +    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult:
    +        if ack not in (AckLevel.LEADER, AckLevel.QUORUM):
    +            raise UnsupportedLevelError(
    +                f"WAL shipping does not support AckLevel.{ack.name}"
    +            )
    +        self._validate_bytes(key, "key")
    +        self._validate_bytes(value, "value")
    +        primary = self._node(self._primary)
    +        self._require_alive(primary)
    +
    +        lsn = primary.applied_lsn + 1
    +        payload = self._encode_set(key, value)
    +        record = WalRecord(timeline=self._timeline, lsn=lsn, payload=payload)
    +        self._append_record(primary, record, mode=SyncMode.STREAMING)
    +        self._retained.append(record)
    +        # PostgreSQL cannot report a normal commit before its own WAL durability
    +        # boundary.  Remote waiting is the independently configurable part.
    +        self.fsync(self._primary)
    +        self._flush_acks[lsn] = set()
    +        for standby in self._standbys():
    +            self._send_record(standby, record)
    +
    +        accepted = True
    +        if ack is AckLevel.QUORUM:
    +            required = set(self._synchronous)
    +            for _ in range(self._ack_timeout):
    +                if required <= self._flush_acks[lsn]:
    +                    break
    +                self.tick()
    +            else:
    +                accepted = False
    +        message = (
    +            "primary WAL flushed; replication is asynchronous"
    +            if ack is AckLevel.LEADER
    +            else (
    +                "all synchronous standbys flushed WAL"
    +                if accepted
    +                else "not every synchronous standby acknowledged before timeout"
    +            )
    +        )
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_client_write_result",
    +            lsn=lsn,
    +            timeline=self._timeline,
    +            ack_level=ack.name,
    +            accepted=accepted,
    +            flush_acks=sorted(self._flush_acks[lsn]),
    +        )
    +        return WriteResult(accepted=accepted, offset=lsn, message=message)
    +
    +    def client_read(
    +        self,
    +        key: bytes,
    +        level: ReadLevel,
    +        node: NodeId | None = None,
    +    ) -> ReadResult:
    +        self._validate_bytes(key, "key")
    +        if level is ReadLevel.LINEARIZABLE:
    +            raise UnsupportedLevelError(
    +                "WAL shipping does not provide a lease/read-index "
    +                "ReadLevel.LINEARIZABLE"
    +            )
    +        if level is ReadLevel.LEADER:
    +            selected = self._primary
    +        elif level is ReadLevel.LOCAL:
    +            selected = self._primary if node is None else node
    +        else:
    +            raise UnsupportedLevelError(
    +                f"WAL shipping does not support ReadLevel.{level.name}"
    +            )
    +        target = self._node(selected)
    +        self._require_alive(target)
    +        return ReadResult(
    +            value=target.data.get(key),
    +            node=selected,
    +            offset=target.applied_lsn,
    +        )
    +
    +    def tick(self) -> None:
    +        self.scheduler.tick()
    +
    +    def run_until_idle(self, *, max_ticks: int = 10_000) -> int:
    +        return self.scheduler.run_until_idle(max_ticks=max_ticks)
    +
    +    def fsync(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        target.durable_wal_bytes = target.wal_bytes
    +        target.durable_records = list(target.records)
    +        target.durable_data = dict(target.data)
    +        target.durable_lsn = target.applied_lsn
    +        target.flushed_lsn = target.applied_lsn
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_fsynced",
    +            node=node,
    +            timeline=target.timeline,
    +            lsn=target.durable_lsn,
    +        )
    +
    +    def crash(self, node: NodeId, *, lose_unfsynced: bool = False) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        if lose_unfsynced:
    +            target.wal_bytes = target.durable_wal_bytes
    +            target.records = list(target.durable_records)
    +            target.data = dict(target.durable_data)
    +            target.applied_lsn = target.durable_lsn
    +            target.flushed_lsn = target.durable_lsn
    +            self.trace.record(
    +                self.clock.now,
    +                "wal_fsync_loss",
    +                node=node,
    +                restored_lsn=target.durable_lsn,
    +            )
    +        target.alive = False
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_node_crashed",
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
    +        self.trace.record(self.clock.now, "wal_node_restarted", node=node)
    +        if node != self._primary:
    +            self._synchronize(node)
    +
    +    def isolate(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "wal_node_isolated", node=node)
    +
    +    def heal(self, node: NodeId) -> None:
    +        target = self._node(node)
    +        self._require_alive(target)
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "wal_node_healed", node=node)
    +        if node != self._primary:
    +            self._synchronize(node)
    +
    +    def promote(self, node: NodeId) -> None:
    +        candidate = self._node(node)
    +        self._require_alive(candidate)
    +        if node == self._primary:
    +            raise ValueError(f"node is already primary: {node}")
    +        old_primary = self._primary
    +        self._timeline += 1
    +        self._primary = node
    +        candidate.timeline = self._timeline
    +        self._retained.clear()
    +        self.fsync(node)
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_standby_promoted",
    +            old_primary=old_primary,
    +            new_primary=node,
    +            timeline=self._timeline,
    +            lsn=candidate.applied_lsn,
    +        )
    +        for standby in self._standbys():
    +            if self._nodes[standby].alive:
    +                self._synchronize(standby)
    +
    +    def probe(self) -> GroupState:
    +        nodes = {
    +            node_id: NodeState(
    +                node_id=node_id,
    +                alive=node.alive,
    +                role="primary" if node_id == self._primary else "standby",
    +                timeline=node.timeline,
    +                wal_bytes=node.wal_bytes,
    +                applied_lsn=node.applied_lsn,
    +                flushed_lsn=node.flushed_lsn,
    +                durable_lsn=node.durable_lsn,
    +                data=MappingProxyType(dict(node.data)),
    +                sync_mode=node.sync_mode,
    +            )
    +            for node_id, node in self._nodes.items()
    +        }
    +        return GroupState(
    +            tick=self.clock.now,
    +            primary=self._primary,
    +            timeline=self._timeline,
    +            synchronous_standbys=self._synchronous,
    +            retained_lsns=tuple(record.lsn for record in self._retained),
    +            nodes=MappingProxyType(nodes),
    +        )
    +
    +    def _synchronize(self, standby_id: NodeId) -> None:
    +        primary = self._node(self._primary)
    +        standby = self._node(standby_id)
    +        self._require_alive(primary)
    +        self._require_alive(standby)
    +        oldest = self._retained[0].lsn if self._retained else None
    +        can_incremental = (
    +            standby.timeline == self._timeline
    +            and (
    +                standby.applied_lsn == primary.applied_lsn
    +                or (
    +                    oldest is not None
    +                    and standby.applied_lsn >= oldest - 1
    +                )
    +            )
    +        )
    +        if can_incremental:
    +            standby.sync_mode = SyncMode.INCREMENTAL
    +            missing = [
    +                record
    +                for record in self._retained
    +                if record.lsn > standby.applied_lsn
    +            ]
    +            self.trace.record(
    +                self.clock.now,
    +                "wal_incremental_catchup_started",
    +                standby=standby_id,
    +                from_lsn=standby.applied_lsn,
    +                records=len(missing),
    +            )
    +            for record in missing:
    +                self._send_record(standby_id, record)
    +            return
    +
    +        standby.sync_mode = SyncMode.FULL
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_full_catchup_started",
    +            standby=standby_id,
    +            lsn=primary.applied_lsn,
    +            timeline=self._timeline,
    +        )
    +        self.network.send(
    +            self._primary,
    +            standby_id,
    +            {
    +                "type": "base_backup",
    +                "timeline": self._timeline,
    +                "source_primary": self._primary,
    +                "wal_bytes": primary.wal_bytes,
    +                "records": tuple(primary.records),
    +                "data": dict(primary.data),
    +                "lsn": primary.applied_lsn,
    +            },
    +        )
    +
    +    def _send_record(self, standby: NodeId, record: WalRecord) -> None:
    +        self.network.send(
    +            self._primary,
    +            standby,
    +            {
    +                "type": "wal_record",
    +                "source_primary": self._primary,
    +                "timeline": record.timeline,
    +                "lsn": record.lsn,
    +                "wal_bytes": record.payload,
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
    +            raise TypeError("WAL message must be a dict")
    +        message_type = payload.get("type")
    +        if message_type == "flush_ack":
    +            if (
    +                target_id == self._primary
    +                and payload.get("timeline") == self._timeline
    +            ):
    +                self._flush_acks.setdefault(int(payload["lsn"]), set()).add(
    +                    message.source
    +                )
    +            return
    +        reason = self._rejection_reason(message, payload)
    +        if reason is not None:
    +            self.trace.record(
    +                self.clock.now,
    +                "wal_rejected",
    +                node=target_id,
    +                source=message.source,
    +                reason=reason,
    +                timeline=payload.get("timeline"),
    +            )
    +            return
    +        if message_type == "wal_record":
    +            record = WalRecord(
    +                timeline=int(payload["timeline"]),
    +                lsn=int(payload["lsn"]),
    +                payload=self._require_bytes(payload["wal_bytes"], "wal_bytes"),
    +            )
    +            if record.lsn != target.applied_lsn + 1:
    +                self._synchronize(target_id)
    +                return
    +            mode = (
    +                SyncMode.INCREMENTAL
    +                if target.sync_mode is SyncMode.INCREMENTAL
    +                else SyncMode.STREAMING
    +            )
    +            self._append_record(target, record, mode=mode)
    +            if target_id in self._synchronous:
    +                self.fsync(target_id)
    +            self.network.send(
    +                target_id,
    +                self._primary,
    +                {
    +                    "type": "flush_ack",
    +                    "timeline": self._timeline,
    +                    "lsn": record.lsn,
    +                },
    +            )
    +        elif message_type == "base_backup":
    +            target.timeline = self._timeline
    +            target.wal_bytes = self._require_bytes(payload["wal_bytes"], "wal_bytes")
    +            target.records = list(payload["records"])
    +            target.data = dict(payload["data"])
    +            target.applied_lsn = int(payload["lsn"])
    +            target.sync_mode = SyncMode.FULL
    +            if target_id in self._synchronous:
    +                self.fsync(target_id)
    +        else:
    +            raise ValueError(f"unknown WAL message: {message_type}")
    +
    +    def _rejection_reason(
    +        self, message: Message, payload: dict[str, Any]
    +    ) -> str | None:
    +        if payload.get("timeline") != self._timeline:
    +            return "stale_timeline"
    +        if (
    +            message.source != self._primary
    +            or payload.get("source_primary") != self._primary
    +        ):
    +            return "non_current_primary"
    +        return None
    +
    +    def _append_record(
    +        self, node: _Node, record: WalRecord, *, mode: SyncMode
    +    ) -> None:
    +        key, value = self._decode_set(record.payload)
    +        node.timeline = record.timeline
    +        node.wal_bytes += record.payload
    +        node.records.append(record)
    +        node.data[key] = value
    +        node.applied_lsn = record.lsn
    +        node.sync_mode = mode
    +        self.trace.record(
    +            self.clock.now,
    +            "wal_applied",
    +            node=node.node_id,
    +            timeline=record.timeline,
    +            lsn=record.lsn,
    +            wal_bytes=record.payload,
    +        )
    +
    +    @staticmethod
    +    def _encode_set(key: bytes, value: bytes) -> bytes:
    +        return (
    +            len(key).to_bytes(4, "big")
    +            + key
    +            + len(value).to_bytes(4, "big")
    +            + value
    +        )
    +
    +    @staticmethod
    +    def _decode_set(payload: bytes) -> tuple[bytes, bytes]:
    +        key_length = int.from_bytes(payload[:4], "big")
    +        key_end = 4 + key_length
    +        value_length = int.from_bytes(payload[key_end : key_end + 4], "big")
    +        value_start = key_end + 4
    +        value_end = value_start + value_length
    +        if value_end != len(payload):
    +            raise ValueError("invalid logical WAL record")
    +        return payload[4:key_end], payload[value_start:value_end]
    +
    +    def _standbys(self) -> tuple[NodeId, ...]:
    +        return tuple(node for node in self._nodes if node != self._primary)
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

该 Group 拥有 Primary/Standby WAL Position、逻辑字节编码、本地 Fsync、配置远端确认、Catch-up、Timeline Promotion 与旧 WAL 拒绝。

**在运行时做什么**

它总在返回前 Flush Primary WAL；只有 `QUORUM` 进入针对命名同步集合的等待循环。

**关键代码**

```python
required = set(self._synchronous)
for _ in range(self._ack_timeout):
    if required <= self._flush_acks[lsn]:
        break
    self.tick()
```

**关键语句理解**

子集检查表示每个已配置同步 Standby 都确认了该 LSN。它刻意不从节点数推导 Voter Majority。

#### WAL Shipping 包边界

把 Group、Record、Sync Mode 与 Probe 作为支撑公共接线导出。

??? note "支撑文件差异（1 个文件）"
    **`src/minidist/protocols/wal_shipping/__init__.py`**

    ```diff
    diff --git a/src/minidist/protocols/wal_shipping/__init__.py b/src/minidist/protocols/wal_shipping/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..546bcd5b8eff11cae59f6f51b167959ca4533d9c
    --- /dev/null
    +++ b/src/minidist/protocols/wal_shipping/__init__.py
    @@ -0,0 +1,5 @@
    +"""PostgreSQL-style WAL shipping protocol."""
    +
    +from .group import GroupState, NodeState, SyncMode, WalShippingGroup
    +
    +__all__ = ["GroupState", "NodeState", "SyncMode", "WalShippingGroup"]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-wal-shipping/tests.txt)`。八条契约证明确认、位置、追赶、Fencing、持久丢失与重放边界。

### 需要真正记住的内容

Timeline 加 LSN 命名 WAL 历史。本地 Flush 与远端等待是不同决策；配置同步 Standby 不是共识 Voter。

### 用自己的话讲清楚

说明本阶段 `QUORUM` 为什么不表示“任意多数派”，并指出它成功返回以前究竟发生了什么。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/09-wal-shipping.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-07...stage-08)

完成后可运行 `git checkout stage-08` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/08-wal-shipping/stage.patch)
