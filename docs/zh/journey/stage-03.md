# Stage 03 · 异步主从复制

### 目标

建立一个确认可早于副本投递的主从复制组，并让由此产生的延迟、追赶与故障转移丢失窗口可观察。

??? note "交付文件"
    - `src/minidist/protocols/async_primary/__init__.py`
    - `src/minidist/protocols/async_primary/group.py`
    - `tests/protocols/test_async_primary.py`

### 当前遇到的问题

公共驱动已经可以请求确认，但还没有实现为这个词赋予具体边界。本阶段需要一个本地接受明确弱于复制或持久共识的协议，给后续比较提供真实的低延迟端点。

### 测试契约

#### 先看会坏在哪里

第一条契约在投递延迟为两个 Tick 时使用 `AckLevel.LEADER` 写入。它立刻读取副本并期待没有值，再推进逻辑时间直到值出现。如果 helper 在返回前偷偷排空网络，就会抹掉刻意设计的确认窗口，误教成同步复制。

??? note "文件差异：tests/protocols/test_async_primary.py"
    ```diff
    diff --git a/tests/protocols/test_async_primary.py b/tests/protocols/test_async_primary.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d3fbff1a2e094e4374049c2618d582d4b014814d
    --- /dev/null
    +++ b/tests/protocols/test_async_primary.py
    @@ -0,0 +1,75 @@
    +from __future__ import annotations
    +
    +import pytest
    +
    +from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
    +from minidist.protocols.async_primary import AsyncPrimaryGroup, SyncMode
    +
    +
    +def test_write_acks_before_replica_converges() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    +
    +    result = group.client_write(b"course", b"MiniDist", AckLevel.LEADER)
    +
    +    assert result.accepted
    +    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    +    group.tick()
    +    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    +    group.tick()
    +    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value == b"MiniDist"
    +
    +
    +@pytest.mark.parametrize("level", [AckLevel.QUORUM, AckLevel.ALL_ISR])
    +def test_async_primary_rejects_stronger_ack_levels(level: AckLevel) -> None:
    +    group = AsyncPrimaryGroup()
    +    with pytest.raises(UnsupportedLevelError, match=level.name):
    +        group.client_write(b"k", b"v", level)
    +
    +
    +def test_async_primary_rejects_linearizable_reads() -> None:
    +    group = AsyncPrimaryGroup()
    +    with pytest.raises(UnsupportedLevelError, match="LINEARIZABLE"):
    +        group.client_read(b"k", ReadLevel.LINEARIZABLE)
    +
    +
    +def test_restart_uses_partial_resync_inside_backlog() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=3, replication_delay=1)
    +    group.client_write(b"a", b"1", AckLevel.LEADER)
    +    group.tick()
    +    group.crash("r1")
    +    group.client_write(b"b", b"2", AckLevel.LEADER)
    +    group.client_write(b"c", b"3", AckLevel.LEADER)
    +
    +    group.restart("r1")
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.nodes["r1"].sync_mode is SyncMode.PARTIAL
    +    assert state.nodes["r1"].data == {b"a": b"1", b"b": b"2", b"c": b"3"}
    +
    +
    +def test_restart_uses_full_resync_outside_backlog() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=2, replication_delay=1)
    +    group.crash("r1")
    +    for number in range(3):
    +        group.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.LEADER)
    +
    +    group.restart("r1")
    +    group.run_until_idle()
    +
    +    state = group.probe()
    +    assert state.nodes["r1"].sync_mode is SyncMode.FULL
    +    assert state.nodes["r1"].data == state.nodes[state.primary].data
    +
    +
    +def test_manual_promotion_can_lose_acknowledged_write() -> None:
    +    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    +    result = group.client_write(b"paid", b"yes", AckLevel.LEADER)
    +
    +    group.crash("primary")
    +    group.promote("r1")
    +    group.run_until_idle()
    +
    +    assert result.accepted
    +    assert group.client_read(b"paid", ReadLevel.LEADER).value is None
    +    assert group.probe().primary == "r1"
    ```

**测试锁定什么**

锁定主节点提前确认、对更强 Level 的诚实拒绝、部分/全量重同步，以及提升落后副本后已确认写入仍可能消失。

**如何构造反例**

测试让网络消息保持待投递，让副本跨越有限 Backlog 断开或崩溃，并提升尚未收到最新写入的节点。

**关键测试语句**

```python
assert result.accepted
assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
```

**失败意味着什么**

确认边界要么暗中等待了副本，要么协议声称了当前拓扑无法提供的保证。

### 基本概念

Primary 是唯一写入口；Replica 是接收端，不参与投票。复制位置由 Lineage Identity 与 Offset 共同表示。有限 Backlog 可以重放仍保留的前缀缺口，超出窗口的副本需要全量状态复制。Promotion 只改变接收新写入的节点，不能追溯性补回候选节点从未收到的数据。

### 为什么需要这个机制

只有学习者能指出尚未发生的工作，“快速确认”才有准确含义。显式化投递与追赶可直接看见取舍：更低响应延迟换来了故障转移时丢弃已接受工作的一段窗口。

### 运行时心智模型

`client_write` 先修改当前 Primary，把带 Lineage 的 Entry 放入 Backlog，为每个 Replica 安排消息，然后返回。Tick 驱动投递。Restart 在副本仍共享 Lineage 且缺口仍保留时选择部分重放，否则发送全量快照。手动 Promotion 立即改变 Primary。

### 机制板块

#### 主节点确认与异步副本流

一个机制板块串起本地确认、延迟投递、Backlog 追赶与提升后的数据丢失。

??? note "文件差异：src/minidist/protocols/async_primary/group.py"
    ```diff
    diff --git a/src/minidist/protocols/async_primary/group.py b/src/minidist/protocols/async_primary/group.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2c0b24863d4d7b248357bd35567645d42129b382
    --- /dev/null
    +++ b/src/minidist/protocols/async_primary/group.py
    @@ -0,0 +1,453 @@
    +"""Redis-style asynchronous primary/replica replication in miniature.
    +
    +The primary applies a command to its local dict and acknowledges without
    +waiting for replicas.  Replicas are sinks: they never vote on a write and only
    +consume the primary's ordered stream.  Each stream has a replication ID and
    +monotonic offset.  A bounded backlog can fill a small gap; a replica whose
    +lineage or offset falls outside that window receives a full snapshot instead.
    +
    +This is intentionally not Sentinel.  Promotion is a public, manual operation
    +so experiments can separate the data-plane weakness (asynchronous loss) from
    +the control-plane question (who decides the new primary).
    +"""
    +
    +from __future__ import annotations
    +
    +from collections import deque
    +from dataclasses import dataclass, field
    +from enum import Enum, auto
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
    +    """Most recent way a replica joined or caught up with its primary."""
    +
    +    NEVER = auto()
    +    STREAMING = auto()
    +    PARTIAL = auto()
    +    FULL = auto()
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class _Entry:
    +    replication_id: str
    +    offset: int
    +    key: bytes
    +    value: bytes
    +
    +
    +@dataclass(slots=True)
    +class _Node:
    +    node_id: NodeId
    +    data: dict[bytes, bytes] = field(default_factory=dict)
    +    replication_id: str = ""
    +    offset: int = 0
    +    alive: bool = True
    +    volatile: dict[str, Any] = field(default_factory=dict)
    +    sync_mode: SyncMode = SyncMode.NEVER
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class NodeState:
    +    """Read-only experiment snapshot of one node."""
    +
    +    node_id: NodeId
    +    alive: bool
    +    role: str
    +    replication_id: str
    +    offset: int
    +    data: Mapping[bytes, bytes]
    +    sync_mode: SyncMode
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class GroupState:
    +    """Read-only experiment snapshot of the whole replication group."""
    +
    +    tick: int
    +    primary: NodeId
    +    nodes: Mapping[NodeId, NodeState]
    +    backlog_offsets: tuple[int, ...]
    +
    +
    +class AsyncPrimaryGroup:
    +    """A deterministic Redis-like asynchronous replication group."""
    +
    +    def __init__(
    +        self,
    +        *,
    +        primary_id: NodeId = "primary",
    +        replica_ids: tuple[NodeId, ...] = ("replica-1", "replica-2"),
    +        backlog_size: int = 8,
    +        replication_delay: int = 1,
    +        seed: int = 0,
    +    ) -> None:
    +        if backlog_size <= 0:
    +            raise ValueError("backlog_size must be positive")
    +        if replication_delay <= 0:
    +            raise ValueError("replication_delay must be positive")
    +        node_ids = (primary_id, *replica_ids)
    +        if len(set(node_ids)) != len(node_ids):
    +            raise ValueError("node IDs must be unique")
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
    +        self._generation = 1
    +        replication_id = self._new_replication_id()
    +        self._nodes = {
    +            node_id: _Node(node_id=node_id, replication_id=replication_id)
    +            for node_id in node_ids
    +        }
    +        self._backlog: deque[_Entry] = deque(maxlen=backlog_size)
    +        for node_id in node_ids:
    +            self.network.register(
    +                node_id,
    +                lambda message, target=node_id: self._receive(target, message),
    +            )
    +        self.trace.record(
    +            self.clock.now,
    +            "group_created",
    +            primary=primary_id,
    +            replicas=list(replica_ids),
    +            replication_id=replication_id,
    +            backlog_size=backlog_size,
    +        )
    +
    +    @property
    +    def primary(self) -> NodeId:
    +        return self._primary
    +
    +    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult:
    +        """Apply locally, enqueue replica messages, then return the requested ack."""
    +
    +        if ack not in (AckLevel.NONE, AckLevel.LEADER):
    +            raise UnsupportedLevelError(
    +                f"async primary does not support AckLevel.{ack.name}"
    +            )
    +        self._validate_bytes(key, "key")
    +        self._validate_bytes(value, "value")
    +        primary = self._nodes[self._primary]
    +        self._require_alive(primary)
    +
    +        primary.offset += 1
    +        primary.data[key] = value
    +        entry = _Entry(
    +            replication_id=primary.replication_id,
    +            offset=primary.offset,
    +            key=key,
    +            value=value,
    +        )
    +        self._backlog.append(entry)
    +        self.trace.record(
    +            self.clock.now,
    +            "primary_applied",
    +            node=primary.node_id,
    +            replication_id=entry.replication_id,
    +            offset=entry.offset,
    +            key=key,
    +            value=value,
    +        )
    +        for node_id in self._nodes:
    +            if node_id != self._primary:
    +                self._send_entry(self._primary, node_id, entry, "stream")
    +
    +        # The deliberate lesson: neither accepted level waits for a replica.
    +        # LEADER means "applied by this primary", not consensus or durability.
    +        self.trace.record(
    +            self.clock.now,
    +            "client_acknowledged",
    +            ack_level=ack.name,
    +            primary=self._primary,
    +            offset=entry.offset,
    +        )
    +        return WriteResult(
    +            accepted=True,
    +            offset=entry.offset,
    +            message="primary accepted; replica delivery is asynchronous",
    +        )
    +
    +    def client_read(
    +        self,
    +        key: bytes,
    +        level: ReadLevel,
    +        node: NodeId | None = None,
    +    ) -> ReadResult:
    +        """Read locally or force routing to the current primary."""
    +
    +        if level is ReadLevel.LINEARIZABLE:
    +            raise UnsupportedLevelError(
    +                "async primary does not support ReadLevel.LINEARIZABLE"
    +            )
    +        self._validate_bytes(key, "key")
    +        if level is ReadLevel.LEADER:
    +            selected = self._primary
    +        elif level is ReadLevel.LOCAL:
    +            selected = self._primary if node is None else node
    +        else:  # Defensive if a future enum value is added.
    +            raise UnsupportedLevelError(
    +                f"async primary does not support ReadLevel.{level.name}"
    +            )
    +        target = self._node(selected)
    +        self._require_alive(target)
    +        value = target.data.get(key)
    +        self.trace.record(
    +            self.clock.now,
    +            "client_read",
    +            level=level.name,
    +            node=selected,
    +            key=key,
    +            value=value,
    +            offset=target.offset,
    +        )
    +        return ReadResult(value=value, node=selected, offset=target.offset)
    +
    +    def tick(self) -> None:
    +        """Advance one deterministic network/scheduler step."""
    +
    +        self.scheduler.tick()
    +
    +    def run_until_idle(self, *, max_ticks: int = 10_000) -> int:
    +        """Public convenience used by labs instead of scheduler internals."""
    +
    +        return self.scheduler.run_until_idle(max_ticks=max_ticks)
    +
    +    def crash(self, node: NodeId) -> None:
    +        """Crash a node, clearing process-local state but retaining its dataset."""
    +
    +        target = self._node(node)
    +        self._require_alive(target)
    +        target.volatile.clear()
    +        target.alive = False
    +        # A queued packet from the old primary must not be delivered after its
    +        # crash; turning the lifecycle failure into links also models that cut.
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.partition(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "protocol_node_crashed", node=node)
    +
    +    def restart(self, node: NodeId) -> None:
    +        """Restart a node and reconnect a replica through Redis-style resync."""
    +
    +        target = self._node(node)
    +        if target.alive:
    +            raise ValueError(f"node already running: {node}")
    +        target.alive = True
    +        for peer in self._nodes:
    +            if peer != node:
    +                self.network.heal(node, peer, bidirectional=True)
    +        self.trace.record(self.clock.now, "protocol_node_restarted", node=node)
    +        if node != self._primary:
    +            self._synchronize(node)
    +
    +    def promote(self, node: NodeId) -> None:
    +        """Manually make an alive replica the new primary."""
    +
    +        candidate = self._node(node)
    +        self._require_alive(candidate)
    +        if node == self._primary:
    +            raise ValueError(f"node is already primary: {node}")
    +        old_primary = self._primary
    +        self._primary = node
    +        self._generation += 1
    +        candidate.replication_id = self._new_replication_id()
    +        candidate.sync_mode = SyncMode.NEVER
    +        self._backlog.clear()
    +        # Promotion starts a new history.  Clearing the backlog prevents bytes
    +        # from the old lineage being advertised as resumable under the new ID.
    +        self.trace.record(
    +            self.clock.now,
    +            "replica_promoted",
    +            old_primary=old_primary,
    +            new_primary=node,
    +            replication_id=candidate.replication_id,
    +            offset=candidate.offset,
    +        )
    +
    +    def probe(self) -> GroupState:
    +        """Return value snapshots for assertions without exposing mutability."""
    +
    +        nodes = {
    +            node_id: NodeState(
    +                node_id=node_id,
    +                alive=node.alive,
    +                role="primary" if node_id == self._primary else "replica",
    +                replication_id=node.replication_id,
    +                offset=node.offset,
    +                data=MappingProxyType(dict(node.data)),
    +                sync_mode=node.sync_mode,
    +            )
    +            for node_id, node in self._nodes.items()
    +        }
    +        return GroupState(
    +            tick=self.clock.now,
    +            primary=self._primary,
    +            nodes=MappingProxyType(nodes),
    +            backlog_offsets=tuple(entry.offset for entry in self._backlog),
    +        )
    +
    +    def _synchronize(self, replica_id: NodeId) -> None:
    +        primary = self._nodes[self._primary]
    +        replica = self._nodes[replica_id]
    +        self._require_alive(primary)
    +        self._require_alive(replica)
    +        oldest_offset = self._backlog[0].offset if self._backlog else None
    +        can_partial = replica.replication_id == primary.replication_id and (
    +            replica.offset == primary.offset
    +            or (oldest_offset is not None and replica.offset >= oldest_offset - 1)
    +        )
    +        if can_partial:
    +            replica.sync_mode = SyncMode.PARTIAL
    +            missing = [
    +                entry for entry in self._backlog if entry.offset > replica.offset
    +            ]
    +            self.trace.record(
    +                self.clock.now,
    +                "partial_resync_started",
    +                primary=self._primary,
    +                replica=replica_id,
    +                from_offset=replica.offset,
    +                entries=len(missing),
    +            )
    +            for entry in missing:
    +                self._send_entry(self._primary, replica_id, entry, "partial")
    +            return
    +
    +        replica.sync_mode = SyncMode.FULL
    +        payload = {
    +            "type": "full_sync",
    +            "replication_id": primary.replication_id,
    +            "offset": primary.offset,
    +            "data": dict(primary.data),
    +        }
    +        self.trace.record(
    +            self.clock.now,
    +            "full_resync_started",
    +            primary=self._primary,
    +            replica=replica_id,
    +            offset=primary.offset,
    +        )
    +        self.network.send(self._primary, replica_id, payload)
    +
    +    def _send_entry(
    +        self, source: NodeId, destination: NodeId, entry: _Entry, mode: str
    +    ) -> None:
    +        self.network.send(
    +            source,
    +            destination,
    +            {
    +                "type": "entry",
    +                "mode": mode,
    +                "replication_id": entry.replication_id,
    +                "offset": entry.offset,
    +                "key": entry.key,
    +                "value": entry.value,
    +            },
    +        )
    +
    +    def _receive(self, target_id: NodeId, message: Message) -> None:
    +        target = self._nodes[target_id]
    +        source = self._nodes.get(message.source)
    +        if not target.alive or source is None or not source.alive:
    +            self.trace.record(
    +                self.clock.now,
    +                "protocol_message_ignored",
    +                message_id=message.message_id,
    +                reason="endpoint_not_alive",
    +            )
    +            return
    +        payload = message.payload
    +        if not isinstance(payload, dict):
    +            raise TypeError("protocol payload must be a dict")
    +        if payload["type"] == "full_sync":
    +            self._apply_full_sync(target, payload)
    +        elif payload["type"] == "entry":
    +            self._apply_entry(target, payload)
    +        else:
    +            raise ValueError(f"unknown replication message: {payload['type']}")
    +
    +    def _apply_entry(self, target: _Node, payload: dict[str, Any]) -> None:
    +        primary = self._nodes[self._primary]
    +        if (
    +            payload["replication_id"] != primary.replication_id
    +            or payload["replication_id"] != target.replication_id
    +            or payload["offset"] != target.offset + 1
    +        ):
    +            # Redis asks for PSYNC when the stream has a gap or changed ID.  A
    +            # replica must not apply later commands over an unknown prefix.
    +            self.trace.record(
    +                self.clock.now,
    +                "replication_gap_detected",
    +                replica=target.node_id,
    +                replica_offset=target.offset,
    +                received_offset=payload["offset"],
    +            )
    +            self._synchronize(target.node_id)
    +            return
    +        target.data[payload["key"]] = payload["value"]
    +        target.offset = payload["offset"]
    +        if payload["mode"] == "stream":
    +            target.sync_mode = SyncMode.STREAMING
    +        self.trace.record(
    +            self.clock.now,
    +            "replica_applied",
    +            replica=target.node_id,
    +            offset=target.offset,
    +            mode=payload["mode"],
    +            key=payload["key"],
    +            value=payload["value"],
    +        )
    +
    +    def _apply_full_sync(self, target: _Node, payload: dict[str, Any]) -> None:
    +        # Snapshot replacement is atomic at one simulator event.  Real Redis
    +        # transfers and loads an RDB over time; that cost is intentionally out
    +        # of scope while the semantic "replace all prior state" remains.
    +        target.data = dict(payload["data"])
    +        target.replication_id = payload["replication_id"]
    +        target.offset = payload["offset"]
    +        target.sync_mode = SyncMode.FULL
    +        self.trace.record(
    +            self.clock.now,
    +            "full_resync_applied",
    +            replica=target.node_id,
    +            offset=target.offset,
    +        )
    +
    +    def _new_replication_id(self) -> str:
    +        return f"repl-{self._generation}"
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
    ```

**是什么，为什么现在需要**

该 Group 在一个协议模型中共同拥有主节点状态、副本状态、保留 Backlog、延迟消息、重启同步与手动提升。

**在运行时做什么**

它先在本地应用写入，再安排复制。读取可以指向本地节点或当前 Primary；不支持的 Quorum 与 Linearizable 请求会失败，而不是被静默降级。

**关键代码**

```python
for node_id in self._nodes:
    if node_id != self._primary:
        self._send_entry(self._primary, node_id, entry, "stream")
```

**关键语句理解**

逐副本安排消息之后既没有等待，也没有 Quorum 检查。因此后续返回只证明当前 Primary 已应用 Entry。

#### 异步主从包边界

导出学习者可见的 Group 与同步模式词汇，不把导入接线当作协议机制。

??? note "支撑文件差异（1 个文件）"
    **`src/minidist/protocols/async_primary/__init__.py`**

    ```diff
    diff --git a/src/minidist/protocols/async_primary/__init__.py b/src/minidist/protocols/async_primary/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0b9c1ad7976e5d9e571d4430f7f31aaa158bd718
    --- /dev/null
    +++ b/src/minidist/protocols/async_primary/__init__.py
    @@ -0,0 +1,5 @@
    +"""Public surface of protocol 1: asynchronous primary/replica replication."""
    +
    +from .group import AsyncPrimaryGroup, GroupState, NodeState, SyncMode
    +
    +__all__ = ["AsyncPrimaryGroup", "GroupState", "NodeState", "SyncMode"]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/03-async-primary/tests.txt)`。六条契约展示延迟可见性、支持边界、追赶模式与已确认写入丢失。

### 需要真正记住的内容

Leader 确认不等于复制。Backlog 只证明仍保留的连续性，Promotion 无法保住提升节点上不存在的写入。

### 用自己的话讲清楚

说明 `client_write` 返回到副本应用 Entry 之间的精确区间，再说明为何在该区间内崩溃并提升会丢失已接受值。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/04-async-primary.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-02...stage-03)

完成后可运行 `git checkout stage-03` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/03-async-primary/stage.patch)
