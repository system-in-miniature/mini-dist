# Stage 01 · 确定性仿真基座

### 目标

建立一个单线程逻辑世界，使时间、消息故障、崩溃损失与每一次观察都可以精确重放。

??? note "交付文件"
    - `pyproject.toml`
    - `src/minidist/__init__.py`
    - `src/minidist/sim/__init__.py`
    - `src/minidist/sim/clock.py`
    - `src/minidist/sim/failure.py`
    - `src/minidist/sim/network.py`
    - `src/minidist/sim/scheduler.py`
    - `src/minidist/sim/trace.py`
    - `tests/sim/test_simulation.py`
    - `uv.lock`
    - `uv.toml`

### 当前遇到的问题

如果复制课程依赖墙钟 Sleep、真实 Socket 或隐式进程状态，就无法区分协议行为与宿主机调度。第一个边界必须在 Leader 或 Replica 出现以前，把所有顺序与故障来源显式化。

### 测试契约

#### 先看会坏在哪里

重放契约把同样八条消息送入两个相同 Seed 的网络，再送入一个不同 Seed 的网络。如果延迟、丢包或乱序依赖宿主时间或全局随机数，前两个 Trace 就会分叉，后续协议实验只剩故事而不是证据。

??? note "文件差异：tests/sim/test_simulation.py"
    ```diff
    diff --git a/tests/sim/test_simulation.py b/tests/sim/test_simulation.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a80c7f40ba68d4b6a8ee20fc32765633ba66601f
    --- /dev/null
    +++ b/tests/sim/test_simulation.py
    @@ -0,0 +1,125 @@
    +from __future__ import annotations
    +
    +import pytest
    +
    +from minidist.sim import (
    +    FailureInjector,
    +    Scheduler,
    +    SimClock,
    +    SimNet,
    +    SimNode,
    +    Trace,
    +)
    +
    +
    +def test_clock_advances_only_by_positive_manual_steps() -> None:
    +    clock = SimClock()
    +
    +    assert clock.now == 0
    +    assert clock.advance(3) == 3
    +    with pytest.raises(ValueError, match="positive"):
    +        clock.advance(0)
    +
    +
    +def test_scheduler_orders_same_tick_events_by_insertion() -> None:
    +    clock = SimClock()
    +    trace = Trace()
    +    scheduler = Scheduler(clock, trace)
    +    observed: list[str] = []
    +
    +    scheduler.schedule(1, lambda: observed.append("first"), label="first")
    +    scheduler.schedule(1, lambda: observed.append("second"), label="second")
    +    assert scheduler.tick() == 2
    +
    +    assert observed == ["first", "second"]
    +    assert clock.now == 1
    +
    +
    +def _network_script(seed: int) -> list[dict[str, object]]:
    +    clock = SimClock()
    +    trace = Trace()
    +    scheduler = Scheduler(clock, trace)
    +    network = SimNet(
    +        seed=seed,
    +        clock=clock,
    +        scheduler=scheduler,
    +        trace=trace,
    +        min_delay=1,
    +        max_delay=3,
    +        drop_rate=0.2,
    +        reorder_rate=0.8,
    +    )
    +    network.register("b", lambda message: None)
    +    for number in range(8):
    +        network.send("a", "b", {"number": number})
    +    scheduler.run_until_idle()
    +    return trace.as_dicts()
    +
    +
    +def test_same_seed_and_script_replay_event_for_event() -> None:
    +    first = _network_script(1729)
    +    replay = _network_script(1729)
    +    other_seed = _network_script(1730)
    +
    +    assert first == replay
    +    assert first != other_seed
    +    dropped = [
    +        event["details"]["message_id"]
    +        for event in first
    +        if event["kind"] == "message_dropped"
    +    ]
    +    delivered = [
    +        event["details"]["message_id"]
    +        for event in first
    +        if event["kind"] == "message_delivered"
    +    ]
    +    delivery_ticks = [
    +        event["tick"] for event in first if event["kind"] == "message_delivered"
    +    ]
    +    assert dropped == [1, 2]
    +    assert delivered == [4, 5, 7, 0, 3, 6]
    +    assert delivery_ticks == [1, 3, 3, 4, 5, 6]
    +
    +
    +def test_directional_and_bidirectional_partitions() -> None:
    +    clock = SimClock()
    +    trace = Trace()
    +    scheduler = Scheduler(clock, trace)
    +    network = SimNet(seed=7, clock=clock, scheduler=scheduler, trace=trace, min_delay=1)
    +    received: list[tuple[str, object]] = []
    +    network.register("a", lambda message: received.append(("a", message.payload)))
    +    network.register("b", lambda message: received.append(("b", message.payload)))
    +
    +    network.partition("a", "b")
    +    network.send("a", "b", "blocked")
    +    network.send("b", "a", "allowed")
    +    scheduler.run_until_idle()
    +    assert received == [("a", "allowed")]
    +
    +    network.heal("a", "b")
    +    network.partition("a", "b", bidirectional=True)
    +    network.send("a", "b", "blocked")
    +    network.send("b", "a", "also blocked")
    +    scheduler.run_until_idle()
    +    assert received == [("a", "allowed")]
    +
    +
    +def test_crash_restart_clears_volatile_and_keeps_persistent_state() -> None:
    +    trace = Trace()
    +    clock = SimClock()
    +    node = SimNode(
    +        node_id="n1",
    +        persistent={"offset": 4},
    +        volatile={"inflight": b"write"},
    +    )
    +    failures = FailureInjector(clock=clock, trace=trace)
    +    failures.register(node)
    +
    +    failures.crash("n1")
    +    assert not node.alive
    +    assert node.volatile == {}
    +    assert node.persistent == {"offset": 4}
    +
    +    failures.restart("n1")
    +    assert node.alive
    +    assert node.persistent == {"offset": 4}
    ```

**测试锁定什么**

锁定只能正向推进的逻辑时间、同 Tick 插入顺序、带种子网络重放、方向性分区，以及崩溃/重启对易失与持久状态的区分。

**如何构造反例**

同一脚本运行两次，记录每个 Message ID 与 Delivery Tick，再执行单向/双向分区，并让同时具有两类状态的节点崩溃。

**关键测试语句**

```python
assert first == replay
assert first != other_seed
```

**失败意味着什么**

实验结果依赖运行机器，或者崩溃擦除了错误的所有权类别。

### 基本概念

离散事件仿真器只在 Harness 请求时推进整数逻辑时钟。事件按 `(due_tick, insertion_order)` 排序。Trace 是只追加观察，不是第二个状态所有者。带 Seed 的网络把概率变成可复现脚本。模拟节点把易失进程内存与持久状态分开。

### 为什么需要这个机制

分布式协议主要讨论顺序与不完整可见性。如果教学环境把顺序藏在线程和耗时中，故障就无法重放或归因。显式 Tick 与故障边界让后续比较保持因果：改变的是协议，实验本身不变。

### 运行时心智模型

Harness 安排工作，推进一个 Tick，并按稳定插入顺序运行所有到期 Callback。`SimNet` 用私有 Seeded RNG 决定丢弃/延迟/乱序，再安排 Delivery。`FailureInjector` 同步改变节点生命周期。每个边界都在当前 Tick 记录 `TraceEvent`。

### 机制板块

#### 逻辑时间与确定性事件顺序

显式化时间、同 Tick 排序与 Trace，使同一实验脚本始终产生相同事件历史。

??? note "文件差异：src/minidist/sim/clock.py"
    ```diff
    diff --git a/src/minidist/sim/clock.py b/src/minidist/sim/clock.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..25a3f3f1efabb12b559d917508dc564a30a978bc
    --- /dev/null
    +++ b/src/minidist/sim/clock.py
    @@ -0,0 +1,25 @@
    +"""A logical clock like those used by deterministic simulation tests.
    +
    +Wall-clock time would make failure boundaries and timeout races depend on
    +machine load.  ``SimClock`` moves only when the harness asks it to, so a tick
    +number is a reproducible part of an experiment rather than a duration claim.
    +"""
    +
    +
    +class SimClock:
    +    """Monotonic integer time advanced explicitly by the simulation."""
    +
    +    def __init__(self) -> None:
    +        self._now = 0
    +
    +    @property
    +    def now(self) -> int:
    +        return self._now
    +
    +    def advance(self, steps: int = 1) -> int:
    +        """Advance by positive logical steps and return the new tick."""
    +
    +        if steps <= 0:
    +            raise ValueError("clock steps must be positive")
    +        self._now += steps
    +        return self._now
    ```

??? note "文件差异：src/minidist/sim/trace.py"
    ```diff
    diff --git a/src/minidist/sim/trace.py b/src/minidist/sim/trace.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c0ce5667a60728f2ece64a5046c3edf1e53f7d01
    --- /dev/null
    +++ b/src/minidist/sim/trace.py
    @@ -0,0 +1,47 @@
    +"""Structured event history for replay assertions and teaching output.
    +
    +Production tracing systems add timestamps, spans, and exporters.  MiniDist
    +keeps the essential contract: every meaningful transition gets an ordered,
    +machine-readable record.  Sequence numbers make equal-tick events unambiguous.
    +"""
    +
    +from __future__ import annotations
    +
    +from dataclasses import asdict, dataclass
    +from typing import Any
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TraceEvent:
    +    """One stable observation in a simulation run."""
    +
    +    sequence: int
    +    tick: int
    +    kind: str
    +    details: dict[str, Any]
    +
    +
    +class Trace:
    +    """Append-only structured event log."""
    +
    +    def __init__(self) -> None:
    +        self._events: list[TraceEvent] = []
    +
    +    @property
    +    def events(self) -> tuple[TraceEvent, ...]:
    +        return tuple(self._events)
    +
    +    def record(self, tick: int, kind: str, **details: Any) -> TraceEvent:
    +        event = TraceEvent(
    +            sequence=len(self._events),
    +            tick=tick,
    +            kind=kind,
    +            details=details,
    +        )
    +        self._events.append(event)
    +        return event
    +
    +    def as_dicts(self) -> list[dict[str, Any]]:
    +        """Return value-based snapshots convenient for exact replay checks."""
    +
    +        return [asdict(event) for event in self._events]
    ```

??? note "文件差异：src/minidist/sim/scheduler.py"
    ```diff
    diff --git a/src/minidist/sim/scheduler.py b/src/minidist/sim/scheduler.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c9b362310e503b2c97e00e82b975534a0cc6331f
    --- /dev/null
    +++ b/src/minidist/sim/scheduler.py
    @@ -0,0 +1,85 @@
    +"""Single-threaded deterministic event scheduling.
    +
    +This models the event queues underneath discrete-event simulators.  A
    +monotonic insertion counter breaks ties at the same logical tick; therefore
    +the result never depends on thread scheduling or heap implementation details.
    +"""
    +
    +from __future__ import annotations
    +
    +import heapq
    +from collections.abc import Callable
    +from dataclasses import dataclass, field
    +
    +from .clock import SimClock
    +from .trace import Trace
    +
    +
    +@dataclass(order=True, slots=True)
    +class _ScheduledEvent:
    +    due_tick: int
    +    order: int
    +    callback: Callable[[], None] = field(compare=False, repr=False)
    +    label: str = field(compare=False)
    +
    +
    +class Scheduler:
    +    """A deterministic priority queue driven one tick at a time."""
    +
    +    def __init__(self, clock: SimClock, trace: Trace) -> None:
    +        self.clock = clock
    +        self.trace = trace
    +        self._queue: list[_ScheduledEvent] = []
    +        self._next_order = 0
    +
    +    @property
    +    def pending(self) -> int:
    +        return len(self._queue)
    +
    +    def schedule(self, delay: int, callback: Callable[[], None], *, label: str) -> None:
    +        if delay < 0:
    +            raise ValueError("event delay must not be negative")
    +        event = _ScheduledEvent(
    +            due_tick=self.clock.now + delay,
    +            order=self._next_order,
    +            callback=callback,
    +            label=label,
    +        )
    +        self._next_order += 1
    +        heapq.heappush(self._queue, event)
    +        self.trace.record(
    +            self.clock.now,
    +            "event_scheduled",
    +            label=label,
    +            due_tick=event.due_tick,
    +            order=event.order,
    +        )
    +
    +    def tick(self) -> int:
    +        """Advance once, run all now-due events, and return their count."""
    +
    +        self.clock.advance()
    +        ran = 0
    +        while self._queue and self._queue[0].due_tick <= self.clock.now:
    +            event = heapq.heappop(self._queue)
    +            self.trace.record(
    +                self.clock.now,
    +                "event_started",
    +                label=event.label,
    +                due_tick=event.due_tick,
    +                order=event.order,
    +            )
    +            event.callback()
    +            ran += 1
    +        return ran
    +
    +    def run_until_idle(self, *, max_ticks: int = 10_000) -> int:
    +        """Tick until no work remains, protecting lessons from infinite loops."""
    +
    +        ticks = 0
    +        while self._queue:
    +            if ticks >= max_ticks:
    +                raise RuntimeError("scheduler did not become idle")
    +            self.tick()
    +            ticks += 1
    +        return ticks
    ```

**是什么，为什么现在需要**

这些组件分别拥有逻辑时间、不可变观察与唯一事件队列，用可重放顺序替代 Sleep 和线程调度。

**在运行时做什么**

Scheduler 计算绝对 Due Tick，加入单调插入编号，并在 Callback 前后记录 Schedule 与 Execution。

**关键代码**

```python
event = _ScheduledEvent(
    due_tick=self.clock.now + delay,
    order=self._next_order,
    callback=callback,
    label=label,
)
```

**关键语句理解**

插入编号不是装饰；没有它，两个同 Tick 事件即使 Seed 不变，也可能在不同运行中交换顺序。

#### 带种子的网络与崩溃边界

把延迟、丢包、乱序、分区、易失状态丢失与重启建模为可复现状态迁移。

??? note "文件差异：src/minidist/sim/network.py"
    ```diff
    diff --git a/src/minidist/sim/network.py b/src/minidist/sim/network.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a2d25d0b5f3b09909b97ce180c8e3a7d3777cc06
    --- /dev/null
    +++ b/src/minidist/sim/network.py
    @@ -0,0 +1,159 @@
    +"""Seeded in-process network fault simulation.
    +
    +``SimNet`` corresponds to the transport fault layer used in deterministic
    +simulation testing: it can delay, drop, reorder, or partition messages while
    +keeping every choice reproducible from an explicit seed.  It transports
    +Python values because serialization is not the lesson in MiniDist.
    +"""
    +
    +from __future__ import annotations
    +
    +import random
    +from collections.abc import Callable
    +from dataclasses import dataclass
    +from typing import Any
    +
    +from .clock import SimClock
    +from .scheduler import Scheduler
    +from .trace import Trace
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Message:
    +    """An immutable envelope visible to node handlers."""
    +
    +    message_id: int
    +    source: str
    +    destination: str
    +    payload: Any
    +
    +
    +class SimNet:
    +    """Deterministic message transport with scriptable fault boundaries."""
    +
    +    def __init__(
    +        self,
    +        *,
    +        seed: int,
    +        clock: SimClock,
    +        scheduler: Scheduler,
    +        trace: Trace,
    +        min_delay: int = 1,
    +        max_delay: int | None = None,
    +        drop_rate: float = 0.0,
    +        reorder_rate: float = 0.0,
    +    ) -> None:
    +        max_delay = min_delay if max_delay is None else max_delay
    +        if min_delay < 0 or max_delay < min_delay:
    +            raise ValueError("network delay range is invalid")
    +        if not 0.0 <= drop_rate <= 1.0:
    +            raise ValueError("drop_rate must be between 0 and 1")
    +        if not 0.0 <= reorder_rate <= 1.0:
    +            raise ValueError("reorder_rate must be between 0 and 1")
    +        self.clock = clock
    +        self.scheduler = scheduler
    +        self.trace = trace
    +        self._rng = random.Random(seed)
    +        self._min_delay = min_delay
    +        self._max_delay = max_delay
    +        self._drop_rate = drop_rate
    +        self._reorder_rate = reorder_rate
    +        self._handlers: dict[str, Callable[[Message], None]] = {}
    +        self._partitions: set[tuple[str, str]] = set()
    +        self._next_message_id = 0
    +        self.trace.record(clock.now, "network_created", seed=seed)
    +
    +    def register(self, node: str, handler: Callable[[Message], None]) -> None:
    +        self._handlers[node] = handler
    +
    +    def partition(
    +        self, source: str, destination: str, *, bidirectional: bool = False
    +    ) -> None:
    +        self._partitions.add((source, destination))
    +        if bidirectional:
    +            self._partitions.add((destination, source))
    +        self.trace.record(
    +            self.clock.now,
    +            "partition_added",
    +            source=source,
    +            destination=destination,
    +            bidirectional=bidirectional,
    +        )
    +
    +    def heal(
    +        self, source: str, destination: str, *, bidirectional: bool = False
    +    ) -> None:
    +        self._partitions.discard((source, destination))
    +        if bidirectional:
    +            self._partitions.discard((destination, source))
    +        self.trace.record(
    +            self.clock.now,
    +            "partition_healed",
    +            source=source,
    +            destination=destination,
    +            bidirectional=bidirectional,
    +        )
    +
    +    def send(self, source: str, destination: str, payload: Any) -> Message:
    +        message = Message(
    +            message_id=self._next_message_id,
    +            source=source,
    +            destination=destination,
    +            payload=payload,
    +        )
    +        self._next_message_id += 1
    +        self.trace.record(
    +            self.clock.now,
    +            "message_sent",
    +            message_id=message.message_id,
    +            source=source,
    +            destination=destination,
    +            payload=payload,
    +        )
    +
    +        if (source, destination) in self._partitions:
    +            self._record_drop(message, "partition")
    +            return message
    +        if self._rng.random() < self._drop_rate:
    +            self._record_drop(message, "seeded_drop")
    +            return message
    +
    +        delay = self._rng.randint(self._min_delay, self._max_delay)
    +        if self._rng.random() < self._reorder_rate:
    +            # Extra seeded jitter deliberately lets a later send overtake this
    +            # one; no host timing participates in that ordering decision.
    +            delay += self._rng.randint(0, max(1, self._max_delay))
    +        self.scheduler.schedule(
    +            delay,
    +            lambda: self._deliver(message),
    +            label=f"deliver:{message.message_id}",
    +        )
    +        return message
    +
    +    def _record_drop(self, message: Message, reason: str) -> None:
    +        self.trace.record(
    +            self.clock.now,
    +            "message_dropped",
    +            message_id=message.message_id,
    +            source=message.source,
    +            destination=message.destination,
    +            reason=reason,
    +        )
    +
    +    def _deliver(self, message: Message) -> None:
    +        if (message.source, message.destination) in self._partitions:
    +            self._record_drop(message, "partition_at_delivery")
    +            return
    +        handler = self._handlers.get(message.destination)
    +        if handler is None:
    +            self._record_drop(message, "unknown_destination")
    +            return
    +        self.trace.record(
    +            self.clock.now,
    +            "message_delivered",
    +            message_id=message.message_id,
    +            source=message.source,
    +            destination=message.destination,
    +            payload=message.payload,
    +        )
    +        handler(message)
    ```

??? note "文件差异：src/minidist/sim/failure.py"
    ```diff
    diff --git a/src/minidist/sim/failure.py b/src/minidist/sim/failure.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..05011571c8a88b86ff7ed7e7da69b646ff1013c7
    --- /dev/null
    +++ b/src/minidist/sim/failure.py
    @@ -0,0 +1,69 @@
    +"""Crash/restart ownership boundary for simulated nodes.
    +
    +Real crash recovery distinguishes memory lost with the process from bytes
    +already durable on disk.  ``FailureInjector`` makes that distinction visible:
    +crash clears the node's volatile mapping, leaves its persistent mapping alone,
    +and marks it unavailable until an explicit restart.
    +"""
    +
    +from __future__ import annotations
    +
    +from dataclasses import dataclass, field
    +from typing import Any
    +
    +from .clock import SimClock
    +from .trace import Trace
    +
    +
    +@dataclass(slots=True)
    +class SimNode:
    +    """Small generic node used to teach volatile versus persistent state."""
    +
    +    node_id: str
    +    persistent: dict[str, Any] = field(default_factory=dict)
    +    volatile: dict[str, Any] = field(default_factory=dict)
    +    alive: bool = True
    +
    +    def on_crash(self) -> None:
    +        self.volatile.clear()
    +
    +    def on_restart(self) -> None:
    +        """Hook for richer nodes; durable state already remains available."""
    +
    +
    +class FailureInjector:
    +    """Apply explicit lifecycle faults and trace each transition."""
    +
    +    def __init__(self, *, clock: SimClock, trace: Trace) -> None:
    +        self.clock = clock
    +        self.trace = trace
    +        self._nodes: dict[str, SimNode] = {}
    +
    +    def register(self, node: SimNode) -> None:
    +        if node.node_id in self._nodes:
    +            raise ValueError(f"node already registered: {node.node_id}")
    +        self._nodes[node.node_id] = node
    +
    +    def crash(self, node_id: str) -> None:
    +        node = self._node(node_id)
    +        if not node.alive:
    +            raise ValueError(f"node already crashed: {node_id}")
    +        # Clear volatile data before publishing the down state so observers can
    +        # never see a crashed node that still owns in-flight process memory.
    +        node.on_crash()
    +        node.alive = False
    +        self.trace.record(self.clock.now, "node_crashed", node=node_id)
    +
    +    def restart(self, node_id: str) -> None:
    +        node = self._node(node_id)
    +        if node.alive:
    +            raise ValueError(f"node already running: {node_id}")
    +        node.on_restart()
    +        node.alive = True
    +        self.trace.record(self.clock.now, "node_restarted", node=node_id)
    +
    +    def _node(self, node_id: str) -> SimNode:
    +        try:
    +            return self._nodes[node_id]
    +        except KeyError as error:
    +            raise KeyError(f"unknown node: {node_id}") from error
    ```

**是什么，为什么现在需要**

网络拥有传输故障，Failure Injector 拥有节点生命周期。协议代码只请求发送并观察 Callback，不暗中决定传输是否成功。

**在运行时做什么**

每次发送获得不可变 ID、Seed 决定的命运与计划 Delivery。崩溃清空易失状态、保留持久状态，并让节点直到显式重启前不可用。

**关键代码**

```python
if (source, destination) in self._partitions:
    self._record_drop(message, "partition")
    return message
```

**关键语句理解**

分区是传输边界中的方向性数据。把它当作节点崩溃，会抹掉旧 Leader 与 Split-brain 实验依赖的不对称性。

#### 包与仿真脚手架

接好包、仿真词汇导出与测试环境，但不把项目接线当成分布式协议。

??? note "支撑文件差异（5 个文件）"
    **`pyproject.toml`**

    ```diff
    diff --git a/pyproject.toml b/pyproject.toml
    new file mode 100644
    index 0000000000000000000000000000000000000000..6d4e05b18bdaa2fcacbc150ea8a36e1a971a56e6
    --- /dev/null
    +++ b/pyproject.toml
    @@ -0,0 +1,22 @@
    +[build-system]
    +requires = ["hatchling"]
    +build-backend = "hatchling.build"
    +
    +[project]
    +name = "minidist"
    +version = "0.1.0"
    +description = "Deterministic distributed replication protocols in miniature"
    +requires-python = ">=3.12"
    +dependencies = []
    +
    +[dependency-groups]
    +dev = [
    +    "pytest>=9,<10",
    +]
    +
    +[tool.hatch.build.targets.wheel]
    +packages = ["src/minidist"]
    +
    +[tool.pytest.ini_options]
    +pythonpath = ["src", "."]
    +testpaths = ["tests"]
    ```

    **`uv.lock`**

    ```diff
    diff --git a/uv.lock b/uv.lock
    new file mode 100644
    index 0000000000000000000000000000000000000000..1b56ff93cd196acf0bd4b977e5db739fb979e7ae
    --- /dev/null
    +++ b/uv.lock
    @@ -0,0 +1,79 @@
    +version = 1
    +revision = 3
    +requires-python = ">=3.12"
    +
    +[[package]]
    +name = "colorama"
    +version = "0.4.6"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/d8/53/6f443c9a4a8358a93a6792e2acffb9d9d5cb0a5cfd8802644b7b1c9a02e4/colorama-0.4.6.tar.gz", hash = "sha256:08695f5cb7ed6e0531a20572697297273c47b8cae5a63ffc6d6ed5c201be6e44", size = 27697, upload-time = "2022-10-25T02:36:22.414Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/d1/d6/3965ed04c63042e047cb6a3e6ed1a63a35087b6a609aa3a15ed8ac56c221/colorama-0.4.6-py2.py3-none-any.whl", hash = "sha256:4f1d9991f5acc0ca119f9d443620b77f9d6b33703e51011c16baf57afb285fc6", size = 25335, upload-time = "2022-10-25T02:36:20.889Z" },
    +]
    +
    +[[package]]
    +name = "iniconfig"
    +version = "2.3.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/72/34/14ca021ce8e5dfedc35312d08ba8bf51fdd999c576889fc2c24cb97f4f10/iniconfig-2.3.0.tar.gz", hash = "sha256:c76315c77db068650d49c5b56314774a7804df16fee4402c1f19d6d15d8c4730", size = 20503, upload-time = "2025-10-18T21:55:43.219Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/cb/b1/3846dd7f199d53cb17f49cba7e651e9ce294d8497c8c150530ed11865bb8/iniconfig-2.3.0-py3-none-any.whl", hash = "sha256:f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12", size = 7484, upload-time = "2025-10-18T21:55:41.639Z" },
    +]
    +
    +[[package]]
    +name = "minidist"
    +version = "0.1.0"
    +source = { editable = "." }
    +
    +[package.dev-dependencies]
    +dev = [
    +    { name = "pytest" },
    +]
    +
    +[package.metadata]
    +
    +[package.metadata.requires-dev]
    +dev = [{ name = "pytest", specifier = ">=9,<10" }]
    +
    +[[package]]
    +name = "packaging"
    +version = "26.2"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/d7/f1/e7a6dd94a8d4a5626c03e4e99c87f241ba9e350cd9e6d75123f992427270/packaging-26.2.tar.gz", hash = "sha256:ff452ff5a3e828ce110190feff1178bb1f2ea2281fa2075aadb987c2fb221661", size = 228134, upload-time = "2026-04-24T20:15:23.917Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/df/b2/87e62e8c3e2f4b32e5fe99e0b86d576da1312593b39f47d8ceef365e95ed/packaging-26.2-py3-none-any.whl", hash = "sha256:5fc45236b9446107ff2415ce77c807cee2862cb6fac22b8a73826d0693b0980e", size = 100195, upload-time = "2026-04-24T20:15:22.081Z" },
    +]
    +
    +[[package]]
    +name = "pluggy"
    +version = "1.6.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/f9/e2/3e91f31a7d2b083fe6ef3fa267035b518369d9511ffab804f839851d2779/pluggy-1.6.0.tar.gz", hash = "sha256:7dcc130b76258d33b90f61b658791dede3486c3e6bfb003ee5c9bfb396dd22f3", size = 69412, upload-time = "2025-05-15T12:30:07.975Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/54/20/4d324d65cc6d9205fabedc306948156824eb9f0ee1633355a8f7ec5c66bf/pluggy-1.6.0-py3-none-any.whl", hash = "sha256:e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746", size = 20538, upload-time = "2025-05-15T12:30:06.134Z" },
    +]
    +
    +[[package]]
    +name = "pygments"
    +version = "2.20.0"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/c3/b2/bc9c9196916376152d655522fdcebac55e66de6603a76a02bca1b6414f6c/pygments-2.20.0.tar.gz", hash = "sha256:6757cd03768053ff99f3039c1a36d6c0aa0b263438fcab17520b30a303a82b5f", size = 4955991, upload-time = "2026-03-29T13:29:33.898Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/f4/7e/a72dd26f3b0f4f2bf1dd8923c85f7ceb43172af56d63c7383eb62b332364/pygments-2.20.0-py3-none-any.whl", hash = "sha256:81a9e26dd42fd28a23a2d169d86d7ac03b46e2f8b59ed4698fb4785f946d0176", size = 1231151, upload-time = "2026-03-29T13:29:30.038Z" },
    +]
    +
    +[[package]]
    +name = "pytest"
    +version = "9.1.1"
    +source = { registry = "https://pypi.org/simple" }
    +dependencies = [
    +    { name = "colorama", marker = "sys_platform == 'win32'" },
    +    { name = "iniconfig" },
    +    { name = "packaging" },
    +    { name = "pluggy" },
    +    { name = "pygments" },
    +]
    +sdist = { url = "https://files.pythonhosted.org/packages/e4/47/b9efed96c114afcfa3c9d3fe98a76a1d14c74a9e266d397cf6eb64be5e01/pytest-9.1.1.tar.gz", hash = "sha256:1088fbde8f2b49d95a549a195707afa7a76a3ce9bcadc26b6d71f0ffda5fe313", size = 1636369, upload-time = "2026-06-19T10:58:32.857Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/24/25/1de2678b631f5a49215c6c96fff41ba892b0a34df68d6d80292b1b48aa7f/pytest-9.1.1-py3-none-any.whl", hash = "sha256:37a86b45efb9a47a61a36449063e8e18d0cab3161329fc099eb21783169c4f0c", size = 386536, upload-time = "2026-06-19T10:58:31.347Z" },
    +]
    ```

    **`uv.toml`**

    ```diff
    diff --git a/uv.toml b/uv.toml
    new file mode 100644
    index 0000000000000000000000000000000000000000..cb6d65e6e61a2a9bb27a78715478c8d935f2014c
    --- /dev/null
    +++ b/uv.toml
    @@ -0,0 +1 @@
    +cache-dir = ".uv-cache"
    ```

    **`src/minidist/__init__.py`**

    ```diff
    diff --git a/src/minidist/__init__.py b/src/minidist/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..993b7e2716303e2cdb47b4f01cea1e500aa0cb91
    --- /dev/null
    +++ b/src/minidist/__init__.py
    @@ -0,0 +1,7 @@
    +"""MiniDist: deterministic, side-by-side distributed protocol lessons.
    +
    +The package intentionally exposes simulation building blocks and protocol
    +implementations, not a production networking or storage abstraction.
    +"""
    +
    +__version__ = "0.1.0"
    ```

    **`src/minidist/sim/__init__.py`**

    ```diff
    diff --git a/src/minidist/sim/__init__.py b/src/minidist/sim/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..04b9370f8c2440ebdcbefb706d15b4de588d970d
    --- /dev/null
    +++ b/src/minidist/sim/__init__.py
    @@ -0,0 +1,18 @@
    +"""Deterministic simulation foundation shared by MiniDist protocols."""
    +
    +from .clock import SimClock
    +from .failure import FailureInjector, SimNode
    +from .network import Message, SimNet
    +from .scheduler import Scheduler
    +from .trace import Trace, TraceEvent
    +
    +__all__ = [
    +    "FailureInjector",
    +    "Message",
    +    "Scheduler",
    +    "SimClock",
    +    "SimNet",
    +    "SimNode",
    +    "Trace",
    +    "TraceEvent",
    +]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/01-deterministic-simulation/tests.txt)`。六条契约证明确定性调度与故障表面；此时还不存在复制保证。

### 需要真正记住的内容

逻辑时间是测试控制的状态；Seeded Fault 是可重放输入；Trace 观察而不拥有行为；崩溃语义必须说明丢失哪类状态。

### 用自己的话讲清楚

解释为什么仅固定随机 Seed 仍不够，还必须显式控制同 Tick 排序与时钟推进。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/02-deterministic-simulation.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/tree/stage-01)

完成后可运行 `git checkout stage-01` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/01-deterministic-simulation/stage.patch)
