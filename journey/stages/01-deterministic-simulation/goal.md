# Stage 01 · Deterministic simulation substrate / 确定性仿真基座

<!-- journey: chapter=2 tests_added=6 -->

## English

### Goal

Build a single-threaded logical world in which time, message faults, crash loss, and every observation can be replayed exactly.

### Deliverable files

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

### The problem at this point

A replication lesson cannot distinguish protocol behavior from host scheduling if it uses wall-clock sleeps, real sockets, or implicit process state. The first boundary must make every source of order and failure explicit before any leader or replica exists.

### Test contract

#### See the failure first

The replay contract sends the same eight messages through two networks with the same seed and one with a different seed. If delay, drop, or reorder consumes host timing or global randomness, the first two traces diverge and the protocol experiments become stories rather than evidence.

<!-- journey-file: tests/sim/test_simulation.py -->
#### Simulation replay contract

##### What this test locks

It locks positive-only logical time, insertion-order tie breaking, seeded network replay, directional partitions, and crash/restart separation between volatile and persistent state.

##### How it constructs the counterexample

It runs identical scripts twice, records every message ID and delivery Tick, then exercises one-way and two-way partitions plus a node crash with both state classes populated.

##### Key test statement

```python
assert first == replay
assert first != other_seed
```

##### What a failure means

An experiment outcome depends on the machine running it, or a crash erases the wrong ownership class.

### Basic concepts

A discrete-event simulator advances a logical integer clock only when the harness asks. Scheduled events are ordered by `(due_tick, insertion_order)`. A trace is an append-only observation, not another state owner. A seeded network turns probability into a reproducible script. A simulated node separates volatile process memory from persistent state.

### Why this mechanism is necessary

Distributed protocols are largely arguments about order and incomplete visibility. If the teaching environment hides order inside threads and elapsed seconds, a failure cannot be replayed or attributed. Explicit ticks and fault boundaries make later comparisons causal: the protocol changes while the experiment stays fixed.

### Runtime mental model

The harness schedules work, advances one Tick, and runs all due callbacks in stable insertion order. `SimNet` decides drop/delay/reorder from its private seeded RNG and schedules delivery. `FailureInjector` changes node lifecycle synchronously. Every boundary records a `TraceEvent` at the current Tick.

### Mechanism blocks

<!-- journey-file: src/minidist/sim/clock.py -->
<!-- journey-file: src/minidist/sim/trace.py -->
<!-- journey-file: src/minidist/sim/scheduler.py -->
#### Logical time and deterministic event order

##### What it is and why it appears

These components own logical time, immutable observations, and the only event queue. They replace sleeps and thread scheduling with one replayable order.

##### Runtime role

The scheduler computes an absolute due Tick, adds a monotonic insertion number, and records scheduling and execution around each callback.

##### Key code

```python
event = _ScheduledEvent(
    due_tick=self.clock.now + delay,
    order=self._next_order,
    callback=callback,
    label=label,
)
```

##### Statement understanding

The insertion number is not cosmetic: without it, two events due at the same Tick could change order across runs even though the test seed is unchanged.

<!-- journey-file: src/minidist/sim/network.py -->
<!-- journey-file: src/minidist/sim/failure.py -->
#### Seeded network and crash boundaries

##### What it is and why it appears

The network owns transport faults; the failure injector owns node lifecycle. Protocol code will request sends and observe callbacks without secretly deciding whether the transport worked.

##### Runtime role

Each send gets an immutable ID, a seeded fate, and a scheduled delivery. Crash clears volatile state, retains persistent state, and makes the node unavailable until explicit restart.

##### Key code

```python
if (source, destination) in self._partitions:
    self._record_drop(message, "partition")
    return message
```

##### Statement understanding

A partition is directional data in the transport boundary. Treating it as a node crash would erase the asymmetry needed for old-leader and split-brain experiments.

<!-- journey-file: pyproject.toml -->
<!-- journey-file: uv.lock -->
<!-- journey-file: uv.toml -->
<!-- journey-file: src/minidist/__init__.py -->
<!-- journey-file: src/minidist/sim/__init__.py -->
#### Package and simulation scaffold

These files install MiniDist, pin the test environment, and expose the simulator vocabulary. They are required wiring, not independent mechanisms.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/01-deterministic-simulation/tests.txt)`. The six contracts prove deterministic scheduling and fault surfaces; no replication guarantee exists yet.

### Durable takeaways

Logical time is test-controlled state. Seeded faults are replayable inputs. Trace observes rather than owns behavior, and crash semantics must name which state class is lost.

### Explain it in your own words

Explain why a fixed random seed is insufficient unless same-Tick event ordering and clock advancement are also explicit.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/02-deterministic-simulation.md)

## 中文

### 目标

建立一个单线程逻辑世界，使时间、消息故障、崩溃损失与每一次观察都可以精确重放。

### 交付文件

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

<!-- journey-file: tests/sim/test_simulation.py -->
#### 仿真重放契约

##### 测试锁定什么

锁定只能正向推进的逻辑时间、同 Tick 插入顺序、带种子网络重放、方向性分区，以及崩溃/重启对易失与持久状态的区分。

##### 如何构造反例

同一脚本运行两次，记录每个 Message ID 与 Delivery Tick，再执行单向/双向分区，并让同时具有两类状态的节点崩溃。

##### 关键测试语句

```python
assert first == replay
assert first != other_seed
```

##### 失败意味着什么

实验结果依赖运行机器，或者崩溃擦除了错误的所有权类别。

### 基本概念

离散事件仿真器只在 Harness 请求时推进整数逻辑时钟。事件按 `(due_tick, insertion_order)` 排序。Trace 是只追加观察，不是第二个状态所有者。带 Seed 的网络把概率变成可复现脚本。模拟节点把易失进程内存与持久状态分开。

### 为什么需要这个机制

分布式协议主要讨论顺序与不完整可见性。如果教学环境把顺序藏在线程和耗时中，故障就无法重放或归因。显式 Tick 与故障边界让后续比较保持因果：改变的是协议，实验本身不变。

### 运行时心智模型

Harness 安排工作，推进一个 Tick，并按稳定插入顺序运行所有到期 Callback。`SimNet` 用私有 Seeded RNG 决定丢弃/延迟/乱序，再安排 Delivery。`FailureInjector` 同步改变节点生命周期。每个边界都在当前 Tick 记录 `TraceEvent`。

### 机制板块

<!-- journey-file: src/minidist/sim/clock.py -->
<!-- journey-file: src/minidist/sim/trace.py -->
<!-- journey-file: src/minidist/sim/scheduler.py -->
#### 逻辑时间与确定性事件顺序

##### 是什么，为什么现在需要

这些组件分别拥有逻辑时间、不可变观察与唯一事件队列，用可重放顺序替代 Sleep 和线程调度。

##### 在运行时做什么

Scheduler 计算绝对 Due Tick，加入单调插入编号，并在 Callback 前后记录 Schedule 与 Execution。

##### 关键代码

```python
event = _ScheduledEvent(
    due_tick=self.clock.now + delay,
    order=self._next_order,
    callback=callback,
    label=label,
)
```

##### 关键语句理解

插入编号不是装饰；没有它，两个同 Tick 事件即使 Seed 不变，也可能在不同运行中交换顺序。

<!-- journey-file: src/minidist/sim/network.py -->
<!-- journey-file: src/minidist/sim/failure.py -->
#### 带种子的网络与崩溃边界

##### 是什么，为什么现在需要

网络拥有传输故障，Failure Injector 拥有节点生命周期。协议代码只请求发送并观察 Callback，不暗中决定传输是否成功。

##### 在运行时做什么

每次发送获得不可变 ID、Seed 决定的命运与计划 Delivery。崩溃清空易失状态、保留持久状态，并让节点直到显式重启前不可用。

##### 关键代码

```python
if (source, destination) in self._partitions:
    self._record_drop(message, "partition")
    return message
```

##### 关键语句理解

分区是传输边界中的方向性数据。把它当作节点崩溃，会抹掉旧 Leader 与 Split-brain 实验依赖的不对称性。

<!-- journey-file: pyproject.toml -->
<!-- journey-file: uv.lock -->
<!-- journey-file: uv.toml -->
<!-- journey-file: src/minidist/__init__.py -->
<!-- journey-file: src/minidist/sim/__init__.py -->
#### 包与仿真脚手架

这些文件安装 MiniDist、锁定测试环境并导出仿真词汇。它们是必要接线，不是独立机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/01-deterministic-simulation/tests.txt)`。六条契约证明确定性调度与故障表面；此时还不存在复制保证。

### 需要真正记住的内容

逻辑时间是测试控制的状态；Seeded Fault 是可重放输入；Trace 观察而不拥有行为；崩溃语义必须说明丢失哪类状态。

### 用自己的话讲清楚

解释为什么仅固定随机 Seed 仍不够，还必须显式控制同 Tick 排序与时钟推进。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/02-deterministic-simulation.md)

