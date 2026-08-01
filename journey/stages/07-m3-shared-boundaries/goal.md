# Stage 07 · Shared durability and observation boundaries / 公共持久性与观察边界

<!-- journey: chapter=2 tests_added=5 -->

## English

### Goal

Make process crash, unflushed power loss, local stale reads, and valid timeout geometry explicit across the simulator, async primary, and Raft.

### Deliverable files

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

<!-- journey-file: tests/sim/test_simulation.py -->
#### Simulator fsync-loss contract

##### What this test locks

It locks last-complete-fsync restoration while keeping ordinary process crash semantics unchanged.

##### How it constructs the counterexample

The test creates two staged persistent updates separated by exactly one fsync, then powers off and restarts the node.

##### Key test statement

```python
failures.crash("n1", lose_unfsynced=True)
failures.restart("n1")
```

##### What a failure means

The failure model cannot distinguish accepted page-cache state from the last durable image.

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### Async local-durability contract

##### What this test locks

Leader acknowledgement remains independent of local fsync: a flushed value survives power loss while a later acknowledged but unflushed value disappears.

##### How it constructs the counterexample

It writes, fsyncs, writes again, loses unflushed state, restarts the same primary, and reads both keys.

##### Key test statement

```python
assert group.client_read(b"window", ReadLevel.LEADER).value is None
```

##### What a failure means

The async model is treating acknowledgement as implicit durability or rolling back beyond the named fsync boundary.

<!-- journey-file: tests/protocols/test_raft.py -->
#### Raft boundary contract

##### What this test locks

Three cases lock timeout/network compatibility, stale local follower reads, and survival of a quorum acknowledgement after explicit unflushed-loss injection.

##### How it constructs the counterexample

It rejects an election timeout below network round-trip delay, isolates a follower for a local read, and crashes the leader after a quorum write.

##### Key test statement

```python
assert group.probe().nodes[new_leader].durable_index >= result.offset
```

##### What a failure means

The model either permits impossible liveness timing, hides local staleness, or acknowledges before Raft's stable-storage obligation.

### Basic concepts

Staged persistent state is visible to the process but not yet in the durable image. `fsync` advances that image. An ordinary process crash clears volatile memory without inventing disk loss; a power-loss fault may restore the last fsync image. Network-delay budget is part of liveness configuration. Local reads describe one node's state and therefore may be stale even in a consensus protocol.

### Why this mechanism is necessary

Cross-protocol experiments need the same fault vocabulary. If “crash” means different hidden things, durability comparisons are invalid and timing failures may be blamed on consensus logic rather than impossible configuration.

### Runtime mental model

The failure injector keeps a current persistent mapping and a separate durable snapshot. Protocol groups expose compatible `fsync` and `crash(..., lose_unfsynced=...)` operations. Async copies its durable data/offset/lineage back on power loss; Raft already persists term, vote, and log before dependent protocol actions and reports that boundary through its probe.

### Mechanism blocks

<!-- journey-file: src/minidist/sim/failure.py -->
#### Explicit staged and durable state

##### What it is and why it appears

The common node lifecycle now owns a last-fsynced snapshot in addition to current process-visible persistent state.

##### Runtime role

`stage_persistent` changes the current mapping, `fsync` copies it into the durable image, and only an explicitly requested loss restores that image before crash callbacks run.

##### Key code

```python
node.persistent.clear()
node.persistent.update(self._durable[node_id])
```

##### Statement understanding

Rollback happens before publishing the node-down state, so observers never see a crashed node owning a newer, non-durable value.

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### Async acknowledgement versus local fsync

##### What it is and why it appears

Each async node now tracks durable data, durable offset, and durable replication identity separately from live state.

##### Runtime role

Explicit fsync snapshots the live primary; a power-loss crash restores exactly that tuple before restart and future catch-up.

##### Key code

```python
if lose_unfsynced:
    target.data = dict(target.durable_data)
    target.offset = target.durable_offset
```

##### Statement understanding

Data and position roll back together. Rolling back only bytes or only offset would make subsequent replication continuity dishonest.

<!-- journey-file: src/minidist/protocols/raft/group.py -->
#### Raft stable storage, timing budget, and local reads

##### What it is and why it appears

Raft gains the shared fsync/crash surface, explicit durable indices in probes, local read routing, and constructor validation for message-delay geometry.

##### Runtime role

The group exposes its already-stable log boundary and prevents a timeout configuration that cannot complete a request/response before elections repeat.

##### Key code

```python
minimum_timeout = 2 * network_delay + heartbeat_interval
if low <= minimum_timeout:
    raise ValueError(
        "lowest election timeout must exceed twice the maximum network "
        "delay plus the heartbeat interval"
    )
```

##### Statement understanding

The minimum election timeout must leave room for a full round trip; deterministic seeds cannot rescue a configuration with no feasible communication window.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/07-m3-shared-boundaries/tests.txt)`. Five added contracts plus prior suites separate durability, local visibility, and liveness geometry.

### Durable takeaways

Process crash is not power loss. Fsync advances a named durable image, local reads can be stale, and timeout configuration must respect transport delay.

### Explain it in your own words

Explain the three states of an acknowledged async write—primary-visible, locally durable, and replica-visible—and which injected fault can remove each.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/02-deterministic-simulation.md) · [Chapter 7](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/07-raft-replication.md)

## 中文

### 目标

在 Simulator、异步主从与 Raft 之间显式表达进程崩溃、未刷盘掉电丢失、本地陈旧读取与合法 Timeout 几何。

### 交付文件

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

<!-- journey-file: tests/sim/test_simulation.py -->
#### Simulator fsync 丢失契约

##### 测试锁定什么

锁定恢复到最后一次完整 Fsync，同时保持普通进程 Crash 语义不变。

##### 如何构造反例

测试创建两个被一次 Fsync 分隔的暂存持久更新，再掉电并重启节点。

##### 关键测试语句

```python
failures.crash("n1", lose_unfsynced=True)
failures.restart("n1")
```

##### 失败意味着什么

故障模型无法区分进程已接受的 Page-cache 状态与最后持久镜像。

<!-- journey-file: tests/protocols/test_async_primary.py -->
#### 异步本地持久性契约

##### 测试锁定什么

Leader Ack 仍独立于本地 Fsync：已刷盘值能在掉电后保留，随后已确认但未刷盘值消失。

##### 如何构造反例

测试写入、Fsync、再次写入、丢失未刷盘状态、重启同一 Primary，再读取两个 Key。

##### 关键测试语句

```python
assert group.client_read(b"window", ReadLevel.LEADER).value is None
```

##### 失败意味着什么

异步模型把确认当成隐式持久化，或者回滚越过了命名 Fsync 边界。

<!-- journey-file: tests/protocols/test_raft.py -->
#### Raft 边界契约

##### 测试锁定什么

三个场景锁定 Timeout/Network 兼容性、落后 Follower 本地读，以及 Quorum Ack 在显式未刷盘丢失注入后的存活。

##### 如何构造反例

它拒绝小于网络往返延迟的 Election Timeout，隔离一个 Follower 做本地读，并在 Quorum Write 后 Crash Leader。

##### 关键测试语句

```python
assert group.probe().nodes[new_leader].durable_index >= result.offset
```

##### 失败意味着什么

模型允许不可能的活性时间配置、隐藏本地陈旧性，或在 Raft 稳定存储义务前确认。

### 基本概念

Staged Persistent State 对进程可见，但尚不属于 Durable Image。`fsync` 推进该镜像。普通 Process Crash 清理易失内存，不凭空制造磁盘丢失；Power-loss Fault 可以恢复最后 Fsync 镜像。Network-delay Budget 属于活性配置。Local Read 描述单节点状态，因此即使在共识协议里也可能陈旧。

### 为什么需要这个机制

跨协议实验需要相同故障词汇。如果“Crash”暗中表示不同事情，持久性比较就无效，时间故障也可能被错怪为共识逻辑而非不可能配置。

### 运行时心智模型

Failure Injector 同时保存当前 Persistent Mapping 与独立 Durable Snapshot。协议 Group 暴露兼容的 `fsync` 和 `crash(..., lose_unfsynced=...)`。异步协议在掉电时恢复 Durable Data/Offset/Lineage；Raft 在依赖它们的协议动作前已持久化 Term、Vote 与 Log，并通过 Probe 报告该边界。

### 机制板块

<!-- journey-file: src/minidist/sim/failure.py -->
#### 显式暂存态与持久态

##### 是什么，为什么现在需要

公共节点生命周期现在除当前进程可见 Persistent State 外，还拥有 Last-fsynced Snapshot。

##### 在运行时做什么

`stage_persistent` 改变当前 Mapping，`fsync` 把它复制进 Durable Image；只有显式请求的丢失才在 Crash Callback 前恢复该镜像。

##### 关键代码

```python
node.persistent.clear()
node.persistent.update(self._durable[node_id])
```

##### 关键语句理解

回滚发生在发布 Node-down 状态以前，因此观察者不会看到已崩溃节点仍拥有更新但未持久值。

<!-- journey-file: src/minidist/protocols/async_primary/group.py -->
#### 异步确认与本地 fsync

##### 是什么，为什么现在需要

每个异步节点现在分别跟踪 Durable Data、Durable Offset 与 Durable Replication Identity，而不是与 Live State 混用。

##### 在运行时做什么

显式 Fsync 快照 Live Primary；Power-loss Crash 在 Restart 与后续 Catch-up 前精确恢复该 Tuple。

##### 关键代码

```python
if lose_unfsynced:
    target.data = dict(target.durable_data)
    target.offset = target.durable_offset
```

##### 关键语句理解

Data 与 Position 必须一起回滚。只回滚字节或只回滚 Offset 都会让后续复制连续性失真。

<!-- journey-file: src/minidist/protocols/raft/group.py -->
#### Raft 稳定存储、时间预算与本地读取

##### 是什么，为什么现在需要

Raft 获得公共 Fsync/Crash Surface、Probe 中的显式 Durable Index、本地读路由，以及消息延迟几何的构造校验。

##### 在运行时做什么

Group 暴露其已有 Stable Log Boundary，并阻止无法在重复选举前完成请求/响应的 Timeout 配置。

##### 关键代码

```python
minimum_timeout = 2 * network_delay + heartbeat_interval
if low <= minimum_timeout:
    raise ValueError(
        "lowest election timeout must exceed twice the maximum network "
        "delay plus the heartbeat interval"
    )
```

##### 关键语句理解

最小 Election Timeout 必须容纳完整往返；确定性 Seed 也无法挽救不存在可行通信窗口的配置。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-m3-shared-boundaries/tests.txt)`。五条新增契约与既有套件共同区分持久性、本地可见性与活性时间几何。

### 需要真正记住的内容

Process Crash 不等于 Power Loss；Fsync 推进命名 Durable Image；Local Read 可以陈旧；Timeout 配置必须尊重 Transport Delay。

### 用自己的话讲清楚

说明一个已确认异步写入的三个状态——Primary 可见、本地持久、副本可见——以及哪种注入故障能移除哪一层。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/02-deterministic-simulation.md) · [第 7 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/07-raft-replication.md)
