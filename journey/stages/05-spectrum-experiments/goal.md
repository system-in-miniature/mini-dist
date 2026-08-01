# Stage 05 · Comparable experiments 1–3 / 可比较实验 1–3

<!-- journey: chapter=8 tests_added=5 -->

## English

### Goal

Convert the first three protocol stories into executable, assertion-friendly experiments that hold the workload constant while varying the replication mechanism.

### Deliverable files

- `labs/__init__.py`
- `labs/exp01_normal_replication.py`
- `labs/exp02_acked_write_loss.py`
- `labs/exp03_partition_old_leader.py`
- `tests/labs/test_experiments.py`

### The problem at this point

Protocol unit tests prove local invariants but do not yet provide one learner-visible comparison surface. Without shared scenarios, it is easy to compare an async happy path with a Raft failure path and attribute workload differences to the protocols.

### Test contract

#### See the failure first

Experiment 2 performs the same write, acknowledgement, immediate leader crash, failover, and read against both protocols. The async result must lose the value while the Raft result retains it. If a lab advances time on only one side, the comparison is contaminated before protocol semantics are observed.

<!-- journey-file: tests/labs/test_experiments.py -->
#### Cross-protocol observation contract

##### What this test locks

Five contracts lock delayed convergence, post-ack failover outcomes for both protocols, and Raft's minority-old-leader fencing result.

##### How it constructs the counterexample

Tests call each experiment with `verbose=False` and assert returned structured facts rather than parsing console prose.

##### Key test statement

```python
assert result.value_before_failover == b"confirmed"
assert result.value_after_failover is None
```

##### What a failure means

The scenario no longer isolates acknowledgement or authority, or the experiment reports a story inconsistent with the protocol's observable state.

### Basic concepts

An experiment has a controlled script, an observation boundary, and a structured result. The independent variable is the protocol; the workload and failure timing stay fixed. Console output is for learners, while dataclass results are the verification API. A comparison is honest only when unsupported guarantees remain visible rather than normalized away.

### Why this mechanism is necessary

The project is a protocol spectrum, not four unrelated implementations. Executable labs connect internal mechanics to externally observable differences and prevent documentation conclusions from drifting away from code.

### Runtime mental model

Each `run_experiment` constructs a deterministic group, issues only public driver calls, captures state exactly at the acknowledgement or partition boundary, advances the simulation when the script says, and returns immutable observations. Optional printing explains those same facts without becoming the source of truth.

### Mechanism blocks

<!-- journey-file: labs/exp01_normal_replication.py -->
#### Normal replication observation

##### What it is and why it appears

Experiment 1 observes a follower at the acknowledgement boundary and after convergence for async primary and Raft.

##### Runtime role

The protocol switch changes group construction and supported ack level while preserving write, observe, advance, observe.

##### Key code

```python
before = group.client_read(
    b"project", ReadLevel.LOCAL, node="replica-1"
).value
group.tick()
```

##### Statement understanding

The first read occurs before the explicit tick, so `None` is evidence of delayed visibility rather than a failed write.

<!-- journey-file: labs/exp02_acked_write_loss.py -->
#### Post-ack crash comparison

##### What it is and why it appears

Experiment 2 pins the failure immediately after acknowledgement and compares what the newly authoritative node can read.

##### Runtime role

Async promotion selects a possibly stale replica; Raft elects from a majority that already stored the committed entry.

##### Key code

```python
group.crash(old_primary)
group.promote("replica-1")
group.run_until_idle()
```

##### Statement understanding

No delivery tick occurs between acknowledgement and crash. That absence creates the async loss window and keeps the comparison causal.

<!-- journey-file: labs/exp03_partition_old_leader.py -->
#### Minority-old-leader partition

##### What it is and why it appears

Experiment 3 records the difference between local leader identity and majority-backed authority during a partition.

##### Runtime role

It writes on the isolated old leader, elects and writes on the majority side, heals, then returns role, term, log, and value evidence.

##### Key code

```python
group.heal(old_leader)
group.run_until_converged()
state = group.probe()
```

##### Statement understanding

Healing alone is not the conclusion; convergence plus a probe proves the old suffix was repaired and the majority value became shared state.

<!-- journey-file: labs/__init__.py -->
#### Experiment package scaffold

This supporting file makes the labs importable; it owns no experiment result or protocol rule.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/05-spectrum-experiments/tests.txt)`. The five contracts bind the three learner-facing experiments to real protocol observations.

### Durable takeaways

Hold the script fixed, observe at named boundaries, and return structured facts. Different outcomes are meaningful only when the experiment itself is comparable.

### Explain it in your own words

Explain why Experiment 2 is stronger evidence than separately demonstrating “async can lose data” and “Raft can survive a crash.”

### Textbook

[Chapter 8](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/08-experiments.md)

## 中文

### 目标

把前三个协议故事变成可执行、可断言的实验：保持工作负载不变，只改变复制机制。

### 交付文件

- `labs/__init__.py`
- `labs/exp01_normal_replication.py`
- `labs/exp02_acked_write_loss.py`
- `labs/exp03_partition_old_leader.py`
- `tests/labs/test_experiments.py`

### 当前遇到的问题

协议单测证明局部不变量，却还没有学习者可见的统一比较面。没有共享场景时，很容易拿异步复制正常路径对比 Raft 故障路径，再把工作负载差异误归因于协议。

### 测试契约

#### 先看会坏在哪里

实验 2 对两个协议执行相同的写入、确认、立即 Crash Leader、故障转移与读取。异步结果必须丢失值，Raft 结果必须保留值。如果 Lab 只在一侧推进时间，比较在观察协议语义前就已被污染。

<!-- journey-file: tests/labs/test_experiments.py -->
#### 跨协议观察契约

##### 测试锁定什么

五条契约锁定延迟收敛、两个协议在确认后故障转移的结果，以及 Raft 少数侧旧 Leader 的 Fencing 结果。

##### 如何构造反例

测试以 `verbose=False` 调用实验并断言返回的结构化事实，不解析控制台文案。

##### 关键测试语句

```python
assert result.value_before_failover == b"confirmed"
assert result.value_after_failover is None
```

##### 失败意味着什么

场景不再隔离确认或权威，或者实验报告的故事与协议可观察状态不一致。

### 基本概念

实验由受控脚本、观察边界与结构化结果组成。自变量是协议；工作负载和故障时机保持不变。控制台输出服务学习者，Dataclass Result 才是验证 API。只有不支持的保证仍然可见，而非被统一抹平，比较才诚实。

### 为什么需要这个机制

项目是一条协议光谱，不是四个互不相关的实现。可执行 Lab 把内部机制连接到外部差异，并防止文档结论与代码漂移。

### 运行时心智模型

每个 `run_experiment` 创建确定性 Group，只调用公开驱动，在确认或分区边界精确取样，按脚本推进仿真，再返回不可变观察。可选打印解释同一组事实，但不成为事实来源。

### 机制板块

<!-- journey-file: labs/exp01_normal_replication.py -->
#### 正常复制观察

##### 是什么，为什么现在需要

实验 1 分别在确认边界与收敛后观察异步主从和 Raft 的 Follower。

##### 在运行时做什么

Protocol Switch 改变 Group 构造与支持的 Ack Level，但保持写入、观察、推进、再观察的顺序。

##### 关键代码

```python
before = group.client_read(
    b"project", ReadLevel.LOCAL, node="replica-1"
).value
group.tick()
```

##### 关键语句理解

第一次读取发生在显式 Tick 以前，因此 `None` 是延迟可见性的证据，不是写入失败。

<!-- journey-file: labs/exp02_acked_write_loss.py -->
#### 确认后崩溃比较

##### 是什么，为什么现在需要

实验 2 把故障固定在确认之后，比较新权威节点能读到什么。

##### 在运行时做什么

异步 Promotion 选择可能落后的 Replica；Raft 从已经保存已提交 Entry 的多数派中选举。

##### 关键代码

```python
group.crash(old_primary)
group.promote("replica-1")
group.run_until_idle()
```

##### 关键语句理解

确认与 Crash 之间没有 Delivery Tick。正是这段缺失制造异步丢失窗口并保持比较的因果性。

<!-- journey-file: labs/exp03_partition_old_leader.py -->
#### 少数侧旧 Leader 分区

##### 是什么，为什么现在需要

实验 3 记录分区期间本地 Leader 身份与多数派权威之间的差别。

##### 在运行时做什么

它在被隔离旧 Leader 上写入，在多数侧选举并写入，恢复网络后返回 Role、Term、Log 与 Value 证据。

##### 关键代码

```python
group.heal(old_leader)
group.run_until_converged()
state = group.probe()
```

##### 关键语句理解

仅仅 Heal 不是结论；收敛再 Probe 才能证明旧后缀已修复且多数侧值成为共享状态。

<!-- journey-file: labs/__init__.py -->
#### 实验包脚手架

这个支撑文件让 Labs 可导入，不拥有实验结果或协议规则。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-spectrum-experiments/tests.txt)`。五条契约把三个学习者可见实验绑定到真实协议观察。

### 需要真正记住的内容

固定脚本，在命名边界观察，并返回结构化事实。只有实验本身可比较，不同结果才有意义。

### 用自己的话讲清楚

说明为什么实验 2 比分别演示“异步会丢数据”和“Raft 能抗崩溃”提供更强证据。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/08-experiments.md)
