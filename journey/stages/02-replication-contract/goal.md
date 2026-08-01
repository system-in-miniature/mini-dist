# Stage 02 · Replication comparison contract / 复制比较契约

<!-- journey: chapter=3 tests_added=1 -->

## English

### Goal

Define one honest driver vocabulary for comparing protocols without silently weakening unsupported guarantees.

### Deliverable files

- `src/minidist/protocols/__init__.py`
- `src/minidist/protocols/types.py`
- `tests/test_public_types.py`

### The problem at this point

The simulator can replay faults, but there is no stable way to ask different protocols for a write acknowledgement or a read strength. Reusing the same word while changing its meaning would make cross-protocol experiments incomparable.

### Test contract

#### See the failure first

The public contract enumerates every acknowledgement and read level in a fixed order and requires explicit result shapes. If a protocol later treats `QUORUM` as `LEADER` or a linearizable read as an arbitrary local read, it must reject the request rather than return a plausible weaker result.

<!-- journey-file: tests/test_public_types.py -->
#### Public comparison vocabulary contract

##### What this test locks

It locks the closed Ack/Read vocabularies, result fields, and the dedicated unsupported-level failure type.

##### How it constructs the counterexample

It inspects enum membership and constructs result values before any protocol implementation exists.

##### Key test statement

```python
assert issubclass(UnsupportedLevelError, ValueError)
```

##### What a failure means

Callers cannot distinguish an invalid guarantee request from a normal protocol outcome.

### Basic concepts

An acknowledgement level says what replication work a client waits for; a read level says where and with what consistency a read may execute. These are comparison inputs, not universal promises. `ReplicationGroup` is a structural driver protocol: implementations share call shapes while retaining different supported subsets.

### Why this mechanism is necessary

Side-by-side experiments need comparable questions and honest refusal. A shared API that silently maps unsupported guarantees to weaker behavior would make the matrix look uniform by deleting the very differences the project teaches.

### Runtime mental model

The experiment selects an `AckLevel` or `ReadLevel`, invokes one `ReplicationGroup`, and receives an immutable result naming acceptance, offset, value, and serving node. The concrete group validates whether that level has meaning for its protocol.

### Mechanism blocks

<!-- journey-file: src/minidist/protocols/types.py -->
<!-- journey-file: src/minidist/protocols/__init__.py -->
#### Explicit comparison vocabulary

##### What it is and why it appears

This module defines the public requests, results, node identity, and minimal driver surface used by every later experiment.

##### Runtime role

Concrete protocols implement the same write/read/tick/lifecycle/probe shape, then reject levels outside their real guarantee set.

##### Key code

```python
class ReplicationGroup(Protocol):
    """Minimal common driver surface used by cross-protocol experiments."""
```

##### Statement understanding

The protocol type standardizes observation, not implementation. It does not give an asynchronous primary quorum semantics merely because the enum contains `QUORUM`.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/02-replication-contract/tests.txt)`. It proves the common language; concrete rejection behavior begins in Stage 03.

### Durable takeaways

A fair comparison holds the question stable and lets supported guarantees differ. Unsupported strength must fail explicitly.

### Explain it in your own words

Explain why sharing an enum does not imply that every protocol implements every enum member.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/03-replication-group.md)

## 中文

### 目标

定义一套诚实的协议比较驱动词汇，不把不支持的保证静默降级。

### 交付文件

- `src/minidist/protocols/__init__.py`
- `src/minidist/protocols/types.py`
- `tests/test_public_types.py`

### 当前遇到的问题

仿真器已经能重放故障，但还没有稳定方式向不同协议请求写确认或读取强度。如果复用同一个词却偷偷改变含义，跨协议实验就无法比较。

### 测试契约

#### 先看会坏在哪里

公共契约以固定顺序枚举所有 Ack 与 Read Level，并要求显式结果形状。后续协议若想把 `QUORUM` 当成 `LEADER`，或把线性一致读当作任意本地读，必须拒绝，而不是返回看似合理的弱结果。

<!-- journey-file: tests/test_public_types.py -->
#### 公共比较词汇契约

##### 测试锁定什么

锁定封闭的 Ack/Read 词汇、结果字段与专用 Unsupported-level Failure 类型。

##### 如何构造反例

在任何协议实现出现以前，直接检查 Enum 成员并构造 Result Value。

##### 关键测试语句

```python
assert issubclass(UnsupportedLevelError, ValueError)
```

##### 失败意味着什么

调用方无法区分无效保证请求与正常协议结果。

### 基本概念

Ack Level 表示客户端等待哪些复制工作；Read Level 表示读取可以在哪里、以何种一致性执行。它们是比较输入，不是通用承诺。`ReplicationGroup` 是结构化驱动协议：实现共享调用形状，但保留不同的支持子集。

### 为什么需要这个机制

并排实验需要可比较的问题与诚实拒绝。如果共享 API 把不支持的保证映射成弱行为，就会通过删除项目要教授的差异来制造表面统一。

### 运行时心智模型

实验选择 `AckLevel` 或 `ReadLevel`，调用一个 `ReplicationGroup`，得到不可变结果，其中说明 Acceptance、Offset、Value 与 Serving Node。具体 Group 负责校验该 Level 对自身协议是否有意义。

### 机制板块

<!-- journey-file: src/minidist/protocols/types.py -->
<!-- journey-file: src/minidist/protocols/__init__.py -->
#### 显式比较词汇

##### 是什么，为什么现在需要

该模块定义所有后续实验使用的公共请求、结果、Node Identity 与最小驱动 Surface。

##### 在运行时做什么

具体协议实现相同的 Write/Read/Tick/Lifecycle/Probe 形状，再拒绝真实保证集合之外的 Level。

##### 关键代码

```python
class ReplicationGroup(Protocol):
    """Minimal common driver surface used by cross-protocol experiments."""
```

##### 关键语句理解

Protocol Type 统一的是观察方式，不是实现。Enum 包含 `QUORUM`，不会因此赋予异步 Primary Quorum 语义。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-replication-contract/tests.txt)`。它证明共同语言；具体拒绝行为从 Stage 03 开始。

### 需要真正记住的内容

公平比较要固定问题，并允许支持的保证不同。不支持的强度必须显式失败。

### 用自己的话讲清楚

解释为什么共享一个 Enum 并不意味着每个协议都实现所有 Enum Member。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/03-replication-group.md)
