# Stage 02 · 复制比较契约

### 目标

定义一套诚实的协议比较驱动词汇，不把不支持的保证静默降级。

??? note "交付文件"
    - `src/minidist/protocols/__init__.py`
    - `src/minidist/protocols/types.py`
    - `tests/test_public_types.py`

### 当前遇到的问题

仿真器已经能重放故障，但还没有稳定方式向不同协议请求写确认或读取强度。如果复用同一个词却偷偷改变含义，跨协议实验就无法比较。

### 测试契约

#### 先看会坏在哪里

公共契约以固定顺序枚举所有 Ack 与 Read Level，并要求显式结果形状。后续协议若想把 `QUORUM` 当成 `LEADER`，或把线性一致读当作任意本地读，必须拒绝，而不是返回看似合理的弱结果。

??? note "文件差异：tests/test_public_types.py"
    ```diff
    diff --git a/tests/test_public_types.py b/tests/test_public_types.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..dff9d4b24c049e80d0a4d03c4ccecda54f717742
    --- /dev/null
    +++ b/tests/test_public_types.py
    @@ -0,0 +1,24 @@
    +from minidist.protocols import (
    +    AckLevel,
    +    ReadLevel,
    +    ReadResult,
    +    UnsupportedLevelError,
    +    WriteResult,
    +)
    +
    +
    +def test_replication_api_levels_and_results_are_explicit() -> None:
    +    assert [level.name for level in AckLevel] == [
    +        "NONE",
    +        "LEADER",
    +        "QUORUM",
    +        "ALL_ISR",
    +    ]
    +    assert [level.name for level in ReadLevel] == [
    +        "LOCAL",
    +        "LEADER",
    +        "LINEARIZABLE",
    +    ]
    +    assert WriteResult(accepted=True, offset=3).offset == 3
    +    assert ReadResult(value=b"value", node="n1", offset=2).value == b"value"
    +    assert issubclass(UnsupportedLevelError, ValueError)
    ```

**测试锁定什么**

锁定封闭的 Ack/Read 词汇、结果字段与专用 Unsupported-level Failure 类型。

**如何构造反例**

在任何协议实现出现以前，直接检查 Enum 成员并构造 Result Value。

**关键测试语句**

```python
assert issubclass(UnsupportedLevelError, ValueError)
```

**失败意味着什么**

调用方无法区分无效保证请求与正常协议结果。

### 基本概念

Ack Level 表示客户端等待哪些复制工作；Read Level 表示读取可以在哪里、以何种一致性执行。它们是比较输入，不是通用承诺。`ReplicationGroup` 是结构化驱动协议：实现共享调用形状，但保留不同的支持子集。

### 为什么需要这个机制

并排实验需要可比较的问题与诚实拒绝。如果共享 API 把不支持的保证映射成弱行为，就会通过删除项目要教授的差异来制造表面统一。

### 运行时心智模型

实验选择 `AckLevel` 或 `ReadLevel`，调用一个 `ReplicationGroup`，得到不可变结果，其中说明 Acceptance、Offset、Value 与 Serving Node。具体 Group 负责校验该 Level 对自身协议是否有意义。

### 机制板块

#### 显式比较词汇

定义确认级别、读取强度与稳定结果，同时不声称每个协议都支持全部级别。

??? note "文件差异：src/minidist/protocols/types.py"
    ```diff
    diff --git a/src/minidist/protocols/types.py b/src/minidist/protocols/types.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..44861a3c2e85fdc5692518ff96d83dcfc9353b89
    --- /dev/null
    +++ b/src/minidist/protocols/types.py
    @@ -0,0 +1,73 @@
    +"""Shared vocabulary for comparing replication protocols.
    +
    +Real systems attach different guarantees to similarly named knobs.  These
    +enums provide a common experiment vocabulary without pretending every
    +protocol supports every level.  Implementations must reject unsupported
    +levels rather than silently weakening the requested guarantee.
    +"""
    +
    +from dataclasses import dataclass
    +from enum import Enum, auto
    +from typing import Protocol, TypeAlias
    +
    +NodeId: TypeAlias = str
    +
    +
    +class AckLevel(Enum):
    +    """How much replication work a client asks to wait for."""
    +
    +    NONE = auto()
    +    LEADER = auto()
    +    QUORUM = auto()
    +    ALL_ISR = auto()
    +
    +
    +class ReadLevel(Enum):
    +    """Where a read may run and what consistency it asks for."""
    +
    +    LOCAL = auto()
    +    LEADER = auto()
    +    LINEARIZABLE = auto()
    +
    +
    +class UnsupportedLevelError(ValueError):
    +    """Raised instead of silently pretending a protocol is stronger."""
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class WriteResult:
    +    """Public result of a write accepted by a replication group."""
    +
    +    accepted: bool
    +    offset: int
    +    message: str = ""
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ReadResult:
    +    """A value together with the node and replication point that served it."""
    +
    +    value: bytes | None
    +    node: NodeId
    +    offset: int
    +
    +
    +class ReplicationGroup(Protocol):
    +    """Minimal common driver surface used by cross-protocol experiments."""
    +
    +    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult: ...
    +
    +    def client_read(
    +        self,
    +        key: bytes,
    +        level: ReadLevel,
    +        node: NodeId | None = None,
    +    ) -> ReadResult: ...
    +
    +    def tick(self) -> None: ...
    +
    +    def crash(self, node: NodeId) -> None: ...
    +
    +    def restart(self, node: NodeId) -> None: ...
    +
    +    def probe(self) -> object: ...
    ```

??? note "文件差异：src/minidist/protocols/__init__.py"
    ```diff
    diff --git a/src/minidist/protocols/__init__.py b/src/minidist/protocols/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c1a94a2dc02bbce84c519db2d4087c86c99ebb4e
    --- /dev/null
    +++ b/src/minidist/protocols/__init__.py
    @@ -0,0 +1,21 @@
    +"""Public comparison interface shared by MiniDist protocol implementations."""
    +
    +from .types import (
    +    AckLevel,
    +    NodeId,
    +    ReadLevel,
    +    ReadResult,
    +    ReplicationGroup,
    +    UnsupportedLevelError,
    +    WriteResult,
    +)
    +
    +__all__ = [
    +    "AckLevel",
    +    "NodeId",
    +    "ReadLevel",
    +    "ReadResult",
    +    "ReplicationGroup",
    +    "UnsupportedLevelError",
    +    "WriteResult",
    +]
    ```

**是什么，为什么现在需要**

该模块定义所有后续实验使用的公共请求、结果、Node Identity 与最小驱动 Surface。

**在运行时做什么**

具体协议实现相同的 Write/Read/Tick/Lifecycle/Probe 形状，再拒绝真实保证集合之外的 Level。

**关键代码**

```python
class ReplicationGroup(Protocol):
    """Minimal common driver surface used by cross-protocol experiments."""
```

**关键语句理解**

Protocol Type 统一的是观察方式，不是实现。Enum 包含 `QUORUM`，不会因此赋予异步 Primary Quorum 语义。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-replication-contract/tests.txt)`。它证明共同语言；具体拒绝行为从 Stage 03 开始。

### 需要真正记住的内容

公平比较要固定问题，并允许支持的保证不同。不支持的强度必须显式失败。

### 用自己的话讲清楚

解释为什么共享一个 Enum 并不意味着每个协议都实现所有 Enum Member。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/03-replication-group.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-01...stage-02)

完成后可运行 `git checkout stage-02` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/02-replication-contract/stage.patch)
