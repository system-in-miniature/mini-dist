# Stage 02 · Replication comparison contract

### Goal

Define one honest driver vocabulary for comparing protocols without silently weakening unsupported guarantees.

??? note "Deliverable files"
    - `src/minidist/protocols/__init__.py`
    - `src/minidist/protocols/types.py`
    - `tests/test_public_types.py`

### The problem at this point

The simulator can replay faults, but there is no stable way to ask different protocols for a write acknowledgement or a read strength. Reusing the same word while changing its meaning would make cross-protocol experiments incomparable.

### Test contract

#### See the failure first

The public contract enumerates every acknowledgement and read level in a fixed order and requires explicit result shapes. If a protocol later treats `QUORUM` as `LEADER` or a linearizable read as an arbitrary local read, it must reject the request rather than return a plausible weaker result.

??? note "File diff: tests/test_public_types.py"
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

**What this test locks**

It locks the closed Ack/Read vocabularies, result fields, and the dedicated unsupported-level failure type.

**How it constructs the counterexample**

It inspects enum membership and constructs result values before any protocol implementation exists.

**Key test statement**

```python
assert issubclass(UnsupportedLevelError, ValueError)
```

**What a failure means**

Callers cannot distinguish an invalid guarantee request from a normal protocol outcome.

### Basic concepts

An acknowledgement level says what replication work a client waits for; a read level says where and with what consistency a read may execute. These are comparison inputs, not universal promises. `ReplicationGroup` is a structural driver protocol: implementations share call shapes while retaining different supported subsets.

### Why this mechanism is necessary

Side-by-side experiments need comparable questions and honest refusal. A shared API that silently maps unsupported guarantees to weaker behavior would make the matrix look uniform by deleting the very differences the project teaches.

### Runtime mental model

The experiment selects an `AckLevel` or `ReadLevel`, invokes one `ReplicationGroup`, and receives an immutable result naming acceptance, offset, value, and serving node. The concrete group validates whether that level has meaning for its protocol.

### Mechanism blocks

#### Explicit comparison vocabulary

Define acknowledgement and read-strength requests plus stable results without claiming every protocol supports every level.

??? note "File diff: src/minidist/protocols/types.py"
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

??? note "File diff: src/minidist/protocols/__init__.py"
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

**What it is and why it appears**

This module defines the public requests, results, node identity, and minimal driver surface used by every later experiment.

**Runtime role**

Concrete protocols implement the same write/read/tick/lifecycle/probe shape, then reject levels outside their real guarantee set.

**Key code**

```python
class ReplicationGroup(Protocol):
    """Minimal common driver surface used by cross-protocol experiments."""
```

**Statement understanding**

The protocol type standardizes observation, not implementation. It does not give an asynchronous primary quorum semantics merely because the enum contains `QUORUM`.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/02-replication-contract/tests.txt)`. It proves the common language; concrete rejection behavior begins in Stage 03.

### Durable takeaways

A fair comparison holds the question stable and lets supported guarantees differ. Unsupported strength must fail explicitly.

### Explain it in your own words

Explain why sharing an enum does not imply that every protocol implements every enum member.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/03-replication-group.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-dist/compare/stage-01...stage-02)

After finishing, use `git checkout stage-02` to compare your result.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/02-replication-contract/stage.patch)
