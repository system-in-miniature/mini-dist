# 第 3 章——复制组抽象

[English](../../tutorial/03-replication-group.md)

若两种协议面对的问题不同，对比就会误导。MiniDist 用一套很小的 `ReplicationGroup` 词汇避免这个问题：写、读、推进、故障、重启和观察。抽象不会强迫每种协议宣称每项保证；它最重要的行为往往是拒绝。

## 学习目标

完成本章后，你将能够：

1. 阅读 `ReplicationGroup` protocol，并把操作分为客户端、生命周期、推进与观察；
2. 把每个 `AckLevel` 和 `ReadLevel` 解释成实验请求，而不是通用承诺；
3. 预测 async-primary 与 Raft 接受哪些 level；
4. 使用 `WriteResult`、`ReadResult` 和不可变 probe 快照，而不依赖私有字段；以及
5. 说明为什么显式 `UnsupportedLevelError` 是正确性功能。

## 一个驱动表面，多种含义

`ReplicationGroup` 是 `src/minidist/protocols/types.py` 中的结构化 `Protocol`。符合它的对象提供 `client_write`、`client_read`、`tick`、`crash`、`restart` 与 `probe`。

| 角色 | 公开操作 | 问题 |
|---|---|---|
| 客户端 | `client_write`、`client_read` | 这项保证请求产生什么结果？ |
| 推进 | `tick` | 一个逻辑步骤后什么成为可能？ |
| 生命周期 | `crash`、`restart` | 节点转换后什么留下？ |
| 观察 | `probe` | 实验可以断言哪些只读事实？ |

具体 group 可按机制增加公开实验操作。`AsyncPrimaryGroup` 增加 `promote`、`isolate`、`heal` 与 `run_until_idle`；`RaftGroup` 增加面向选举和收敛的辅助方法。它们是实验控制面，不是假装成隐藏的线协议。

结构化 protocol 没有基类实现。async 与 Raft 无需继承一套会模糊差异的共享语义；实验依赖行为和显式结果对象，而不是继承树。

## 确认级别问“成功前发生了什么？”

`src/minidist/protocols/types.py` 中的 `AckLevel` 有四项：

- `NONE`：不请求等待复制；
- `LEADER`：请求当前 leader/primary 边界；
- `QUORUM`：请求多数派复制边界；
- `ALL_ISR`：请求同步副本集合中的全部成员。

名字本身不够。对于异步协议，`AsyncPrimaryGroup.client_write` 接受 `NONE` 与 `LEADER`。两条路径都会在 primary 内存字典中应用 KV、递增流 offset、入队消息并返回，不等待 replica。源码注释精确说明：此处 `LEADER` 是“已由该 primary 应用”，不是共识或耐久性。

Raft 不同。`src/minidist/protocols/raft/group.py` 的 `RaftGroup.client_write` 拒绝 `NONE`、`LEADER` 和 `ALL_ISR`，只接受 `QUORUM`。它追加日志条目，直到条目达到 Raft commit 边界才返回 `accepted=True`。所以 `LEADER` 不是可移植安全单位，`ALL_ISR` 也不描述 Raft 成员模型。

因此，必须逐实现检查枚举支持。只有同时比较请求的确认边界与实现的边界，才能比较两次成功写。

## 读取级别同时问“在哪里？”和“凭什么？”

`ReadLevel` 有三项：

- `LOCAL`：读取指定节点，或实现默认的本地节点；
- `LEADER`：路由到当前选择的 leader/primary；
- `LINEARIZABLE`：要求证明服务节点仍有足够权威，能够线性化读取。

`AsyncPrimaryGroup.client_read` 接受 `LOCAL` 与 `LEADER`。指定 replica 的 `LOCAL` 可以有意观察落后值；`LEADER` 路由到 `self._primary`，但不联系 replica 或建立 quorum；`LINEARIZABLE` 抛出 `UnsupportedLevelError`。

`RaftGroup.client_read` 接受三个 level。`LOCAL` 专门读取指定节点，让实验 6
暴露 stale follower，不声明任何权威；leader read 只路由到已知 leader，本身
不构成 quorum 证明；linearizable 路径执行简化的当前任期 heartbeat barrier，
并要求多数派成功响应。[Raft 映射](../mapping.md#协议-4-与-raft-论文映射)把直接
leader read 标为与可能推断出的保证“语义相反”，把 read-index barrier 标为
“有意简化”。

因此，词汇阻止了常见错误：把“从 leader 读”等同于“线性化读”。位置与一致性证据是不同维度。

## 结果携带复制位置

`WriteResult` 是不可变 dataclass，包含 `accepted`、`offset` 和可选 message；`ReadResult` 包含 `value`、服务 `node` 和它的 `offset`。它们都位于 `src/minidist/protocols/types.py`。

offset 必须结合上下文解释：

- async 中它是单调复制流 offset；
- Raft 中它是日志/提交索引；
- 失败的 Raft quorum 写可返回 `accepted=False` 和本地尝试的日志索引。

异步路径的人类可读消息 `primary accepted; replica delivery is asynchronous` 是提醒，不是证明。证明来自 `AsyncPrimaryGroup.client_write` 的顺序：本地应用、发送条目、记录确认，在推进 scheduler 前返回。

`probe` 提供更丰富的断言快照。`AsyncPrimaryGroup.probe` 复制每个可变数据字典，以 `MappingProxyType` 包裹，再返回包含角色、复制 ID、offset、sync mode、当前 tick 和 backlog offsets 的 `NodeState`/`GroupState`。Raft 同样返回任期、角色、日志、commit index 和已应用数据的值快照。

这种观察是白盒但只读的：机制可以测试，实验却不能通过改私有字段制造结果。

## “不支持”也是实验结果

假设通用 benchmark 向每个 group 请求 `AckLevel.QUORUM`，并把失败静默映射成各自最强模式。异步 group 会在本地 primary 边界返回成功，却被贴上 majority 标签。表格看似整齐，结论却是假的。

继承自 `ValueError` 的 `UnsupportedLevelError` 阻止了这个错误。`AsyncPrimaryGroup.client_write` 在改状态前检查 level；`AsyncPrimaryGroup.client_read` 也在选节点前拒绝 `LINEARIZABLE`。Raft 实现独立检查自己的支持集合。

下列测试固定了契约：

- `tests/protocols/test_async_primary.py::test_async_primary_rejects_stronger_ack_levels`；
- `tests/protocols/test_async_primary.py::test_async_primary_rejects_linearizable_reads`；
- `tests/protocols/test_raft.py::test_raft_rejects_non_quorum_ack_levels`；
- `tests/test_public_types.py::test_replication_api_levels_and_results_are_explicit`。

这里的异常不是缺少打磨，而是防止语义扁平化的数据点。

## 动手实验：建立支持矩阵

运行：

```bash
uv run python -c 'from minidist.protocols import *; from minidist.protocols.async_primary import AsyncPrimaryGroup
g=AsyncPrimaryGroup()
for ack in (AckLevel.QUORUM,AckLevel.ALL_ISR):
 try:g.client_write(b"k",b"v",ack)
 except UnsupportedLevelError as e:print(e)
try:g.client_read(b"k",ReadLevel.LINEARIZABLE)
except UnsupportedLevelError as e:print(e)'
```

本仓实测：

```text
async primary does not support AckLevel.QUORUM
async primary does not support AckLevel.ALL_ISR
async primary does not support ReadLevel.LINEARIZABLE
```

被拒绝的写不会修改 primary，因为校验发生在 offset 递增前。可用 `group.probe()` 验证。

继续运行契约测试：

```bash
uv run pytest -q tests/test_public_types.py tests/protocols/test_async_primary.py
```

写作时仓库状态的实测输出：

```text
.............                                                            [100%]
13 passed in 0.02s
```

测试耗时随机器变化，不是协议指标；有意义的是测试数和零失败。

## 对照真实系统 API

共享 level 是教学词汇，不是 Redis、PostgreSQL、Kafka 与 Raft 共同实现的标准 API。

Redis 普通写通常在 primary 处理命令后返回。`WAIT` 可以等待 replica 确认，但[Redis 行为映射](../mapping.md#协议机制与真实系统复制机制映射)明确指出，它不会把 Redis 变成强一致 quorum log。MiniDist async group 完全省略 `WAIT` 并拒绝 `QUORUM`。Kafka 的 `acks=all` 作用于 ISR，并与 `min.insync.replicas` 联动；第 10 章实现该有界教学路径。PostgreSQL 同步提交则由第 9 章映射为等待全部已配置同步 standby，不是 Raft 多数派。

Kafka 的 `acks=all` 根据 ISR 判断，并与 `min.insync.replicas` 交互；MiniDist 只预留 `ALL_ISR` 词汇，尚无 ISR 实现。PostgreSQL 的同步提交模式涉及 WAL 耐久性与 standby 选择，不能缩减为本枚举。Raft commit 是任期规则下的多数派日志复制，没有 ISR。

读取控制同样不同。Redis replica read 可以陈旧；Raft 线性化读需要领导权/quorum 论证。生产系统还增加会话、重试、去重、超时、拓扑发现、认证与线协议错误，MiniDist 结果 dataclass 都未建模。

解释 level 前请看[实验矩阵](../experiments.md#复制协议实验矩阵)。四种实现与实验 1–7 都已有断言单元格，但接口词汇本身仍不是实现证据。

## 练习

### 理解题

1. 自动把 `QUORUM` 降为 `LEADER` 为什么使对比失效？
2. `ReadLevel.LEADER` 与 `ReadLevel.LINEARIZABLE` 有什么区别？

??? note "参考答案"

    1. 调用方问的是成功前是否达到多数派边界。本地 primary 应用回答不了，降级后却会报告比机制更强的保证。
    2. `LEADER` 选择位置；`LINEARIZABLE` 还要求当前权威证据，本仓 Raft 以简化 quorum heartbeat barrier 实现。

### 动手题

3. 写临时测试，证明被拒绝的 async `QUORUM` 写不会改变 primary offset 和 data。

   **验收：**运行 `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch03.py`，必须得到 `1 passed`，且仓库源码不变。

??? note "参考答案"

    ```python
    import pytest
    from minidist.protocols import AckLevel, UnsupportedLevelError
    from minidist.protocols.async_primary import AsyncPrimaryGroup

    def test_rejected_level_has_no_side_effect():
        group = AsyncPrimaryGroup()
        before = group.probe().nodes[group.primary]
        with pytest.raises(UnsupportedLevelError):
            group.client_write(b"k", b"v", AckLevel.QUORUM)
        after = group.probe().nodes[group.primary]
        assert after.offset == before.offset == 0
        assert dict(after.data) == dict(before.data) == {}
    ```

4. 制作 Markdown 表，列出 async-primary 与 Raft 对全部 4 个 ack level 和 3 个 read level 的支持。每个格子必须来自源码分支或测试，不能根据名字推断。

   **验收：**每个支持格写出具体方法；每个拒绝格写出 `UnsupportedLevelError`。async 应接受 `NONE/LEADER` 写和 `LOCAL/LEADER` 读；Raft 应接受 `QUORUM` 写和 `LEADER/LINEARIZABLE` 读。

??? note "参考答案"

    | 协议 | 写级别 | 读级别 |
    |---|---|---|
    | Async primary | `NONE`、`LEADER`；拒绝 `QUORUM`、`ALL_ISR` | `LOCAL`、`LEADER`；拒绝 `LINEARIZABLE` |
    | Raft | `QUORUM`；拒绝 `NONE`、`LEADER`、`ALL_ISR` | `LOCAL`、`LEADER`、`LINEARIZABLE`；`LOCAL` 只用于观察 |

## 小结

`ReplicationGroup` 统一问题，而不统一答案。显式 level 使确认与读取请求可比较；不可变结果与 probe 使观察可检查；有意抛出的异常防止不支持的保证混入图表。有了固定词汇，第 4 章将分析第一个完整协议：异步 primary、replica sink、流 offset、有界部分重同步和全量回退。
