# 第 5 章：换主与世代栅栏

故障转移并不只是把“副本”标签改成“主节点”。它改变了哪一段历史具有
权威。本章沿着 MiniDist 中的一次提升操作，说明为什么旧历史里延迟到达的
数据包绝不能覆盖新历史。

## 学习目标

完成本章后，你将能够：

- 解释为什么异步复制中已经确认的写入仍可能在换主后消失；
- 从拓扑变更一路跟踪 `promote()` 对 generation 与 lineage 的修改；
- 区分世代栅栏与复制 offset 的顺序检查；
- 预测为什么排队中的旧 full sync 或 stream entry 会被拒绝；
- 使用公开的 probe 与 trace 验证提升后的主动收敛。

## 1. 换主问题本质上是历史问题

异步主节点在本地应用写入后就返回确认。在
`src/minidist/protocols/async_primary/group.py` 中，
`AsyncPrimaryGroup.client_write()` 先增加主节点 offset、修改字典、把
`_Entry` 放进有界 backlog，再为每个副本调度一条消息。它不会在返回前推进
scheduler：

```python
primary.offset += 1
primary.data[key] = value
self._backlog.append(entry)
for node_id in self._nodes:
    if node_id != self._primary:
        self._send_entry(self._primary, node_id, entry, "stream")
```

关键边界不只是 Python 语句的先后顺序。消息投递属于确定性 scheduler，要到
后续 tick 才发生。因此成功的 `AckLevel.LEADER` 只证明“这个主节点应用了
该值”，不能证明“其他节点已经拥有它”“它已经持久化”，也不能证明“未来的
主节点一定包含它”。这正是“已确认写丢失”实验背后的机制。

假设主节点 offset 为 5，副本为 4。如果 entry 5 到达前主节点失败，提升该
副本会产生一段不包含 entry 5 的权威历史。只比较数值 offset 无法保证 entry
5 安全：一段历史里的 offset 5 可以与另一段历史里的 offset 5 代表完全不同
的命令。复制位置只有放在线性谱系中才有意义。

MiniDist 在每条复制消息中使用两个字段表示这个身份：

- `generation`：复制组的手动提升 epoch；
- `replication_id`：当前主节点数据流的标识符。

Offset 只在这个身份内部排序。Generation 与 replication ID 决定这个身份是否
仍有权威。

## 2. 提升会开启新谱系

阅读 `src/minidist/protocols/async_primary/group.py` 中的
`AsyncPrimaryGroup.promote()`。它的状态转换可以准确拆成六步：

1. 验证候选节点存在、存活且尚不是主节点；
2. 把 `_primary` 指向候选节点；
3. 增加 `_generation`；
4. 通过 `_new_replication_id()` 给候选节点分配新 ID；
5. 清空 backlog；
6. 对其他所有存活节点启动 `_synchronize()`。

清空 backlog 不是普通清理。里面的 entry 属于旧的
`(generation, replication_id)`。如果把它们放在候选节点的新 ID 下继续发布，
就等于错误宣称两段历史连续。MiniDist 选择更保守的规则：ID 不同时执行 full
resynchronization。

新主节点会保留现有数据与数值 offset。这让丢失窗口保持可观察：提升不会凭空
补出缺失写入。这个调用也不会“选举”候选节点；它是实验显式提供的控制面操作。
数据面只负责执行该决策的后果。

最后，提升是**主动的**。即使之后没有任何客户端写入，它仍立即对所有存活副本
调用 `_synchronize()`。在 `AsyncPrimaryGroup._synchronize()` 中，只有副本
与主节点 ID 相同，且副本 offset 仍被 backlog 覆盖时，才能部分同步。提升刚
完成时 ID 不同，因此通常发送 `full_sync`，其中包含新主节点的完整字典、
offset、generation 与 ID。

## 3. 消息到达时才执行栅栏检查

仅修改当前主节点还不够，因为 scheduler 中已经排队的消息仍然存在。决定性
守卫是 `src/minidist/protocols/async_primary/group.py` 中的
`AsyncPrimaryGroup._replication_rejection_reason()`。`_receive()` 在应用
entry 或 full sync 前先检查：

```python
if message.source != self._primary or ...:
    return "non_current_primary"
if payload.get("generation") != self._generation:
    return "stale_generation"
if payload.get("replication_id") != primary.replication_id:
    return "stale_replication_id"
```

三项检查针对不同故障形态：

- **当前所有者**：旧主节点实际发出的消息会被拒绝，即使 payload 声称别的来源；
- **世代**：来自同一节点身份的旧消息也不能跨越后续提升；
- **复制 ID**：废弃数据流中的消息不能混入当前数据流。

全部通过后，`_receive()` 才会调用 `_apply_entry()` 或
`_apply_full_sync()`。随后 `_apply_entry()` 执行谱系内部规则：收到的 offset
必须恰好等于 `target.offset + 1`。发现缺口时会触发 `_synchronize()`，而不
是在未知前缀上直接应用后缀。

这也解释了为什么排队中的旧 full sync 尤其危险，以及为什么必须把它挡在
栅栏外。`AsyncPrimaryGroup._apply_full_sync()` 会替换目标节点的整个字典。
如果只检查 offset，旧主节点迟到的快照就可能擦除新主节点已接受的写入。先检查
权威再分派消息，阻止了这种状态替换。

拒绝是可观察的，不是静默行为。`_receive()` 会记录
`replication_message_rejected` trace event，包括消息类型、来源、generation、
replication ID 与原因。因此测试和实验可以验证具体触发了哪道栅栏。

## 4. 愈合后的主动收敛

`AsyncPrimaryGroup.heal()` 会修复某个节点与所有节点间的双向链路。如果该节点
此时是副本，它会立即调用 `_synchronize()`。这在双主实验后很关键：

1. 隔离旧主节点；
2. 在旧侧接受一条本地脏写；
3. 手动提升一个副本，并在新侧接受另一条写；
4. 愈合旧主节点。

愈合后的旧节点属于新 generation 的副本。它的 ID 不匹配，因此会收到 full
sync，用新主节点状态替换旧侧脏状态。收敛是确定性的，但它**不会**合并两侧；
旧侧脏写会消失。栅栏保护选定的权威，不负责解决应用层冲突。

`AsyncPrimaryGroup.probe()` 暴露节点数据、角色、offset、谱系与同步模式的
不可变副本。这是实验接口，不是管理协议。可以用它断言结果，而无需修改私有状态。

## 5. 与真实 Redis 对照

仓库的[行为映射与 DIFFERENCES 清单](../mapping.md)明确分类了
这一边界。Redis 使用 replication ID 与 offset 完成部分同步。可以通过
`REPLICAOF NO ONE` 请求提升，也可以由 Sentinel/Cluster 协调。Sentinel 和
Redis Cluster 还有控制面 epoch，用于限制过期所有权。

MiniDist 有意压缩了这些层次：

- `promote()` 是手动调用；没有故障检测器、Sentinel 法定人数、副本选择策略或
  自动选举；
- 只保留一个当前 replication ID，而 Redis 可以保留 secondary replication
  ID，帮助副本跨越提升继续部分同步；
- full sync 在一个仿真事件里原子复制 Python 字典；真实 Redis 会创建、传输、
  加载 RDB，并继续流式传递缓冲命令；
- 节点 crash 保留教学数据集并清理进程内控制状态；真实重启数据取决于 RDB/AOF
  配置；
- 没有 `WAIT`、磁盘 fsync 模型、RESP 传输、Cluster slot 所有权或 Sentinel
  配置交换。

等价声明很窄，却很有用：异步确认可以先于副本投递；谱系与 offset 共同标识
复制位置；过期复制流量不能跨越权威变更。MiniDist generation 是教学 epoch，
并不是完整的 Redis Sentinel 或 Cluster 协议。

## 6. 动手实验：捕获旧消息

在仓库根目录运行：

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup

g = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
g.client_write(b"old", b"dirty", AckLevel.LEADER)
g.promote("r1")
g.run_until_idle()
s = g.probe()
rejected = [e.details["reason"] for e in g.trace.events
            if e.kind == "replication_message_rejected"]
print("primary:", s.primary)
print("r1 lineage:", s.nodes["r1"].replication_id)
print("old data on r1:", s.nodes["r1"].data.get(b"old"))
print("rejections:", rejected)
PY
```

实测输出：

```text
primary: r1
r1 lineage: repl-2
old data on r1: None
rejections: ['non_current_primary']
```

旧主节点应用了 `old=dirty`，但消息仍在队列里时 `r1` 已成为主节点。提升产生
generation 2 与 `repl-2`。消息到达时，物理来源已不是当前主节点，因此第一道
栅栏就拒绝了它。旧状态没有被合并进新历史。

还可以运行定向回归套件：

```bash
uv run pytest -q tests/protocols/test_async_primary.py
```

实测输出：

```text
............                                                             [100%]
12 passed in 0.04s
```

它覆盖排队 entry 与 full-sync 拒绝、generation/ID 校验，以及没有新写入时的
提升收敛。耗时可能变化；12 项通过且零失败才是验收事实。

## 7. 练习

### 理解题

1. 为什么仅有 `(offset=5)` 不足以判断复制消息是否属于当前历史？
2. 为什么 full-sync 消息必须在 `_apply_full_sync()` 前通过权威栅栏？

??? note "参考答案"

    1. Offset 只是某一段历史内部的位置。不同主节点可能在同一个数值 offset
       产生不同命令，因此还需要 generation/replication lineage。
    2. Full sync 会替换整个字典。如果先应用后校验，过期权威已经有机会破坏
       当前状态。

### 动手题

3. 不修改 `src/`，把上面的实验复制到临时文件。增加第二个副本，在提升前隔离
   它，之后再愈合。验收：打印愈合节点的 `replication_id`、offset 和 data，
   并证明它们与提升后的主节点一致。
4. 设计一个回归测试：保持排队消息的 source 与 replication ID 正确，只修改
   generation。验收：trace 中必须出现 `stale_generation`，目标字典仍为空。

??? note "参考答案"

    3. 使用 `replica_ids=("r1", "r2")` 构造，调用 `isolate("r2")`，提升
       `r1`，再调用 `heal("r2")` 与 `run_until_idle()`。比较 `probe()`
       返回的两个快照，不要比较可变私有字典。
    4. 参考 `tests/protocols/test_async_primary.py` 中的
       `test_current_primary_delivery_requires_current_generation_and_replication_id`：
       通过 `g.network` 发送，令 `source_primary="primary"`、
       `generation=0`，并使用当前 `replication_id`。断言拒绝原因与 `{}` 数据。
       如果要提出生产修改，应只在答案中展示 diff；本题无需修改仓库源码。

## 小结

提升选定新权威，因此会开启新历史。MiniDist 把这个概念落实为三项到达时检查：
当前主节点、generation 与 replication ID；随后才用 offset 排列已接受谱系中的
entry。主动同步让副本收敛到选定历史，但无法找回异步确认后只存在于旧主节点的
写入。第 6 章将用 Raft 选举替代手动提升：节点会推进任期、交换选票，并由多数派
选出权威。
