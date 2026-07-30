# 第 4 章——异步主从

[English](../../tutorial/04-async-primary.md)

协议 1 有意保持简单：一个指定 primary 接受写，replica 消费其有序流，客户端看到成功前没有 replica 投票。简单性使核心取舍极其清楚：正常运行时复制能够收敛，故障转移后却仍可能丢失已确认写。

## 学习目标

完成本章后，你将能够：

1. 从 primary 本地应用，经 backlog 插入、计划投递，追踪到 replica 应用；
2. 解释为什么 replication ID 与 offset 共同标识一段历史中的位置；
3. 根据 backlog 覆盖与 lineage 预测 partial/full resync；
4. 展示 promotion 如何建立新 generation 并 fence 已排队的旧 primary 流量；
5. 重现已确认写丢失，且不把它误解为磁盘故障。

## Primary 在复制前确认

`src/minidist/protocols/async_primary/group.py` 中的 `AsyncPrimaryGroup.__init__` 创建 `SimClock`、`Trace`、`Scheduler` 和固定延迟的 `SimNet`。它选择 generation 1，导出 replication ID `repl-1`，分配给所有节点，创建有界 `deque` backlog，并为每个节点注册接收处理器。

`AsyncPrimaryGroup.client_write` 的写路径如下：

1. 拒绝 `NONE`、`LEADER` 之外的 ack level；
2. 要求 key/value 为 bytes，且 primary 存活；
3. 递增 primary offset，修改本地字典；
4. 创建包含 generation、replication ID、offset、key、value 的 `_Entry`；
5. 追加到有界 backlog；
6. 对每个非 primary 节点调用 `_send_entry`；
7. 记录 `client_acknowledged` 并返回 `accepted=True`。

第 6、7 步之间没有 `tick` 或 `run_until_idle`。`SimNet.send` 把投递计划到未来逻辑 tick，所以 replica 不参与确认决策。

`AsyncPrimaryGroup._apply_entry` 保护流顺序。payload 的 replication ID 必须同时匹配当前 primary 与目标 replica，并且 incoming offset 必须为 `target.offset + 1`。合法条目修改 replica data、推进 offset，并记录 `replica_applied`。发现 gap 时调用 `_synchronize`，而不是在未知前缀之上应用条目。

## Replica sink 意味着不投票

每个 `_Node` 有 data、replication ID、offset、liveness、volatile state 与 `SyncMode`，没有 vote 或 ack state。唯一写 API 通过 `self._primary`；replica 只接收 `entry` 或 `full_sync` 消息，并在 lineage 检查通过后应用。

这与 quorum replication 的决策边界不同。增加两个 replica 不会增强 `AckLevel.LEADER`：primary 仍在任何 replica 收到消息前返回。更多 replica 只是推进后可能有更多副本，不是成功前的多数派条件。

`AsyncPrimaryGroup.client_read` 暴露相应陈旧性。指定 replica 的 `LOCAL` 可能返回旧值和旧 offset；`LEADER` 路由到当前 primary；`LINEARIZABLE` 被拒绝。

## 复制位置是 `(ID, offset)`

单独一个 offset 只能说明“某条流中的第几个条目”，不能说明是哪段历史。因此 MiniDist 同时携带 `replication_id` 与单调 `offset`。

同一历史内，每次写递增 offset，backlog 保存有序 `_Entry`。promotion 时，`AsyncPrimaryGroup.promote` 递增 `_generation`，通过 `_new_replication_id` 给 candidate 新 ID，清空 backlog，并为所有其他存活节点启动同步。candidate 保留 data 与数字 offset，但 `(repl-2, 1)` 不等于 `(repl-1, 1)`。

每次投递还携带 `source_primary` 与 generation。`AsyncPrimaryGroup._replication_rejection_reason` 在以下情况拒绝消息：

- envelope 或 payload source 不是当前 primary；
- generation 已过期；
- replication ID 不同于当前 primary ID。

因为计划中的回调可能活过角色切换，所以 fencing 必不可少。测试 `test_queued_old_primary_full_sync_is_rejected_after_promotion` 与 `test_queued_old_primary_backlog_entry_is_rejected_after_promotion` 断言旧工作不能覆盖新 lineage。

接收方在投递时校验权威，而不只在发送方最初排队时校验，因为消息等待期间权威可能已经变化。

这仍不是自动选主。调用方显式执行 `promote`；MiniDist generation 记录这个手工拓扑 epoch。

## 有界 backlog：前缀已知时部分同步

`AsyncPrimaryGroup._synchronize` 选择修复模式。replica 与 primary 的 replication ID 相同，并且满足下列之一时可 partial resync：

- offset 已相等；或
- replica offset 至少为 backlog 最老条目的前一个 offset。

方法筛选 backlog 中 offset 大于 replica offset 的条目，以 `partial` 模式发送。若 lineage 不同或缺失前缀已掉出 deque，则发送 `full_sync` payload，其中包含 primary 完整 data 副本、当前 ID 与 offset。

`_apply_full_sync` 在一个仿真事件中原子替换目标字典。真实数据运输与加载需要时间；这里保留的只有“旧状态被整体替换”这一语义。

`SyncMode` 暴露最近一次加入/追赶路径：

- `NEVER`：尚未应用同步，或节点刚被 promote；
- `STREAMING`：正常有序投递；
- `PARTIAL`：选择 backlog 追赶；
- `FULL`：选择并应用全状态替换。

## Crash、restart、heal 与 promote

`AsyncPrimaryGroup.crash` 清空 volatile state、标记节点死亡，并 partition 所有相邻链路；节点 dataset 被有意保留。replica restart 会 heal 链路并调用 `_synchronize`；heal 一个存活但隔离的 replica 也会同步。

只有存活且非 primary 的节点能被 promote。过程不询问 quorum，也不选择最新 replica；这是实验者的责任。因此 promote 陈旧 replica 可能丢弃只存在于已 crash 旧 primary 的已确认写。

promotion 后，每个存活 replica 都主动同步。heal 后的旧 primary 已成为 replica；因 replication ID 不同，它接受 full sync，并丢失不属于新历史的脏数据。这能建立收敛，却不能让 partition 两侧先前的确认都变安全。

## 动手实验 1：正常流投递

运行：

```bash
uv run python -c 'from minidist.protocols import AckLevel,ReadLevel,UnsupportedLevelError; from minidist.protocols.async_primary import AsyncPrimaryGroup; g=AsyncPrimaryGroup(replica_ids=("r1",),backlog_size=2,replication_delay=1,seed=7); w=g.client_write(b"k",b"v",AckLevel.LEADER); print(w); print("before",g.client_read(b"k",ReadLevel.LOCAL,node="r1")); g.tick(); print("after",g.client_read(b"k",ReadLevel.LOCAL,node="r1")); print("backlog",g.probe().backlog_offsets);
try: g.client_write(b"x",b"y",AckLevel.QUORUM)
except UnsupportedLevelError as e: print(type(e).__name__+":",e)'
```

本仓实测：

```text
WriteResult(accepted=True, offset=1, message='primary accepted; replica delivery is asynchronous')
before ReadResult(value=None, node='r1', offset=0)
after ReadResult(value=b'v', node='r1', offset=1)
backlog (1,)
UnsupportedLevelError: async primary does not support AckLevel.QUORUM
```

同一对象同时展示延迟收敛和对更强 ack level 的诚实拒绝。

## 动手实验 2：部分与全量重同步

运行：

```bash
uv run python -c 'from minidist.protocols import AckLevel; from minidist.protocols.async_primary import AsyncPrimaryGroup
def demo(backlog,writes):
 g=AsyncPrimaryGroup(replica_ids=("r1",),backlog_size=backlog,replication_delay=1); g.crash("r1");
 for i in range(writes):g.client_write(f"k{i}".encode(),str(i).encode(),AckLevel.LEADER)
 g.restart("r1"); g.run_until_idle(); s=g.probe().nodes["r1"]; print("backlog",backlog,"writes",writes,"mode",s.sync_mode.name,"offset",s.offset,"data",dict(s.data))
demo(3,2); demo(2,3)'
```

本仓实测：

```text
backlog 3 writes 2 mode PARTIAL offset 2 data {b'k0': b'0', b'k1': b'1'}
backlog 2 writes 3 mode FULL offset 3 data {b'k0': b'0', b'k1': b'1', b'k2': b'2'}
```

第一次 replica 从 offset 0 开始，backlog 仍从 1 开始。第二次容量 2 的 deque 只保留 offset 2、3；offset 0 无法证明拥有 offset 1，所以必须全量替换。

## 动手实验 3：丢失已确认写

运行：

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
```

本仓实测：

```text
实验 2：ack 后立即 crash leader（异步主从）
1) client 写入 order:42=confirmed；leader 在 offset=1 返回 ack=True。
2) 故障前从 leader 读取：b'confirmed'。
3) ack 后立即 crash primary，随后完成 failover。
4) 新 leader=replica-1，读取 order:42：None。
结论：已确认写丢失。
```

`labs/exp02_acked_write_loss.py::run_experiment` 在确认与 crash 之间没有 tick。这个丢失是复制保证的反例，不是磁盘损坏仿真。

## 对照真实 Redis

[真实系统映射](../mapping.md#协议机制与真实系统复制机制映射)精确分类了保留的思想：

- 普通 primary 确认不需要 replica 参与；
- replica 是有序流消费者；
- replication ID + offset；
- 有界 backlog 部分重同步；
- 历史或覆盖不匹配时全量替换；
- 异步故障转移的已确认写丢失窗口。

MiniDist 简化或省略许多 Redis 机制。真实 Redis 复制运输字节流并使用 `PSYNC`；MiniDist 发送结构化 SET 条目。真实 full sync 生成、运输、加载 RDB，并缓冲并发变化；MiniDist 原子替换 dict。真实 promotion 由运维、Sentinel 或 Cluster 协调；MiniDist 提供手工 `promote`。Redis 可在换主后保留 primary/secondary replication ID 以改善 partial resync；MiniDist 只保留一个当前 ID，跨 generation 通常 full-sync。

保留 dataset 的 crash 不是 Redis 持久化。本仓没有 RDB、AOF、fsync、进程、socket、认证、`WAIT`、只读 replica 策略、重连时序或 Sentinel 选举。generation fence 是教学用手工 promotion epoch，不是完整 Sentinel/Cluster configuration epoch 协议。

这些边界也列在 [DIFFERENCES 清单](../mapping.md#differences-清单)和[实验矩阵](../experiments.md#复制协议实验矩阵)中。尤其不能因为单元测试覆盖有界 resync，就声称未来的慢 replica 和重连实验已经完成。

## 练习

### 理解题

1. 为什么 offset 7 不足以判断两个节点共享同一历史？
2. replica offset 为 `r`、backlog 最老 offset 为 `o` 时，何时可以 partial resync？

??? note "参考答案"

    1. 相同数字可以出现在不同 lineage；还必须比较 replication ID/generation。
    2. ID 必须相同，并且 `r` 等于 primary offset，或 `r >= o - 1`；后者证明全部缺失条目仍被保留。

### 动手题

3. 写临时测试，证明刚好超出 backlog 覆盖的 replica 使用 `SyncMode.FULL`。

   **验收：**backlog 容量为 2，replica 在 offset 0 crash，执行三次写，restart 后断言 `FULL`、offset 3、data 与 primary 相同。测试放在 `/tmp`，得到 `1 passed`，不得修改 `src/`。

??? note "参考答案"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.async_primary import AsyncPrimaryGroup, SyncMode

    def test_outside_backlog_full_syncs():
        group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=2)
        group.crash("r1")
        for i in range(3):
            group.client_write(str(i).encode(), b"v", AckLevel.LEADER)
        group.restart("r1")
        group.run_until_idle()
        state = group.probe()
        assert state.nodes["r1"].sync_mode is SyncMode.FULL
        assert state.nodes["r1"].offset == 3
        assert state.nodes["r1"].data == state.nodes[state.primary].data
    ```

4. 写临时回归测试：以 delay 2 排队条目，在投递前 promote 陈旧 replica，运行到空闲，断言旧条目不存在，并存在 reason 为 `non_current_primary` 的 `replication_message_rejected`。

   **验收：**测试通过，且只使用公开的 `probe`、`trace` 和 group 方法。

??? note "参考答案"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.async_primary import AsyncPrimaryGroup

    def test_promotion_fences_queued_old_entry():
        group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
        group.client_write(b"old", b"dirty", AckLevel.LEADER)
        group.promote("r1")
        group.run_until_idle()
        assert b"old" not in group.probe().nodes["r1"].data
        assert any(
            event.kind == "replication_message_rejected"
            and event.details["reason"] == "non_current_primary"
            for event in group.trace.events
        )
    ```

## 小结

异步主从通过把本地确认与 replica 推进分离，获得简单写路径。有序条目、`(replication ID, offset)` 与有界 backlog 让正常 streaming 和追赶可见；full sync 修复未知前缀；generation 检查在手工 promotion 后 fence 过期的排队流量。这些都不会把写变成 quorum commit。第 5 章将继续沿着同一条异步协议路径，专门分析换主时的 generation/lineage fencing，以及旧 full-sync 为何必须被拒绝。
