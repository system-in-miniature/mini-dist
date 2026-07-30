# 第 9 章：WAL 运输、Flush 边界与 Timeline

[English](../../tutorial/09-wal-shipping.md)

Raft 把命令复制为共识日志中的 entry。协议 2 提出另一种问题：如果 primary
先把每次修改编码成有序 write-ahead log record，在本地 flush，再把相同字节
运输给 standby，会发生什么？MiniDist 的 `WalShippingGroup` 保留了这条
PostgreSQL 式路径，同时让共享状态机足够小，可以逐步检查。

## 学习目标

完成本章后，你将能够：

1. 跟踪一条 SET 从逻辑 WAL 编码、本地 flush、运输、standby replay 到 flush
   acknowledgement 的全过程；
2. 区分 MiniDist 的异步 `LEADER` 与同步 `QUORUM` 边界；
3. 根据 timeline、LSN 与 retention 预测增量追赶或 base backup 替换；
4. 解释 promotion 为什么创建新 timeline，以及旧 timeline 排队 WAL 为什么
   必须被拒绝；
5. 准确说明本教学模型保留和省略了哪些 PostgreSQL 机制。

## 1. 被复制的是有序字节 record

`src/minidist/protocols/wal_shipping/group.py` 中的
`WalShippingGroup.client_write()` 校验 bytes key/value，选择
`lsn = primary.applied_lsn + 1`，再调用 `_encode_set()`：

```python
return (
    len(key).to_bytes(4, "big") + key
    + len(value).to_bytes(4, "big") + value
)
```

两个长度字段让 record 对任意二进制内容安全。`_decode_set()` 反向解析布局；
若声明的结尾不等于真实 payload 长度，就拒绝它。`WalRecord` 再给这些字节附上
当前 `timeline` 与单调 `lsn`。`_append_record()` 把原始 payload 追加到
`wal_bytes`，存储解码后的值，推进 `applied_lsn`，并记录 `wal_applied`。

这比发送 Python `(key, value)` tuple 更接近字节运输，但它不是 PostgreSQL
物理 WAL。真实 PostgreSQL 记录 crash recovery 所需的 page 与 transaction
效果；MiniDist 运输逻辑 SET，是为了让四种协议复用同一个 KV 状态机。保留的
机制是：standby 消费一条由位置标识的有序字节历史；被简化的是字节所描述的内容。

接收路径要求 `record.lsn == target.applied_lsn + 1`。Gap 不会越过未知前缀
直接 apply；`_receive()` 会调用 `_synchronize()`，判断 retained WAL 是否
覆盖缺失后缀，否则执行全量 base backup。

## 2. 本地 flush 与远端等待是两个选择

本地 append 后，`client_write()` 把 record 放入有界 `_retained` deque，
并调用 `fsync(self._primary)`。`fsync()` 把 live WAL、record、data 与 LSN
复制到节点 durable image，同时更新 `flushed_lsn`。之后才向每个 standby
发送 record。

请求的 acknowledgement level 决定是否继续等待：

- `AckLevel.LEADER` 在 primary 模拟 WAL flush 后返回，standby delivery
  仍为异步；
- `AckLevel.QUORUM` 等待**全部已配置的 synchronous standby** replay 并
  flush，否则到 `ack_timeout` 后失败；
- `NONE` 与 `ALL_ISR` 抛出 `UnsupportedLevelError`。

共享名称 `QUORUM` 是实验词汇，不表示此路径使用 Raft 多数派。
`client_write()` 中 required set 就是 `set(self._synchronous)`。
Synchronous standby apply 后调用 `fsync(target_id)`，再返回 `flush_ack`。
异步 standby 在本模型中也回 ack，但不属于 required set。

同步集合为空时，集合条件自然成立；所以不能只看 enum 名称推断 durability。
反过来，一个隔离的已配置同步 standby 会让 `QUORUM` 返回
`accepted=False`，即使其他副本已经拥有数据。此时写已在 primary 本地 flush；
结果只表示请求的远端确认条件未在确定性 timeout 内满足。

`tests/protocols/test_wal_shipping.py` 的
`test_leader_ack_exposes_ordered_logical_wal_bytes_before_standby_apply`、
`test_quorum_means_every_configured_synchronous_standby_flushed` 与
`test_quorum_write_fails_when_a_synchronous_standby_is_unreachable`
分别固定了这些边界。

## 3. Live、flushed 与 durable 位置

每个 `_Node` 分开保存 `applied_lsn`、`flushed_lsn` 和 `durable_lsn`。
普通 replay 只推进 live apply 位置；同步 standby 还会 flush。`probe()` 公开
三个位置的不可变快照，不允许调用者修改协议状态。

只有显式断电注入才把差异变成结果。普通 `crash(node)` 保留进程内 live state；
`crash(node, lose_unfsynced=True)` 则从最近 durable image 恢复
`wal_bytes`、record、data 与 LSN，并记录 `wal_fsync_loss`。Restart 后，
standby 会从恢复后的位置重新同步，丢失的后缀可以再次送达。

这是模拟 fsync 边界，不是磁盘实现。仓库没有文件、sector、checksum、torn
record、WAL writer process 或 recovery 启动耗时。模型只让实验区分“已收到”
和“已 flush”，不声称实现真实 storage engine。

读取是独立维度。`client_read()` 支持 `LOCAL` 与 `LEADER`。本地 standby 可
滞后；leader-routed read 读取当前 primary 已 apply 的数据。它拒绝
`LINEARIZABLE`，因为同步 WAL commit 本身不能证明 primary 仍拥有当前读取权威。

## 4. Retention 决定增量还是全量

`_retained` 是最近 `WalRecord` 的有界 deque。Standby restart 或 heal 时，
`_synchronize()` 只在以下条件允许增量追赶：

1. standby timeline 等于 group 当前 timeline；
2. standby LSN 已等于 primary，或它至少位于最老 retained record 前一位。

满足时，只按序发送 standby LSN 之后的 record，并把 `SyncMode` 设为
`INCREMENTAL`。若缺失前缀已超出 retention，或 timeline 不同，primary 会
发送含 WAL bytes、records、data、LSN 和 timeline 的 `base_backup`；
`_receive()` 在一个模拟事件中替换 standby 状态并标记 `FULL`。

这个边界对应 PostgreSQL 的 WAL retention，以及缺失所需 segment 后重新做
base backup 的需要。MiniDist 不实现 archive recovery、replication slot、
segment file、checkpoint 或传输成本。这里的 “base backup” 是原子 snapshot
payload，不是数据库持续写入期间复制的目录。

实验 5 对照了四种协议：

```bash
uv run python labs/exp05_replica_reconnect.py
```

实测输出：

```text
实验 5 [async]：inside_window=incremental; outside_window=full
实验 5 [wal]：inside_window=incremental; outside_window=full
实验 5 [isr]：inside_window=incremental; outside_window=full
实验 5 [raft]：inside_window=log replay; outside_window=log replay
```

相同单词不表示相同 artifact：async replay 结构化 backlog，WAL 运输 retained
byte record，ISR 复制 retained leader-log entry，而 M3 Raft 根本没有
compaction 边界。

## 5. Promotion 分叉 timeline

`WalShippingGroup.promote()` 只接受 live standby。它递增 `_timeline`，
选择新 primary，把新 timeline 赋给该节点，清空旧分支 retained WAL，flush
提升节点，记录 `wal_standby_promoted`，再为其他 live standby 发起同步。

已经进入 `Scheduler` 的消息不会消失。因此每个 `wal_record` 与
`base_backup` 都携带 `timeline` 和 `source_primary`。
`_rejection_reason()` 在 delivery 时先拒绝 stale timeline 或非当前
physical/logical source，之后才允许修改状态。

这条顺序对全量替换尤其重要：若先 apply 旧 base backup 再校验，新分支状态已经
被破坏。Timeline fencing 选择一个分支，不会合并两个分叉，也不能找回从未到达
promotion candidate 的异步写。

## 6. 动手实验：观察 flush 与 timeline fencing

运行：

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.wal_shipping import WalShippingGroup

g = WalShippingGroup(standby_ids=("s1", "s2"), replication_delay=2)
w = g.client_write(b"course", b"MiniDist", AckLevel.LEADER)
before = g.probe()
print("ack:", w.accepted, "lsn:", w.offset)
print("primary durable:", before.nodes["primary"].durable_lsn)
print("s1 before delivery:", before.nodes["s1"].applied_lsn)
g.tick()
old_timeline = g.probe().timeline
g.promote("s1")
g.run_until_idle()
after = g.probe()
print("timeline:", old_timeline, "->", after.timeline)
print("primary:", after.primary)
print("stale timeline rejected:", any(
    e.kind == "wal_rejected" and e.details["reason"] == "stale_timeline"
    for e in g.trace.events
))
print("leader read:", g.client_read(b"course", ReadLevel.LEADER).value)
PY
```

实测输出：

```text
ack: True lsn: 1
primary durable: 1
s1 before delivery: 0
timeline: 1 -> 2
primary: s1
stale timeline rejected: True
leader read: None
```

最后的 `None` 不是 decoder 错误。Promotion 发生在 `s1` 收到 LSN 1 之前，
所以新权威分支从未包含该异步提交值；旧 packet 随后被 timeline fence 拒绝。

运行定向测试：

```bash
uv run pytest -q tests/protocols/test_wal_shipping.py
```

实测输出：

```text
.........                                                                [100%]
9 passed in 0.03s
```

## 7. 对照真实 PostgreSQL

[协议映射](../mapping.md#协议-2-与-postgresql-wal-运输的映射)明确分类了边界。
有序 WAL、LSN replay、本地与同步远端 flush、retention 限制的追赶、base
backup fallback 以及 timeline 分支 fencing 都保留了 PostgreSQL 方向。

MiniDist 省略物理 WAL record、transaction、checkpoint、WAL segment、
archive command、replication slot、WAL sender/receiver process、recovery
conflict、级联 standby、`synchronous_standby_names` 选择语法和真实存储。
它的 `QUORUM` 等待全部配置的同步 standby；PostgreSQL 还有更丰富的 `FIRST`
与 `ANY` 选择。整数 timeline 也不实现 timeline-history file 与共同祖先恢复。

因此诚实结论是机制相似，而非 wire-compatible：MiniDist 在同一个确定性实验室
中，让 commit waiting、byte-stream position、catch-up coverage 与 promotion
后的分支权威变得可执行。

## 8. 练习

### 理解题

1. WAL `QUORUM` 为什么不能与 Raft `QUORUM` 互换？
2. Standby 为什么可能 `applied_lsn == 1`，但 `durable_lsn == 0`？
3. 即使数字 LSN 相同，timeline 不同为什么仍强制全量追赶？

??? note "参考答案"

    1. WAL 等待配置的 synchronous-standby 集合全部成员；Raft 在 term 与
       election 规则下由固定多数派 commit。
    2. 异步 replay 推进 live state，却没有在该 standby 调用 `fsync()`；
       只有 durable image 能跨越显式 fsync-loss crash。
    3. 不同分支上的同一数字不标识同一历史；直接 replay 后缀会拼接无关前缀。

### 动手题

4. 在临时测试中，把 `s1`、`s2` 都配置为同步 standby，隔离 `s2`，请求
   `AckLevel.QUORUM`。断言 `accepted is False`，同时 primary
   `durable_lsn` 等于尝试写入的 offset。不要修改 `src/`。

??? note "参考答案"

    ```python
    from minidist.protocols import AckLevel
    from minidist.protocols.wal_shipping import WalShippingGroup

    def test_remote_wait_can_fail_after_local_flush():
        g = WalShippingGroup(
            standby_ids=("s1", "s2"),
            synchronous_standby_ids=("s1", "s2"),
            ack_timeout=4,
        )
        g.isolate("s2")
        result = g.client_write(b"k", b"v", AckLevel.QUORUM)
        assert not result.accepted
        assert g.probe().nodes[g.primary].durable_lsn == result.offset
    ```

    保存为 `/tmp/test_wal_ch09.py`，运行
    `PYTHONPATH=src:. uv run pytest -q /tmp/test_wal_ch09.py`。

## 小结

WAL shipping 把有序字节历史与远端 flush 等待策略分离。MiniDist primary 总会
跨越模拟本地 durability 边界；异步 `LEADER` 在此返回，`QUORUM` 则额外等待
全部配置的同步 standby。保留的 LSN 支持增量 replay，缺失前缀要求 base
backup，promotion 创建 timeline 并 fence 旧分支流量。第 10 章转向另一种动态
边界：Kafka 风格 ISR membership、high watermark、`min.insync.replicas`、
controller epoch 与 lease。
