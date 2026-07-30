> **语言**：[English](../experiments.md) | 简体中文

# 复制协议实验矩阵

`tick` 是确定性的事件排序步，不是墙上时钟。下表每个单元格都有协议 pytest
或只调用公开 API 的 lab 断言支撑。

| 实验 | 协议 1：异步主从 | 协议 2：WAL 运输 | 协议 3：ISR + HW | 协议 4：Raft |
|---|---|---|---|---|
| 1. 正常复制 | `LEADER` 在 replica apply 前返回；流消息送达后收敛。 | `LEADER` 在 primary 本地 WAL flush 后、异步 standby apply 前返回；`QUORUM` 等待全部配置的同步 standby flush。 | `LEADER` 可留下 follower 缺口；`ALL_ISR` 仅在当前 ISR 全部拥有该 offset 且 HW 推进后返回。 | `QUORUM` 在多数派复制后返回；commit 传播后全部存活状态机收敛。 |
| 2. ack 后杀 leader | 立即 promote stale replica 会丢已确认写。 | 异步 `LEADER` 写可能不在被 promote 的 standby；同步 `QUORUM` 写存在于全部配置的同步候选。 | `ALL_ISR` 写已位于所有 ISR 成员的 HW 以下，controller clean election 后保留。 | QUORUM 已提交前缀存活，多数派选出新 leader 后仍可读。 |
| 3. 旧 leader 位于少数派 | 两个手工指定 primary 都能确认本地脏写；愈合后新 generation 胜出。 | promote 增加 timeline；旧 timeline 排队 WAL 在 apply 前被拒，standby 跟随新分支。 | controller 增加 leader epoch；旧 epoch append 被栅栏，clean election 从 HW 开始。 | 旧 leader 无法 commit；多数派的更高任期 leader 在愈合后覆盖未提交后缀。 |
| 4. 慢副本 | primary 不依赖 replica 投票，仍可写；慢 replica stale。 | 异步 standby 不影响本地 commit；若配置为同步 standby，则会阻塞 `QUORUM`。 | controller 将滞后 follower 移出 ISR；剩余 ISR 仍满足 `min.insync.replicas` 时继续写。 | 成员固定，但三节点中一个慢 follower 不影响多数派 commit。 |
| 5. 落后副本重连 | backlog 内 partial/增量；超出 backlog 或 lineage 不同则全量快照替换。 | retained WAL 内增量运输；早于最老保留 LSN 或新 timeline 时 base backup/全量追赶。 | leader 保留日志内增量 fetch；早于保留起点则全量快照，追到 HW 后才重入 ISR。 | M3 日志不压缩，始终通过 `nextIndex` 回退和增量日志重放修复；snapshot/全量传输属 v2。 |
| 6. 读一致性矩阵 | `LOCAL`：lagging replica stale。`LEADER`：fresh 但无权威证明。`LINEARIZABLE`：不支持。 | `LOCAL`：lagging standby stale。`LEADER`：fresh 但无 lease 证明。`LINEARIZABLE`：不支持。 | `LOCAL`：stale。`LEADER`：可能暴露未提交 append。`LINEARIZABLE`：直到 HW/lease/min.insync 证明权威才放行并返回 fresh。 | `LOCAL`：lagging follower stale。`LEADER`：fresh 但可能是分区旧 leader。`LINEARIZABLE`：当前任期 read-index 多数派后 fresh。 |
| 7. 脑裂读与租约 | 旧 primary 的 `LOCAL` 返回 promote 前值；不提供 linearizable/lease guard。 | 旧 timeline primary 的 `LOCAL` 返回 promote 前值；timeline 栅栏写入而非孤立 stale 读，也不提供 lease read。 | 旧 leader `LOCAL` stale；controller 所有权与逻辑 leader lease 拒绝被替换的 leader。 | 旧 leader `LOCAL` stale；当前 leader read-index 成功，分区旧 leader 无法取得 quorum barrier。 |

## ReadLevel 结果矩阵

实验 6 区分状态，不把各种失败都包装成“没有 stale”：

| 协议 | LOCAL | LEADER | LINEARIZABLE |
|---|---|---|---|
| 异步主从 | stale | fresh | unsupported |
| WAL 运输 | stale | fresh | unsupported |
| ISR + HW | stale | fresh 但未提交 | 无 HW lease 时 blocked |
| Raft | stale | fresh | read-index 后 fresh |

## 运行方法

```bash
uv run python labs/exp04_slow_replica.py
uv run python labs/exp05_replica_reconnect.py
uv run python labs/exp06_read_consistency.py
uv run python labs/exp07_split_brain_lease.py
uv run pytest -q tests/labs/test_experiments.py
```

实验 4、5、7 接受 `--protocol async|wal|isr|raft|all`，默认打印四列；实验 6
总是打印完整 3 × 4 矩阵。脚本只调用 replication group 公开 API，不直接调用
RPC handler，也不修改 scheduler/network 内部状态。

## 确定性与持久化边界

两个新协议都有同 seed、逐事件相同的 Trace 回放测试。显式
`lose_unfsynced=True` crash 是断电注入：节点回滚到最近一次模拟 fsync；
普通 `crash(node)` 保留已接受的持久状态。Raft 在依赖其结果的 RPC response
前持久化 term/vote/log；WAL `LEADER` flush primary，WAL `QUORUM` 还 flush
全部同步 standby；Kafka 式副本暴露类似 page cache 的未 fsync copy，但
`ALL_ISR` 依靠多副本保护；异步 primary 可确认尚未 fsync 的本地 dataset。
