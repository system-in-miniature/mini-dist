> **Language**: [English](../mapping.md) | 简体中文

# 协议机制与真实系统复制机制映射

标注含义：

- **等价**：保留了实验关注的关键状态与故障语义。
- **有意简化**：方向相同，但删去了与当前课程问题无关的工程细节。
- **语义相反**：名称或统一接口可能暗示更强保证，实现会明确指出反例。

| MiniDist 机制 | 真实 Redis 对应 | 档位 | 边界说明 |
|---|---|---|---|
| primary 写本地状态后立即 ack，replica 不参与写决策 | Redis 默认异步复制，master 不等待 replica 才向普通写请求返回 | **等价** | 这正是实验 2 中已确认写可在 failover 后丢失的根因。 |
| replica 只消费 primary 的有序写流 | Redis replica 接收并应用 master replication stream | **等价** | 副本不是 quorum voter，也不会把自己的确认加入 client ack。 |
| replication ID + 单调 offset | Redis replication ID 与 replication offset | **等价** | 两者共同标识一条历史上的复制位置，不能只比较数字 offset。 |
| 有界 backlog 内执行部分重同步 | Redis `PSYNC`/partial resynchronization 与 replication backlog | **等价** | 落后副本请求 backlog 中缺失部分，不替换已有完整状态。 |
| 超出 backlog 或 replication ID 不同则全量替换 dict | Redis full resynchronization，传输 RDB 后继续命令流 | **有意简化** | 快照在一个仿真事件中原子替换；不模拟 fork、RDB 字节、socket 缓冲和加载时间。 |
| backlog 保存结构化 SET 条目 | Redis backlog 保存复制协议的字节流 | **有意简化** | MiniDist 关注窗口和 offset，不教授 RESP 编码或字节边界。 |
| promote 是显式公开方法 | `REPLICAOF NO ONE`，或 Sentinel/Cluster 控制面触发角色切换 | **有意简化** | 不实现故障检测、投票、Sentinel 配置纪元或自动选主，以便单独观察数据面。 |
| 新 primary 获得新 replication ID，旧 backlog 清空 | Redis promotion 形成新复制历史，并保留 secondary replication ID 帮助部分重同步 | **有意简化** | MiniDist 只保留一个当前 ID，因此换主后的旧节点会走全量同步。 |
| crash 清空易失控制状态、保留教学节点的 dataset | Redis 重启后的 dataset 取决于 RDB/AOF 与持久化配置 | **有意简化** | 这里把 dataset 当作持久态，以隔离“复制尚未到达新主”这一变量；不是无持久化 Redis 的断电模型。 |
| `AckLevel.LEADER` 表示 primary 已在内存 dict 应用 | 统一接口草案把 `LEADER` 描述为 leader 落盘；Redis 普通写成功不等于 fsync | **语义相反** | 该级别在协议 1 中绝不代表磁盘 durability。返回消息与实验 2 都刻意揭示这点。 |
| 拒绝 `AckLevel.QUORUM` / `ALL_ISR` | Redis 的 `WAIT` 可等待 replica ack，但仍不把 Redis 变成强一致 quorum 日志 | **有意简化** | 当前协议只建模默认异步写路径；不偷偷把 `WAIT` 当作 Raft commit。 |
| 拒绝 `ReadLevel.LINEARIZABLE` | Redis 默认 replica 读可 stale，failover 也不提供 read-index 式证明 | **等价** | `LOCAL` 可读旧值，`LEADER` 只做路由，不声称共识线性化。 |

## DIFFERENCES 清单

本里程碑没有实现 Redis Sentinel、Cluster、`WAIT`、RDB/AOF 策略、磁盘
fsync 丢失窗口或真实 RESP 传输。它们不能从当前测试成功推断为“已支持”。
协议 1 的可证明结论只限于：异步 ack、replica sink、offset/backlog
重同步、全量替换与手动换主。

# 协议 4 与 Raft 论文映射

这里的“Raft 论文”指 Diego Ongaro 与 John Ousterhout 的
*In Search of an Understandable Consensus Algorithm*。实现按论文 Figure 2
的状态与 RPC 规则组织，而不是把共享内存状态更新伪装成 RPC。

| MiniDist Raft 机制 | 论文小节 | 对应关系与实验边界 |
|---|---|---|
| follower / candidate / leader 与单调任期 | §5、§5.1 | 收到更高任期 RequestVote、AppendEntries 或响应时更新持久任期并降级；实验 3 由此 fence 旧 leader。 |
| 随机化 election timeout、候选人自投票、RequestVote 多数派选主 | §5.2、§5.6 | timeout 从构造函数 seed 所属的私有随机源抽取；不读取墙上时间或模块级随机状态。 |
| 候选日志必须至少与 voter 一样新 | §5.4.1 | 按 `(lastLogTerm, lastLogIndex)` 字典序判断，保护已提交日志不会选出到缺失它的 leader。 |
| AppendEntries 心跳与日志复制 | §5.2、§5.3 | 所有 RPC 经 `SimNet`；heartbeat 是 entries 为空的 AppendEntries。 |
| `prevLogIndex` / `prevLogTerm` 一致性检查、冲突后缀删除、`nextIndex` 回退重试 | §5.3、Figure 2 | follower 返回冲突任期的起点；leader 从该点重发。实验 3 愈合时覆盖少数派旧 leader 的未提交分叉。 |
| 多数派复制推进 commitIndex | §5.3 | `matchIndex` 覆盖多数派才可能提交；QUORUM 是唯一支持的写 ack。 |
| 只用“当前任期条目”按副本数推进 commitIndex | §5.4.2 | 代码 why 注释直接引用该节反例；旧任期条目不能仅因当前存于多数派就直接计数提交，而是随当前任期条目的已提交前缀间接提交。 |
| leader 当选后追加 no-op | §8 | no-op 提供当前任期可提交点，并在恢复/换主后安全提交此前前缀；它不改变 KV。 |
| `currentTerm`、`votedFor`、log 持久，其他状态易失 | §5 / Figure 2 | crash 保留三项；restart 以 follower、commitIndex=0、空状态机重建，随后从 leaderCommit 重放已提交前缀。 |
| LEADER 直读 | §8 的 client interaction 背景 | 只做路由到当前已知最高任期 leader，不额外证明 leader 仍掌握多数派，因此不声称线性化。 |
| LINEARIZABLE read-index 简化屏障 | §8 的只读请求规则 | leader 在当前任期发空 AppendEntries，并收到多数派成功响应后读取已应用状态机；不把读命令写入日志。 |

## 本实现与教科书 Raft 的偏差

- **有意简化：静态三节点或更多节点。** 不实现 §6 的 joint consensus
  成员变更；成员表在 `RaftGroup` 构造后不变。
- **有意简化：无 snapshot / compaction。** 不实现论文 §7；日志无界，
  落后 follower 永远通过 `nextIndex` 回退重放。
- **有意简化：冲突优化只返回 conflict index。** 教科书允许逐项递减；
  本实现返回冲突任期首位置以少走重复 tick，但没有实现完整
  conflict-term 跳跃优化。
- **有意简化：同步教学 client。** `client_write` 和线性一致读会在调用内
  驱动逻辑 tick，直到多数派成功或确定性超时；没有 future、重试 ID、
  session 或论文 §8 的重复请求去重表。
- **有意简化：read-index 是单轮 heartbeat barrier。** 它验证当前任期
  多数派可达，不实现 lease read、批量 read-index 或跨线程 apply wait。
- **等价但非磁盘模拟：持久字段是进程内 durable mapping。** crash/restart
  语义与 Figure 2 一致，但没有 fsync 延迟、torn write 或真实文件。
- **教学可观测性扩展。** `probe()`、`isolate()`、`heal()`、
  `run_until_leader()` 与 `run_until_converged()` 是实验 harness API，
  不是 Raft wire protocol 的组成部分。

本实现不支持 `AckLevel.NONE`、`LEADER`、`ALL_ISR` 或 `ReadLevel.LOCAL`；
这些组合显式抛出 `UnsupportedLevelError`，不会静默降级保证。
