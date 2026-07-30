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
| 每条复制投递携带 source primary、generation 与 replication ID，接收端只接受当前世代 | Redis 用 replication ID 校验复制历史；Sentinel/Cluster 控制面用配置纪元 fence 过期所有权 | **有意简化** | MiniDist generation 是进程内手动换主纪元，不是完整 Sentinel/Cluster 配置纪元协议；它阻止旧 primary 排队的 full-sync 与 backlog 投递越过换主。 |
| promote 后立即为每个 alive 副本发起同步 | Redis 副本会被重定向或重连到选定 master，并按其历史同步 | **有意简化** | 不实现发现或自动 failover 控制面；显式 `promote` 负责拓扑切换并主动启动收敛。分区中的投递会被丢弃，`heal` 会重试同步。 |
| crash 清空易失控制状态、保留教学节点的 dataset | Redis 重启后的 dataset 取决于 RDB/AOF 与持久化配置 | **有意简化** | 这里把 dataset 当作持久态，以隔离“复制尚未到达新主”这一变量；不是无持久化 Redis 的断电模型。 |
| `AckLevel.LEADER` 表示 primary 已在内存 dict 应用 | 统一接口草案把 `LEADER` 描述为 leader 落盘；Redis 普通写成功不等于 fsync | **语义相反** | 该级别在协议 1 中绝不代表磁盘 durability。返回消息与实验 2 都刻意揭示这点。 |
| 拒绝 `AckLevel.QUORUM` / `ALL_ISR` | Redis 的 `WAIT` 可等待 replica ack，但仍不把 Redis 变成强一致 quorum 日志 | **有意简化** | 当前协议只建模默认异步写路径；不偷偷把 `WAIT` 当作 Raft commit。 |
| 拒绝 `ReadLevel.LINEARIZABLE` | Redis 默认 replica 读可 stale，failover 也不提供 read-index 式证明 | **等价** | `LOCAL` 可读旧值，`LEADER` 只做路由，不声称共识线性化。 |

## 协议 1 DIFFERENCES 清单

本里程碑没有实现 Redis Sentinel、Cluster、`WAIT`、RDB/AOF 策略或真实
RESP 传输。M3 持久化边界是进程内 fsync image，不等同于生产持久化模式。

# 协议 2 与 PostgreSQL WAL 运输的映射

| MiniDist WAL 机制 | PostgreSQL 对应机制 | 分类 | 边界说明 |
|---|---|---|---|
| 带单调 LSN 的有序逻辑 SET 字节 | 按 LSN 标识的有序 WAL 字节 | **有意简化** | 真实 WAL 是物理的；逻辑 SET 在保留字节运输与顺序的同时复用统一 KV 状态机。 |
| `LEADER` flush primary WAL，在异步 standby apply 前返回 | 本地提交加异步流复制 | **等价** | 被 promote 的异步 standby 可能缺少已确认后缀。 |
| `QUORUM` 等待全部配置的同步 standby flush | `synchronous_commit` 加同步 standby 配置 | **有意简化** | 这里表示全部配置的同步 standby，不是数字多数派，也不实现完整 `ANY/FIRST` 语法。 |
| standby 严格按 LSN 解码/apply | WAL receiver/replay 跟随一条有序流 | **等价** | gap 会触发追赶，不会在未知前缀上继续 apply。 |
| retained WAL 内增量追赶；更老位置触发 base backup | WAL retention/archive slot 与新 base backup | **等价** | 省略 archive 获取与传输成本；快照替换是单个事件。 |
| promote 增加 timeline 并拒绝旧 timeline WAL | timeline history 保护 promote 后的恢复分支 | **等价** | history 文件与共同祖先查找简化为整数分支栅栏。 |
| `LOCAL` 可 stale；拒绝 `LINEARIZABLE` | hot-standby replay 可滞后；同步提交不是读租约 | **等价** | 不把写 durability 偷换成读权威。 |

## 协议 2 DIFFERENCES 清单

- **有意简化：**逻辑 KV WAL、固定同步 standby tuple、原子 base backup，
  不实现事务/checkpoint/archive restore。
- **等价分支安全：**旧 timeline record 在 apply 前被拒。
- **若泛化则语义相反：**这里 `QUORUM` 是全部同步 standby，不是 Raft 多数派。

# 协议 3 与 Kafka ISR 复制的映射

| MiniDist ISR 机制 | Kafka 对应机制 | 分类 | 边界说明 |
|---|---|---|---|
| 进程内 controller 拥有 leader、leader epoch 与 ISR | Kafka controller/metadata quorum 裁决 partition leadership 与 ISR | **有意简化** | controller 可用性与 metadata 复制不在本数据面模型中。 |
| lag timeout 移出 follower；追到 HW 才允许重入 | 按 replica lag 与追赶结果收缩/扩张 ISR | **等价** | 逻辑 tick 代替墙上时钟 lag/fetch timing。 |
| HW 是当前 ISR 的最小 log-end offset | Kafka partition high watermark | **等价** | committed read 只暴露 HW 以下 entry。 |
| `ALL_ISR` 等待当前 ISR 并执行 `min.insync.replicas` | producer `acks=all` 加 `min.insync.replicas` | **等价** | 动态 ISR 不是 Raft 多数派。 |
| `LEADER` 不等待 ISR/HW | Kafka `acks=1` | **等价** | leader read 可暴露 HW 以上数据。 |
| controller election 增加 leader epoch；拒绝旧 epoch packet | leader-epoch fencing 与日志截断 | **等价** | producer/transaction epoch 是另一机制，未实现。 |
| retained log 内增量追赶；窗口外全量快照 | segment retention 与 replica bootstrap | **有意简化** | Kafka 传输 log segment，不会原子替换 KV snapshot。 |
| 逻辑 leader lease 栅栏权威读实验 | controller 所有权加教学 lease | **有意简化** | Kafka 不公开 MiniDist `lease_read`；它只隔离旧 leader stale 机制。 |

## 协议 3 DIFFERENCES 清单

- **有意简化：**始终可用的进程内 controller、一写一 record，无 batch、
  幂等、事务或 segment file。
- **等价：**ISR 收缩/重入、HW、min.insync、ALL_ISR 与 leader epoch rejection
  保留故障语义。
- **若泛化则语义相反：**`ALL_ISR` 是动态集合，可能少于固定配置的多数派。

# 协议 4 与 Raft 论文映射

这里的“Raft 论文”指 Diego Ongaro 与 John Ousterhout 的
*In Search of an Understandable Consensus Algorithm*。实现按论文 Figure 2
的状态与 RPC 规则组织，而不是把共享内存状态更新伪装成 RPC。

| MiniDist Raft 机制 | 论文小节 | 档位 | 对应关系与实验边界 |
|---|---|---|---|
| follower / candidate / leader 与单调任期 | §5、§5.1 | **等价** | 收到更高任期 RequestVote、AppendEntries 或响应时更新持久任期并降级；实验 3 由此 fence 旧 leader。 |
| 随机化 election timeout、候选人自投票、RequestVote 多数派选主 | §5.2、§5.6 | **等价** | timeout 从构造函数 seed 所属的私有随机源抽取；不读取墙上时间或模块级随机状态。 |
| 候选日志必须至少与 voter 一样新 | §5.4.1 | **等价** | 按 `(lastLogTerm, lastLogIndex)` 字典序判断，保护已提交日志不会选出到缺失它的 leader。 |
| AppendEntries 心跳与日志复制 | §5.2、§5.3 | **等价** | 所有 RPC 经 `SimNet`；heartbeat 是 entries 为空的 AppendEntries。 |
| `prevLogIndex` / `prevLogTerm` 一致性检查、冲突后缀删除、`nextIndex` 回退重试 | §5.3、Figure 2 | **等价** | follower 返回冲突任期的起点；leader 从该点重发。实验 3 愈合时覆盖少数派旧 leader 的未提交分叉。 |
| 多数派复制推进 commitIndex | §5.3 | **等价** | `matchIndex` 覆盖多数派才可能提交；QUORUM 是唯一支持的写 ack。 |
| 只用“当前任期条目”按副本数推进 commitIndex | §5.4.2 | **等价** | 代码 why 注释直接引用该节反例；旧任期条目不能仅因当前存于多数派就直接计数提交，而是随当前任期条目的已提交前缀间接提交。 |
| leader 当选后追加 no-op | §8 | **有意简化** | no-op 提供当前任期可提交点，并在恢复/换主后安全提交此前缀；它不改变 KV，且省略 §8 的 client session 处理。 |
| `currentTerm`、`votedFor`、log 持久，其他状态易失 | §5 / Figure 2 | **有意简化** | crash 在模拟 stable-storage 边界保留这些字段；restart 重建易失状态，再从 leaderCommit 重放已提交前缀。 |
| LEADER 直读 | §8 的 client interaction 背景 | **语义相反** | 只路由到当前已知最高任期 leader，不证明它仍掌握多数派；刻意不提供调用者可能从“leader read”推断的线性一致保证。 |
| LINEARIZABLE read-index 简化屏障 | §8 的只读请求规则 | **有意简化** | leader 在当前任期发空 AppendEntries，并收到多数派成功响应后读取已应用状态机；读命令不入日志，并省略 lease、批处理与 apply-wait 机制。 |

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
- **无真实文件的等价 stable-storage 顺序：**persistent field 在依赖其结果的
  Raft response 前到达模拟 fsync 边界；没有 fsync 延迟、torn write、
  checksum 或真实文件。
- **教学可观测性扩展。** `probe()`、`isolate()`、`heal()`、
  `run_until_leader()` 与 `run_until_converged()` 是实验 harness API，
  不是 Raft wire protocol 的组成部分。

本实现不支持 `AckLevel.NONE`、`LEADER` 或 `ALL_ISR`。`ReadLevel.LOCAL`
专门用于 stale-read 实验，不声明权威；`LINEARIZABLE` 仍由 read-index 保护。
