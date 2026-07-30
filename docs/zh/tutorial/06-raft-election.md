# 第 6 章：Raft I——选举

异步协议把提升决策交给实验；Raft 则让权威成为协议运行的结果。服务器推进任期、
向同伴请求投票，并且只有收集到多数票后才能成为 leader。本章沿着 MiniDist 的
这些状态转换，区分选举安全与最终选出 leader 这两个问题。

## 学习目标

完成本章后，你将能够：

- 找出 MiniDist 中 Raft 的持久与易失选举状态；
- 解释随机逻辑时间 deadline 如何减少反复平票；
- 应用“每任期一票”与日志新旧程度投票规则；
- 跟踪更高任期如何从 RPC 触发 leader 降级；
- 区分“已经观察到一个 leader”和“所有排队 RPC 已经稳定”。

## 1. 任期是权威的逻辑纪元

`src/minidist/protocols/raft/group.py` 中的 `_Node` dataclass 按 Raft
Figure 2 划分状态。`current_term`、`voted_for` 与 `log` 在仿真 crash 模型中
持久存在；角色、deadline、已收集选票、复制游标、commit index 与状态机都是
易失状态。

任期是单调递增的选举纪元，不是墙上时钟时间戳，也不代表特定节点。当 follower
的选举 deadline 到期时，`RaftGroup._start_election()` 执行一次本地原子转换：

```python
candidate.role = Role.CANDIDATE
candidate.current_term += 1
candidate.voted_for = candidate.node_id
candidate.votes = {candidate.node_id}
```

候选者重置 deadline，并通过 `SimNet` 向每个同伴发送 `request_vote`。请求包含
任期以及候选者最后日志的 index 和 term。RPC 是调度消息，而不是直接 Python
方法调用，因此分区与延迟会在和复制相同的边界影响选举。

任期提供栅栏。在 `RaftGroup._receive_request_vote()` 中，接收者看到更高任期
就调用 `_step_down()`；`_receive_vote_response()` 与
`_receive_append_response()` 也执行同一规则。在
`_receive_append_entries()` 中，当前 candidate 或 leader 接受同任期或更高
任期的 AppendEntries 权威时也会降级。`_step_down()` 在需要时更新任期、清空
新任期的 `voted_for`、把角色改为 follower、丢弃 leader/candidate 易失记录，
并选择新 deadline。

过期 leader 在分区中仍可能认为自己是 leader。只有当要求权威的操作无法完成时，
这种本地认知才不会破坏安全。分区愈合后，更高任期 RPC 会迫使它降级。

## 2. 随机超时让进展可复现

每个非 leader 节点都由 `RaftGroup.tick()` 驱动。Scheduler 先投递到期消息；
随后，任何到达 election deadline 的存活 follower 或 candidate 都调用
`_start_election()`。Leader 则在 `heartbeat_due` 到达时发送心跳。

`RaftGroup._reset_election_deadline()` 从配置区间抽取整数，加到当前逻辑 tick：

```python
timeout = self._rng.randint(self._timeout_low, self._timeout_high)
node.election_deadline = self.clock.now + timeout
```

随机源是复制组私有的 `random.Random(seed)`。MiniDist 不读取模块级全局随机数或
墙上时钟。使用相同 seed 和命令序列，timeout 抽样与最终 trace 都可复现。

随机化并不能证明选举安全；它解决的是活性问题：不同 deadline 让某个候选者更
可能先于其他节点请求投票。安全来自法定人数交集与投票规则。MiniDist 还要求
最小 election timeout 大于 heartbeat interval，避免在仿真配置假设下，连接
正常的 follower 比常规心跳更早超时。

仿真 tick 只是排序单位。“超时为 5 tick”不代表五毫秒，本模型也不提供延迟
基准。

## 3. 投票受任期与日志共同约束

`RaftGroup._receive_request_vote()` 判断候选者日志是否至少和投票者一样新：

```python
up_to_date = (
    candidate_last_term,
    candidate_last_index,
) >= (voter_last_term, voter_last_index)
```

字典序比较有意先看 last term，再看 last index。即使日志更长，如果最后任期
更旧，也不算更“新”。这条规则阻止缺少已提交 entry 的候选者从拥有这些 entry
的多数派手中赢得选举。

接收者只有在三个条件全部成立时才投赞成票：

1. 请求任期等于自己的当前任期；
2. `voted_for` 为空或已经是该候选者；
3. 候选者日志至少一样新。

投票后，投票者持久保存 `voted_for` 并重置 deadline。响应包含投票者当前任期
与决定。Trace event `raft_vote_decided` 记录 voter、candidate、term、
`granted` 与 `up_to_date`。

`RaftGroup._receive_vote_response()` 只在目标仍是 candidate 且响应任期等于
当前任期时计票。旧一轮竞选中延迟到达的赞成票不能在新任期选出 candidate。
唯一选票集合达到 `_quorum` 后才调用 `_become_leader()`。

三个节点的 quorum 为 2。任意两个多数派必有交集，因此同任期中两个不同候选者
不可能分别从“每任期只投一票”的服务器处都收集到多数票。这就是选举安全：
每任期至多一个 leader。它不意味着选举立刻完成；同时竞选、分区或反复平票都
可能推迟进展。

## 4. 成为 leader 会启动复制工作

`RaftGroup._become_leader()` 修改角色，初始化 `next_index` 与
`match_index`，设置 heartbeat deadline，并在新任期追加一个 no-op entry，
然后广播 AppendEntries。

赢得选举并不依赖 no-op。它提供了一个可以安全提交的当前任期 entry。一旦 no-op
提交，它之前的整个前缀也随之提交。这把选举连接到第 7 章要学习的 §5.4.2
提交规则。

公开辅助函数 `run_until_leader()` 反复查找存活 leader，若没有就推进一个 tick。
它一观察到 leader 就返回；如果暂时存在多个本地 leader 视图，就选最高任期者。
它是实验驱动器，不是额外的 Raft RPC，也不能证明所有排队响应都已经到达。

因此，函数刚返回时取得的快照可能仍显示另一个节点是 candidate。这不违反选举
安全：candidate 不是第二个 leader。再推进几个 tick，新 leader 的 AppendEntries
会到达，重置 follower deadline，并让 candidate 降级。

## 5. Crash 与重启中的选举状态

`RaftGroup.crash()` 把服务器标记为死亡并调用 `_clear_volatile()`；term、vote
与 log 保留。`RaftGroup.restart()` 把它标记为存活，再调用
`_reset_volatile()`，恢复 follower 角色并选择新 deadline。

持久保存 `voted_for` 非常重要。假如 crash 清除了它，却保留任期，服务器可能
先给候选者 A 投票，重启后又在同一任期给 B 投票。两个候选者就可能在不同时刻
分别凑出多数。MiniDist 在内存中保留该字段，而不是写磁盘，所以它建模的是状态
转换，不包括 fsync、撕裂写或存储损坏。

## 6. 与论文及生产 Raft 对照

仓库的 [Raft 映射与行为差异](../mapping.md)
把这些函数对应到 Raft §§5.1、5.2、5.4.1 与 5.6。选举状态与 RequestVote
规则在教学边界上等价，但 MiniDist 有意缩小了范围：

- 成员是至少三个节点的固定集合；没有 §6 的 joint-consensus 重配置；
- 持久状态是进程内 durable mapping，不是文件、WAL、存储引擎或 fsync 协议；
- RPC 是确定性 `SimNet` 中的 Python payload 字典，不是带编码、认证、背压、
  重传与进程调度的网络消息；
- 同步 lab helper 在调用内部驱动 tick；没有生产客户端、future、retry、
  request ID 与去重；
- `probe()`、`run_until_leader()`、`isolate()` 与 `heal()` 是观察及故障注入
  API，不是 Raft wire operation。

真实 Raft 衍生系统还会做论文以外的工程选择：pre-vote、防破坏性任期增长、
leadership transfer、snapshot、成员管理、持久存储格式与 batching。MiniDist
不宣称支持这些功能。它的有效声明是：任期、多数投票、日志新旧规则与降级行为
保留了选举机制。

## 7. 动手实验：观察带 seed 的选举

运行：

```bash
uv run python - <<'PY'
from minidist.protocols.raft import RaftGroup

g = RaftGroup(node_ids=("n1", "n2", "n3"), seed=11,
              election_timeout_range=(4, 7), heartbeat_interval=1)
leader = g.run_until_leader()
for _ in range(3):
    g.tick()
s = g.probe()
print("leader:", leader)
print("term:", s.nodes[leader].current_term)
print("roles:", {n: x.role.value for n, x in s.nodes.items()})
print("granted votes:", [(e.details["voter"], e.details["candidate"],
                         e.details["term"])
                        for e in g.trace.events
                        if e.kind == "raft_vote_decided"
                        and e.details["granted"]])
PY
```

实测输出：

```text
leader: n1
term: 2
roles: {'n1': 'leader', 'n2': 'follower', 'n3': 'follower'}
granted votes: [('n3', 'n1', 2)]
```

`n1` 已经包含自己的票，因此 `n3` 的这一票达到了两节点 quorum。由于 seed
确定的第一轮竞选出现平票，选举直到第二任期才完成。额外三个 tick 只是为了让
稳定角色更容易观察，不是为了“制造” leader。

用下面的命令验证更广的选举行为：

```bash
uv run pytest -q tests/protocols/test_raft.py -k 'leader or replay'
```

实测输出：

```text
.....                                                                    [100%]
5 passed, 5 deselected in 0.04s
```

所选测试覆盖只观察到一个 leader，以及相同 seed 下逐事件重放一致。耗时可能
变化；5 项选中测试通过且零失败才是验收事实。

## 8. 练习

### 理解题

1. 为什么 Raft 投票时比较 `(lastLogTerm, lastLogIndex)`，而不是只比较日志
   长度？
2. 被隔离的旧 leader 能否仍在本地保持 `LEADER` 角色而不违反选举安全？它必须
   无法完成什么操作？

??? note "参考答案"

    1. 新任期 entry 优先于旧任期 entry。只看长度可能偏好更长但过期的历史，
       从而选出缺少已提交前缀的候选者。
    2. 可以，直到它听见更高任期。安全要求它无法取得当前多数派并提交；愈合后
       更高任期 RPC 必须让它降级。

### 动手题

3. 把选举脚本运行两次并比较 `g.trace.as_dicts()`。验收：seed 11 的两个列表
   相等；只改变 seed 时允许得到不同 trace。
4. 设计一个只用于测试的实验，验证“每任期一票”可跨重启持久。验收：节点投出
   一票后，crash 并 restart，再让另一个候选者发送同任期请求，观察
   `granted=False`。不要修改 `src/`。

??? note "参考答案"

    3. 把构造与 `run_until_leader()` 放进返回 `trace.as_dicts()` 的函数，然后
       断言 `run(11) == run(11)`。这也是
       `tests/protocols/test_raft.py` 中
       `test_same_seed_replays_election_and_partition_heal_exactly()` 的模式。
    4. 定向测试可以通过 `group.network.send()` 投递 RequestVote 字典并推进
       tick。先通过 `probe()` 记录原 term 与 `voted_for`，调用 `crash()` 和
       `restart()`，再发送竞争请求。验收断言应放在临时测试或建议 diff 中，
       不写入仓库源码。

## 小结

Raft 选举把权威变成多数派决策。Candidate 推进任期、给自己投票、广播最后日志
坐标，并且只有拿到合法同任期 quorum 后才能获胜。随机且确定性的 deadline
帮助仿真取得进展；持久 term/vote 与更高任期降级规则提供安全。新 leader 随后
追加当前任期 no-op 并启动 AppendEntries。第 7 章将沿着该 RPC 深入日志匹配、
冲突修复、提交、应用与 crash 恢复。
