# 第 7 章：Raft II——日志与提交

选举只选出 leader，并不会自动让每条 entry 都安全。Raft 还必须先让日志达成
一致，按正确规则证明多数派保存了某条 entry，并且只把已提交前缀应用到状态机。
本章跟踪完整流水线，随后 crash 一个 follower，暴露持久状态与易失状态之间的
边界。

## 学习目标

完成本章后，你将能够：

- 跟踪客户端写入的 append、replication、commit 与 apply；
- 解释 `prevLogIndex`/`prevLogTerm` 一致性检查；
- 预测 `nextIndex` 如何修复 follower 的冲突后缀；
- 说明 Raft“只按当前任期 entry 计数提交”规则的必要性；
- 解释重启节点如何根据 durable log 重建状态机。

## 1. 客户端写入首先是未提交 entry

`src/minidist/protocols/raft/group.py` 中的 `RaftGroup.client_write()`
只接受 `AckLevel.QUORUM`。它找到当前 leader，追加一个包含下一个 index、当前
term、key 与 value 的 `LogEntry`，记录 leader 自己的 `match_index`，广播
AppendEntries，再调用 `_wait_for_commit()`。

Append 不等于 commit。Entry 只存在于 leader 时，如果随后选出其他 leader，它
仍可能被覆盖。`_wait_for_commit()` 推进逻辑 tick，并且只有同一节点仍是该
entry 任期中存活的 leader，且 `commit_index` 已经达到该 entry index 时才返回
true。超时会返回 `accepted=False`；MiniDist 不会悄悄把 QUORUM 降级成本地
确认。

日志在 index 0 有一个 sentinel。新选出的 leader 还会在
`RaftGroup._become_leader()` 中追加当前任期 no-op。因此第一条客户端 entry
通常位于 index 2：sentinel、选举 no-op、客户端命令。

## 2. AppendEntries 证明共同前缀

`RaftGroup._send_append()` 根据 leader 为每个 follower 保存的 `next_index`
构造 RPC。Payload 包含：

- leader term 与 ID；
- `prev_log_index` 与 `prev_log_term`；
- 从 `next_index` 开始的全部 entry；
- leader 的 `commit_index`；
- 可选 read context。

前一条 entry 的坐标是紧凑的一致性证明。在
`RaftGroup._receive_append_entries()` 中，follower 会立即拒绝旧任期 leader；
否则它在需要时降级并重置 election deadline。

接着，follower 检查 `prev_log_index` 是否存在，以及该位置 term 是否等于
`prev_log_term`。任一检查失败，它都会返回包含 conflict index 的失败响应，
而不会把候选后缀直接接在无法确认的前缀之后。

前缀匹配时，follower 遍历收到的 entry。如果某 index 已存在但 term 不同，它会
从该 index 开始删除日志，再追加 leader 的 entry：

```python
if target.log[index].term != entry.term:
    del target.log[index:]
    target.log.append(entry)
```

已达成一致的前缀不动。第一次冲突之后的 entry 并没有独立权威；它们来自已被
当前 leader 取代的历史。新 entry 则正常追加。

成功响应报告匹配 index。在 `RaftGroup._receive_append_response()` 中，leader
提高该 follower 的 `match_index`，并令 `next_index = match_index + 1`。失败
时，它把 `next_index` 退回响应中的 conflict index，并立即重试
`_send_append()`。MiniDist 返回冲突任期的第一个位置，避免逐 index 重试，但
没有实现论文中完整的 conflict-term 跳转优化。

## 3. 已复制不一定已提交

每次收到成功 append response，leader 都调用 `RaftGroup._advance_commit()`。
它从日志尾部向当前 commit index 倒序扫描候选位置，计算有多少个
`match_index` 不小于该位置。然而只有多数副本仍然不够：

```python
if (replicated >= self._quorum
        and leader.log[index].term == leader.current_term):
    leader.commit_index = index
```

为什么必须要求当前任期？Raft 论文 §5.4.2 构造了一段历史：某条旧任期 entry
虽然已经存在于多数节点，仍可能在之后的选举中被覆盖。仅根据这条旧 entry 的
副本数，就会过早标记它已提交。

安全规则是：

1. 通过副本计数提交 leader **当前任期**的 entry；
2. 由于日志是前缀结构，提交该 entry 会间接提交它之前的所有 entry，包括旧
   任期 entry。

这就是选举 no-op 的用途。即使没有客户端流量，新 leader 也可以复制并提交当前
任期 no-op，从而确立继承前缀的安全性。MiniDist 在 `_advance_commit()` 中的
why-comment 直接引用该反例，而不是把任期检查写成无解释条件。

推进后，leader 调用 `_apply_committed()`，再广播 AppendEntries，让 follower
得知新的 `leader_commit`。

## 4. 应用使用独立的单调游标

每个节点都跟踪 `commit_index` 与 `last_applied`。
`RaftGroup._apply_committed()` 每次把 `last_applied` 增加 1，直到追上
`commit_index`。No-op 的 `key is None`，不改变用户数据；普通命令则把 key
与 value 写进节点字典。

两个游标分离表达了两条不变量：

- entry 绝不能在提交前应用；
- 每个已提交 entry 必须按日志顺序应用，并且在一次内存恢复周期内至多应用一次。

Follower 在 `_receive_append_entries()` 中先用自己的最后日志 index 限制收到的
`leader_commit`，然后才 apply。Leader 无法让 follower 应用尚未收到的 index。

仿真在消息处理内部同步 apply。生产系统通常有独立 apply loop、batching、持久
状态机 snapshot 与等待中的客户端。MiniDist 保留了顺序边界，但不建模这些调度
和存储成本。

## 5. 更高任期修复被分区的旧 leader

在少数派分区中，旧 leader 可以在本地 append entry。`client_write()` 等待
多数派，因此该操作返回 `accepted=False`。与此同时，连通的多数派选出更高任期
leader，并提交不同后缀。

分区愈合后，更高任期 AppendEntries 到达旧 leader。
`_receive_append_entries()` 调用 `_step_down()`。一致性检查最终发现分叉，
重试路径退回 `next_index`。旧的未提交后缀被删除并替换为当前 leader 的后缀。

任期栅栏本身不会重写日志；它建立权威。`prevLogIndex`/`prevLogTerm`、冲突删除
与 `nextIndex` 重试才执行真正的收敛。第 8 章的实验 3 会观察完整转换。

## 6. Crash 恢复：持久日志，重建状态机

MiniDist 的 `_Node` 注释把 `current_term`、`voted_for` 与 `log` 标为持久。
`RaftGroup.crash()` 调用 `_clear_volatile()`，重置角色、commit/apply index、
字典数据、timer、vote、复制游标与 read ack。`RaftGroup.restart()` 把节点
恢复为 follower，并设置新 election deadline。

因此重启刚完成时，节点可以拥有完整 durable log，同时 `commit_index == 0` 且
字典为空。这不是模型中的协议数据丢失。Leader heartbeat 携带
`leader_commit`；follower 把 commit index 推进到自身日志可达到的位置，再由
`_apply_committed()` 重放已提交前缀。

本模型假设持久字段更新能原子存活。它没有文件、fsync、撕裂记录、checksum、
snapshot 或 log compaction。它验证 Raft 状态分类与重放行为，不验证生产存储
引擎。

## 7. 与论文及生产 Raft 对照

[仓库行为矩阵](../mapping.md)把日志匹配、
冲突后缀删除、多数派提交与当前任期规则标为等价于 Raft §§5.3 与 5.4.2。它还
记录了有意保留的缺口：

- 日志无界；没有实现 §7 的 snapshot installation 与 compaction；
- 成员固定；没有 joint consensus；
- 持久字段位于进程内 durable mapping，没有磁盘故障或 fsync 时序；
- AppendEntries 传输结构化 Python 值，而非编码 RPC；
- 冲突修复使用 conflict index，但没有完整 conflict-term 优化；
- 客户端调用同步驱动 tick，没有 session、retry ID 或重复请求表；
- 状态机立即、单线程应用。

生产 Raft 实现通常还会批量处理 log entry、流水化 AppendEntries、限制 inflight
请求、snapshot 状态、压缩日志、通过存储抽象持久 hard state 与 entry，并把
committed 通知和 applied 通知分开。MiniDist 的声明是语义层面的：前缀检查、
基于任期的冲突修复、当前任期提交与顺序 apply 在这里可直接检查。

## 8. 动手实验：重启与重放

在仓库根目录运行：

```bash
uv run python - <<'PY'
from minidist.protocols import AckLevel
from minidist.protocols.raft import RaftGroup

g = RaftGroup(node_ids=("n1", "n2", "n3"), seed=23,
              election_timeout_range=(4, 7), heartbeat_interval=1)
leader = g.run_until_leader()
r = g.client_write(b"k", b"v", AckLevel.QUORUM)
g.run_until_converged()
follower = next(n for n in g.probe().nodes if n != leader)
before = g.probe().nodes[follower]
g.crash(follower); g.restart(follower)
after = g.probe().nodes[follower]
print("write:", r.accepted, "index:", r.offset)
print("log terms:", [e.term for e in before.log])
print("before restart:", before.commit_index, before.data.get(b"k"))
print("after restart:", after.commit_index, after.data.get(b"k"))
g.run_until_converged()
recovered = g.probe().nodes[follower]
print("after catch-up:", recovered.commit_index, recovered.data.get(b"k"))
PY
```

实测输出：

```text
write: True index: 2
log terms: [0, 1, 1]
before restart: 2 b'v'
after restart: 0 None
after catch-up: 2 b'v'
```

Sentinel term 为 0；leader no-op 与客户端命令 term 为 1。重启清除 follower
易失的 commit/application 状态，但保留这些日志 entry。收敛过程投递
`leader_commit=2`，重放后重新得到 `k=v`。

运行定向协议套件：

```bash
uv run pytest -q tests/protocols/test_raft.py
```

实测输出：

```text
..........                                                               [100%]
10 passed in 0.05s
```

它覆盖 quorum commit、crash recovery、partition repair、无 quorum 时
linearizable read 失败，以及确定性重放。耗时可能变化；10 项通过且零失败才是
验收事实。

## 9. 练习

### 理解题

1. 为什么 follower 可以删除冲突后缀，却不能删除由
   `prevLogIndex`/`prevLogTerm` 确立的匹配前缀？
2. 为什么当前任期 no-op 可以安全地间接提交旧任期 entry，而直接按旧 entry
   计数却不安全？

??? note "参考答案"

    1. 匹配坐标根据 Raft log-matching property 证明 leader 与 follower 共享
       该前缀。第一个不同 term 后的 entry 属于被取代分支，不能仅凭“已经存在”
       就得到保护。
    2. 存储当前任期 entry 的多数派建立了未来 leader 通过选举限制必须包含的
       前缀，因此旧前缀会随之提交；只统计旧 entry 副本数不具备该证明。

### 动手题

3. 扩展临时重放脚本，分别打印 crash 前、刚 restart、收敛后的
   `last_applied`。验收：它与字典结果一起呈现 `2 -> 0 -> 2`。
4. 起草但不要应用一个错误的一行 diff：从 `_advance_commit()` 删除
   `leader.log[index].term == leader.current_term`。随后根据 Raft §5.4.2 写出
   回归测试验收条件：存在于多数派的旧任期 entry，在当前任期 entry 提交前不得
   推进 `commit_index`。

??? note "参考答案"

    3. 从不可变快照读取 `before.last_applied`、`after.last_applied` 与
       `recovered.last_applied`。在当前同步 apply 模型中，预期序列与
       `commit_index` 一致。
    4. 故意错误的建议 diff 会把条件缩减成只有
       `replicated >= self._quorum`。测试必须构造论文中的“旧任期、多数副本”
       状态（临时测试可布置私有状态），调用推进路径并断言不能直接 commit；
       随后追加并复制当前任期 no-op，断言整个前缀都提交。把它保留为练习 diff，
       不要编辑 `src/`。

## 小结

Raft 通过四个独立阶段把 leader 本地 append 变成安全状态机更新：验证共同前缀，
复制并修复后缀，在多数派提交当前任期 entry，再按顺序应用已提交前缀。Crash
恢复保留 term、vote 与 log，同时根据后续 leader 消息重建易失 commit 与 apply
状态。第 8 章将使用同一仿真器，在正常运行、换主与分区条件下，把这些语义与
异步主从行为进行对照。
