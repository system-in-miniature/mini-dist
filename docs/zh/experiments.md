> **Language**: [English](../experiments.md) | 简体中文

# 复制协议实验矩阵

`tick` 是逻辑时间步，不是墙上时钟或性能数字。协议 1 与协议 4 的结论
来自本仓库 lab + pytest；协议 2/3 仍是后续里程碑占位。

| 实验 | 协议 1：异步主从 | 协议 2：WAL 运输 | 协议 3：ISR + HW | 协议 4：Raft |
|---|---|---|---|---|
| 1. 正常复制 | **已断言**：primary 在 tick 0 ack；replica 初读为空，复制消息送达后 offset 与值收敛 | _待实现_ | _待实现_ | **已断言**：先选主；写仅在多数派复制后返回 QUORUM ack；leaderCommit 随后传播，三节点状态机与 commit index 收敛 |
| 2. ack 后杀 leader | **已断言**：不推进 tick 就 crash primary、promote stale replica；新 primary 读不到已 ack 的写 | _待实现_ | _待实现_ | **已断言**：QUORUM ack 后立即 crash leader；持有该日志前缀的多数派选出新 leader，read-index 读取仍为 `confirmed` |
| 3. 少数派含旧 leader 的网络分区 | 当前协议 1 没有自动选举/任期 fencing；M1 只提供显式 `promote`，因此不伪造同语义自动分区实验 | _待实现_ | _待实现_ | **已断言**：旧 leader 的本地追加不能得到 QUORUM ack；多数派在更高任期选主并继续提交；愈合后旧 leader 降级，其冲突后缀被回退并与新 leader 收敛 |
| 4. 慢副本 | _后续实验_ | _待实现_ | _待实现_ | _待实现_ |
| 5. 落后副本重连 | _后续实验_ | _待实现_ | _待实现_ | _待实现_ |
| 6. 读一致性矩阵 | _后续实验_ | _待实现_ | _待实现_ | _待实现_ |
| 7. 脑裂读 | _后续实验_ | _待实现_ | _待实现_ | _待实现_ |

## 如何运行

```bash
uv run python labs/exp01_normal_replication.py
uv run python labs/exp01_normal_replication.py --protocol raft
uv run python labs/exp02_acked_write_loss.py
uv run python labs/exp02_acked_write_loss.py --protocol raft
uv run python labs/exp03_partition_old_leader.py
uv run pytest -q tests/labs/test_experiments.py
```

三个 lab 只调用复制组公开 API。异步路径使用写、读、`tick`、
`run_until_idle`、`crash`、`promote` 与 `probe`；Raft 路径使用写、读、
`run_until_leader`、`run_until_converged`、`crash`、`isolate`、`heal`
与 `probe`。没有直接调用网络、scheduler、RPC handler 或内部投递钩子
来制造结论。

## 确定性边界

Raft 的网络选择与选举超时都只从构造函数注入的 `seed` 派生。回放测试
执行一次初始选举、一次旧 leader 分区、一次多数派换主和一次愈合收敛，
并断言两次同 seed 运行的完整 `Trace.as_dicts()` 逐项相等。逻辑 tick、
消息 ID、超时抽样和状态转移顺序都是回放契约的一部分。
