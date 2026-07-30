# 第 4 章：动手实验

[English](../labs-guide.md)

先执行 `uv sync --dev`，再从仓库根目录运行命令。每个脚本都只使用公开的
复制组 API 和固定 seed，因此事件顺序与观察结果可以复现。

## 实验 1：正常复制

源码：
[exp01_normal_replication.py](https://github.com/system-in-miniature/MiniDist/blob/main/labs/exp01_normal_replication.py)

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

预期：推进前 follower 值为 `None`，收敛后为 `b'MiniDist'`。异步协议最终
offset 为 `1`，Raft 的 commit index 为 `2`。更重要的看点是输出中的确认
边界：leader 立即确认与多数派复制后确认。

## 实验 2：已确认写与换主

源码：
[exp02_acked_write_loss.py](https://github.com/system-in-miniature/MiniDist/blob/main/labs/exp02_acked_write_loss.py)

```bash
uv run python labs/exp02_acked_write_loss.py --protocol async
uv run python labs/exp02_acked_write_loss.py --protocol raft
```

预期：两边写入都返回 `ack=True`。leader 随即故障后，异步协议从提升的旧副本
读到 `None`，Raft 则读到 `b'confirmed'`。重点观察每种协议在 ack 时究竟承诺了什么。

## 实验 3：网络分区与旧 leader fencing

源码：
[exp03_partition_old_leader.py](https://github.com/system-in-miniature/MiniDist/blob/main/labs/exp03_partition_old_leader.py)

```bash
uv run python labs/exp03_partition_old_leader.py --protocol async
uv run python labs/exp03_partition_old_leader.py --protocol raft
```

预期：异步协议分区两侧都接受本地脏写；愈合后按新 primary 世代收敛，旧侧脏写
消失。Raft 拒绝被隔离旧 leader 的 quorum 写，选出更高任期 leader，最终只有
`majority=b'continues'`。

用[实验矩阵](experiments.md)把现象对应到已支持的读/确认级别和明确限制。
仿真 tick 只表示事件顺序，不是毫秒或性能数据。
