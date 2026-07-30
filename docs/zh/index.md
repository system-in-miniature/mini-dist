# MiniDist 教程

[English](../index.md)

MiniDist 在同一个确定性的进程内仿真环境中比较复制协议谱系。当前实现把
Redis 式异步主从复制与教科书 Raft 并排放置，让读者在同一故障模型下观察
确认、换主、网络分区与 fencing 语义。

## 安装

需要 Python 3.12+ 和 [uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/system-in-miniature/MiniDist.git
cd MiniDist
uv sync --dev
```

## 第一个实验

```bash
uv run python labs/exp01_normal_replication.py --protocol async
uv run python labs/exp01_normal_replication.py --protocol raft
```

两次运行最终都会在副本上看到 `b'MiniDist'`，但确认边界不同：异步 primary
在消息送达前就确认，Raft 则在多数派复制后才确认。

## 阅读顺序

先通过第 2 章了解仓库结构，再读机制映射。运行三个实验后，使用实验矩阵比较
已支持语义和明确非目标。

完整里程碑边界见
[中文 README](https://github.com/system-in-miniature/MiniDist/blob/main/README.zh-CN.md)。
当前仓库没有 `docs/superpowers/` 设计历史存档；README、映射、实验矩阵和
可执行测试就是现有事实来源。
