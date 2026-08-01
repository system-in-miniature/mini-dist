# 自主重建

每个 Stage 都是一节可独立浏览的完整课：先理解当前问题、基本概念与必要性，再按机制板块连接相关文件和关键语句，最后用验证证据和自己的话完成理解闭环。

这是三种学习模式中的浏览器自主学习路径。按主题学习请进入[机制教程](../index.md)；需要 CLI 互动请查看 [Agent 带教使用教程](../agent-guided.md)。

如果希望在编辑器里聚焦当前增量，运行 `python -m journey.tools.build_journey study N`，再打开 `../MiniDist-journey-workspace`。

| Stage | 主题 | 新增测试 | 教材章节 |
|---:|---|---:|---:|
| [01](stage-01.md) | 确定性仿真基座 | 6 | [2](../tutorial/02-deterministic-simulation.md) |
| [02](stage-02.md) | 复制比较契约 | 1 | [3](../tutorial/03-replication-group.md) |
| [03](stage-03.md) | 异步主从复制 | 6 | [4](../tutorial/04-async-primary.md) |
| [04](stage-04.md) | Raft 权威与已提交复制 | 8 | [6](../tutorial/06-raft-election.md) |
| [05](stage-05.md) | 可比较实验 1–3 | 5 | [8](../tutorial/08-experiments.md) |
| [06](stage-06.md) | 提升谱系 Fencing | 5 | [5](../tutorial/05-failover-fencing.md) |
| [07](stage-07.md) | 公共持久性与观察边界 | 5 | [2](../tutorial/02-deterministic-simulation.md) |
| [08](stage-08.md) | WAL Shipping 与同步刷盘 | 8 | [9](../tutorial/09-wal-shipping.md) |
| [09](stage-09.md) | ISR 成员与 High Watermark | 10 | [10](../tutorial/10-isr.md) |
| [10](stage-10.md) | 四协议实验矩阵 | 5 | [8](../tutorial/08-experiments.md) |
