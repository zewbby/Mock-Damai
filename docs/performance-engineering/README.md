# Performance Engineering

本目录只记录 `performance-engineering` 分支阶段的性能工程与可靠性验证工作。

项目业务主链路已经基本完整。本阶段不以继续扩展“大麦业务功能”为主要目标，而是把票务抢购场景作为高并发系统实验载体，通过可复现的数据回答以下问题：

- 当前单实例系统的稳定吞吐边界在哪里？
- Submit TPS 与 Order Creation TPS 分别是多少？
- 第一个真实瓶颈出现在哪一层？
- 已实现的库存分桶、背压、事务消息等设计是否真正产生收益？
- 横向扩展到多实例后，正确性和吞吐如何变化？
- 发生重复消息、数据库回滚、MQ Lag、Redis 故障等异常时，系统是否能够恢复？

## 阶段

| Phase | 目标 | 状态 |
| --- | --- | --- |
| Phase 1 | Baseline：建立可信、可复现的性能基线 | 讨论中 |
| Phase 2 | Bottleneck Profiling：定位真实瓶颈 | 未开始 |
| Phase 3 | Targeted Optimization：只优化已确认瓶颈 | 未开始 |
| Phase 4 | Multi-Instance Validation：验证横向扩展 | 未开始 |
| Phase 5 | Failure Injection：故障注入与恢复验证 | 未开始 |
| Phase 6 | Benchmark & Architecture Report：形成最终报告 | 未开始 |

## 文档规则

1. **先测量，后优化。** Phase 1 原则上不修改核心业务逻辑。
2. **所有性能结论必须可复现。** 至少记录代码 commit、配置、机器环境、数据规模、压测参数和原始结果位置。
3. **入口吞吐与创单吞吐分开统计。** 异步系统不能只用 HTTP TPS 描述整体能力。
4. **拒绝量必须分类。** 限流、等待室、售罄等业务拒绝不能和 HTTP 5xx、连接错误混为一谈。
5. **不使用未经验证的宣传数字。** 本机数据只描述本机测试环境，不代表生产容量。
6. **当前默认交易消息链路以 RocketMQ 为准。** 旧文档中将 Kafka 作为异步创单默认主链路的内容仅作为历史参考。

## 当前文档

- [Phase 1 - Baseline](phase-1-baseline.md)
- [Baseline Environment](baseline-environment.md)
- [Baseline Scenarios](baseline-scenarios.md)

## 压测资产

旧版 `docs/performance/` 压测计划与报告已删除，避免与当前 RocketMQ 默认主链路和新的性能测试口径产生冲突。

仍保留可执行资产：

- `scripts/jmeter/`：JMeter 测试计划；
- `scripts/load/`：数据准备、环境重置与压测执行脚本。

Phase 1 可执行资产已经完成第一轮重构：

- `async-order-closed-loop.jmx` + `run-preflight-jmeter.sh`：Preflight；
- `async-order-target-rate.jmx` + `run-async-order-jmeter.sh`：Formal Target-Rate Baseline；
- `prepare-async-order-jmeter-data.sh`：按 Waiting Room 开关准备数据；
- `summarize-jmeter-result.py`：只统计异步提交 sampler。

旧固定 QPS burst 档位不再作为容量判断依据。
