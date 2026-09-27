# 性能测试入口

`docs/performance/` 只保留当前有效的性能测试入口，不再保存旧 Phase、固定 QPS 阶梯或与当前消息链路不一致的压测教程。

项目当前处于 **Performance Engineering / Phase 1 — Baseline**。阶段性实验设计在 `performance-engineering` 分支持续演进，`master` 这里只保留不会和实验迭代重复维护的稳定口径。

## 现在该看什么

| 目的 | 当前入口 | 说明 |
| --- | --- | --- |
| 看 Phase 1 总计划 | `performance-engineering` 分支 `docs/performance-engineering/phase-1-baseline.md` | Baseline 的问题定义、压力模型与稳定吞吐判定 |
| 看正式测试环境 | `performance-engineering` 分支 `docs/performance-engineering/baseline-environment.md` | MacBook Air M4 作为 Load Generator，Windows 作为单实例 SUT |
| 看数据模型 | `performance-engineering` 分支 `docs/performance-engineering/baseline-scenarios.md` | 单热点票档、`quantity=1`、用户池、库存与 CSV 规则 |
| 执行压测 | `scripts/jmeter/` + `scripts/load/` | 可执行资产；正式 Baseline 前必须按 Phase 1 口径审查参数 |
| 保存原始结果 | `reports/` | JTL、HTML、日志等本地生成物，不提交仓库 |
| 保存正式结论 | `performance-engineering` 分支的 Baseline 结果文档 | 记录可复现的汇总结果、瓶颈和结论 |

阶段文档入口：

https://github.com/zhubaozhenshuai666-lang/Mock-Damai/tree/performance-engineering/docs/performance-engineering

## Phase 1 当前不变口径

### 测试拓扑

- Load Generator：MacBook Air M4，仅运行 JMeter 和必要的采集脚本。
- SUT：Windows，运行单个 Spring Boot 实例 + MySQL + Redis + RocketMQ。
- 同机压测只用于 Smoke Test，不进入正式 Benchmark。
- 多实例属于 Phase 4，不提前混入 Phase 1。

### 交易链路

正式 Baseline 以当前默认 RocketMQ 交易命令链路为准：

```text
POST /api/orders/async
  ↓
RocketMQ Transaction Half Message
  ↓
本地事务内执行 Redis Lua 预扣并写事务标记
  ↓
Transaction Message Commit
  ↓
Async Order Consumer
  ↓
MySQL 条件扣库存 + 创建正式订单
```

`flash-sale` profile 的启动 Guardrail 还强制要求：

- `publisher-mode=rocketmq`；
- RocketMQ Transaction Message 开启；
- 入口 `ticket_order_request` 预落库关闭；
- In-Flight Control 开启，且单票档上限不得低于 `50000`；
- 订单超时使用 RocketMQ 延迟消息，扫描任务只作兜底。

因此 Capacity Baseline **不能**在保留 `flash-sale` profile 的同时设置：

```text
SMART_TICKET_ASYNC_ORDER_IN_FLIGHT_CONTROL_ENABLED=false
```

如果希望 In-Flight 不成为容量瓶颈，应保持它开启并把阈值固定在足够高的位置，而不是违反 Guardrail 关闭它。

### Capacity Baseline V1

- 单 Show / 单 Session / 单热点 Ticket Category；
- `quantity=1`；
- Waiting Room、Rate Limit、Risk Control、Activity Isolation、Backpressure 可以关闭，避免入口治理提前截断核心容量；
- In-Flight Control 保持开启；
- `POLL_RESULT=false`，不把结果查询压力混入提交吞吐；
- 库存必须足够，不允许因售罄提前结束容量测试；
- Kafka、Redis Stream、Outbox 不进入第一轮 Baseline。

### 起始 QPS

正式 Baseline 不再从任意固定的 `10 / 20 / 50 / 100 / 200 QPS` 阶梯开始。

先做短时 closed-loop Preflight Probe，得到：

```text
Q_probe_peak
```

再用：

```text
Q_start ≈ round_practical(Q_probe_peak × 0.50)
```

作为正式 Calibration Sweep 的起点。历史文档中的固定 QPS 只代表当时本机调试建议，不再是当前性能计划。

## 怎么跑

### Smoke Test

现有 `scripts/load/run-async-order-jmeter.sh` 可以继续用于验证 JMeter、Token、网络和交易链路是否跑通。

Smoke Test 的结果不能写入正式 Baseline。

### Formal Baseline

正式执行时以 `performance-engineering` 分支 Phase 1 文档为准，并在运行前确认当前脚本参数与文档一致。

当前 `master` 资产存在两个需要特别注意的历史默认值：

1. `run-async-order-jmeter.sh` 默认 `POLL_RESULT=true`；Capacity Baseline 必须显式改为 `false`。
2. 旧数据准备脚本会生成 Waiting Room `admissionToken`；Capacity Baseline 关闭 Waiting Room 后，这类 Redis Key 不应成为正式测试的无关负载。

也就是说：**脚本能跑通，不等于它已经符合正式 Baseline 口径。**

## 看什么指标

| 指标 | 当前定义 |
| --- | --- |
| Submit TPS | `POST /api/orders/async` 被系统接受的提交吞吐；不要用包含 Token 获取或结果查询的聚合 Throughput 替代 |
| Order Creation TPS | 异步消费者真正创建正式订单的吞吐；可用 `order.created.count` 的时间窗口增量和数据库结果交叉校验 |
| P95 / P99 | 重点看异步提交接口本身的延迟分位数，不能只看全部 Sampler 聚合值 |
| Error Rate | 业务拒绝与系统失败分开统计；HTTP 5xx、连接错误、Timeout 单独列出 |
| RocketMQ Accumulation | 观察 Producer / Consumer 差值、积压是否持续增长，以及停止施压后多久回落 |
| Request State | `QUEUED / PROCESSING / SUCCESS / FAILED / COMPENSATED` 的最终分布 |
| Resource | Windows CPU、JVM / GC、MySQL CPU / Connections / Threads_running / Lock Wait、Redis CPU / ops、Hikari active / pending |
| Oversell | 必须为 `0` |
| Convergence | 停止施压后积压清空、请求状态收敛和 Redis / MySQL 库存恢复一致所需时间 |

注意：HTTP `200` 或业务 `code=0` 只说明异步提交被接受，不等于正式订单已经创建。

## 结果记在哪里

每次正式 Run 至少记录：

- Git branch / commit SHA；
- Spring Profiles 和关键配置；
- Load Generator / SUT 硬件与软件版本；
- 数据规模、用户池、库存、Bucket 数；
- Threads、Ramp-up、Duration、目标 QPS；
- Submit TPS、Order Creation TPS、P95、P99；
- 业务拒绝、系统失败；
- RocketMQ 最大积压与回落时间；
- JVM / MySQL / Redis 关键资源指标；
- 最终订单请求状态；
- 库存一致性和 Oversell 结论。

大型 JTL、JMeter HTML、运行日志继续放在本地 `reports/`，不要提交仓库。正式性能文档只保存可复现所需的参数、聚合结果、关键截图或小型数据摘要。

## 旧文档处置

| 原文件 | 标记 | 处理 |
| --- | --- | --- |
| `async-order-jmeter-load-test-guide.md` | 合并 | 有效的脚本入口与执行注意事项收敛到本入口和 Phase 1 文档；原文件删除 |
| `formal-jmeter-pressure-test-plan.md` | 重写 | 历史固定 QPS、同机 M4 环境、Kafka 主链路等口径已被 Phase 1 Baseline 替代；原文件删除 |
| `phase2-pressure-test-report.md` | 删除 | 只有指标标题，没有真实 Phase 2 结果，也与当前 Phase 1 阶段命名冲突 |

不额外建立 `archive/`。Git 历史已经保留旧内容，继续在文档树中保留历史压测计划只会增加误用概率。
