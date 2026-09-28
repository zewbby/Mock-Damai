# Phase 1 — Baseline

> 状态：讨论中  
> 目标：在不修改核心业务逻辑的前提下，建立可复现、可解释的单实例性能基线。

## 1. Phase 1 要回答的问题

Phase 1 不追求“最高 TPS”这个单一数字，而是先回答：

1. 当前默认 RocketMQ 异步创单链路，在单实例下稳定能接收多少抢票请求？
2. 异步消费者每秒真正能创建多少正式订单？
3. Submit TPS 与 Order Creation TPS 之间什么时候开始出现差距？
4. MQ Lag 从哪个压力档位开始持续增长？
5. P95 / P99 在什么压力区间开始恶化？
6. 第一个资源瓶颈来自应用、Redis、RocketMQ、MySQL，还是压测机本身？
7. 在压力升高时，系统是“正确拒绝”还是出现 5xx、连接失败、卡死、库存不一致等系统性失败？
8. 压测结束后，异步链路是否能够最终收敛到一致状态？

## 2. 当前默认测试对象

Phase 1 默认只测试当前主链路：

```text
POST /api/orders/async
        ↓
Redis Lua 预扣
        ↓
RocketMQ Transaction Message
        ↓
Async Order Consumer
        ↓
MySQL 条件扣库存 + 创建正式订单
```

默认消息模式必须保持：

```text
publisher-mode = rocketmq
rocket-mq-transaction-message-enabled = true
```

Kafka、Redis Stream、Outbox 不进入第一轮 Baseline，避免同时比较多个变量。

## 3. 两类 Baseline

### Baseline A — Capacity Baseline

目的：测量系统本身的处理能力，而不是策略配置能挡掉多少请求。

要求：

- 保留真实 JWT、幂等、Redis Lua、RocketMQ、MySQL 创单链路；
- Waiting Room、Rate Limit、Risk Control、In-Flight 等保护策略不能成为主要瓶颈；
- 可以通过配置提高阈值或关闭非核心门控，但不能修改核心业务代码；
- 库存必须充足，避免售罄提前结束实验；
- 请求必须使用足够多的独立用户 / Token / requestId；
- 不在请求后同步轮询结果，避免把查询流量混入提交吞吐。

主要回答：

```text
入口最大稳定 Submit TPS 是多少？
异步 Order Creation TPS 是多少？
什么时候开始产生持续 MQ Lag？
第一个真实资源瓶颈在哪里？
```

### Baseline B — Flash-Sale Baseline

目的：保留实际抢票保护策略，观察高峰流量下系统如何削峰和拒绝。

要求：

- Flash-sale profile；
- Waiting Room；
- Rate Limit；
- Risk Control；
- Activity Isolation；
- In-Flight Control；
- 少量库存 + 大量用户请求。

这一组不以“所有请求成功”为目标。

要区分：

```text
业务拒绝
- 限流
- 无资格
- 重复提交
- 售罄
- 等待室拒绝

系统失败
- HTTP 5xx
- Socket / Connection Error
- Timeout
- MQ 发布失败
- 请求长期卡在 QUEUED / PROCESSING
- 超卖 / 库存不一致
```

## 4. 为什么必须拆成 A / B

如果只打开全部保护策略压测，入口可能在很低的 QPS 就大量拒绝请求。

这时看到的：

```text
高 Throughput
低 DB CPU
MQ Lag = 0
```

不代表系统创单能力很强，也可能只是大量请求根本没有进入核心链路。

反过来，如果为了跑分把所有保护全部删除，又无法说明真实抢票场景下系统行为。

因此 Phase 1 用两条基线分别回答：

| Baseline | 回答的问题 |
| --- | --- |
| Capacity Baseline | 核心交易链路能处理多少 |
| Flash-Sale Baseline | 保护策略面对洪峰时是否正确工作 |

## 5. Capacity Baseline V1 配置决策

Capacity Baseline 的目标是测量核心交易链路的处理能力，因此关闭会在入口提前截断流量的治理策略，但保留所有核心正确性机制。

### 保留

- JWT；
- 一次性 Idempotency Token；
- requestId 幂等；
- Redis Lua 原子库存预扣；
- Stock Bucket；
- RocketMQ Transaction Message；
- Consumer 幂等；
- ticket_order_request 状态机；
- MySQL 条件扣减库存；
- 正式订单创建。

### 关闭

通过环境变量关闭：

```bash
SMART_TICKET_WAITING_ROOM_ENABLED=false
SMART_TICKET_RATE_LIMIT_ENABLED=false
SMART_TICKET_RISK_CONTROL_ENABLED=false
SMART_TICKET_ACTIVITY_ISOLATION_ENABLED=false
SMART_TICKET_RATE_LIMIT_BACKPRESSURE_ENABLED=false
```

原因是这些入口治理模块会在核心交易链路真正达到瓶颈之前主动拒绝或削减流量，导致无法观察自然容量边界。

In-Flight Control 是例外：当前 `flash-sale` profile 的 `AsyncOrderSubmitGuardrail` 强制要求它开启，并要求单票档上限至少为 `50000`。因此 Capacity Baseline 保持 In-Flight Control 开启，并冻结一个足够高、不会在预期测试区间内先触发的阈值；不能通过关闭它来绕过 Guardrail。

Capacity Baseline 允许出现：

```text
Submit TPS > Order Creation TPS
           ↓
      RocketMQ Accumulation
```

只有这样才能判断核心链路在什么压力档位开始无法持续追平。

### Profile

继续使用当前高并发运行配置：

```text
SPRING_PROFILES_ACTIVE=local,flash-sale
```

Phase 1 不为了制造“优化空间”而退回弱配置，也不在测试过程中临时调参。

Flash-sale profile 中与 Consumer、HikariCP、Stock Bucket 等相关的参数需要在第一轮正式测试前完整冻结并记录。

---

## 6. 固定变量

正式 Baseline 开始后，以下变量必须记录且单轮测试中不能随意变化：

### 代码

- Git branch
- Git commit SHA
- Java version
- Spring Boot version

### 应用配置

- Spring Profiles
- RocketMQ producer / consumer 参数
- Consumer 线程数
- Batch Consumer 开关
- HikariCP 连接池
- Stock Bucket 数量
- Waiting Room
- Rate Limit
- Risk Control
- Backpressure
- In-Flight Control

### 数据

- 用户数量
- Ticket Category
- 初始库存
- 每单数量
- Bucket 数量

### 环境

- OS
- CPU
- Memory
- JDK
- MySQL version
- Redis version
- RocketMQ version
- JMeter version
- JMeter 是否与服务端同机

## 7. 需要采集的指标

### 7.1 HTTP / JMeter

必须记录：

- Requests / sec
- Submit TPS
- Error %
- P50
- P95
- P99
- Max Latency

### 7.2 Async Order

必须单独计算：

- Order Creation TPS
- SUCCESS / FAILED / COMPENSATED
- QUEUED 数量
- PROCESSING 数量
- 从提交到正式订单创建的异步完成延迟

> HTTP 200 / code=0 只代表请求被系统接受，不代表订单已经创建。

### 7.3 RocketMQ

必须观察：

- Producer Send TPS
- Consumer TPS
- Consumer Lag / Accumulation
- Retry
- DLQ
- 压测停止后 Lag 回落到 0 所需时间

### 7.4 Redis

至少记录：

- CPU
- ops/sec
- connected_clients
- keyspace hits / misses
- Lua 相关耗时（若当前可采集）
- 库存最终一致性

### 7.5 MySQL

至少记录：

- CPU
- Connections
- Threads_running
- QPS / TPS
- Slow Query
- Row Lock / Lock Wait
- HikariCP active / pending

### 7.6 JVM / Application

至少记录：

- Process CPU
- Heap Used
- GC Count / Pause
- Live Threads
- HTTP Thread
- 关键业务 Counter
- In-Flight Request

## 8. 起始 QPS 估算：Preflight Capacity Probe

正式 Calibration Sweep 不使用任意固定的起始 QPS，例如 100 QPS。

起始 QPS 必须由当前代码配置和一次短时预评估共同决定。

### 8.1 静态容量结构

当前 `flash-sale` profile 的关键并发配置：

```text
Hikari maximum-pool-size       = 80
RocketMQ consume threads       = 48
RocketMQ consume thread max    = 160
Async batch workers            = 16
Async batch size               = 64
Async batch max wait           = 20 ms
Stock buckets                  = 128
```

单热点票档不会天然退化成单 Worker。

当前 routing key 包含：

```text
activityScope + bucketVersion + bucketNo
```

Stock Bucket 为 128，而本地 Batch Worker 为 16，因此热点票档的消息可以分散到多个本地消费 shard。

这说明 100 QPS 明显低于当前并发结构值得测试的区域，但这些配置仍然不能直接推导真实 TPS，因为缺少：

- Windows SUT 实际 CPU / Memory；
- MySQL 实际事务耗时；
- consumeBatch 实际 batch occupancy；
- Redis Lua 延迟；
- RocketMQ send / consume 延迟；
- 单批事务内 SQL 锁等待。

### 8.2 Preflight Probe 的目的

Preflight 不是正式 Benchmark，不进入最终性能结论。

它只回答：

> 当前这台 SUT 大致在哪个数量级开始接近处理能力，从而决定正式 Baseline 从哪里开始。

### 8.3 Probe 方法

保持 Capacity Baseline 的配置：

- Waiting Room OFF；
- Rate Limit OFF；
- Risk Control OFF；
- Activity Isolation OFF；
- In-Flight ON，阈值固定在足够高的位置，不让它先成为容量瓶颈；
- Backpressure OFF；
- `quantity=1`；
- 单热点票档；
- `POLL_RESULT=false`；
- RocketMQ Transaction Message；
- 单 Spring Boot 实例。

然后临时取消 Submit QPS 限制，使用 closed-loop 短时探测。

按线程阶梯：

```text
32 threads
64 threads
128 threads
256 threads（仅前一档仍明显线性增长时）
```

每档：

```text
Warm-up: 20–30s
Measure: 30–60s
Run: 1 次
```

观察：

- 实际 Submit TPS；
- Order Creation TPS；
- P95 / P99；
- RocketMQ Accumulation；
- Windows CPU；
- MySQL CPU / Threads_running；
- Hikari active / pending；
- JVM CPU / GC。

### 8.4 如何得到 Estimated Capacity

定义：

```text
Q_probe_peak
=
Probe 中仍保持健康、且继续增加线程后吞吐提升开始显著变小的最高 Submit TPS
```

这里不把单纯的最高瞬时 TPS 当作容量。

如果：

```text
64 threads  -> 2400 TPS
128 threads -> 3600 TPS
256 threads -> 3700 TPS
```

则大致可以认为容量数量级靠近：

```text
~3600–3700 TPS
```

而不是继续猜测 5000 或 10000。

### 8.5 正式起始 QPS

正式 Calibration Sweep 起始值：

```text
Q_start = round_practical(Q_probe_peak × 0.50)
```

即从预估容量约 50% 的位置开始。

例如：

```text
Q_probe_peak ≈ 3600 TPS

Q_start ≈ 1800 QPS
```

然后使用比例阶梯，而不是固定绝对阶梯：

```text
0.50 × Q_probe_peak
0.70 × Q_probe_peak
0.85 × Q_probe_peak
1.00 × Q_probe_peak
1.15 × Q_probe_peak
```

这样测试点天然围绕当前机器的真实容量边界。

### 8.6 为什么不用静态公式直接算 TPS

从配置可以得到并发结构，但不能得到服务时间。

例如 Batch Worker 理论并发为 16，Batch Size 最大 64，但真实吞吐取决于：

```text
每个 batch 实际聚合多少条消息
×
每个 batch 数据库事务耗时
×
锁等待
×
MQ / Redis / CPU 开销
```

如果直接使用：

```text
16 × 64 / 20ms
```

之类的公式，会得到没有工程意义的理论上限，因为 `20ms` 只是 batch max-wait，并不是数据库事务服务时间。

因此 Phase 1 使用：

```text
Static Architecture Assessment
        +
Short Preflight Probe
        ↓
Estimated Capacity
        ↓
Formal Starting QPS
```

而不是拍脑袋选择一个起点。

---

## 9. 压力模型

第一版暂定使用阶梯式压力，而不是一上来冲极限。

正式数值在测试环境确认后锁定。

示意：

| Stage | Target QPS | Duration | Purpose |
| --- | ---: | ---: | --- |
| Warm-up | 低 | 60–120s | JVM / 连接池 / 缓存预热 |
| S1 | 待定 | 180s | 低压力稳定性 |
| S2 | 待定 | 180s | 中低压力 |
| S3 | 待定 | 180s | 中压力 |
| S4 | 待定 | 180s | 接近容量边界 |
| S5 | 待定 | 180s | 超过稳定容量，观察退化 |

每个 Stage 之间必须重置会影响下一轮结果的业务数据，避免库存、请求状态、Token 和缓存状态污染后续实验。

## 10. 稳定吞吐的判定

不能简单把“JMeter 打出的最大 TPS”定义为系统吞吐。

暂定只有同时满足以下条件，才叫 Stable Throughput：

- 无不可解释 HTTP 5xx / 连接错误；
- P99 没有持续发散；
- MQ Lag 不持续单调增长；
- 压测停止后积压能够在合理时间内清空；
- ticket_order_request 最终能够收敛；
- 无超卖；
- Redis / MySQL 库存最终一致；
- JVM 不发生持续 Full GC / OOM；
- 数据库连接池没有长期 pending；
- 重复执行同一档位，结果波动在可接受范围内。

## 11. 每个档位至少重复 3 次

单次跑出来的结果不能直接进入最终 Benchmark。

每个候选稳定档位至少重复 3 次，记录：

- Median；
- Min / Max；
- 波动幅度；
- 是否存在异常 Run。

只有结果可重复，才进入 Baseline Report。

## 12. Phase 1 明确不做

本阶段先不做：

- 为提升数字而修改核心业务代码；
- Consumer Batch 优化；
- MySQL Batch Insert；
- Bucket 数量调优；
- JVM 参数专项调优；
- 多实例部署；
- Redis Cluster；
- MySQL 主从；
- 故障注入。

这些内容必须建立在 Baseline 之后，否则没有可信的 Before / After。

## 13. 决策记录与待确认事项

### 已确认

1. **正式 Baseline 使用两台物理机器。**
   - MacBook Air M4：JMeter Load Generator；
   - Windows：System Under Test。
2. **JMeter 与服务端分离。** 同机测试只用于 Smoke Test，不进入正式 Benchmark。
3. **Phase 1 只运行一个 Spring Boot 实例。** 多实例属于 Phase 4。
4. MySQL、Redis、RocketMQ 暂时与 Spring Boot 同机，先建立当前单机部署形态的基线。

详细环境决策见 [baseline-environment.md](baseline-environment.md)。

### 已确认的数据模型

1. Capacity Baseline 固定 `quantity=1`；
2. 第一轮只压一个热点 Ticket Category；
3. 用户池按 `max(1000, THREADS × 4)` 准备；
4. CSV Rows 按理论请求量预留 20%；
5. 库存按 CSV Rows 再预留至少 10%，保证不因售罄提前结束。

详细见 [baseline-scenarios.md](baseline-scenarios.md)。

### 已确认的起始 QPS 方法

正式起始 QPS 不使用固定绝对值。先执行短时 Preflight Capacity Probe，得到 `Q_probe_peak`，再取约 50% 作为正式 Calibration Sweep 起点。

### 已确认的执行资产

1. Preflight 使用 `scripts/load/run-preflight-jmeter.sh` + `scripts/jmeter/async-order-closed-loop.jmx`；
2. Preflight 不设置目标 QPS，由线程数形成 closed-loop 压力；
3. Formal Baseline 使用 `scripts/load/run-async-order-jmeter.sh` + `scripts/jmeter/async-order-target-rate.jmx`；
4. Formal Baseline 显式传入 TARGET_QPS，结果轮询固定关闭；Constant Throughput Timer 仅提供有限线程下的 Target-Rate 近似，必须记录实际 attempt TPS 与 Target 命中率；
5. HTTP 侧正式指标通过 `scripts/load/summarize-jmeter-result.py` 只统计异步提交 sampler，并同时报告幂等 Token 辅助请求速率与 Target 命中率。
6. Phase 1 V1 每次 Submit 前仍获取一次幂等 Token，因此 `Q_probe_peak` 包含 Token 发行开销；如需纯 Submit Capacity，后续必须作为独立实验显式改变请求模型。

### 待确认

1. 正式比例压力阶梯是否采用 50% / 70% / 85% / 100% / 115%；
2. 单档正式持续时间与 Warm-up；
3. Phase 1 指标采集使用 Actuator + 原生指标，还是直接引入 Prometheus 级采集；
4. 稳定容量边界的具体判定阈值。

## 14. 输出物

Phase 1 完成后，本目录至少应新增：

```text
phase-1-baseline.md
baseline-environment.md
baseline-scenarios.md
baseline-results.md
raw-results/
```

其中 `raw-results/` 是否直接提交仓库，需要根据文件大小决定；大型 JMeter HTML / JTL 仍应避免直接提交。
