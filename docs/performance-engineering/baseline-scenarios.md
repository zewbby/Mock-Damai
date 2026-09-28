# Phase 1 — Baseline Scenarios

> 状态：讨论中  
> 当前先锁定 Capacity Baseline V1 的数据模型；压力阶梯和指标采集方案继续讨论。

## 1. Capacity Baseline V1 数据模型

### 单热点票档

第一轮只测试：

```text
1 Show
1 Session
1 Ticket Category
```

原因是 Phase 1 需要主动制造真实热点，而不是把压力分散到多个票档后得到一个更好看的数字。

多票档、多活动并行属于后续扩展实验，不进入第一版 Baseline。

### 每单数量

固定：

```text
quantity = 1
```

原因：

```text
1 SUCCESS order
=
1 ticket sold
=
1 stock deduction
```

这样库存与订单的一致性校验最直接。

Phase 1 不需要模拟用户一次买 2 张或更多票的业务分布；先减少实验变量。

## 2. 压测用户池

当前代码存在 `OrderSubmitGuard`：

```text
key = userId + ticketCategoryId
TTL = 10 seconds
```

它用于防止同一用户、同一票档的并发重复提交。

正常提交链路结束后会释放 Guard，因此 Capacity Baseline **不需要为每一个请求准备一个全新的用户**。

用户池的目标只是：

> 避免同一个用户在同一时刻被多个 JMeter 线程重复使用，从而把 ORDER_REPEAT_SUBMIT 当成系统容量瓶颈。

第一版规则：

```text
USER_COUNT >= max(1000, THREADS × 4)
```

其中：

- `THREADS × 4` 给并发调度留出余量；
- 最低 1000 个用户，避免低线程档位的数据分布过于集中；
- 同一个 JWT 可以在后续非并发请求中复用；
- 每次提交仍必须获取并消费新的 Idempotency Token。

如果实际测试证明用户 Guard 仍产生可观测冲突，再扩大用户池，而不是一开始准备几十万用户。

## 3. CSV 请求行数

当前 JMeter CSV：

```text
recycle = false
stopThread = true
shareMode = all
```

因此正式测试必须准备覆盖整轮测试的请求行数。

定义：

```text
EXPECTED_REQUESTS = TARGET_QPS × DURATION_SECONDS
ROWS = ceil(EXPECTED_REQUESTS × 1.20)
```

预留 20% 行数，避免调度误差或阶段调整导致 CSV 提前耗尽。

注意：

> ROWS 是请求样本数量，不等于 USER_COUNT。

CSV 可以循环复用用户池中的 JWT，但每一行代表一次独立请求样本。

## 4. 库存规模

Capacity Baseline 不能因为售罄提前结束。

第一版规则：

```text
STOCK_QUANTITY >= ceil(ROWS × QUANTITY × 1.10)
```

当前 `QUANTITY=1`，因此：

```text
STOCK_QUANTITY >= ceil(ROWS × 1.10)
```

也就是在 CSV 预留量基础上再增加 10% 库存安全余量。

库存每一档测试前重新初始化，禁止继承上一轮剩余库存。

## 5. 一致性校验

因为 `quantity=1`，一轮测试最终应满足：

```text
initial_stock
-
successful_orders
=
final_available_stock
```

同时需要校验：

- ticket_stock；
- ticket_stock_bucket 汇总；
- Redis 可售库存；
- SUCCESS 订单数量；
- FAILED / COMPENSATED 数量；
- 是否存在长期 QUEUED / PROCESSING。

这里的“successful_orders”最终以正式订单和订单请求状态收敛结果为准，不能用 HTTP 200 数量替代。

## 6. 当前 JMeter 资产

脚本已经按 Capacity Baseline V1 拆分职责。

### 数据准备

`prepare-async-order-jmeter-data.sh` 支持：

```text
WAITING_ROOM_ENABLED=false
```

此时：

- 不生成 admissionToken；
- 不写 Waiting Room Redis Key；
- CSV 仍保留 admissionToken 列为空，保持两类测试共用同一字段结构。

Flash-Sale Baseline 显式设置：

```text
WAITING_ROOM_ENABLED=true
```

才生成一次性 admissionToken。

### Closed-loop Preflight

执行资产：

```text
scripts/load/run-preflight-jmeter.sh
scripts/jmeter/async-order-closed-loop.jmx
```

特点：

- 没有 Constant Throughput Timer；
- 线程数决定并发；
- CSV `recycle=true`，用户池可以重复使用；
- `POLL_RESULT=false`；
- 只用于估算 `Q_probe_peak`，不进入正式 Benchmark。

### Formal Target-Rate

执行资产：

```text
scripts/load/run-async-order-jmeter.sh
scripts/jmeter/async-order-target-rate.jmx
```

特点：

- TARGET_QPS 必须显式传入；
- Constant Throughput Timer 只控制异步提交 sampler；
- 这是有限线程下的 Target-Rate 近似，不是严格 open workload；正式结果必须同时记录 TARGET_QPS 与实际 attempt TPS；
- CSV `recycle=false`；
- 正式运行默认不轮询异步结果；
- CSV 行数至少覆盖理论请求量，仍建议保留 20% 余量。

### 幂等 Token 辅助流量

Phase 1 V1 每个 Submit 前仍执行一次 `GET /api/orders/idempotency-token`。因此 Submit TPS 只统计异步提交 sampler，但 SUT 还承受约同量级的 Token HTTP / Redis 写流量。`Q_probe_peak` 描述的是这套请求模型下的容量数量级，不等于隔离 Submit 接口后的理论上限。

### HTTP 结果摘要

`summarize-jmeter-result.py` 只统计：

```text
02 提交异步下单请求
```

因此不会把幂等 Token 请求或结果查询混进 Submit TPS / P95 / P99。

旧 `run-burst-order-jmeter.sh` 已删除。固定的 300 / 2000 / 5000 / 10000 QPS 本机档位不能代替 Preflight 容量估算。

## 7. 当前已经锁定

Capacity Baseline V1：

| 变量 | 决策 |
| --- | --- |
| Show | 1 个 |
| Session | 1 个 |
| Ticket Category | 1 个热点票档 |
| Quantity | 1 |
| User Pool | `max(1000, THREADS × 4)` |
| CSV Rows | `TARGET_QPS × DURATION × 1.20` |
| Stock | `ROWS × 1.10` 以上 |
| Waiting Room | OFF |
| Result Polling | OFF |
| Message Mode | RocketMQ Transaction Message |
| Spring Boot | 单实例 |

## 8. 下一步待讨论

1. 正式比例压力阶梯；
2. 单档持续时间与 Warm-up；
3. Formal Open-loop 下 THREADS 与 TARGET_QPS 的配比规则；
4. RocketMQ / JVM / Redis / MySQL 指标采集方式；
5. 稳定容量边界的具体判定阈值。
