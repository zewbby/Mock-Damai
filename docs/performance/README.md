# 性能测试入口

项目当前处于 **Performance Engineering / Phase 1 — Baseline**。本目录保存稳定入口；阶段性实验设计继续在 `performance-engineering` 分支演进。

## 当前执行资产

| 目的 | 文件 |
| --- | --- |
| Closed-loop Preflight | `scripts/load/run-preflight-jmeter.sh` + `scripts/jmeter/async-order-closed-loop.jmx` |
| Formal Open-loop Baseline | `scripts/load/run-async-order-jmeter.sh` + `scripts/jmeter/async-order-open-loop.jmx` |
| 数据准备 | `scripts/load/prepare-async-order-jmeter-data.sh` |
| 环境重置 | `scripts/load/reset-load-test-env.sh` |
| Submit 指标摘要 | `scripts/load/summarize-jmeter-result.py` |
| Flash-Sale 售罄洪峰 | `scripts/load/prepare-soldout-flood-env.sh` + `run-soldout-flood-jmeter.sh` |

脚本详细职责见 [scripts/load/README.md](../../scripts/load/README.md)。

阶段文档入口：

https://github.com/zewbby/Mock-Damai/tree/performance-engineering/docs/performance-engineering

## Phase 1 不变口径

- MacBook Air M4：Load Generator；
- Windows：单实例 Spring Boot + MySQL + Redis + RocketMQ；
- Capacity Baseline：Waiting Room / Rate Limit / Risk Control / Activity Isolation / Backpressure OFF；
- In-Flight Control 保持 ON，并冻结足够高的阈值；
- `quantity=1`；
- 默认交易命令链路：RocketMQ Transaction Message；
- `POLL_RESULT=false`；
- Submit TPS 与 Order Creation TPS 分开统计；
- Oversell 必须为 0。

## Preflight 与正式 Baseline

Preflight 使用 **Closed-loop**：不设置目标 QPS，由线程数驱动并发，只用于找到当前 SUT 的容量数量级。

```text
32 threads
64 threads
128 threads
256 threads（仅前一档仍明显线性增长时）
```

得到 `Q_probe_peak` 后：

```text
Q_start ≈ round_practical(Q_probe_peak × 0.50)
```

正式 Baseline 使用 **Open-loop**，围绕容量边界设置目标 QPS，而不是从任意固定的 10 / 20 / 50 / 100 / 200 QPS 开始。

## 数据准备

Capacity Baseline：

```text
USER_COUNT >= max(1000, THREADS × 4)
ROWS >= ceil(TARGET_QPS × DURATION × 1.20)
STOCK >= ceil(ROWS × 1.10)
WAITING_ROOM_ENABLED=false
```

`prepare-async-order-jmeter-data.sh` 在 Waiting Room 关闭时不会再写无关 admissionToken Redis Key。

## 指标

| 指标 | 定义 |
| --- | --- |
| Submit TPS | `POST /api/orders/async` 被接受的提交吞吐 |
| Order Creation TPS | 异步消费者真正创建正式订单的吞吐 |
| P95 / P99 | 异步提交 sampler 的响应延迟 |
| Error Rate | 业务拒绝与系统失败分开统计 |
| RocketMQ Accumulation | 峰值积压与停止施压后的回落时间 |
| Oversell | 必须为 `0` |
| Convergence | 请求状态、MQ 积压和 Redis / MySQL 库存最终收敛时间 |

JMeter HTML 的 All Samples 包含幂等 Token 请求，不能直接当 Submit TPS。正式 HTTP 侧摘要使用：

```bash
python3 scripts/load/summarize-jmeter-result.py reports/.../result.jtl
```

## 结果保存

大型 JTL、JMeter HTML 和运行日志继续放在本地 `reports/`，不提交仓库。正式结果文档记录 commit、配置、环境、数据规模、压力参数、聚合指标和一致性结论。
