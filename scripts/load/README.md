# Load Test Scripts

本目录只保留当前性能工程需要的可执行资产。不要再按“本机 300 / 2000 / 5000 / 10000 QPS 档位”理解系统容量。

## 职责

| 文件 | 职责 | 推荐运行位置 |
| --- | --- | --- |
| `ensure-load-users.sh` | 准备可复用压测用户池 | Load Generator 或能访问 SUT HTTP 的机器 |
| `prepare-async-order-jmeter-data.sh` | 登录用户并生成 JMeter CSV；Capacity Baseline 默认不生成 Waiting Room Token | Load Generator |
| `run-preflight-jmeter.sh` | Closed-loop Preflight，按线程阶梯估算 `Q_probe_peak` | Mac Load Generator |
| `run-async-order-jmeter.sh` | Open-loop 正式 Baseline，显式指定目标 QPS | Mac Load Generator |
| `summarize-jmeter-result.py` | 只统计 `02 提交异步下单请求` 的 Submit TPS / P95 / P99 | Load Generator |
| `reset-load-test-env.sh` | 清理交易数据、重置库存、清 Redis、重新预热 | Windows SUT |
| `prepare-soldout-flood-env.sh` | Flash-Sale 售罄洪峰环境准备 | 能直连 MySQL / Redis 的 SUT 侧 |
| `run-soldout-flood-jmeter.sh` | Flash-Sale Baseline 的售罄洪峰入口压力 | Load Generator |

旧 `run-burst-order-jmeter.sh` 已删除。固定机器档位会把“脚本预设值”误当成系统容量，与 Phase 1 的 Preflight 方法冲突。

## JMeter 计划

- `scripts/jmeter/async-order-closed-loop.jmx`：Preflight；无吞吐 Timer，CSV 可循环复用用户池。
- `scripts/jmeter/async-order-open-loop.jmx`：正式目标 QPS；Constant Throughput Timer 只限制异步提交 sampler。

两个计划默认都关闭结果轮询，且 GUI Listener 默认禁用。

## Capacity Baseline 数据准备

用户池：

```text
USER_COUNT >= max(1000, THREADS × 4)
```

正式 Open-loop：

```text
EXPECTED_REQUESTS = TARGET_QPS × DURATION_SECONDS
ROWS >= ceil(EXPECTED_REQUESTS × 1.20)
STOCK >= ceil(ROWS × 1.10)
QUANTITY = 1
WAITING_ROOM_ENABLED = false
POLL_RESULT = false
```

例子中的 QPS 不在脚本里写死。先执行 Preflight，再由 `Q_probe_peak` 决定正式 `Q_start`。

## Preflight

先准备用户池 CSV：

```bash
USER_COUNT=1000 \
ROWS=1000 \
QUANTITY=1 \
WAITING_ROOM_ENABLED=false \
OUT_FILE=/tmp/async-order-users-preflight.csv \
./scripts/load/prepare-async-order-jmeter-data.sh
```

再按线程阶梯执行：

```bash
THREADS=32  ./scripts/load/run-preflight-jmeter.sh
THREADS=64  ./scripts/load/run-preflight-jmeter.sh
THREADS=128 ./scripts/load/run-preflight-jmeter.sh
```

只有前一档仍明显线性增长时才继续更高线程。

## Formal Open-Loop

根据 Preflight 得到 `Q_probe_peak` 后，计算正式起点约为：

```text
Q_start ≈ 0.50 × Q_probe_peak
```

准备对应 CSV 后显式运行：

```bash
THREADS=<本档线程数> \
TARGET_QPS=<本档目标QPS> \
DURATION_SECONDS=180 \
./scripts/load/run-async-order-jmeter.sh
```

脚本不会提供一个“看起来合理”的默认 TARGET_QPS，避免把默认值误认为容量结论。

## 结果口径

JMeter HTML 仍保留完整请求视图，但正式 Submit TPS 不使用 All Samples。

`summarize-jmeter-result.py` 只读取：

```text
02 提交异步下单请求
```

并输出：

- attempt TPS；
- accepted Submit TPS；
- Error Rate；
- P50 / P95 / P99 / Max。

Order Creation TPS、RocketMQ Accumulation、Redis / MySQL / JVM 指标必须单独采集，不能从 JMeter Submit TPS 推导。
