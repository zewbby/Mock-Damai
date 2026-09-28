# Load Test Scripts

本目录只保留当前 Performance Engineering / Phase 1 需要的可执行资产。脚本参数不是容量结论；正式结果必须由 Preflight、Target-Rate Run 和 SUT 指标共同解释。

## 文件职责

| 文件 | 处理 | 职责 |
| --- | --- | --- |
| `ensure-load-users.sh` | 保留 | 准备可复用压测用户池 |
| `prepare-async-order-jmeter-data.sh` | 保留 | 登录用户并生成 CSV；Capacity Baseline 不生成 admissionToken |
| `run-preflight-jmeter.sh` | 重写 | Closed-loop Preflight，显式线程数，估算 `Q_probe_peak` |
| `run-async-order-jmeter.sh` | 重写 | Formal Target-Rate Capacity Baseline |
| `summarize-jmeter-result.py` | 重写 | Submit TPS、延迟、Target 命中率和 Token 辅助流量 |
| `reset-load-test-env.sh` | 重写 | 清交易状态，通过当前库存初始化接口重建 bucket |
| `prepare-soldout-flood-env.sh` | 重写 | 按显式 Flash-Sale 参数准备库存、用户和 admissionToken |
| `run-soldout-flood-jmeter.sh` | 重写 | Flash-Sale Sold-Out Flood；不再内置固定压力档 |

## JMeter 计划

- `scripts/jmeter/async-order-closed-loop.jmx`：Preflight，没有吞吐 Timer。
- `scripts/jmeter/async-order-target-rate.jmx`：正式目标速率，Constant Throughput Timer 只作用于 `02 提交异步下单请求`。

旧 `async-order-open-loop.jmx` 已删除。Constant Throughput Timer + 有限线程只能做 rate-controlled approximation，不应表述成严格 Open Loop。

两个 JMX 直接在 GUI 打开时只保留安全 Smoke 默认值：1 thread、短时运行；Target-Rate 默认约 1 QPS。正式性能测试必须通过 runner 显式传参。

## Capacity Baseline 固定规则

```text
USER_COUNT >= max(1000, THREADS × 4)
ROWS >= ceil(TARGET_QPS × DURATION_SECONDS × 1.20)
STOCK >= ceil(ROWS × QUANTITY × 1.10)
QUANTITY = 1
WAITING_ROOM_ENABLED = false
POLL_RESULT = false
```

Formal runner 会检查 CSV 中不同 authToken 的数量以及 20% 行数余量。

## Preflight

```bash
USER_COUNT=1000 ROWS=1000 QUANTITY=1 WAITING_ROOM_ENABLED=false \
OUT_FILE=/tmp/async-order-users-preflight.csv \
./scripts/load/prepare-async-order-jmeter-data.sh

THREADS=32  ./scripts/load/run-preflight-jmeter.sh
THREADS=64  ./scripts/load/run-preflight-jmeter.sh
THREADS=128 ./scripts/load/run-preflight-jmeter.sh
```

Preflight 默认 Ramp-up 10s、Warm-up 20s、Measure 40s。只有前一档仍明显增长时才继续更高线程。

## Formal Target-Rate

由 `Q_probe_peak` 决定正式起点：

```text
Q_start ≈ 0.50 × Q_probe_peak
```

正式运行必须显式传入参数：

```bash
THREADS=<线程数> TARGET_QPS=<目标Submit QPS> \
RAMP_SECONDS=30 WARMUP_SECONDS=30 DURATION_SECONDS=180 \
./scripts/load/run-async-order-jmeter.sh
```

`WARMUP_SECONDS` 必须至少覆盖 Ramp-up，并小于总 Duration。统计脚本会排除 Warm-up。

Constant Throughput Timer 不是严格 open workload。结果必须同时记录 `TARGET_QPS`、`attempt_tps`、`target_achievement_percent`。偏差超过 5% 时摘要会给 warning。

## 幂等 Token 流量

Phase 1 V1 仍采用：

```text
GET /api/orders/idempotency-token
↓
POST /api/orders/async
```

因此 Submit 指标只统计第二步，但 SUT 实际还承受约同量级的 Token HTTP / Redis 写流量。`Q_probe_peak` 是这套请求模型下的容量数量级，不是隔离 Submit 接口后的理论上限。

## 环境重置

`reset-load-test-env.sh` 不再按 `bucket_version=1` 自己分配 bucket。它清理交易状态和目标票档旧 bucket 后，调用当前库存初始化接口，让应用按实际 `activeVersion + defaultBucketCount` 重建并预热。

Capacity Baseline 默认保留生成好的 CSV；Flash-Sale admissionToken 是一次性的，每轮仍必须重新生成。

## Flash-Sale Sold-Out Flood

Flash-Sale 不再内置库存、请求量或 QPS 默认场景。准备和执行时显式传参数：

```bash
STOCK_QUANTITY=<库存> USER_COUNT=<用户池> THREADS=<线程数> \
TARGET_QPS=<目标Submit QPS> DURATION_SECONDS=<秒数> \
./scripts/load/prepare-soldout-flood-env.sh

STOCK_QUANTITY=<同一库存> ORDER_QUANTITY=1 THREADS=<同一线程数> \
TARGET_QPS=<同一目标QPS> DURATION_SECONDS=<同一秒数> \
./scripts/load/run-soldout-flood-jmeter.sh
```

## 结果口径

`summarize-jmeter-result.py` 以 `02 提交异步下单请求` 计算 Submit 延迟和吞吐，同时报告 `01 获取下单幂等 Token` 的辅助请求速率。

JMeter 不能替代 SUT 指标。正式结果仍需单独采集 Order Creation TPS、RocketMQ Accumulation、Redis/MySQL/JVM 资源、请求最终状态、库存一致性和 Oversell Count。

大型 JTL、HTML 和日志继续放在 `reports/`，不提交仓库。
