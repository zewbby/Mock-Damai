# Load Test Scripts

本目录只保留当前 Performance Engineering / Phase 1 需要的可执行资产。脚本参数不是容量结论；正式结果必须由 Preflight、Target-Rate Run 和 SUT 指标共同解释。

## 照着做：单热点票档 Capacity Baseline

以下步骤用于测**核心交易链路容量**。示例环境是 Windows 运行单实例应用、MySQL、Redis、RocketMQ，Mac 运行 JMeter；票档 `2`、每单 `1` 张、压测账号池 `5000` 人。把示例 IP 和密码换成自己的实际值。`5000` 是可轮换的账号数，`THREADS=128` 才是 JMeter 并发线程数。两台机器使用同一版本的压测脚本，并记录 `git rev-parse HEAD`、`git status --short`、测试时间和网络连接方式。

**只在专用压测库操作。** `reset-load-test-env.sh` 会清空整个库中的交易、消息和补偿表，还会删除相关 Redis Key；先执行不带 `CONFIRM_RESET=YES` 的预览命令，核对目标库和票档。每一档测试结束后，先保存结果，等 MQ 积压清空、请求进入终态，再重置并开始下一档；不能清理仍在处理的请求，也不能沿用上一档的剩余库存。

### 1. Windows：启动被测服务

在**启动应用的环境**设置以下变量，然后启动或重启应用。若使用 JDK 21、Maven 和 Git Bash，可在仓库根目录运行；若从 IDE 或其他方式启动，把同样的变量配置到那个启动方式中。已经运行的进程不会因另一个终端 `export` 而改变配置。

```bash
export SPRING_PROFILES_ACTIVE=local,flash-sale
export SMART_TICKET_WAITING_ROOM_ENABLED=false
export SMART_TICKET_RATE_LIMIT_ENABLED=false
export SMART_TICKET_RISK_CONTROL_ENABLED=false
export SMART_TICKET_ACTIVITY_ISOLATION_ENABLED=false
export SMART_TICKET_RATE_LIMIT_BACKPRESSURE_ENABLED=false
mvn spring-boot:run
```

确保 MySQL、Redis、RocketMQ 均可用；保留 `flash-sale` 所要求的 In-Flight Control，以及 JWT、幂等、Redis 预扣、事务消息、MySQL 最终扣减。容量测试期间冻结配置，不在各档之间调参。在 Windows 的另一个 Git Bash 终端执行以下步骤。

### 2. Windows：准备账号，并在每档开始前重置

在仓库根目录执行。首次准备账号，或需要确认现有 5000 个账号都能用同一密码登录时，执行 `ensure-load-users.sh`；Mac 随后必须使用相同的手机号前缀和密码。输入密码时终端不回显。

```bash
export BASE_URL=http://127.0.0.1:8081
curl -fsS "$BASE_URL/actuator/health" | jq -r '.status'  # 预期 UP
read -r -s -p '压测用户密码: ' USER_PASSWORD; echo
export USER_PASSWORD
USER_COUNT=5000 ./scripts/load/ensure-load-users.sh
unset USER_PASSWORD
read -r -s -p 'MySQL 密码: ' SMART_TICKET_DB_PASSWORD; echo
read -r -s -p '管理员密码: ' SMART_TICKET_ADMIN_PASSWORD; echo
export SMART_TICKET_DB_PASSWORD SMART_TICKET_ADMIN_PASSWORD
```

Preflight 示例准备 `100000` 张，避免像之前的 `10000` 张那样在测试末尾碰到库存边界。**每次运行 32、64、128 线程中的任一档之前**，都先在 Windows 执行下面两条；第一条只预览，第二条才会清理专用压测环境。如果预估 60 秒内的提交量可能超过库存，先提高库存；Preflight 的 CSV 本身会循环。

```bash
RESET_STOCK_QUANTITY=100000 ./scripts/load/reset-load-test-env.sh
CONFIRM_RESET=YES RESET_STOCK_QUANTITY=100000 ./scripts/load/reset-load-test-env.sh
```

重置输出应显示票档 `2` 的 `total_stock`、`available_stock` 都为 `100000`，并显示当前 bucket 版本和汇总库存。若实际使用其他票档，同时设置 `TICKET_CATEGORY_ID`，且 Mac 生成数据时也使用相同 ID。

### 3. Mac：检查工具并准备 Preflight 数据

在 Mac 上进入同一仓库的根目录，用 Bash 执行。需要 `jmeter`、`python3`、`jq`、`curl`、`awk`；如果 `jmeter` 未加入 `PATH`，设置 `JMETER_BIN=/实际路径/apache-jmeter/bin/jmeter`，再检查该路径可执行。JMeter 正式压测由下方脚本以 [CLI 模式](https://jmeter.apache.org/usermanual/get-started.html)运行。当前正式 Target-Rate runner 只支持 CSV，意味着 JWT 会暂存在 Mac 的 `/tmp`；若压测机禁止落盘 JWT，现有 runtime-auth 计划只能做 Preflight，不能冒充正式 Target-Rate 结果。

```bash
bash
cd /path/to/Mock-Damai
export BASE_URL=http://192.168.1.100:8081
export JMETER_BIN="${JMETER_BIN:-jmeter}"
command -v "$JMETER_BIN" python3 jq curl awk
"$JMETER_BIN" -v
curl -fsS "$BASE_URL/actuator/health" | jq -r '.status'  # 预期 UP
umask 077
read -r -s -p '压测用户密码: ' USER_PASSWORD; echo
export USER_PASSWORD
USER_COUNT=5000 ROWS=5000 QUANTITY=1 WAITING_ROOM_ENABLED=false \
  OUT_FILE=/tmp/async-order-users-preflight.csv \
  ./scripts/load/prepare-async-order-jmeter-data.sh
unset USER_PASSWORD
```

CSV 包含 JWT，保存在 Mac 的 `/tmp`，不提交仓库；过期后重新生成。Preflight JMX 的 CSV **会循环**（`recycle=true`），这里每个账号先写一行，跑完整个线程档位时重复使用账号。若同一账号过快重用，触发 `ORDER_REPEAT_SUBMIT`，增加账号池、重新准备 CSV 后重测该档。

### 4. Mac：跑 Preflight，确定正式起点

Windows 每次先按第 2 步重置；Mac 每次只运行一条。默认每档 Ramp-up `10s`、Warm-up `20s`、测量 `40s`，输出会自动排除 Warm-up。

```bash
THREADS=32  ./scripts/load/run-preflight-jmeter.sh
# Windows 重置库存后：
THREADS=64  ./scripts/load/run-preflight-jmeter.sh
# Windows 再次重置库存后：
THREADS=128 ./scripts/load/run-preflight-jmeter.sh
```

记录每档输出的 `attempt_tps`、`submit_tps`、`failed`、`p95_ms`、`p99_ms` 和下面第 6 步的服务端指标。只有前一档仍健康、加线程后吞吐继续明显上升，才继续更高线程。若出现库存不足、重复提交、HTTP/连接错误，先排因并重测，不把该档当作 `Q_probe_peak`。取保持健康且增线程后吞吐增幅开始变小的最高 Submit TPS 作为近似 `Q_probe_peak`。

### 5. 正式 Target-Rate：算数据、重置、再运行

第一档 `TARGET_QPS` 取新测得的 `Q_probe_peak` 约一半，后续可按 `0.70`、`0.85`、`1.00`、`1.15` 倍逐档测试。**下面的 `100 QPS` 仅是假设 Preflight 得到 `Q_probe_peak≈200` 的算例，不是本机已验证容量。** 此例设 `THREADS=128`、`DURATION_SECONDS=180`、`RAMP_SECONDS=WARMUP_SECONDS=30`：

```text
CSV 行数 >= ceil(100 × 180 × 1.20) = 21600
初始库存 >= ceil(21600 × 1 × 1.10) = 23760；示例取 30000
用户数 >= max(1000, 128 × 4) = 1000；这里沿用 5000
```

先在 Mac 为该档重新生成 CSV（密码与 Windows 准备账号时相同）：

```bash
read -r -s -p '压测用户密码: ' USER_PASSWORD; echo
export USER_PASSWORD
USER_COUNT=5000 ROWS=21600 QUANTITY=1 WAITING_ROOM_ENABLED=false \
  OUT_FILE=/tmp/async-order-users-formal.csv \
  ./scripts/load/prepare-async-order-jmeter-data.sh
unset USER_PASSWORD
```

再在 Windows 专用压测环境中预览并重置为 `30000` 张；确认输出的初始库存正确，才回 Mac 运行：

```bash
# Windows Git Bash：
RESET_STOCK_QUANTITY=30000 ./scripts/load/reset-load-test-env.sh
CONFIRM_RESET=YES RESET_STOCK_QUANTITY=30000 ./scripts/load/reset-load-test-env.sh
```

```bash
# Mac Bash：
THREADS=128 TARGET_QPS=100 RAMP_SECONDS=30 WARMUP_SECONDS=30 \
  DURATION_SECONDS=180 ./scripts/load/run-async-order-jmeter.sh
```

每换一个 `TARGET_QPS`，按上面的公式重新计算 `ROWS`、库存，重新生成 CSV，并先在 Windows 重置；同一候选稳定档位至少重复 3 次，每次也重置。正式 JMX 不循环使用 CSV，runner 会校验账号数和至少 20% 的行数余量。`WARMUP_SECONDS` 必须不小于 `RAMP_SECONDS`，并且小于 `DURATION_SECONDS`。

### 6. 同时记录服务端指标，再判断结果

Mac 另开一个终端，**测量开始前启动**下面的采样，结束后按 `Ctrl+C`；应用的 Actuator Counter 是进程启动后的累计值，相邻两次 `order.created.count` 之差除以两次采样的实际间隔（约 10 秒），才是该时间段的创单 TPS。结果文件会留在 `reports/`，与 JTL 一起保存。

```bash
cd /path/to/Mock-Damai
BASE_URL=http://192.168.1.100:8081
mkdir -p reports
SAMPLE_FILE="reports/order-created-$(date +%Y%m%d-%H%M%S).txt"
while true; do
  printf '%s ' "$(date '+%F %T')"
  curl -fsS "$BASE_URL/actuator/metrics/order.created.count" |
    jq -r '.measurements[] | select(.statistic == "COUNT") | .value'
  sleep 10
done | tee "$SAMPLE_FILE"
```

每档开始和结束时也记下 `rate.limit.rejected.count`、`async.order.request.failed.count` 和 `soldout.fastfail.count` 的值。它们也是累计 Counter，要看**本档增量**。在 RocketMQ 的 Consumer 监控中记录实际消费者组（默认 `smart-ticket-async-order-create`）的积压量及测试停止后清空所需时间；已有 `mqadmin` 时，可在 Windows 运行 `mqadmin consumerProgress -g smart-ticket-async-order-create -n <NameServer地址:端口>` 查看，参数见 [RocketMQ 官方说明](https://rocketmq.apache.org/docs/cli-admin-tool/)。同步记录 Windows 的 CPU/内存、JVM/GC、MySQL 连接与慢查询、Redis 资源。每档结束后，在 Windows 的 MySQL 客户端执行以下**只读**查询，保存输出；所有请求进入终态、MQ 积压清空后，才运行下一次重置。

```bash
# Mac：每档开始和结束各执行一次，分别保存输出
for metric in rate.limit.rejected.count async.order.request.failed.count soldout.fastfail.count; do
  printf '%s ' "$metric"
  curl -fsS "$BASE_URL/actuator/metrics/$metric" |
    jq -r '.measurements[] | select(.statistic == "COUNT") | .value'
done
```

```sql
SELECT status, COUNT(*) FROM ticket_order_request GROUP BY status;
SELECT ticket_category_id, total_stock, available_stock, locked_stock, sold_stock
FROM ticket_stock WHERE ticket_category_id = 2;
SELECT bucket_version, SUM(total_stock), SUM(available_stock),
       SUM(locked_stock), SUM(sold_stock)
FROM ticket_stock_bucket WHERE ticket_category_id = 2 GROUP BY bucket_version;
SELECT COUNT(*) FROM ticket_order WHERE ticket_category_id = 2;
```

其中 `INIT`、`PRE_DEDUCTED`、`QUEUED`、`PROCESSING` 不应长期残留；有失败或补偿记录时继续核对原因和库存。正式订单数及最终库存要用这些结果判断，不能用 JMeter 的 HTTP 成功数代替。

runner 会打印结果目录；正式运行的 `result.jtl`、`jmeter.log`、`html/index.html` 位于 `reports/jmeter-baseline/<RUN_ID>/`。看 `02 提交异步下单请求` 的 `attempt_tps`、`submit_tps`、错误率及 P95/P99；`01 获取下单幂等 Token` 是额外的实际服务流量。`order.created.count` 的时间序列只取 Warm-up 结束到测试结束的区间计算创单 TPS，测试结束后继续观察积压何时清空。`target_achievement_percent` 应接近 100%，偏离目标超过 5% 的档位不能说“命中了目标 QPS”。HTTP 提交成功只说明请求被接受，**不是正式订单已生成**；若 MQ 积压持续增长、停止后不能清空，或请求状态/库存不收敛，该档不能算稳定容量。

若某档出现业务断言失败，用少量线程在 GUI 的 View Results Tree 看响应 JSON 的 `code/message` 来定位，再回到 CLI 重跑；不要只凭 JTL 的固定断言提示判断“限流”或“库存不足”。这里的目标是 Capacity Baseline；验证等待室、限流和售罄保护时，按下方 Flash-Sale 场景另测。

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

其中 `ROWS` 和对应的 `STOCK` 公式适用于不循环 CSV 的 Formal Target-Rate；Preflight 要保证库存覆盖整档实际提交量。Formal runner 会检查 CSV 中不同 authToken 的数量以及 20% 行数余量。

## Preflight

```bash
USER_COUNT=5000 ROWS=5000 QUANTITY=1 WAITING_ROOM_ENABLED=false \
OUT_FILE=/tmp/async-order-users-preflight.csv \
./scripts/load/prepare-async-order-jmeter-data.sh

THREADS=32  ./scripts/load/run-preflight-jmeter.sh
THREADS=64  ./scripts/load/run-preflight-jmeter.sh
THREADS=128 ./scripts/load/run-preflight-jmeter.sh
```

Preflight 的 CSV 会循环；默认 Ramp-up 10s、Warm-up 20s、Measure 40s。每档前按上方步骤重置库存，只有前一档仍明显增长时才继续更高线程。


### Runtime Auth（Mac 不落盘用户数据）

当压测机不允许保存用户 CSV/JWT 时，先在 Mac 按上方 `read -r -s` 的方式设置并 `export USER_PASSWORD`，再使用 runtime-auth 计划：

```bash
USER_COUNT=5000 BASE_URL=http://<WIN_IP>:8081 THREADS=32 \
  ./scripts/load/run-preflight-runtime-auth-jmeter.sh
```

该模式在 setUp 阶段登录 Windows 端已存在的压测用户，JWT 仅保存在 JMeter JVM 内存中；不生成用户 CSV，不把 JWT 写入 JTL。用户账号仍由 Windows 端的 MySQL 保存。目前仓库没有 runtime-auth 的正式 Target-Rate runner；该模式只用于 Preflight。

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

当前本地压测基线默认为 `mock_damai` 数据库、票档 `2`、库存 `10000`。首次使用前设置数据库和后台密码；之后每轮压测结束直接执行重置命令：

```bash
export SMART_TICKET_DB_PASSWORD='<本地数据库密码>'
export SMART_TICKET_ADMIN_PASSWORD='<本地管理员密码>'
CONFIRM_RESET=YES ./scripts/load/reset-load-test-env.sh
```

需要临时使用其他数据库、票档或库存时，可通过 `DB_NAME`、`TICKET_CATEGORY_ID`、`RESET_STOCK_QUANTITY` 覆盖默认值。脚本会清理交易表和相关 Redis 压测状态，保留压测用户与生成的 CSV。

Redis 使用 Docker 时不需要在宿主机安装 `redis-cli`。脚本会优先使用宿主机命令；找不到时，按映射到 `REDIS_PORT`（默认 `6379`）的容器自动执行容器内的 `redis-cli`。如果该端口对应多个容器，通过 `REDIS_CONTAINER_NAME=<容器名>` 明确指定。

Windows Git Bash 找不到 `mysql` 时，脚本会自动查找 `C:\Program Files\MySQL\MySQL Server *\bin\mysql.exe`；非标准安装路径可通过 `MYSQL_BIN=<mysql.exe路径>` 指定。

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
