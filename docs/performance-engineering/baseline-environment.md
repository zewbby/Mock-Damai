# Phase 1 — Baseline Environment

> 状态：已确认（硬件明细待补齐）

## 1. 正式 Baseline 拓扑

正式 Baseline 使用两台物理机器，压测机与被测系统分离。

```text
┌────────────────────────┐
│ Load Generator         │
│                        │
│ MacBook Air M4         │
│ JMeter                 │
│                        │
│ 只负责制造请求与记录   │
│ HTTP 侧压测结果        │
└───────────┬────────────┘
            │
            │ LAN
            ▼
┌──────────────────────────────┐
│ System Under Test (SUT)      │
│                              │
│ Windows                      │
│                              │
│ Spring Boot Instance × 1     │
│ MySQL                        │
│ Redis                        │
│ RocketMQ                     │
└──────────────────────────────┘
```

## 2. 环境职责

### MacBook Air M4 — Load Generator

只运行：

- JMeter；
- 必要的结果采集脚本。

正式 Baseline 期间不在 Mac 上运行：

- Spring Boot；
- MySQL；
- Redis；
- RocketMQ。

这样可以避免 JMeter 与被测系统争抢 CPU、内存和 IO，降低压测机对结果的污染。

### Windows — System Under Test

运行：

- 单实例 Spring Boot；
- MySQL；
- Redis；
- RocketMQ。

Phase 1 明确保持 **单个 Spring Boot 实例**。

多实例部署属于 Phase 4，不提前混入 Phase 1。

## 3. Smoke Test 与正式 Baseline 的区别

开发阶段允许在同一台机器上做 Smoke Test，用于验证：

- JMeter 脚本能否运行；
- Token / admission 数据是否正确；
- RocketMQ 是否能够发送和消费；
- SQL 是否正常；
- 指标采集是否可用。

同机 Smoke Test 的结果：

> **不得进入正式 Benchmark。**

只有 Mac 作为 Load Generator、Windows 作为 SUT 的两机测试结果，才允许进入正式 Baseline Report。

## 4. 网络要求

优先使用稳定局域网连接。

优先级：

```text
有线 LAN
>
稳定 Wi-Fi LAN
>
其他网络环境
```

如果正式测试使用 Wi-Fi，必须在结果中明确记录，因为网络抖动会直接影响 HTTP P95 / P99 / Max Latency。

## 5. 为什么 Phase 1 不继续拆分 MySQL / Redis / RocketMQ

Phase 1 的目标是建立当前“单机部署形态”的性能基线，而不是模拟生产集群。

因此暂时把：

```text
Spring Boot
MySQL
Redis
RocketMQ
```

共同视作一个 SUT。

如果 Phase 2 的 Profiling 证明某个中间件是主要瓶颈，再设计隔离实验，例如：

```text
MySQL 成为瓶颈
→ 将 MySQL 独立部署后复测

RocketMQ 成为瓶颈
→ 将 RocketMQ 独立部署后复测
```

这样才能形成有意义的 Before / After，而不是一开始就堆复杂部署。

## 6. 正式测试前必须补齐的硬件信息

### Load Generator

已知：

- Device: MacBook Air M4
- CPU: Apple M4

待正式执行时记录：

- CPU core；
- Memory；
- macOS version；
- JDK version；
- JMeter version。

### SUT

待记录：

- Windows version；
- CPU 型号与核心数；
- Memory；
- 磁盘类型；
- JDK version；
- MySQL version；
- Redis version；
- RocketMQ version。

这些信息属于测试结果的一部分，不允许省略。

## 7. Windows 换电脑部署（2026-10-10）

迁移分支：`codex/windows-migration-20261010`。用户确认新机器为 Windows，旧订单、账号与压测数据不迁移；新机器从 schema/data 初始化。源码与环境模板进 Git，数据库、实际密码、Token CSV 和日志不进 Git。

### 旧电脑实测清单

| 项目 | 2026-10-10 核对结果 |
| --- | --- |
| 仓库 | `D:\zewbbyTest\Mock-Damai`，原分支 `performance-engineering`，起点 `2ce1fbe` |
| Windows / CPU / 内存 | Windows 11 amd64；i5-12600KF，10 核 16 线程；31.8 GiB |
| JDK / Maven | Microsoft OpenJDK 21.0.12.1；Maven 3.9.16 |
| Maven 安装位置 | `D:\zewbbyTest\maven-3.9.16\apache-maven-3.9.16`（工具可共享，不随项目删除） |
| MySQL | Windows 服务 `MySQL84`，8.4.11，3306；配置 `D:\zewbbyTest\mysql\data\my.ini`；数据 `D:\zewbbyTest\mysql\data\Data` |
| 数据库 | `mock_damai`；库列表除系统库外只发现此库；核对时有 6417 条正式订单 |
| Redis | 容器 `Mock-Damai`，`redis:7.2-alpine`，6379，DB 0，无密码；匿名 `/data` 卷 |
| RocketMQ | `apache/rocketmq:5.3.2`；`mock-damai-namesrv` 9876；`mock-damai-broker` 10909/10911/10912 |
| Broker 配置 | `D:\zewbbyTest\rocketmq\broker.conf`，实际内容为 `brokerIP1=127.0.0.1`；网络 `mock-damai-net` |
| Docker Engine | 29.8.1；另有 HnuHole 容器和数据卷，不能使用全局 prune 或整体删除 Docker/WSL |
| 应用 | JDK 21，Spring Boot 3.5.13，默认 `local,flash-sale`，HTTP 8081；核对时无 Java 应用进程 |
| Kafka | 本机未部署；忽略的 local 配置实际关闭 `domain-event.enabled` 和 `local-message.sender-enabled` |
| 压测附加工具 | Git Bash、curl、jq、Python 3、MySQL 客户端、JMeter；JMeter 压测机使用 Mac，版本需在正式运行时另行记录 |

README 的旧 Quick Start 库名 `smart_ticket_lite` 与实际 local 模板不一致，已改为 `mock_damai`。历史库的说明继续保留在 SQL 文档中。迁移前存在压测 README 删除、Kafka producer 幂等默认 false 的本地修改；原始 patch 已暂存在忽略的 reports 中，迁移分支恢复已提交版本，不携带该可靠性参数修改。

### 新电脑准备

安装 Git（含 Git Bash）、JDK 21、Maven 3.9+、Docker Desktop，启用 WSL2/Linux containers。设置 `JAVA_HOME`，将 Java/Maven 加入 PATH；重新打开 **Windows PowerShell**，检查 `git --version`、`java -version`、`mvn -version`、`docker version`、`docker compose version`。Docker 命令必须能访问 Server。无需另装 Windows MySQL/Redis/RocketMQ 服务。

在 PowerShell 执行：

```powershell
New-Item -ItemType Directory -Force D:\zewbbyTest | Out-Null
Set-Location D:\zewbbyTest
git clone --branch codex/windows-migration-20261010 https://github.com/zewbby/Mock-Damai.git
Set-Location .\Mock-Damai
powershell -ExecutionPolicy Bypass -File scripts/windows/start.ps1
powershell -ExecutionPolicy Bypass -File scripts/windows/start.ps1 -App
```

第一条脚本命令创建并复用本地随机密钥 `deploy/windows/.env.local`（Git 忽略），首次复制 local 配置模板，再启动 MySQL/Redis/NameServer/Broker，并等待 MySQL schema 就绪、Redis healthy、Broker `ACTIVATED=true`。第二条加载相同密钥与环境变量，在当前窗口前台运行应用。`Ctrl+C` 停应用。不能只凭容器 `Up` 判定就绪。

新部署统一使用独立端口，避免与本机已有服务冲突：

| 服务 | 宿主机地址 | 容器端口 |
| --- | --- | --- |
| MySQL | 127.0.0.1:13306 | 3306 |
| Redis | 127.0.0.1:16379 | 6379 |
| NameServer | 127.0.0.1:19876 | 9876 |
| Broker | 127.0.0.1:20909/20911/20912 | 同宿主机 |
| 应用 | http://127.0.0.1:8081 | 宿主机 Java 进程 |

Compose project 为 `mock-damai-migration`，四个命名卷由此项目独立拥有。Broker 公布 `127.0.0.1:20911`，适配本机宿主机 Java；若把 Java 改为容器或远端部署，必须同步改公布地址。Windows 新部署 MySQL 镜像使用 `mysql:8.4`；tag 可能更新，正式性能测试需额外记录镜像 digest、实际 `SELECT VERSION()` 和资源限制，旧机性能结果不能直接当新机器基线。

默认启动保留 flash-sale 的库存、事务消息、幂等、限流和等待室策略；只显式关闭本地未部署的 Kafka 领域事件与 sender。没有把 Kafka 假定为已经可用。若后续开启领域事件，先按源码配置准备 Kafka 再打开对应开关。

在第二个 PowerShell 窗口检查：

```powershell
Set-Location D:\zewbbyTest\Mock-Damai
Invoke-RestMethod http://127.0.0.1:8081/actuator/health
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml ps
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml exec -T redis redis-cli PING
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml exec -T broker sh mqadmin clusterList -n namesrv:9876
```

预期为 health `UP`、Redis `PONG`、Broker activated 为 true。进一步使用 `docs/api/` 的样例登录/注册和查询演出；样例密码从 API 样例核对。完整抢票、支付、补偿与正式容量验收须另行执行，健康检查不能替代这些验收。

### 常见失败与停止

- 下载镜像失败：核对 Docker Desktop 网络，再运行同一命令，不要清卷掩盖失败。
- 端口占用：用 `Get-NetTCPConnection -LocalPort 13306,16379,19876,20911,8081` 排查；修改 Compose 端口时要同步脚本连接地址和 Broker 公布端口。
- 就绪超时：执行下面 logs 命令；SQL 初始化只对空 MySQL 卷执行。已有数据卷重启不会重新导入 SQL。
- 丢失 `.env.local` 但已有 MySQL 数据：新随机密码不会修改已有数据库密码，恢复原文件或明确丢弃数据再重建。

```powershell
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml logs --tail 100
# 停止环境，保留数据
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml down
# 仅当明确要删除该部署的全部测试数据时
docker compose --env-file deploy/windows/.env.local -f deploy/windows/compose.yml down --volumes
```

`.env.local`、`application-local.yml`、生成的 JMeter CSV/JTL/报告只留本地。换干净电脑不复制旧密钥。压测步骤继续使用已有 [scripts/load/README.md](../../scripts/load/README.md)；Git Bash 的数据库/Redis/NameServer参数需改为上述新端口，例如 `DB_PORT=13306 REDIS_PORT=16379`，设置 `MYSQL_BIN` 为实际客户端路径。应用启动窗口与压测窗口都要使用各自需要的环境变量；脚本子进程设置不会回写父终端。

### 验证和旧环境清理记录

迁移脚本与新库启动验证结果、远端提交核验及旧环境清理范围将在完成后记录于此。旧机删除范围仅为本项目 MySQL 库/专用数据目录、上述三个旧容器及 Redis 专属卷、RocketMQ 配置/专属网络和本项目生成物。Git、JDK、Maven、Docker Desktop、WSL、HnuHole、论文和其他项目保留。

2026-10-10 首次验证：PowerShell 语法解析通过，Compose 配置校验通过；拉取 `mysql:8.4` 时因 D 盘仅剩约 10 MB 出现 `input/output error`，随后 Docker Server 不可用。尚未得到新库、Broker 或应用健康通过证据。此次失败不是部署成功；先推送资产，再清理本项目占用并重试。未修改 Java 业务代码，未运行完整 Maven/压测验收。
