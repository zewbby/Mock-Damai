# 测试目录说明

测试按“被测边界”组织，不按历史开发 Phase 组织。测试文件的价值由它保护的正确性边界决定，不按文件大小删除。

## 测试层次

| 层次 | 目录 | 主要职责 |
| --- | --- | --- |
| 纯单元 / 组件 | `auth/`、`config/`、`idempotency/`、`ratelimit/`、`service/`、`service/impl/`、`task/` | 状态机、算法、配置、缓存、限流、补偿和服务逻辑 |
| Web 边界 | `controller/` | 权限、参数、当前用户边界和后台接口行为 |
| MQ adapter contract | `mq/` + publisher tests | Kafka / RocketMQ adapter、批量调度、重试和消息映射；不声称启动真实 Broker |
| SQL / 文档契约 | `mapper/` | Mapper XML、schema、脚本和文档关键事实 |
| 基础设施集成 | `integration/` | 真实 MySQL + Redis Testcontainers，验证 SQL、事务、Redis Lua、Outbox 和共享 Consumer Core |

## 集成测试边界

`BaseIntegrationTest` 当前只启动：

```text
MySQL 8 Testcontainer
Redis 7 Testcontainer
Spring Boot / MockMvc
```

**不启动 Kafka 或 RocketMQ Broker。**

因此 `application-test.yml` 固定：

```text
async-order-submit.publisher-mode = outbox
persist-request-before-publish = true
local-message.sender-enabled = false
```

异步下单集成流程是：

```text
HTTP /api/orders/async
  ↓
真实 Redis Lua 预扣
  ↓
真实 MySQL ticket_order_request + local_message
  ↓
BaseIntegrationTest 显式领取 ASYNC_CREATE_ORDER Outbox
  ↓
共享 AsyncCreateOrderConsumer
  ↓
真实 MySQL 创单 / 库存状态流转
```

这条链路验证的是 **核心交易状态与基础设施集成**，不是 Broker transport。

如果未来要声称“真实 RocketMQ 集成测试”或“真实 Kafka 集成测试”，必须显式启动对应 Broker（Testcontainer 或独立测试环境）并把该测试与默认 `mvn test` 的依赖边界写清楚。

## 为什么仍保留 Kafka / Redis Stream 测试

当前默认 `flash-sale` 交易命令强制使用 RocketMQ，但：

- Kafka 交易命令模式在离开 `flash-sale` profile 后仍有代码路径；
- Redis Stream adapter 虽被当前 Guardrail 禁止启用，但实现仍在仓库中；
- RocketMQ 是默认主链路。

因此这些 adapter 的单元测试继续保留。它们保护“代码存在时必须满足的适配器契约”，但不能被描述成当前默认生产链路的集成验证。

## 测试资源

- `resources/application-test.yml`：固定集成测试消息边界，避免依赖开发机 Kafka / RocketMQ。
- `resources/mockito-extensions/org.mockito.plugins.MockMaker`：Mockito 配置，不删除。
- `integration/BaseIntegrationTest.java`：加载 `docs/sql/schema.sql` 和 `docs/sql/data.sql`，并管理 MySQL / Redis Testcontainers。

## 运行

仓库根目录执行：

```bash
mvn test
```

有 Docker 时运行 Testcontainers 集成测试；无 Docker 时这组测试按 Testcontainers 配置跳过。

修改 schema、Mapper、Lua、异步订单状态机、MQ adapter 或测试 profile 后，都应运行完整 `mvn test`，不要只依赖 Mock 测试。
