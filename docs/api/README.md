# API 调试索引

`docs/api/` 只保留当前接口样例，并按业务领域组织，不再按历史开发 Phase 命名。

这些文件面向 IntelliJ IDEA HTTP Client。默认地址：

```text
http://localhost:8081
```

本地执行前先确认应用、MySQL、Redis、RocketMQ 已启动。默认 `local,flash-sale` profile 下 Waiting Room 开启，因此普通异步抢票必须先获得 `admissionToken`；完整流程见 [order.http](order.http)。

## 当前文件

| 文件 | 用途 |
| --- | --- |
| [auth.http](auth.http) | 注册、登录、当前用户和退出登录 |
| [show.http](show.http) | 演出、场次和票档查询 |
| [artist-ranking.http](artist-ranking.http) | 演出搜索和艺人热榜 |
| [purchase-plan.http](purchase-plan.http) | 实名观演人、预约计划、开售后提交 |
| [order.http](order.http) | Waiting Room、幂等 Token、异步抢票、结果查询和取消 |
| [payment.http](payment.http) | 创建 / 查询支付单和 mock 回调 |
| [admin-stock.http](admin-stock.http) | 库存预热、一致性检查和补偿 |
| [admin-messages.http](admin-messages.http) | Local Message 与 Dead Letter 运维 |
| [ops.http](ops.http) | Actuator、容量评估、元数据预热和降级 |

## 当前主链路

普通抢票：

```text
登录
  ↓
进入 Waiting Room
  ↓
管理员 / 流控侧发放 admissionToken
  ↓
获取一次性 Idempotency Token
  ↓
POST /api/orders/async
  ↓
RocketMQ Transaction Message
  ↓
异步消费者创建正式订单
  ↓
GET /api/order-requests/{requestId}
```

预约抢票：

```text
创建观演人
  ↓
创建 / 编辑 / 完成 Purchase Plan
  ↓
开售后取得 admissionToken + idempotencyToken
  ↓
POST /api/purchase-plans/{planId}/submit
  ↓
进入同一异步创单链路
```

## 已废弃兼容接口

以下 Controller 路径仍可能存在，但不再提供当前调试样例：

- `POST /api/orders`：同步下单，已废弃；
- `POST /api/orders/{id}/pay`：绕过 `payment_order` 的旧支付入口，已废弃；
- `GET /api/users/{userId}/orders`：旧路径，会忽略 path 中的 userId，只返回当前登录用户订单。

历史 `phase1-*`、`phase2-*`、`phase3-*`、`phase4-*`、`phase5-*` 文件已经从当前文档树移除。需要追溯旧流程时使用 Git 历史。

## 消息模式边界

默认 `flash-sale` 交易命令链路是 RocketMQ Transaction Message。

Kafka / Outbox 只有离开 `flash-sale` profile 后才能作为异步创单交易命令模式；Redis Stream publisher 已被当前 Guardrail 拒绝。Local Message 仍用于领域事件和部分可靠消息，因此 [admin-messages.http](admin-messages.http) 仍有当前运维价值。

## 样例账号

`docs/sql/data.sql` 当前提供：

```text
USER  13800000001 / Test123456
ADMIN 13800000002 / Test123456
```

这些只用于本地 / 测试数据，不应作为真实环境账号。
