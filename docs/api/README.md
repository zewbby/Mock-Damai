# API 调试索引

`.http` 文件供 IntelliJ HTTP Client 等工具执行；`phase2-api.md` 是历史说明。当前服务默认地址为 `http://localhost:8081`，需要登录的请求必须先替换 token 变量。

## 消息模式边界

默认 `flash-sale` 交易命令链路使用 RocketMQ Transaction Message。Kafka / Outbox 只有离开 `flash-sale` profile 后才能作为异步创单交易命令模式，Redis Stream publisher 已被当前 Guardrail 拒绝。Outbox 的下游仍是 Kafka Consumer（`local_message → Kafka → KafkaAsyncCreateOrderConsumer`），自动投递时需要 Local Message sender 开启。领域事件默认通过 Local Message 投递 Kafka。

## 当前链路

普通抢票通过 `POST /api/orders/async` 提交，使用 `GET /api/order-requests/{requestId}` 查询结果。

这些样例对应当前代码路径，优先按此顺序阅读或执行：

1. [`phase1-auth-api.http`](phase1-auth-api.http)：注册和登录。
2. [`show.http`](show.http)：演出列表和详情。
3. [`artist-ranking.http`](artist-ranking.http)：演出搜索与艺人热榜。
4. [`show-cache-api.http`](show-cache-api.http)：演出查询缓存。
5. [`user.http`](user.http)：登录后查询当前用户资料。
6. [`purchase-plan.http`](purchase-plan.http)：实名观演人、预约计划及开售后的抢票提交。
7. [`async-order-submit-api.http`](async-order-submit-api.http)：普通异步下单入口和幂等 token。
8. [`async-order-result-api.http`](async-order-result-api.http)：按 `requestId` 查询异步处理结果和正式订单。
9. [`phase4-idempotency-token-api.http`](phase4-idempotency-token-api.http)：幂等 token 的成功、重复和错误用例；其中同步下单步骤仅用于兼容调试。
10. [`phase5-redis-stock-api.http`](phase5-redis-stock-api.http)：Redis 预扣和库存治理。
11. [`phase2-stock-consistency-api.http`](phase2-stock-consistency-api.http)：Redis/MySQL 库存一致性。
12. [`phase5-reliable-message-api.http`](phase5-reliable-message-api.http)：本地消息表与可靠事件管理；异步创单的 Outbox 验证仅适用于切换到 Outbox 模式，默认 RocketMQ 模式走事务消息。
13. [`phase2-consumer-dlq-api.http`](phase2-consumer-dlq-api.http)：死信查询和人工处理。
14. [`phase4-rate-limit-api.http`](phase4-rate-limit-api.http)：限流和下单保护。
15. [`phase4-actuator-and-cost.http`](phase4-actuator-and-cost.http)：Actuator 和运营指标。
16. [`stock-preheat-api.http`](stock-preheat-api.http)：后台库存预热。
17. [`order-timeout-api.http`](order-timeout-api.http)：异步订单超时关闭验证。

## 历史兼容与调试

这些文件保留为旧阶段实现对照。部分请求缺少现行鉴权或幂等参数，不能直接用于当前服务的主链路验证：

- [`order.http`](order.http)
- [`order-relation-validation-api.http`](order-relation-validation-api.http)
- [`order-status-api.http`](order-status-api.http)
- [`order-submit-guard-api.http`](order-submit-guard-api.http)
- [`phase1-order-permission-api.http`](phase1-order-permission-api.http)
- [`phase1-payment-api.http`](phase1-payment-api.http)
- [`phase2-api.md`](phase2-api.md)
- [`phase2-full-flow.http`](phase2-full-flow.http)
- [`async-order-consumer-api.http`](async-order-consumer-api.http)
- [`phase3-async-order-full-flow.http`](phase3-async-order-full-flow.http)

`POST /api/orders` 已废弃；这些样例中出现的请求体 `userId` 和直接支付入口属于旧阶段用法。新的压测和主链路验证必须使用带 Bearer token 的当前样例和 `/api/orders/async`。
