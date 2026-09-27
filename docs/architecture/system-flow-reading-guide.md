# Mock-Damai 系统源码阅读指南

本文用于按真实执行链路阅读 Mock-Damai 源码。它不重复 README 的项目介绍，也不试图穷举所有 Controller、配置项和运维接口。

阅读本文时以当前源码和有效配置为准。默认启动配置激活 local,flash-sale；如果注释、旧文档与实际调用链或启动 Guardrail 不一致，以可执行代码和有效配置为准。

## 先建立主链路

顶层只需要先理解下面这条业务链路：

~~~mermaid
flowchart LR
    U["用户"]
    S["提交抢票请求"]
    G["入口治理与资格校验"]
    T["可靠提交异步创单任务<br/>RocketMQ 事务消息 + Redis 预扣"]
    C["异步消费者创建正式订单<br/>MySQL 条件扣库存"]
    P["PENDING_PAYMENT"]
    PAID["PAID"]
    CANCEL["CANCELLED<br/>释放库存"]
    CLOSED["CLOSED<br/>释放库存"]
    FAIL["失败补偿 / DLQ / 对账"]
    Q["按 requestId 查询异步结果"]

    U --> S --> G --> T --> C
    C -->|成功| P
    C -->|失败| FAIL
    P -->|支付成功| PAID
    P -->|主动取消| CANCEL
    P -->|超时关闭| CLOSED
    S -. "POST 返回 requestId 后可并行查询" .-> Q
    Q -. "观察处理状态" .-> C
~~~

这张图刻意不展开 Half Message、事务标记、批处理补建等实现细节。它只回答四个问题：

- 抢票入口不直接创建正式订单。
- Redis 预扣是入口资格，不是最终库存事实。
- 正式订单由异步消费者在 MySQL 条件扣库存成功后创建。
- 支付、取消、超时和失败补偿共同完成库存闭环。

## 默认运行边界

默认 application.yml 激活 local,flash-sale。flash-sale 下的 AsyncOrderSubmitGuardrail 会强制以下约束：

- 抢票交易命令必须使用 RocketMQ publisher mode。
- 必须开启 RocketMQ Transaction Message。
- 必须关闭入口 ticket_order_request 预落库。
- 必须开启 in-flight 控制。
- 订单超时消息必须使用 RocketMQ 延迟消息，扫描任务只做兜底。

因此，Outbox 和 Kafka 不能在 flash-sale profile 下替代 RocketMQ 承载抢票交易命令。它们只有在离开该 profile 并满足对应配置时才属于可选交易命令模式。

Redis Stream 相关实现和配置字段仍保留在代码中，但当前 publisher-mode 启动校验只接受 outbox、kafka、rocketmq，因此 Redis Stream 不应被描述为当前可启用的抢票发布模式。

同时要区分另一条默认开启的路径：领域事件默认启用，OrderCreated、PaymentPaid、StockChanged 等事件通过 Local Message / Outbox 写入本地消息表，再由 LocalMessagePublishTask 投递 Kafka。这里的 Kafka 不是默认抢票交易命令通道，但仍属于默认领域事件链路。

## 推荐阅读顺序

### 入口与提交编排

先读：

1. src/main/java/com/zewbby/smartticket/controller/OrderController.java
2. src/main/java/com/zewbby/smartticket/service/impl/OrderServiceImpl.java
3. src/main/java/com/zewbby/smartticket/config/AsyncOrderSubmitProperties.java
4. src/main/java/com/zewbby/smartticket/config/AsyncOrderSubmitGuardrail.java

重点方法是 OrderServiceImpl.submitAsyncOrder。

先理解入口做什么，不要立刻追 Mapper：

- 从登录态获取用户身份。
- 做风控、用户/IP/活动/票档限流和在途容量控制。
- 做重复提交保护、等待室资格和一次性幂等 Token 校验。
- 生成或复用 requestId。
- 构造 AsyncCreateOrderMessage。
- 进入当前 publisher mode 对应的异步提交路径。

在默认 flash-sale 配置下，提交线程不会先创建正式订单，也不会先在 MySQL 预落 ticket_order_request。

### RocketMQ 事务消息与 Redis 预扣

继续读：

1. src/main/java/com/zewbby/smartticket/service/impl/RocketMqAsyncOrderMessagePublisher.java
2. OrderServiceImpl.executeAsyncOrderSubmitLocalTransaction
3. src/main/java/com/zewbby/smartticket/service/StockLuaService.java
4. src/main/resources/lua/stock_pre_deduct.lua
5. src/main/resources/lua/stock_bucket_pre_deduct.lua
6. src/main/java/com/zewbby/smartticket/service/AsyncOrderTransactionMarkerService.java

默认执行顺序必须按源码理解：

~~~text
发送 RocketMQ Transaction Half Message
        ↓
执行 RocketMQ 本地事务
        ↓
Redis Lua 原子预扣
        ↓
写入预扣结果 / 事务标记
        ↓
RocketMQ Transaction Message Commit 或 Rollback
        ↓
Commit 后消息才对消费者可见
~~~

这里的 Commit 指 RocketMQ 事务消息提交，不是 MySQL 业务事务提交。

Redis 预扣负责快速判断和削峰。即使 Redis 预扣成功，消费者仍必须执行 MySQL 条件扣减，因此 Redis 不是最终库存事实。

### 消费者与正式创单

继续读：

1. src/main/java/com/zewbby/smartticket/mq/RocketMqAsyncCreateOrderConsumer.java
2. src/main/java/com/zewbby/smartticket/mq/AsyncCreateOrderBatchDispatcher.java
3. src/main/java/com/zewbby/smartticket/mq/AsyncCreateOrderConsumer.java
4. src/main/java/com/zewbby/smartticket/mapper/OrderRequestMapper.java
5. src/main/java/com/zewbby/smartticket/mapper/TicketStockMapper.java
6. src/main/java/com/zewbby/smartticket/mapper/TicketStockBucketMapper.java
7. src/main/java/com/zewbby/smartticket/mapper/OrderMapper.java

RocketMqAsyncCreateOrderConsumer 只是 RocketMQ 监听入口。真正完成 request 状态抢占、MySQL 条件扣库存和创建正式订单的是 AsyncCreateOrderConsumer。

默认 flash-sale 配置开启 AsyncCreateOrderBatchDispatcher 的批处理。批量路径会先按消息补建 QUEUED request，再批量抢占为 PROCESSING：

~~~text
消息批次
  ↓
insertIgnoreBatch(QUEUED)
  ↓
tryMarkProcessingBatch
  ↓
PROCESSING
  ↓
批量条件扣 MySQL 库存
  ↓
批量创建 ticket_order
  ↓
request -> SUCCESS + orderId
~~~

单条消费或批处理回退路径不同：当入口未预落 request 时，可以根据消息直接补建 PROCESSING request，再继续扣库存和创单。

因此不要把所有消费者路径简化成唯一的 QUEUED -> PROCESSING 状态来源；要结合批量路径和单条/回退路径阅读。

### requestId 查询是观察路径

继续读：

1. OrderController 中 GET /api/order-requests/{requestId}
2. OrderServiceImpl.getOrderRequestResult
3. src/main/java/com/zewbby/smartticket/service/AsyncOrderRequestResultCacheService.java

POST /api/orders/async 返回 requestId 后，客户端即可并行查询处理状态，不需要等到 SUCCESS 后才发起查询。

查询顺序是：

~~~text
结果缓存命中
  → 直接返回

结果缓存未命中
  → 按 userId + requestId 查询 MySQL
  → 转换结果并回填缓存
~~~

结果缓存只是查询优化，不是订单事实源。当前提交线程会写 QUEUED 缓存，消费者会写终态缓存，两者都直接覆盖同一个 Redis key，没有状态版本或“终态不可降级”的条件更新。静态代码上存在较晚的 QUEUED 写覆盖较早终态写的竞争窗口；该风险尚未通过并发测试复现，因此文档不保证轮询一定立刻看到最终状态。最终持久状态仍以 MySQL 为准。

### 支付、取消与超时关闭

继续读：

1. src/main/java/com/zewbby/smartticket/controller/PaymentController.java
2. src/main/java/com/zewbby/smartticket/service/impl/PaymentServiceImpl.java
3. OrderServiceImpl.cancelOrder
4. OrderServiceImpl.closeTimeoutOrder
5. src/main/java/com/zewbby/smartticket/mq/OrderTimeoutProducer.java
6. src/main/java/com/zewbby/smartticket/mq/RocketMqOrderTimeoutConsumer.java
7. src/main/java/com/zewbby/smartticket/task/OrderTimeoutScanTask.java

正式订单创建后进入 PENDING_PAYMENT。

- 支付成功：订单变为 PAID，并确认库存流转。
- 主动取消：订单变为 CANCELLED，并释放相应库存。
- 超时关闭：RocketMQ 延迟消息为主触发，扫描任务兜底；仍为 PENDING_PAYMENT 时才进入 CLOSED 并释放库存。

订单已经进入 PAID、CANCELLED、CLOSED 等终态时，重复触发不能再次释放库存。

### 失败恢复、补偿与死信

继续读：

1. AsyncCreateOrderConsumer 的失败分类和 Redis 补偿逻辑
2. src/main/resources/lua/stock_rollback.lua
3. src/main/java/com/zewbby/smartticket/domain/entity/DeadLetterMessage.java
4. src/main/java/com/zewbby/smartticket/service/StockConsistencyService.java
5. src/main/java/com/zewbby/smartticket/task/StockConsistencyScanTask.java

重点理解三个边界：

- Redis 已预扣但正式订单创建失败时，需要按 requestId 幂等释放预扣。
- 补偿本身也必须防重复执行，不能因为重复消费把库存多加。
- 无法自动恢复的异常需要进入 DLQ、对账或人工治理，而不是简单重试到成功。

### 领域事件、Outbox 与 Kafka

最后再读：

1. src/main/java/com/zewbby/smartticket/service/impl/LocalMessageDomainEventPublisher.java
2. src/main/java/com/zewbby/smartticket/service/LocalMessageService.java
3. src/main/java/com/zewbby/smartticket/task/LocalMessagePublishTask.java
4. src/main/java/com/zewbby/smartticket/config/DomainEventProperties.java

这条链路不要和默认抢票交易命令混在一起：

~~~text
订单 / 支付 / 库存业务事务
        ↓
创建 local_message 领域事件
        ↓
事务提交后尝试立即发送
        ↓
LocalMessagePublishTask
        ↓
Kafka
        ↓
失败时由本地消息状态和定时扫描重试
~~~

默认 domain-event.enabled=true，因此领域事件路径属于默认运行能力；但它不改变“flash-sale 抢票交易命令必须走 RocketMQ 事务消息”的约束。

### 后台治理与可观测性

完成主链路后，再看：

- AdminLocalMessageController
- AdminDeadLetterMessageController
- AdminStockController
- AdminOpsMetricsController
- ObservabilityMetricsService
- StockAdjustmentService
- StockBucketPorterService

这些模块用于异常治理、库存修复、消息重试、指标观察和分桶迁移，不应成为第一次阅读主链路的入口。

## 关键状态模型

异步抢票 request 的持久状态需要结合创建路径理解：

~~~text
批量默认路径：
QUEUED -> PROCESSING -> SUCCESS
                    -> FAILED -> COMPENSATED

单条 / 回退路径：
根据消息补建 PROCESSING
        -> SUCCESS
        -> FAILED -> COMPENSATED
~~~

正式订单状态：

~~~text
PENDING_PAYMENT -> PAID
PENDING_PAYMENT -> CANCELLED
PENDING_PAYMENT -> CLOSED
~~~

Redis 预扣记录、ticket_order_request 和 ticket_order 分别承担不同职责，不能把其中任何一个单独理解成完整订单状态机。

## 按问题定位代码

| 想确认的问题 | 优先阅读 |
| --- | --- |
| 抢票入口为什么不直接打 MySQL | OrderServiceImpl.submitAsyncOrder、StockLuaService |
| RocketMQ 与 Redis 到底谁先执行 | RocketMqAsyncOrderMessagePublisher、executeAsyncOrderSubmitLocalTransaction |
| 如何防止最终超卖 | AsyncCreateOrderConsumer、TicketStockMapper / TicketStockBucketMapper |
| 重复消息为什么不会重复创单 | request 状态抢占、tryMarkProcessing / Batch 处理 |
| requestId 如何查询最终结果 | getOrderRequestResult、AsyncOrderRequestResultCacheService |
| 消费失败后 Redis 库存怎么恢复 | AsyncCreateOrderConsumer、stock_rollback.lua |
| 支付、取消、超时如何闭环 | PaymentServiceImpl、cancelOrder、closeTimeoutOrder |
| Kafka 在默认系统里做什么 | LocalMessageDomainEventPublisher、LocalMessagePublishTask |
| 库存出现差异如何治理 | StockConsistencyService、StockAdjustmentService |
| 多实例下哪些地方需要特别检查 | request 状态抢占、补偿 CAS、定时任务、In-Flight、消息幂等 |

## 30 分钟最短阅读路径

如果只想快速讲清楚系统，按以下顺序：

1. README 的系统架构和核心设计。
2. OrderController 的异步抢票和 request 查询接口。
3. OrderServiceImpl.submitAsyncOrder。
4. RocketMqAsyncOrderMessagePublisher。
5. executeAsyncOrderSubmitLocalTransaction + StockLuaService。
6. RocketMqAsyncCreateOrderConsumer + AsyncCreateOrderBatchDispatcher。
7. AsyncCreateOrderConsumer 的 request 抢占、MySQL 扣库存和创单。
8. PaymentServiceImpl。
9. cancelOrder / closeTimeoutOrder。
10. AsyncCreateOrderConsumer 的失败补偿。

看完后应能准确说明：请求如何进入系统、Redis 和 RocketMQ 的真实顺序、谁负责最终库存、requestId 如何观察异步状态，以及订单如何在支付、取消、超时和失败时收敛。