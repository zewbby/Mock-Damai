# Mock-Damai 艺人热榜设计

本文只解释当前艺人热榜的口径、计分、边界和验证方式。接口清单留给代码与 docs/api，外部产品机制只作为语义参考，不作为当前实现事实来源。

本模块不是对大麦内部热榜算法的复刻。大麦未公开的精确权重、反作弊模型、用户分层和排序服务实现不在本文假设范围内。

## 设计目标

当前热榜解决的是一个工程问题：把来自不同业务入口的艺人兴趣和转化信号，以低成本方式聚合成可查询的周期榜和 HOT 榜。

主要目标：

- 不让详情、搜索、购票意向、支付等不同强度的行为简单等价。
- 在 Redis 中完成高频去重和计分，避免排行榜写入阻塞主业务。
- 对高基数艺人的重复增量做边际递减。
- 普通周期榜保留累计口径，HOT 榜强调最近事件并做时间衰减。
- Redis 排行榜不可用时降级，不阻断详情、搜索、预约提交或支付主链路。

## 当前已确认接入的信号

| 信号 | 组件 | 默认事件权重 | 当前接入位置 | 说明 |
| --- | --- | ---: | --- | --- |
| 演出详情点击 | INTEREST | 1.0 | ShowController.getShowDetail | 成功取得详情后记录艺人兴趣 |
| 搜索命中艺人 | INTEREST | 0.3 | ShowSearchServiceImpl.search | 仅关键词与返回结果中的艺人名互相包含时记录，避免给所有搜索结果艺人加分 |
| 预约抢票提交 | INTENT | 3.0 | TicketPurchasePlanServiceImpl.submit | orderService.submitAsyncOrder 成功返回后记录 |
| 支付成功流程 | PAID | 10.0 | PaymentServiceImpl | 订单状态与库存确认成功后尝试记录，但仍位于数据库事务最终提交之前 |

购票意向当前只由“预约计划提交抢票”这一业务入口接入。普通 POST /api/orders/async 本身不直接写 purchase intent，因此不能把 INTENT 描述成所有异步下单请求的统一信号。

ArtistRankingService 还暴露 content interaction 和 task contribution 的记录能力，并保留对应组件权重，但本文不把未核实到业务调用入口的服务方法写成“当前已接入入口”。

## 身份、规范化与去重

写入前先规范化艺人字符串，再计算身份和去重 key。

登录用户身份：

~~~text
user:{userId}
~~~

未登录请求：

~~~text
ip:{sha256(clientIp)}
~~~

匿名请求不会把明文 IP 放进 Redis key，且默认只使用 0.35 的质量系数；登录用户默认质量系数为 1.0。

当前去重维度可以概括为：

~~~text
自然日 + action + identity + sha256(normalizedArtist)
~~~

Lua 使用 SET ... NX + TTL 抢占去重键。默认 TTL 为 86400 秒，因此同一身份、同一艺人、同一动作在该去重窗口内只会成功计分一次。

艺人在 ZSET 中也不是直接使用展示名称。当前 member 为：

~~~text
artist:{sha256(normalizedArtist) 的前 32 位}
~~~

规范化展示名称另存到 Hash，查询时再恢复。

这比直接把自由文本作为 ZSET member 更稳定，但它仍然是“由名称派生的 ID”，不是独立的 artist_id 主数据。艺人别名、改名和联合演出仍可能导致身份边界不稳定。

## 写入计分

写入阶段不要用一个“所有权重直接相乘”的公式描述，因为当前实现分成事件写入和查询聚合两层。

### 事件基础权重

先计算本次事件输入权重：

~~~text
w = eventWeight × qualityFactor
~~~

例如默认情况下：

- 登录用户详情点击：1.0 × 1.0。
- 匿名用户详情点击：1.0 × 0.35。
- 登录用户预约抢票意向：3.0 × 1.0。
- 登录用户支付信号：10.0 × 1.0。

组件权重此时还没有参与计算。

### Lua 边际递减

Lua 在同一个原子脚本中完成：

1. 抢占去重 key。
2. 写入艺人 member 与展示名称映射。
3. 分别更新 all、daily、weekly、monthly、hourly 对应组件 ZSET。
4. 设置周期 key TTL。

对每个目标 ZSET，先读取该艺人的当前分数 c，再根据饱和尺度 S 计算本次增量：

~~~text
delta =
S × [
  ln(1 + (c + w) / S)
  -
  ln(1 + c / S)
]
~~~

然后执行：

~~~text
ZINCRBY key delta artistMember
~~~

默认 saturationScale 为 100。

因此“边际递减”不是一个固定乘数，而是依赖目标 ZSET 当前分数的非线性增量。分数越高，相同事件带来的新增分数越小。

## 普通周期榜查询

ALL、DAILY、WEEKLY、MONTHLY 查询时，才把不同组件分数按组件权重合并。

当前默认组件权重：

| 组件 | 默认权重 |
| --- | ---: |
| CONTENT | 0.40 |
| INTEREST | 0.25 |
| INTENT | 0.15 |
| PAID | 0.12 |
| TASK | 0.08 |

查询阶段可概括为：

~~~text
periodScore(artist)
=
Σ componentPeriodScore(artist, component)
  × componentWeight(component)
~~~

实现使用 Redis ZSET unionAndStore 对多个组件结果加权合并，再按分数倒序读取 TopN。

注意：组件权重是在查询阶段应用，不是在事件写入 Lua 时应用。

## HOT 榜时间衰减

HOT 榜不直接读取普通周期累计值，而是读取最近若干小时的组件小时桶。

默认参数：

~~~text
hotWindowHours = 24
hotHalfLifeHours = 6
~~~

对于距离当前小时 offset 的桶，时间权重为：

~~~text
decay(offset)
=
0.5 ^ (offset / hotHalfLifeHours)
~~~

查询过程分两层：

~~~text
每个组件：
最近小时桶 × 时间衰减
        ↓
得到该组件 HOT 临时分数

所有组件：
组件 HOT 分数 × componentWeight
        ↓
得到最终 HOT 排名
~~~

因此当前实现不存在“HOT 榜没有时间衰减”的情况。普通 ALL / DAILY / WEEKLY / MONTHLY 保留周期累计口径，HOT 才额外进行小时衰减。

## Redis 数据边界

当前写入按组件维护：

~~~text
ranking:artist:{component}:all
ranking:artist:{component}:daily:{date}
ranking:artist:{component}:weekly:{week}
ranking:artist:{component}:monthly:{month}
ranking:artist:{component}:hourly:{hour}
~~~

另外维护：

- 去重 key。
- 艺人 member 到展示名称的 Hash。
- 查询过程中短生命周期的临时聚合 ZSET。

具体 key 拼装以 RedisKeyConstant 为准，不应从本文复制字符串作为业务契约。

## 可靠性与业务边界

### 排行榜不是业务事实源

排行榜写入是附属信号。Redis 故障时 ArtistRankingService 捕获异常并降级，不能阻断详情、搜索、预约提交或支付主流程。

因此排行榜分数不能作为订单、支付、库存或审计事实。

### purchase intent 的口径有限

当前 INTENT 信号只在预约计划成功提交异步抢票请求后记录。它表达的是“预约抢票入口已接受提交”，不是“所有用户发生了统一购票意向”，也不代表正式订单已经创建。

### paid 信号是尽力而为，不是已提交支付事实

PaymentServiceImpl 在支付成功事务流程中，先完成订单状态更新和库存确认，再调用排行榜记录；但该 Redis 写入发生在 Spring 数据库事务最终提交之前。

因此理论上可能出现：

~~~text
数据库事务内部已更新订单
        ↓
Redis 已写 paid 信号
        ↓
后续步骤异常
        ↓
MySQL 事务回滚
~~~

Redis 排行榜不会跟随 MySQL 事务回滚。paid 信号应理解为支付成功流程中的尽力而为转化信号，不能描述为“已经提交的支付事实”。支付事实仍以数据库事务结果为准。

### 当前艺人身份仍来自自由文本

ZSET member 已经使用名称派生哈希，而不是直接存展示名称；但没有独立 artist_id 主数据，别名、改名和联合演出仍可能造成同一艺人被拆分。

### 当前去重策略偏粗

“身份 + 艺人 + 动作 + 日窗口”可以阻止最简单的重复刷新，但也会把一天内真实的多次互动折叠成一次。若未来需要更精细的社区互动榜，应按动作设计不同幂等维度和时间窗口，而不是继续复用统一日去重。

### 当前权重是工程配置，不是外部平台公式

事件权重、组件权重、质量系数、饱和尺度和 HOT 半衰期都来自 ArtistRankingProperties。它们用于当前项目实验和行为区分，不代表大麦或任何真实商业平台的内部参数。

## 与大麦公开机制的关系

本项目只参考“票务场景可以由多种互动和交易信号形成热度”的公开产品语义，不把大麦未公开的精确计分公式、反作弊模型、用户分层和排序基础设施当作已知事实。

因此文档只比较概念边界，不以“复刻大麦算法”作为验收目标。

## 验证重点

当前实现至少应持续验证：

1. 同一身份、同一艺人、同一动作在去重窗口内不会重复计分。
2. 未登录请求使用哈希 IP，且匿名质量系数低于登录用户。
3. 搜索只有在关键词与艺人名称匹配时才记录搜索信号。
4. Redis 故障不会阻断详情、搜索、预约提交和支付主流程。
5. Lua 对 all / daily / weekly / monthly / hourly 的更新与去重保持原子。
6. 普通周期榜只做组件加权，不错误加入 HOT 时间衰减。
7. HOT 榜对更旧小时桶使用更低权重，并在组件聚合后得到 TopN。
8. 预约抢票意向只在当前已接入的预约提交路径记录。
9. paid 信号的测试不能把 Redis 分数当作数据库事务已提交证明。
10. 艺人展示名称变化或别名场景不应被误认为已有稳定 artist_id 支持。