# 文档导航

Mock-Damai 的文档按用途分目录。运行时依赖的 SQL、接口调试样例和压测脚本都保留在固定目录；历史调试材料集中说明，不与当前主链路混排。

## 目录

| 目录 | 用途 |
| --- | --- |
| [`architecture/`](architecture/) | 系统流程、领域设计和架构说明 |
| [`adr/`](adr/) | 已确认的架构决策记录 |
| [`api/`](api/) | 按领域组织的当前 HTTP 调试请求，见 [`api/README.md`](api/README.md) |
| [`performance/`](performance/) | 当前性能测试入口、Phase 1 口径与结果记录规则 |
| [`sql/`](sql/) | 当前建库基线、样例数据和旧本地库修复，见 [`sql/README.md`](sql/README.md) |
| [`superpowers/plans/`](superpowers/plans/) | 开发过程计划，不是运行时文档 |

## 入口文档

- [项目说明](../README.md)：启动、配置、主链路和验证方式。
- [系统流程阅读指南](architecture/system-flow-reading-guide.md)：按顺序阅读源码和主链路。
- [艺人热榜设计](architecture/artist-ranking-design.md)：搜索、行为计分和榜单周期。
- [领域上下文](../CONTEXT.md)：预约计划、观演人、抢票请求和正式订单的术语边界。
- [预约与抢票提交分离 ADR](adr/0001-预约与抢票提交分离.md)：已采纳的架构决策。
- [预约计划接口样例](api/purchase-plan.http)：从观演人选择到开售后提交抢票。
- [API 调试索引](api/README.md)：按认证、演出、预约、订单、支付和运维领域组织的当前样例。
- [性能测试入口](performance/README.md)：当前 Phase 1 计划、运行口径、指标定义和结果记录规则。
- [SQL 使用说明](sql/README.md)：新库初始化、样例数据与旧本地库修复边界。
- [测试目录说明](../src/test/README.md)：测试分类、资源依赖和运行边界。

## 文档规则

- 当前业务链路的说明必须链接到实际存在的源码、脚本或 SQL 文件。
- `api/` 只保留当前可执行或当前仍有运维价值的样例；已废弃接口和旧 Phase 调试流程通过 Git 历史追溯，不与当前入口混排。
- `reports/` 和根目录 `jmeter.log` 是本地生成物，不纳入文档树，也不应提交。
- 修改 SQL 文件名或目录前，必须同步检查 `src/test` 的 `@Sql` 和 `Path.of(...)` 引用。
