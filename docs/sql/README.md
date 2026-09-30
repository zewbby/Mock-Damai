# SQL 使用说明

`docs/sql/` 只保留三类 SQL：当前新库基线、样例数据、旧本地库修复。

## 文件职责

| 文件 | 状态 | 用途 |
| --- | --- | --- |
| [schema.sql](schema.sql) | 保留 | 当前新库唯一结构基线；会 DROP 并重建表，包含当前索引 |
| [data.sql](data.sql) | 保留 | 本地 / 集成测试样例数据；依赖 `schema.sql` |
| [local-schema-repair.sql](local-schema-repair.sql) | 保留 | 旧本地开发库增量修复；不是新库初始化脚本，也不是生产迁移系统 |

旧的 `performance-indexes.sql` 已删除。它的大部分索引已经进入 `schema.sql`，在新库初始化后再次执行会产生重复索引或重复索引名问题。

## 新建本地数据库

```bash
mysql -h 127.0.0.1 -P 3306 -u root -p -e '
CREATE DATABASE IF NOT EXISTS smart_ticket_lite
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;'

mysql -h 127.0.0.1 -P 3306 -u root -p smart_ticket_lite < docs/sql/schema.sql
mysql -h 127.0.0.1 -P 3306 -u root -p smart_ticket_lite < docs/sql/data.sql
```

`schema.sql` 是破坏性脚本。不要对需要保留数据的数据库执行。

## 旧本地库升级

只有明确需要保留本地旧数据时才使用：

```bash
MYSQL_PWD='你的MySQL密码' \
mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u root \
  -D smart_ticket_lite < docs/sql/local-schema-repair.sql
```

执行前先备份。

`local-schema-repair.sql` 的职责是兼容历史开发库，因此部分新增列允许 NULL、部分表结构比新建库更宽松；**当前目标结构仍以 `schema.sql` 为准**。它不能替代 Flyway / Liquibase 一类正式版本化迁移工具。

## 测试依赖

`src/test/java/com/zewbby/smartticket/integration/BaseIntegrationTest.java` 通过：

```text
@Sql(scripts = {
  "file:docs/sql/schema.sql",
  "file:docs/sql/data.sql"
})
```

直接加载这两个文件。

`MapperSqlContractTest` 也会读取 `schema.sql` 和 `local-schema-repair.sql` 检查字段与索引契约。因此修改这些文件时必须同步运行：

```bash
mvn test
```

## 维护规则

- 新建库需要的表、字段和索引直接进入 `schema.sql`；
- 本地旧库也必须补齐的结构变化，同步更新 `local-schema-repair.sql`；
- 样例账号、演出、场次和库存只放 `data.sql`；
- 不再创建独立“性能索引补丁”文件来重复 `schema.sql`；
- 如果未来引入正式 Migration 工具，再把版本化 DDL 从本目录迁移到对应 migration 目录。
