#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-${SMART_TICKET_DB_PASSWORD:-}}"
DB_NAME="${DB_NAME:-smart_ticket_lite}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
TICKET_CATEGORY_ID="${TICKET_CATEGORY_ID:-2}"
RESET_STOCK="${RESET_STOCK:-true}"
RESET_STOCK_QUANTITY="${RESET_STOCK_QUANTITY:-1000}"
CONFIRM_RESET="${CONFIRM_RESET:-NO}"
ADMIN_PHONE="${ADMIN_PHONE:-13800000002}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-${SMART_TICKET_ADMIN_PASSWORD:-}}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DATABASE="${REDIS_DATABASE:-0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-${SMART_TICKET_REDIS_PASSWORD:-}}"

if [[ "$CONFIRM_RESET" != "YES" ]]; then
  cat <<EOF
当前是预览模式，不会删除数据。
真正执行：
CONFIRM_RESET=YES ./scripts/load/reset-load-test-env.sh

该脚本需要直连 MySQL / Redis，正式两机 Baseline 推荐在 Windows SUT 侧执行。
EOF
  exit 0
fi

if [[ -z "$DB_PASSWORD" ]]; then
  echo "缺少数据库密码：SMART_TICKET_DB_PASSWORD"
  exit 1
fi
for cmd in mysql redis-cli jq curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "缺少命令：$cmd"
    exit 1
  fi
done

mysql_cmd=(mysql --protocol=TCP -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "-p${DB_PASSWORD}" -D "$DB_NAME")
redis_cmd=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DATABASE")
if [[ -n "$REDIS_PASSWORD" ]]; then
  redis_cmd+=(--no-auth-warning -a "$REDIS_PASSWORD")
fi

echo "1. 清理 MySQL 压测交易数据..."
"${mysql_cmd[@]}" <<SQL
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE payment_flow_log;
TRUNCATE TABLE payment_callback_log;
TRUNCATE TABLE payment_order;
TRUNCATE TABLE dead_letter_message;
TRUNCATE TABLE local_message;
TRUNCATE TABLE ticket_order_request;
TRUNCATE TABLE ticket_order;
TRUNCATE TABLE stock_compensation_record;
TRUNCATE TABLE stock_consistency_record;
SET FOREIGN_KEY_CHECKS = 1;
SQL

if [[ "$RESET_STOCK" == "true" ]]; then
  echo "2. 重置 MySQL 库存和现有 bucket..."
  "${mysql_cmd[@]}" <<SQL
UPDATE ticket_stock
SET total_stock = ${RESET_STOCK_QUANTITY},
    available_stock = ${RESET_STOCK_QUANTITY},
    locked_stock = 0,
    sold_stock = 0,
    version = version + 1,
    updated_at = NOW()
WHERE ticket_category_id = ${TICKET_CATEGORY_ID};

SET @bucket_count := (
  SELECT COUNT(*)
  FROM ticket_stock_bucket
  WHERE ticket_category_id = ${TICKET_CATEGORY_ID}
    AND bucket_version = 1
);
SET @base := IF(@bucket_count > 0, FLOOR(${RESET_STOCK_QUANTITY} / @bucket_count), 0);
SET @remain := IF(@bucket_count > 0, MOD(${RESET_STOCK_QUANTITY}, @bucket_count), 0);

UPDATE ticket_stock_bucket
SET total_stock = @base + IF(bucket_no < @remain, 1, 0),
    available_stock = @base + IF(bucket_no < @remain, 1, 0),
    locked_stock = 0,
    sold_stock = 0,
    version = version + 1,
    updated_at = NOW()
WHERE ticket_category_id = ${TICKET_CATEGORY_ID}
  AND bucket_version = 1;
SQL
fi

echo "3. 清理 Redis 压测 key..."
redis_patterns=(
  "waiting-room:admission:*"
  "waiting-room:queue:*"
  "waiting-room:sequence:*"
  "order:idempotency:*"
  "order:async:result:*"
  "order:async:inflight:*"
  "rate:*"
  "rate:limit:*"
  "risk:order:*"
  "ticket:stock:deducted:*"
  "ticket:stock:compensated:*"
  "ticket:soldout:*"
)

for pattern in "${redis_patterns[@]}"; do
  while IFS= read -r key; do
    [[ -n "$key" ]] && "${redis_cmd[@]}" DEL "$key" >/dev/null
  done < <("${redis_cmd[@]}" --scan --pattern "$pattern")
done

if [[ "$RESET_STOCK" == "true" ]]; then
  echo "4. 通过应用接口重新预热 Redis / bucket..."
  if [[ -z "$ADMIN_PASSWORD" ]]; then
    echo "缺少 SMART_TICKET_ADMIN_PASSWORD，无法完成库存预热。"
    exit 1
  fi

  login_response=$(curl -sS -X POST "${BASE_URL}/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"phone\":\"${ADMIN_PHONE}\",\"password\":\"${ADMIN_PASSWORD}\"}")
  admin_token=$(printf '%s' "$login_response" | jq -r '.data.token // empty')
  if [[ -z "$admin_token" ]]; then
    echo "管理员登录失败：$login_response"
    exit 1
  fi

  curl -sS -X POST "${BASE_URL}/api/admin/ticket-categories/${TICKET_CATEGORY_ID}/stock/preheat" \
    -H "Authorization: Bearer ${admin_token}" | jq .
fi

echo "5. 清理 JMeter 临时 CSV..."
rm -f /tmp/async-order-users-formal.csv /tmp/async-order-users-preflight.csv /tmp/async-order-users.csv

echo
echo "恢复完成。当前库存："
"${mysql_cmd[@]}" -e "
SELECT * FROM ticket_stock WHERE ticket_category_id = ${TICKET_CATEGORY_ID};
SELECT bucket_version, COUNT(*) bucket_count, SUM(total_stock) total_stock,
       SUM(available_stock) available_stock, SUM(locked_stock) locked_stock,
       SUM(sold_stock) sold_stock
FROM ticket_stock_bucket
WHERE ticket_category_id = ${TICKET_CATEGORY_ID}
GROUP BY bucket_version;
"
