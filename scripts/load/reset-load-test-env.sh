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
DELETE_GENERATED_CSV="${DELETE_GENERATED_CSV:-false}"

if [[ "$CONFIRM_RESET" != "YES" ]]; then
  cat <<EOF
当前是预览模式，不会删除数据。
真正执行：
CONFIRM_RESET=YES ./scripts/load/reset-load-test-env.sh

该脚本会清理压测交易数据，并在 RESET_STOCK=true 时通过后台库存初始化接口重建当前 active bucket version。
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
if [[ "$RESET_STOCK" == "true" && -z "$ADMIN_PASSWORD" ]]; then
  echo "RESET_STOCK=true 时缺少 SMART_TICKET_ADMIN_PASSWORD"
  exit 1
fi

export MYSQL_PWD="$DB_PASSWORD"
mysql_cmd=(mysql --protocol=TCP -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -D "$DB_NAME")
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
TRUNCATE TABLE ticket_order_audience;
TRUNCATE TABLE ticket_order_request;
TRUNCATE TABLE ticket_order;
TRUNCATE TABLE stock_compensation_record;
TRUNCATE TABLE stock_consistency_record;
SET FOREIGN_KEY_CHECKS = 1;
SQL

if [[ "$RESET_STOCK" == "true" ]]; then
  echo "2. 清除旧库存交易状态与 bucket 行..."
  "${mysql_cmd[@]}" <<SQL
UPDATE ticket_stock
SET locked_stock = 0,
    sold_stock = 0,
    available_stock = total_stock,
    updated_at = NOW()
WHERE ticket_category_id = ${TICKET_CATEGORY_ID};

DELETE FROM ticket_stock_bucket
WHERE ticket_category_id = ${TICKET_CATEGORY_ID};
SQL
else
  echo "2. RESET_STOCK=false，保留 MySQL 库存结构。"
fi

echo "3. 清理 Redis 压测状态..."
redis_patterns=(
  "waiting-room:admission:*"
  "waiting-room:queue:*"
  "waiting-room:sequence:*"
  "order:idempotency:*"
  "order:submit:user:*"
  "order:async:result:*"
  "order:async:inflight:*"
  "rate:*"
  "risk:order:*"
  "ticket:stock:deducted:*"
  "ticket:stock:compensated:*"
  "ticket:stock:${TICKET_CATEGORY_ID}"
  "ticket:stock:${TICKET_CATEGORY_ID}:*"
  "ticket:soldout:${TICKET_CATEGORY_ID}*"
)

for pattern in "${redis_patterns[@]}"; do
  while IFS= read -r key; do
    [[ -n "$key" ]] && "${redis_cmd[@]}" DEL "$key" >/dev/null
  done < <("${redis_cmd[@]}" --scan --pattern "$pattern")
done

if [[ "$RESET_STOCK" == "true" ]]; then
  echo "4. 通过应用库存初始化接口按当前 activeVersion / bucketCount 重建并预热..."
  login_response=$(curl -sS -X POST "${BASE_URL}/api/auth/login"     -H 'Content-Type: application/json'     -d "{\"phone\":\"${ADMIN_PHONE}\",\"password\":\"${ADMIN_PASSWORD}\"}")
  admin_token=$(printf '%s' "$login_response" | jq -r '.data.token // empty')
  if [[ -z "$admin_token" ]]; then
    echo "管理员登录失败：$login_response"
    exit 1
  fi

  init_response=$(curl -sS -X POST     "${BASE_URL}/api/admin/ticket-categories/${TICKET_CATEGORY_ID}/stock/init"     -H "Authorization: Bearer ${admin_token}"     -H 'Content-Type: application/json'     -d "{\"availableStock\":${RESET_STOCK_QUANTITY}}")
  init_code=$(printf '%s' "$init_response" | jq -r '.code // empty')
  if [[ "$init_code" != "0" ]]; then
    echo "库存初始化失败：$init_response"
    exit 1
  fi
  printf '%s\n' "$init_response" | jq .
else
  echo "4. RESET_STOCK=false，跳过库存初始化。"
fi

if [[ "$DELETE_GENERATED_CSV" == "true" ]]; then
  echo "5. 删除生成的 JMeter CSV..."
  rm -f /tmp/async-order-users-formal.csv /tmp/async-order-users-preflight.csv /tmp/async-order-users.csv
else
  echo "5. 保留现有 JMeter CSV；Flash-Sale 的 admissionToken CSV 仍需每轮重新生成。"
fi

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
