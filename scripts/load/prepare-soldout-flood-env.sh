#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-${SMART_TICKET_DB_PASSWORD:-}}"
DB_NAME="${DB_NAME:-smart_ticket_lite}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"

STOCK_QUANTITY="${STOCK_QUANTITY:-600}"
USER_COUNT="${USER_COUNT:-1000}"
ROWS="${ROWS:-1000}"
QUANTITY="${QUANTITY:-1}"
TICKET_CATEGORY_ID="${TICKET_CATEGORY_ID:-2}"
USER_PHONE_PREFIX="${USER_PHONE_PREFIX:-139010}"
USER_PHONE_WIDTH="${USER_PHONE_WIDTH:-5}"
SKIP_USER_SYNC="${SKIP_USER_SYNC:-false}"

if [[ -z "$DB_PASSWORD" ]]; then
  echo "缺少数据库密码：SMART_TICKET_DB_PASSWORD"
  exit 1
fi
if [[ -z "${SMART_TICKET_ADMIN_PASSWORD:-}" ]]; then
  echo "缺少后台密码：SMART_TICKET_ADMIN_PASSWORD"
  exit 1
fi
if [[ "$QUANTITY" -lt 1 ]]; then
  echo "QUANTITY 必须大于 0"
  exit 1
fi

EXPECTED_SUCCESS_ORDERS=$((STOCK_QUANTITY / QUANTITY))

cat <<CONFIG
Mock-Damai Flash-Sale 环境准备
STOCK_QUANTITY=$STOCK_QUANTITY
QUANTITY=$QUANTITY
EXPECTED_SUCCESS_ORDERS<=$EXPECTED_SUCCESS_ORDERS
USER_COUNT=$USER_COUNT
ROWS=$ROWS
TICKET_CATEGORY_ID=$TICKET_CATEGORY_ID
SKIP_USER_SYNC=$SKIP_USER_SYNC
CONFIG

echo
echo "1. 重置交易数据与库存..."
BASE_URL="$BASE_URL" \
CONFIRM_RESET=YES \
RESET_STOCK_QUANTITY="$STOCK_QUANTITY" \
TICKET_CATEGORY_ID="$TICKET_CATEGORY_ID" \
"$ROOT_DIR/scripts/load/reset-load-test-env.sh"

echo
echo "2. 准备压测用户..."
if [[ "$SKIP_USER_SYNC" != "true" ]]; then
  BASE_URL="$BASE_URL" \
  USER_COUNT="$USER_COUNT" \
  USER_PHONE_PREFIX="$USER_PHONE_PREFIX" \
  USER_PHONE_WIDTH="$USER_PHONE_WIDTH" \
  "$ROOT_DIR/scripts/load/ensure-load-users.sh"
else
  echo "跳过用户准备，假定目标用户已经存在。"
fi

echo
echo "3. 生成 Flash-Sale CSV，并写入一次性 admissionToken..."
BASE_URL="$BASE_URL" \
ROWS="$ROWS" \
USER_COUNT="$USER_COUNT" \
USER_PHONE_PREFIX="$USER_PHONE_PREFIX" \
USER_PHONE_WIDTH="$USER_PHONE_WIDTH" \
QUANTITY="$QUANTITY" \
TICKET_CATEGORY_ID="$TICKET_CATEGORY_ID" \
WAITING_ROOM_ENABLED=true \
"$ROOT_DIR/scripts/load/prepare-async-order-jmeter-data.sh"

echo
echo "4. MySQL 核验..."
mysql --protocol=TCP -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "-p${DB_PASSWORD}" -D "$DB_NAME" <<SQL
SELECT ticket_category_id, total_stock, available_stock, locked_stock, sold_stock
FROM ticket_stock
WHERE ticket_category_id = ${TICKET_CATEGORY_ID};

SELECT bucket_version,
       COUNT(*) AS bucket_count,
       SUM(total_stock) AS total_stock,
       SUM(available_stock) AS available_stock,
       SUM(locked_stock) AS locked_stock,
       SUM(sold_stock) AS sold_stock
FROM ticket_stock_bucket
WHERE ticket_category_id = ${TICKET_CATEGORY_ID}
GROUP BY bucket_version;
SQL

echo
echo "环境准备完成："
echo "REQUESTS=$ROWS STOCK_QUANTITY=$STOCK_QUANTITY ORDER_QUANTITY=$QUANTITY ./scripts/load/run-soldout-flood-jmeter.sh"
