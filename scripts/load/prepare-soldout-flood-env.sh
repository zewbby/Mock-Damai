#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-${SMART_TICKET_DB_PASSWORD:-}}"
DB_NAME="${DB_NAME:-smart_ticket_lite}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"

STOCK_QUANTITY="${STOCK_QUANTITY:-}"
USER_COUNT="${USER_COUNT:-}"
TARGET_QPS="${TARGET_QPS:-}"
DURATION_SECONDS="${DURATION_SECONDS:-}"
THREADS="${THREADS:-}"
QUANTITY="${QUANTITY:-1}"
TICKET_CATEGORY_ID="${TICKET_CATEGORY_ID:-2}"
USER_PHONE_PREFIX="${USER_PHONE_PREFIX:-139010}"
USER_PHONE_WIDTH="${USER_PHONE_WIDTH:-5}"
SKIP_USER_SYNC="${SKIP_USER_SYNC:-false}"

for name in STOCK_QUANTITY USER_COUNT TARGET_QPS DURATION_SECONDS THREADS; do
  if [[ -z "${!name}" ]] || ! [[ "${!name}" =~ ^[0-9]+$ ]] || [[ "${!name}" -lt 1 ]]; then
    echo "Flash-Sale 环境准备必须显式设置正整数 ${name}"
    exit 1
  fi
done
if ! [[ "$QUANTITY" =~ ^[0-9]+$ ]] || [[ "$QUANTITY" -lt 1 ]]; then
  echo "QUANTITY 必须是大于 0 的整数"
  exit 1
fi
if [[ -z "$DB_PASSWORD" ]]; then
  echo "缺少数据库密码：SMART_TICKET_DB_PASSWORD"
  exit 1
fi
if [[ -z "${SMART_TICKET_ADMIN_PASSWORD:-}" ]]; then
  echo "缺少后台密码：SMART_TICKET_ADMIN_PASSWORD"
  exit 1
fi

MIN_USERS=$((THREADS * 4))
if [[ "$MIN_USERS" -lt 1000 ]]; then
  MIN_USERS=1000
fi
if [[ "$USER_COUNT" -lt "$MIN_USERS" ]]; then
  echo "USER_COUNT=$USER_COUNT 不足；当前 THREADS=$THREADS 至少需要 $MIN_USERS"
  exit 1
fi

EXPECTED_REQUESTS=$((TARGET_QPS * DURATION_SECONDS))
ROWS=$(( (EXPECTED_REQUESTS * 120 + 99) / 100 ))
EXPECTED_SUCCESS_ORDERS=$((STOCK_QUANTITY / QUANTITY))

cat <<CONFIG
Mock-Damai Flash-Sale 环境准备
STOCK_QUANTITY=$STOCK_QUANTITY
QUANTITY=$QUANTITY
EXPECTED_SUCCESS_ORDERS<=$EXPECTED_SUCCESS_ORDERS
USER_COUNT=$USER_COUNT
THREADS=$THREADS
TARGET_QPS=$TARGET_QPS
DURATION_SECONDS=$DURATION_SECONDS
EXPECTED_REQUESTS=$EXPECTED_REQUESTS
CSV_ROWS=$ROWS
TICKET_CATEGORY_ID=$TICKET_CATEGORY_ID
CONFIG

echo
echo "1. 重置交易数据与库存..."
BASE_URL="$BASE_URL" CONFIRM_RESET=YES RESET_STOCK_QUANTITY="$STOCK_QUANTITY" TICKET_CATEGORY_ID="$TICKET_CATEGORY_ID" "$ROOT_DIR/scripts/load/reset-load-test-env.sh"

echo
echo "2. 准备压测用户..."
if [[ "$SKIP_USER_SYNC" != "true" ]]; then
  BASE_URL="$BASE_URL"   USER_COUNT="$USER_COUNT"   USER_PHONE_PREFIX="$USER_PHONE_PREFIX"   USER_PHONE_WIDTH="$USER_PHONE_WIDTH"   "$ROOT_DIR/scripts/load/ensure-load-users.sh"
else
  echo "跳过用户准备，假定目标用户已经存在。"
fi

echo
echo "3. 生成 Flash-Sale CSV，并写入一次性 admissionToken..."
BASE_URL="$BASE_URL" ROWS="$ROWS" USER_COUNT="$USER_COUNT" USER_PHONE_PREFIX="$USER_PHONE_PREFIX" USER_PHONE_WIDTH="$USER_PHONE_WIDTH" QUANTITY="$QUANTITY" TICKET_CATEGORY_ID="$TICKET_CATEGORY_ID" WAITING_ROOM_ENABLED=true "$ROOT_DIR/scripts/load/prepare-async-order-jmeter-data.sh"

echo
echo "4. MySQL 核验..."
export MYSQL_PWD="$DB_PASSWORD"
mysql --protocol=TCP -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -D "$DB_NAME" <<SQL
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
echo "环境准备完成。使用同一组压力参数运行："
echo "STOCK_QUANTITY=$STOCK_QUANTITY ORDER_QUANTITY=$QUANTITY THREADS=$THREADS TARGET_QPS=$TARGET_QPS DURATION_SECONDS=$DURATION_SECONDS ./scripts/load/run-soldout-flood-jmeter.sh"
