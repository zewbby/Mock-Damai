#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

export TEST_PLAN="${TEST_PLAN:-$ROOT_DIR/scripts/jmeter/async-order-target-rate.jmx}"
export DATA_FILE="${DATA_FILE:-/tmp/async-order-users-formal.csv}"
export REPORT_ROOT="${REPORT_ROOT:-$ROOT_DIR/reports/jmeter-soldout-flood}"
export HEAP="${HEAP:--Xms512m -Xmx2g -XX:MaxMetaspaceSize=256m}"

TARGET_QPS="${TARGET_QPS:-}"
DURATION_SECONDS="${DURATION_SECONDS:-}"
THREADS="${THREADS:-}"
STOCK_QUANTITY="${STOCK_QUANTITY:-}"
ORDER_QUANTITY="${ORDER_QUANTITY:-1}"
RAMP_SECONDS="${RAMP_SECONDS:-1}"
WARMUP_SECONDS="${WARMUP_SECONDS:-$RAMP_SECONDS}"

for name in TARGET_QPS DURATION_SECONDS THREADS STOCK_QUANTITY; do
  if [[ -z "${!name}" ]] || ! [[ "${!name}" =~ ^[0-9]+$ ]] || [[ "${!name}" -lt 1 ]]; then
    echo "Flash-Sale Sold-Out Flood 必须显式设置正整数 ${name}"
    exit 1
  fi
done
if ! [[ "$ORDER_QUANTITY" =~ ^[0-9]+$ ]] || [[ "$ORDER_QUANTITY" -lt 1 ]]; then
  echo "ORDER_QUANTITY 必须是大于 0 的整数"
  exit 1
fi

export TARGET_QPS DURATION_SECONDS THREADS RAMP_SECONDS WARMUP_SECONDS
EXPECTED_SUCCESS_ORDERS=$((STOCK_QUANTITY / ORDER_QUANTITY))
EXPECTED_REQUESTS=$((TARGET_QPS * DURATION_SECONDS))

cat <<CONFIG
Mock-Damai Flash-Sale Sold-Out Flood
STOCK_QUANTITY=$STOCK_QUANTITY
ORDER_QUANTITY=$ORDER_QUANTITY
EXPECTED_SUCCESS_ORDERS<=$EXPECTED_SUCCESS_ORDERS
EXPECTED_REQUESTS≈$EXPECTED_REQUESTS
TARGET_QPS=$TARGET_QPS
DURATION_SECONDS=$DURATION_SECONDS
THREADS=$THREADS
RAMP_SECONDS=$RAMP_SECONDS
WARMUP_SECONDS=$WARMUP_SECONDS
POLL_RESULT=false
DATA_FILE=$DATA_FILE
消息主链路=RocketMQ Transaction Message
CONFIG

exec "$ROOT_DIR/scripts/load/run-async-order-jmeter.sh"
