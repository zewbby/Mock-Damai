#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JMETER_BIN="${JMETER_BIN:-jmeter}"
TEST_PLAN="${TEST_PLAN:-$ROOT_DIR/scripts/jmeter/async-order-closed-loop.jmx}"
export HEAP="${HEAP:--Xms512m -Xmx2g -XX:MaxMetaspaceSize=256m}"

DATA_FILE="${DATA_FILE:-/tmp/async-order-users-preflight.csv}"
REPORT_ROOT="${REPORT_ROOT:-$ROOT_DIR/reports/jmeter-preflight}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULT_DIR="$REPORT_ROOT/$RUN_ID"
JTL_FILE="$RESULT_DIR/result.jtl"
LOG_FILE="$RESULT_DIR/jmeter.log"
HTML_DIR="$RESULT_DIR/html"

BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
THREADS="${THREADS:-}"
RAMP_SECONDS="${RAMP_SECONDS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-20}"
MEASURE_SECONDS="${MEASURE_SECONDS:-40}"
POLL_RESULT=false
RISK_DECISION="${RISK_DECISION:-pass}"
DO_PREWARM="${DO_PREWARM:-false}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

if [[ -z "$THREADS" || "$THREADS" -lt 1 ]]; then
  echo "Preflight 必须显式设置 THREADS，例如 THREADS=64"
  exit 1
fi

DURATION_SECONDS=$((WARMUP_SECONDS + MEASURE_SECONDS))
MIN_USER_ROWS=$((THREADS * 4))
if [[ "$MIN_USER_ROWS" -lt 1000 ]]; then
  MIN_USER_ROWS=1000
fi

if ! command -v "$JMETER_BIN" >/dev/null 2>&1; then
  echo "找不到 JMeter：$JMETER_BIN"
  exit 1
fi
if [[ ! -f "$TEST_PLAN" || ! -f "$DATA_FILE" ]]; then
  echo "找不到 TEST_PLAN 或 DATA_FILE"
  echo "TEST_PLAN=$TEST_PLAN"
  echo "DATA_FILE=$DATA_FILE"
  exit 1
fi

DATA_ROWS=$(($(wc -l < "$DATA_FILE") - 1))
if [[ "$DATA_ROWS" -lt "$MIN_USER_ROWS" ]]; then
  echo "Preflight 用户池不足：DATA_ROWS=$DATA_ROWS, 需要至少 $MIN_USER_ROWS"
  echo "先执行：USER_COUNT=$MIN_USER_ROWS ROWS=$MIN_USER_ROWS WAITING_ROOM_ENABLED=false OUT_FILE=$DATA_FILE ./scripts/load/prepare-async-order-jmeter-data.sh"
  exit 1
fi

mkdir -p "$RESULT_DIR" "$HTML_DIR"

cat <<CONFIG
Mock-Damai Closed-Loop Preflight
BASE_URL=$BASE_URL
THREADS=$THREADS
RAMP_SECONDS=$RAMP_SECONDS
WARMUP_SECONDS=$WARMUP_SECONDS
MEASURE_SECONDS=$MEASURE_SECONDS
DURATION_SECONDS=$DURATION_SECONDS
DATA_ROWS=$DATA_ROWS
RESULT_DIR=$RESULT_DIR
CONFIG

"$JMETER_BIN" -n \
  -t "$TEST_PLAN" \
  -l "$JTL_FILE" \
  -j "$LOG_FILE" \
  -e -o "$HTML_DIR" \
  -Jbase_url="$BASE_URL" \
  -Jdata_file="$DATA_FILE" \
  -Jthreads="$THREADS" \
  -Jramp_seconds="$RAMP_SECONDS" \
  -Jduration_seconds="$DURATION_SECONDS" \
  -Jpoll_result=false \
  -Jrisk_decision="$RISK_DECISION" \
  -Jdo_prewarm="$DO_PREWARM" \
  -Jadmin_token="$ADMIN_TOKEN"

echo
python3 "$ROOT_DIR/scripts/load/summarize-jmeter-result.py" "$JTL_FILE" --warmup-seconds "$WARMUP_SECONDS"
echo
echo "Preflight 只用于估算 Q_probe_peak，不进入正式 Benchmark。"
echo "原始结果：$JTL_FILE"
