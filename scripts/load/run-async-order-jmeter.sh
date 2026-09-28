#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JMETER_BIN="${JMETER_BIN:-jmeter}"
TEST_PLAN="${TEST_PLAN:-$ROOT_DIR/scripts/jmeter/async-order-open-loop.jmx}"
export HEAP="${HEAP:--Xms512m -Xmx2g -XX:MaxMetaspaceSize=256m}"

DATA_FILE="${DATA_FILE:-/tmp/async-order-users-formal.csv}"
REPORT_ROOT="${REPORT_ROOT:-$ROOT_DIR/reports/jmeter-baseline}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULT_DIR="$REPORT_ROOT/$RUN_ID"
JTL_FILE="$RESULT_DIR/result.jtl"
LOG_FILE="$RESULT_DIR/jmeter.log"
HTML_DIR="$RESULT_DIR/html"

BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
THREADS="${THREADS:-}"
TARGET_QPS="${TARGET_QPS:-}"
DURATION_SECONDS="${DURATION_SECONDS:-}"
RAMP_SECONDS="${RAMP_SECONDS:-30}"
POLL_RESULT="${POLL_RESULT:-false}"
POLL_MAX_ATTEMPTS="${POLL_MAX_ATTEMPTS:-20}"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-300}"
RISK_DECISION="${RISK_DECISION:-pass}"
DO_PREWARM="${DO_PREWARM:-false}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
WARMUP_SECONDS="${WARMUP_SECONDS:-0}"

for name in THREADS TARGET_QPS DURATION_SECONDS; do
  if [[ -z "${!name}" ]]; then
    echo "正式 Open-Loop 运行必须显式设置 ${name}"
    exit 1
  fi
done

if [[ "$THREADS" -lt 1 || "$TARGET_QPS" -lt 1 || "$DURATION_SECONDS" -lt 1 ]]; then
  echo "THREADS、TARGET_QPS、DURATION_SECONDS 必须大于 0"
  exit 1
fi

for cmd in "$JMETER_BIN" python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "找不到命令：$cmd"
    exit 1
  fi
done
if [[ ! -f "$TEST_PLAN" || ! -f "$DATA_FILE" ]]; then
  echo "找不到 TEST_PLAN 或 DATA_FILE"
  echo "TEST_PLAN=$TEST_PLAN"
  echo "DATA_FILE=$DATA_FILE"
  exit 1
fi

TARGET_QPM=$((TARGET_QPS * 60))
DATA_ROWS=$(($(wc -l < "$DATA_FILE") - 1))
EXPECTED_REQUESTS=$((TARGET_QPS * DURATION_SECONDS))
RECOMMENDED_ROWS=$(( (EXPECTED_REQUESTS * 120 + 99) / 100 ))

if [[ "$DATA_ROWS" -lt "$EXPECTED_REQUESTS" ]]; then
  echo "CSV 数据行不足：DATA_ROWS=$DATA_ROWS, EXPECTED_REQUESTS=$EXPECTED_REQUESTS"
  exit 1
fi
if [[ "$DATA_ROWS" -lt "$RECOMMENDED_ROWS" ]]; then
  echo "警告：建议 ROWS >= EXPECTED_REQUESTS × 1.20，即至少 $RECOMMENDED_ROWS 行。"
fi

mkdir -p "$RESULT_DIR" "$HTML_DIR"

cat <<CONFIG
Mock-Damai Open-Loop Baseline
BASE_URL=$BASE_URL
TEST_PLAN=$TEST_PLAN
DATA_FILE=$DATA_FILE
THREADS=$THREADS
RAMP_SECONDS=$RAMP_SECONDS
DURATION_SECONDS=$DURATION_SECONDS
TARGET_QPS=$TARGET_QPS
POLL_RESULT=$POLL_RESULT
DO_PREWARM=$DO_PREWARM
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
  -Jtarget_qps="$TARGET_QPS" \
  -Jtarget_qpm="$TARGET_QPM" \
  -Jpoll_result="$POLL_RESULT" \
  -Jpoll_max_attempts="$POLL_MAX_ATTEMPTS" \
  -Jpoll_interval_ms="$POLL_INTERVAL_MS" \
  -Jrisk_decision="$RISK_DECISION" \
  -Jdo_prewarm="$DO_PREWARM" \
  -Jadmin_token="$ADMIN_TOKEN"

echo
python3 "$ROOT_DIR/scripts/load/summarize-jmeter-result.py" "$JTL_FILE" --warmup-seconds "$WARMUP_SECONDS"
echo
echo "原始结果：$JTL_FILE"
echo "JMeter 日志：$LOG_FILE"
echo "HTML 报告：$HTML_DIR/index.html"
