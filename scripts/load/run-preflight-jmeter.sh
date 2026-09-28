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
RISK_DECISION="${RISK_DECISION:-pass}"
DO_PREWARM="${DO_PREWARM:-false}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

if [[ -z "$THREADS" ]] || ! [[ "$THREADS" =~ ^[0-9]+$ ]] || [[ "$THREADS" -lt 1 ]]; then
  echo "Preflight 必须显式设置正整数 THREADS，例如 THREADS=64"
  exit 1
fi
for name in RAMP_SECONDS WARMUP_SECONDS MEASURE_SECONDS; do
  if ! [[ "${!name}" =~ ^[0-9]+$ ]]; then
    echo "${name} 必须是非负整数"
    exit 1
  fi
done
if [[ "$MEASURE_SECONDS" -lt 1 ]]; then
  echo "MEASURE_SECONDS 必须大于 0"
  exit 1
fi
if [[ "$WARMUP_SECONDS" -lt "$RAMP_SECONDS" ]]; then
  echo "WARMUP_SECONDS 必须 >= RAMP_SECONDS"
  exit 1
fi

DURATION_SECONDS=$((WARMUP_SECONDS + MEASURE_SECONDS))
MIN_USERS=$((THREADS * 4))
if [[ "$MIN_USERS" -lt 1000 ]]; then
  MIN_USERS=1000
fi

for cmd in "$JMETER_BIN" python3 awk; do
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

DATA_ROWS=$(($(wc -l < "$DATA_FILE") - 1))
UNIQUE_AUTH_TOKENS=$(awk -F, 'NR > 1 && $1 != "" && !seen[$1]++ {count++} END {print count + 0}' "$DATA_FILE")
if [[ "$UNIQUE_AUTH_TOKENS" -lt "$MIN_USERS" ]]; then
  echo "Preflight 用户池不足：distinct_auth_tokens=$UNIQUE_AUTH_TOKENS，需要至少 $MIN_USERS"
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
DISTINCT_AUTH_TOKENS=$UNIQUE_AUTH_TOKENS
POLL_RESULT=false
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
