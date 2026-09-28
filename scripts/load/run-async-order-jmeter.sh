#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JMETER_BIN="${JMETER_BIN:-jmeter}"
TEST_PLAN="${TEST_PLAN:-$ROOT_DIR/scripts/jmeter/async-order-target-rate.jmx}"
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
WARMUP_SECONDS="${WARMUP_SECONDS:-$RAMP_SECONDS}"
RISK_DECISION="${RISK_DECISION:-pass}"
DO_PREWARM="${DO_PREWARM:-false}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

for name in THREADS TARGET_QPS DURATION_SECONDS; do
  if [[ -z "${!name}" ]]; then
    echo "正式 Target-Rate Baseline 必须显式设置 ${name}"
    exit 1
  fi
  if ! [[ "${!name}" =~ ^[0-9]+$ ]] || [[ "${!name}" -lt 1 ]]; then
    echo "${name} 必须是大于 0 的整数"
    exit 1
  fi
done

for name in RAMP_SECONDS WARMUP_SECONDS; do
  if ! [[ "${!name}" =~ ^[0-9]+$ ]]; then
    echo "${name} 必须是非负整数"
    exit 1
  fi
done
if [[ "$WARMUP_SECONDS" -lt "$RAMP_SECONDS" ]]; then
  echo "WARMUP_SECONDS 必须 >= RAMP_SECONDS，避免把线程爬升阶段计入正式窗口"
  exit 1
fi
if [[ "$WARMUP_SECONDS" -ge "$DURATION_SECONDS" ]]; then
  echo "WARMUP_SECONDS 必须小于 DURATION_SECONDS"
  exit 1
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

TARGET_QPM=$((TARGET_QPS * 60))
DATA_ROWS=$(($(wc -l < "$DATA_FILE") - 1))
EXPECTED_REQUESTS=$((TARGET_QPS * DURATION_SECONDS))
RECOMMENDED_ROWS=$(( (EXPECTED_REQUESTS * 120 + 99) / 100 ))
MIN_USERS=$((THREADS * 4))
if [[ "$MIN_USERS" -lt 1000 ]]; then
  MIN_USERS=1000
fi
UNIQUE_AUTH_TOKENS=$(awk -F, 'NR > 1 && $1 != "" && !seen[$1]++ {count++} END {print count + 0}' "$DATA_FILE")

if [[ "$UNIQUE_AUTH_TOKENS" -lt "$MIN_USERS" ]]; then
  echo "压测用户池不足：distinct_auth_tokens=$UNIQUE_AUTH_TOKENS，需要至少 $MIN_USERS"
  exit 1
fi
if [[ "$DATA_ROWS" -lt "$RECOMMENDED_ROWS" ]]; then
  echo "正式 Baseline CSV 行数不足：DATA_ROWS=$DATA_ROWS，需要至少 $RECOMMENDED_ROWS"
  echo "规则：ROWS >= ceil(TARGET_QPS × DURATION_SECONDS × 1.20)"
  exit 1
fi

mkdir -p "$RESULT_DIR" "$HTML_DIR"

cat <<CONFIG
Mock-Damai Target-Rate Capacity Baseline
BASE_URL=$BASE_URL
TEST_PLAN=$TEST_PLAN
DATA_FILE=$DATA_FILE
THREADS=$THREADS
RAMP_SECONDS=$RAMP_SECONDS
WARMUP_SECONDS=$WARMUP_SECONDS
DURATION_SECONDS=$DURATION_SECONDS
TARGET_QPS=$TARGET_QPS
TARGET_QPM=$TARGET_QPM
DISTINCT_AUTH_TOKENS=$UNIQUE_AUTH_TOKENS
DATA_ROWS=$DATA_ROWS
POLL_RESULT=false
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
  -Jpoll_result=false \
  -Jrisk_decision="$RISK_DECISION" \
  -Jdo_prewarm="$DO_PREWARM" \
  -Jadmin_token="$ADMIN_TOKEN"

echo
python3 "$ROOT_DIR/scripts/load/summarize-jmeter-result.py" "$JTL_FILE" \
  --warmup-seconds "$WARMUP_SECONDS" \
  --target-qps "$TARGET_QPS"
echo
echo "原始结果：$JTL_FILE"
echo "JMeter 日志：$LOG_FILE"
echo "HTML 报告：$HTML_DIR/index.html"
