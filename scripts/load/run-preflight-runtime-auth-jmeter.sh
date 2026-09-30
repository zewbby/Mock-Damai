#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JMETER_BIN="${JMETER_BIN:-jmeter}"
TEST_PLAN="${TEST_PLAN:-$ROOT_DIR/scripts/jmeter/async-order-closed-loop-runtime-auth.jmx}"
export HEAP="${HEAP:--Xms512m -Xmx2g -XX:MaxMetaspaceSize=256m}"

REPORT_ROOT="${REPORT_ROOT:-$ROOT_DIR/reports/jmeter-preflight-runtime-auth}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RESULT_DIR="$REPORT_ROOT/$RUN_ID"
JTL_FILE="$RESULT_DIR/result.jtl"
LOG_FILE="$RESULT_DIR/jmeter.log"
HTML_DIR="$RESULT_DIR/html"

BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
THREADS="${THREADS:-}"
USER_COUNT="${USER_COUNT:-1000}"
USER_PHONE_PREFIX="${USER_PHONE_PREFIX:-139010}"
USER_PHONE_WIDTH="${USER_PHONE_WIDTH:-5}"
USER_PASSWORD="${USER_PASSWORD:-}"
LOGIN_PARALLELISM="${LOGIN_PARALLELISM:-16}"
SHOW_ID="${SHOW_ID:-1}"
SESSION_ID="${SESSION_ID:-1}"
TICKET_CATEGORY_ID="${TICKET_CATEGORY_ID:-2}"
QUANTITY="${QUANTITY:-1}"
RAMP_SECONDS="${RAMP_SECONDS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-20}"
MEASURE_SECONDS="${MEASURE_SECONDS:-40}"
RISK_DECISION="${RISK_DECISION:-pass}"

if [[ -z "$THREADS" ]] || ! [[ "$THREADS" =~ ^[0-9]+$ ]] || [[ "$THREADS" -lt 1 ]]; then
  echo "必须设置正整数 THREADS，例如 THREADS=32"
  exit 1
fi
if [[ -z "$USER_PASSWORD" ]]; then
  echo "必须通过环境变量 USER_PASSWORD 提供压测用户密码；脚本不会把密码写入文件"
  exit 1
fi

MIN_USERS=$((THREADS * 4))
if [[ "$MIN_USERS" -lt 1000 ]]; then MIN_USERS=1000; fi
if [[ "$USER_COUNT" -lt "$MIN_USERS" ]]; then
  echo "压测用户池不足：USER_COUNT=$USER_COUNT，需要至少 $MIN_USERS"
  exit 1
fi

DURATION_SECONDS=$((WARMUP_SECONDS + MEASURE_SECONDS))
mkdir -p "$RESULT_DIR" "$HTML_DIR"

echo "Mock-Damai Runtime-Auth Closed-Loop Preflight"
echo "BASE_URL=$BASE_URL"
echo "THREADS=$THREADS"
echo "USER_COUNT=$USER_COUNT"
echo "JWT_STORAGE=JMeter JVM memory only"
echo "RESULT_DIR=$RESULT_DIR"

"$JMETER_BIN" -n   -t "$TEST_PLAN"   -l "$JTL_FILE"   -j "$LOG_FILE"   -e -o "$HTML_DIR"   -Jbase_url="$BASE_URL"   -Jthreads="$THREADS"   -Jramp_seconds="$RAMP_SECONDS"   -Jduration_seconds="$DURATION_SECONDS"   -Juser_count="$USER_COUNT"   -Juser_phone_prefix="$USER_PHONE_PREFIX"   -Juser_phone_width="$USER_PHONE_WIDTH"   -Juser_password="$USER_PASSWORD"   -Jlogin_parallelism="$LOGIN_PARALLELISM"   -Jshow_id="$SHOW_ID"   -Jsession_id="$SESSION_ID"   -Jticket_category_id="$TICKET_CATEGORY_ID"   -Jquantity="$QUANTITY"   -Jadmission_token=""   -Jpoll_result=false   -Jrisk_decision="$RISK_DECISION"

echo
python3 "$ROOT_DIR/scripts/load/summarize-jmeter-result.py" "$JTL_FILE" --warmup-seconds "$WARMUP_SECONDS"
echo
echo "原始结果：$JTL_FILE"
