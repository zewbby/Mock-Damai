#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
ROWS="${ROWS:-1200}"
USER_COUNT="${USER_COUNT:-1000}"
USER_PHONE_PREFIX="${USER_PHONE_PREFIX:-139010}"
USER_PHONE_WIDTH="${USER_PHONE_WIDTH:-5}"
USER_PASSWORD="${USER_PASSWORD:-Test123456}"
LOGIN_PARALLELISM="${LOGIN_PARALLELISM:-16}"
SHOW_ID="${SHOW_ID:-1}"
SESSION_ID="${SESSION_ID:-1}"
TICKET_CATEGORY_ID="${TICKET_CATEGORY_ID:-2}"
QUANTITY="${QUANTITY:-1}"
WAITING_ROOM_ENABLED="${WAITING_ROOM_ENABLED:-false}"
ADMISSION_TTL_SECONDS="${ADMISSION_TTL_SECONDS:-7200}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DATABASE="${REDIS_DATABASE:-0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-${SMART_TICKET_REDIS_PASSWORD:-}}"
OUT_FILE="${OUT_FILE:-/tmp/async-order-users-formal.csv}"

if ! command -v jq >/dev/null 2>&1; then
  echo "缺少 jq"
  exit 1
fi

if [[ "$ROWS" -lt 1 || "$USER_COUNT" -lt 1 || "$QUANTITY" -lt 1 ]]; then
  echo "ROWS、USER_COUNT、QUANTITY 必须大于 0"
  exit 1
fi

if [[ "$WAITING_ROOM_ENABLED" != "true" && "$WAITING_ROOM_ENABLED" != "false" ]]; then
  echo "WAITING_ROOM_ENABLED 只允许 true / false"
  exit 1
fi

redis_cmd=()
if [[ "$WAITING_ROOM_ENABLED" == "true" ]]; then
  if ! command -v redis-cli >/dev/null 2>&1; then
    echo "WAITING_ROOM_ENABLED=true 时需要 redis-cli"
    exit 1
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    echo "WAITING_ROOM_ENABLED=true 时需要 openssl"
    exit 1
  fi
  redis_cmd=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DATABASE")
  if [[ -n "$REDIS_PASSWORD" ]]; then
    redis_cmd+=(--no-auth-warning -a "$REDIS_PASSWORD")
  fi
fi

tmp_file="${OUT_FILE}.tmp"
login_dir="$(mktemp -d /tmp/mock-damai-login-tokens.XXXXXX)"
trap 'rm -rf "$login_dir" "$tmp_file"' EXIT
printf 'authToken,showId,sessionId,ticketCategoryId,quantity,admissionToken\n' > "$tmp_file"

echo "登录 ${USER_COUNT} 个压测用户并刷新 JWT..."
echo "ROWS=${ROWS}, QUANTITY=${QUANTITY}, WAITING_ROOM_ENABLED=${WAITING_ROOM_ENABLED}"
echo "BASE_URL=${BASE_URL}, LOGIN_PARALLELISM=${LOGIN_PARALLELISM}"

login_one_user() {
  local n="$1"
  local phone response code user_id user_token
  phone=$(printf "%s%0${USER_PHONE_WIDTH}d" "$USER_PHONE_PREFIX" "$n")
  response=$(curl -sS -X POST "${BASE_URL}/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"phone\":\"${phone}\",\"password\":\"${USER_PASSWORD}\"}")
  code=$(printf '%s' "$response" | jq -r '.code // empty')
  if [[ "$code" != "200" ]]; then
    printf 'failed,%s,%s\n' "$phone" "$response" > "${login_dir}/${n}.fail"
    exit 1
  fi
  user_id=$(printf '%s' "$response" | jq -r '.data.userId')
  user_token=$(printf '%s' "$response" | jq -r '.data.token')
  printf '%s,%s,%s\n' "$n" "$user_id" "$user_token" > "${login_dir}/${n}.ok"
}

export BASE_URL USER_PHONE_PREFIX USER_PHONE_WIDTH USER_PASSWORD login_dir
export -f login_one_user

set +e
seq 1 "$USER_COUNT" | xargs -n 1 -P "$LOGIN_PARALLELISM" bash -c 'login_one_user "$@"' _
login_status=$?
set -e

failed_count=$(find "$login_dir" -name '*.fail' | wc -l | tr -d ' ')
if [[ "$failed_count" != "0" ]]; then
  echo "有 ${failed_count} 个压测用户登录失败，前 10 条："
  find "$login_dir" -name '*.fail' -print0 | xargs -0 cat | head -10
  exit 1
fi

if [[ "$login_status" != "0" ]]; then
  echo "并发登录压测用户失败，login_status=${login_status}"
  exit "$login_status"
fi

declare -a user_ids
declare -a user_tokens
for n in $(seq 1 "$USER_COUNT"); do
  IFS=, read -r _ user_id user_token < "${login_dir}/${n}.ok"
  user_ids[$n]="$user_id"
  user_tokens[$n]="$user_token"
done

echo "生成 ${ROWS} 行 JMeter CSV：${OUT_FILE}"
for row in $(seq 1 "$ROWS"); do
  user_index=$(( ((row - 1) % USER_COUNT) + 1 ))
  user_id="${user_ids[$user_index]}"
  auth_token="${user_tokens[$user_index]}"
  admission_token=""

  if [[ "$WAITING_ROOM_ENABLED" == "true" ]]; then
    admission_token="admit_$(openssl rand -hex 16)"
    redis_key="waiting-room:admission:ticket:${TICKET_CATEGORY_ID}:user:${user_id}:token:${admission_token}"
    "${redis_cmd[@]}" SET "$redis_key" 1 EX "$ADMISSION_TTL_SECONDS" >/dev/null
  fi

  printf '%s,%s,%s,%s,%s,%s\n' \
    "$auth_token" "$SHOW_ID" "$SESSION_ID" "$TICKET_CATEGORY_ID" "$QUANTITY" "$admission_token" >> "$tmp_file"
done

mv "$tmp_file" "$OUT_FILE"
trap 'rm -rf "$login_dir"' EXIT

echo
echo "生成完成。"
wc -l "$OUT_FILE"
awk -F, 'NR==2 {print "columns=" NF ", tokenPrefix=" substr($1,1,10) ", admission=" ($6=="" ? "<empty>" : substr($6,1,6))}' "$OUT_FILE"
echo "JMeter DATA_FILE=${OUT_FILE}"
