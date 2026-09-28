#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8081}"
USER_COUNT="${USER_COUNT:-1000}"
USER_PHONE_PREFIX="${USER_PHONE_PREFIX:-139010}"
USER_PHONE_WIDTH="${USER_PHONE_WIDTH:-5}"
USER_PASSWORD="${USER_PASSWORD:-Test123456}"
USERNAME_PREFIX="${USERNAME_PREFIX:-load_user_}"
USER_PARALLELISM="${USER_PARALLELISM:-8}"

if ! command -v jq >/dev/null 2>&1; then
  echo "缺少 jq"
  exit 1
fi

if [[ "$USER_COUNT" -lt 1 ]]; then
  echo "USER_COUNT 必须大于 0"
  exit 1
fi

echo "准备 ${USER_COUNT} 个压测用户..."
echo "BASE_URL=${BASE_URL}"
echo "phone=${USER_PHONE_PREFIX}<${USER_PHONE_WIDTH}位序号>"
echo "并发数=${USER_PARALLELISM}"

work_dir="$(mktemp -d /tmp/mock-damai-load-users.XXXXXX)"
trap 'rm -rf "$work_dir"' EXIT

prepare_one_user() {
  local n="$1"
  local phone username login_response login_code register_response register_code
  phone=$(printf "%s%0${USER_PHONE_WIDTH}d" "$USER_PHONE_PREFIX" "$n")
  username=$(printf "%s%0${USER_PHONE_WIDTH}d" "$USERNAME_PREFIX" "$n")

  login_response=$(curl -sS -X POST "${BASE_URL}/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"phone\":\"${phone}\",\"password\":\"${USER_PASSWORD}\"}")
  login_code=$(printf '%s' "$login_response" | jq -r '.code // empty')

  if [[ "$login_code" == "200" ]]; then
    printf 'existing,%s\n' "$phone" > "${work_dir}/${n}.ok"
    return
  fi

  register_response=$(curl -sS -X POST "${BASE_URL}/api/auth/register" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${username}\",\"phone\":\"${phone}\",\"password\":\"${USER_PASSWORD}\"}")
  register_code=$(printf '%s' "$register_response" | jq -r '.code // empty')
  if [[ "$register_code" == "200" ]]; then
    printf 'created,%s\n' "$phone" > "${work_dir}/${n}.ok"
  else
    printf 'failed,%s,%s\n' "$phone" "$register_response" > "${work_dir}/${n}.fail"
    exit 1
  fi
}

export BASE_URL USER_PHONE_PREFIX USER_PHONE_WIDTH USER_PASSWORD USERNAME_PREFIX work_dir
export -f prepare_one_user

set +e
seq 1 "$USER_COUNT" | xargs -n 1 -P "$USER_PARALLELISM" bash -c 'prepare_one_user "$@"' _
xargs_status=$?
set -e

failed_count=$(find "$work_dir" -name '*.fail' | wc -l | tr -d ' ')
if [[ "$failed_count" != "0" ]]; then
  echo "有 ${failed_count} 个压测用户准备失败，前 10 条："
  find "$work_dir" -name '*.fail' -print0 | xargs -0 cat | head -10
  exit 1
fi

if [[ "$xargs_status" != "0" ]]; then
  echo "并发准备压测用户失败，xargs_status=${xargs_status}"
  exit "$xargs_status"
fi

existing=$(awk -F, '$1 == "existing" {count++} END {print count + 0}' "$work_dir"/*.ok 2>/dev/null)
created=$(awk -F, '$1 == "created" {count++} END {print count + 0}' "$work_dir"/*.ok 2>/dev/null)

echo
echo "压测用户准备完成：已存在=${existing}，新建=${created}，总数=${USER_COUNT}"
