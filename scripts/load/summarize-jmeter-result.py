#!/usr/bin/env python3
import argparse
import csv
import math
from pathlib import Path

parser = argparse.ArgumentParser(description="Summarize Mock-Damai JMeter submit results.")
parser.add_argument("jtl", type=Path)
parser.add_argument("--label", default="02 提交异步下单请求")
parser.add_argument("--token-label", default="01 获取下单幂等 Token")
parser.add_argument("--warmup-seconds", type=float, default=0.0)
parser.add_argument("--target-qps", type=float, default=None)
args = parser.parse_args()

samples = []
with args.jtl.open("r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    required = {"timeStamp", "elapsed", "label", "success"}
    missing = required.difference(reader.fieldnames or [])
    if missing:
        raise SystemExit(f"JTL 缺少字段: {sorted(missing)}")
    for row in reader:
        if row.get("label") not in {args.label, args.token_label}:
            continue
        try:
            ts = int(row["timeStamp"])
            elapsed = float(row["elapsed"])
        except (TypeError, ValueError):
            continue
        samples.append({
            "ts": ts,
            "elapsed": elapsed,
            "label": row.get("label"),
            "ok": str(row.get("success", "")).lower() == "true",
            "response_code": str(row.get("responseCode", "")),
        })

submit_rows = sorted((s for s in samples if s["label"] == args.label), key=lambda x: x["ts"])
if not submit_rows:
    raise SystemExit(f"没有找到 sampler: {args.label}")

cutoff = submit_rows[0]["ts"] + int(args.warmup_seconds * 1000)
measured = [s for s in submit_rows if s["ts"] >= cutoff]
if not measured:
    raise SystemExit("Warm-up 之后没有可统计的 Submit 样本")

start = measured[0]["ts"]
end = max(s["ts"] + s["elapsed"] for s in measured)
duration = max((end - start) / 1000.0, 0.001)
latencies = sorted(s["elapsed"] for s in measured)
attempts = len(measured)
successes = sum(1 for s in measured if s["ok"])
failures = attempts - successes

token_rows = [s for s in samples if s["label"] == args.token_label and start <= s["ts"] <= end]
token_failures = sum(1 for s in token_rows if not s["ok"])

def percentile(values, p):
    idx = max(0, min(len(values) - 1, math.ceil(len(values) * p) - 1))
    return values[idx]

def is_http_or_transport_failure(sample):
    if sample["ok"]:
        return False
    code = sample["response_code"]
    if not code:
        return True
    try:
        numeric = int(code)
    except ValueError:
        return True
    return numeric < 200 or numeric >= 300

http_or_transport_failures = sum(1 for s in measured if is_http_or_transport_failure(s))
business_or_assertion_failures = failures - http_or_transport_failures
attempt_tps = attempts / duration
submit_tps = successes / duration

print("JMeter Submit Sampler Summary")
print(f"label={args.label}")
print(f"warmup_seconds={args.warmup_seconds:.1f}")
print(f"measure_duration_seconds={duration:.3f}")
print(f"attempts={attempts}")
print(f"accepted={successes}")
print(f"failed={failures}")
print(f"business_or_assertion_failures={business_or_assertion_failures}")
print(f"http_or_transport_failures={http_or_transport_failures}")
print(f"error_rate_percent={failures * 100.0 / attempts:.3f}")
print(f"attempt_tps={attempt_tps:.3f}")
print(f"submit_tps={submit_tps:.3f}")
print(f"p50_ms={percentile(latencies, 0.50):.3f}")
print(f"p95_ms={percentile(latencies, 0.95):.3f}")
print(f"p99_ms={percentile(latencies, 0.99):.3f}")
print(f"max_ms={max(latencies):.3f}")

if token_rows:
    print(f"token_requests={len(token_rows)}")
    print(f"token_request_tps={len(token_rows) / duration:.3f}")
    print(f"token_failures={token_failures}")

if args.target_qps is not None:
    if args.target_qps <= 0:
        raise SystemExit("--target-qps 必须大于 0")
    achievement = attempt_tps * 100.0 / args.target_qps
    deviation = (attempt_tps - args.target_qps) * 100.0 / args.target_qps
    print(f"target_qps={args.target_qps:.3f}")
    print(f"target_achievement_percent={achievement:.3f}")
    print(f"target_deviation_percent={deviation:.3f}")
    if abs(deviation) > 5.0:
        print("warning=实际 Submit attempt TPS 与 TARGET_QPS 偏差超过 5%；该 Run 不能被描述为准确命中目标速率。")
