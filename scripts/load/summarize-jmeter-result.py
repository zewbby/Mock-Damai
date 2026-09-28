#!/usr/bin/env python3
import argparse
import csv
import math
from pathlib import Path

parser = argparse.ArgumentParser(description="Summarize the async-order submit sampler from a JMeter CSV JTL.")
parser.add_argument("jtl", type=Path)
parser.add_argument("--label", default="02 提交异步下单请求")
parser.add_argument("--warmup-seconds", type=float, default=0.0)
args = parser.parse_args()

rows = []
with args.jtl.open("r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    required = {"timeStamp", "elapsed", "label", "success"}
    missing = required.difference(reader.fieldnames or [])
    if missing:
        raise SystemExit(f"JTL 缺少字段: {sorted(missing)}")
    for row in reader:
        if row.get("label") != args.label:
            continue
        try:
            ts = int(row["timeStamp"])
            elapsed = float(row["elapsed"])
        except (TypeError, ValueError):
            continue
        rows.append((ts, elapsed, str(row.get("success", "")).lower() == "true"))

if not rows:
    raise SystemExit(f"没有找到 sampler: {args.label}")

rows.sort(key=lambda x: x[0])
cutoff = rows[0][0] + int(args.warmup_seconds * 1000)
measured = [r for r in rows if r[0] >= cutoff]
if not measured:
    raise SystemExit("Warm-up 之后没有可统计样本")

start = measured[0][0]
end = max(ts + elapsed for ts, elapsed, _ in measured)
duration = max((end - start) / 1000.0, 0.001)
latencies = sorted(elapsed for _, elapsed, _ in measured)
attempts = len(measured)
successes = sum(1 for _, _, ok in measured if ok)
failures = attempts - successes

def percentile(values, p):
    idx = max(0, min(len(values) - 1, math.ceil(len(values) * p) - 1))
    return values[idx]

print("JMeter Submit Sampler Summary")
print(f"label={args.label}")
print(f"warmup_seconds={args.warmup_seconds:.1f}")
print(f"measure_duration_seconds={duration:.3f}")
print(f"attempts={attempts}")
print(f"accepted={successes}")
print(f"failed={failures}")
print(f"error_rate_percent={failures * 100.0 / attempts:.3f}")
print(f"attempt_tps={attempts / duration:.3f}")
print(f"submit_tps={successes / duration:.3f}")
print(f"p50_ms={percentile(latencies, 0.50):.3f}")
print(f"p95_ms={percentile(latencies, 0.95):.3f}")
print(f"p99_ms={percentile(latencies, 0.99):.3f}")
print(f"max_ms={max(latencies):.3f}")
