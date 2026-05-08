#!/usr/bin/env python3
"""Bench regression gate.

Compares two bench JSON results (a baseline and a candidate) and exits non-zero
if the candidate has regressed beyond the configured threshold (default 30%).

Usage:
  bench/regress.py --baseline bench/results/baseline.json --candidate bench/results/<latest>.json
  bench/regress.py --baseline bench/results/baseline.json --candidate-latest

The metrics that gate:
  * throughput_rps   — must NOT drop more than --threshold (default 0.30 = 30%)
  * p99_us           — must NOT increase more than --threshold

The bar is intentionally loose (30%) because micro-bench noise on a shared CI
runner is significant. CI uses this as a guard against major regressions, not
fine-grained perf tuning.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys


def load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def find_latest(results_dir: str) -> str | None:
    candidates = sorted(glob.glob(os.path.join(results_dir, "*.json")))
    return candidates[-1] if candidates else None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", required=True, help="baseline JSON path")
    p.add_argument("--candidate", help="candidate JSON path")
    p.add_argument(
        "--candidate-latest",
        action="store_true",
        help="use newest file in bench/results/",
    )
    p.add_argument(
        "--results-dir",
        default="bench/results",
        help="directory to scan when --candidate-latest is given",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.30,
        help="max allowed relative drift (0.30 = 30%%)",
    )
    args = p.parse_args()

    if not args.candidate and not args.candidate_latest:
        p.error("either --candidate or --candidate-latest required")

    cand_path = args.candidate
    if not cand_path:
        cand_path = find_latest(args.results_dir)
        if not cand_path:
            print(f"no candidate found in {args.results_dir}", file=sys.stderr)
            return 2

    base = load(args.baseline)
    cand = load(cand_path)

    print(f"baseline:  {args.baseline}")
    print(f"candidate: {cand_path}")
    print(
        f"  baseline  throughput={base.get('throughput_rps', 0):.1f}rps "
        f"p99={base.get('p99_us', 0)}us"
    )
    print(
        f"  candidate throughput={cand.get('throughput_rps', 0):.1f}rps "
        f"p99={cand.get('p99_us', 0)}us"
    )

    base_tp = float(base.get("throughput_rps", 0))
    cand_tp = float(cand.get("throughput_rps", 0))
    base_p99 = float(base.get("p99_us", 0)) or 1.0
    cand_p99 = float(cand.get("p99_us", 0))

    failures = []
    if base_tp > 0:
        tp_drift = (base_tp - cand_tp) / base_tp
        print(f"  throughput drift: {tp_drift * 100:+.1f}% (negative=regressed)")
        if tp_drift > args.threshold:
            failures.append(
                f"throughput regressed by {tp_drift * 100:.1f}% > "
                f"{args.threshold * 100:.0f}%"
            )

    p99_drift = (cand_p99 - base_p99) / base_p99
    print(f"  p99 latency drift: {p99_drift * 100:+.1f}% (positive=regressed)")
    if p99_drift > args.threshold:
        failures.append(
            f"p99 regressed by {p99_drift * 100:.1f}% > {args.threshold * 100:.0f}%"
        )

    if failures:
        print("FAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("OK: within threshold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
