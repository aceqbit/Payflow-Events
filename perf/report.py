#!/usr/bin/env python3
"""make report: docs/results.json (+ the Prometheus API over each run's measure window) -> docs/results.md and PNGs.

No number in the output is typed by hand. Every figure is either a field of a results.json row or a Prometheus query
result for that row's RUN_ID and measure window; a figure that cannot be fetched is printed as n/a, never estimated.
Improvement percentages are computed here from those figures: (before - after) / before for lower-is-better metrics.
Rows get their window from run_start_epoch / run_end_epoch (set by the run scripts through the verifier).
"""
import json
import os
import platform
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(os.environ.get("RESULTS_FILE", ROOT / "docs" / "results.json"))
OUT_MD = Path(os.environ.get("RESULTS_MD", ROOT / "docs" / "results.md"))
PROM = os.environ.get("PROM_URL", "http://localhost:9090")
SECTIONS = []


def section(title):
    def deco(fn):
        SECTIONS.append((title, fn))
        return fn
    return deco


# ---------------------------------------------------------------- environment

def machine_spec():
    mem = "n/a"
    try:
        if platform.system() == "Windows":
            import ctypes

            class MSX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong), ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong), ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong), ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong), ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            s = MSX()
            s.dwLength = ctypes.sizeof(MSX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s))
            mem = f"{s.ullTotalPhys / 2**30:.0f} GB"
        else:
            with open("/proc/meminfo") as f:
                mem = f"{int(f.readline().split()[1]) / 2**20:.0f} GB"
    except Exception:
        pass
    return (f"{platform.system()} {platform.release()}, {platform.machine()}, {os.cpu_count()} logical CPUs, {mem} RAM, "
            f"{platform.processor() or 'cpu n/a'}")


# ---------------------------------------------------------------- prometheus

def prom_query(expr, at=None):
    qs = {"query": expr}
    if at:
        qs["time"] = str(at)
    try:
        with urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode(qs), timeout=10) as r:
            res = json.load(r)["data"]["result"]
        return float(res[0]["value"][1]) if res else None
    except Exception:
        return None


def window(row):
    s, e = row.get("run_start_epoch"), row.get("run_end_epoch")
    if s is None or e is None or e <= s:
        return None
    return int(s), int(e), int(e - s)


def fmt(v, unit="", digits=3):
    return "n/a" if v is None or v != v else f"{v:.{digits}f}{unit}"


def cap(v, top, digits=3):
    """A quantile that lands on the histogram's top bucket is only a lower bound; say so instead of printing it as exact."""
    if v is None or v != v:
        return "n/a"
    return f">={top:g} (top bucket)" if v >= top * 0.97 else f"{v:.{digits}f}"


def pct(before, after):
    """Improvement of a lower-is-better metric, in percent. None when either side is missing or the base is zero."""
    if before is None or after is None or before == 0:
        return None
    return (before - after) / before * 100.0


def fpct(v):
    return "n/a" if v is None else f"{v:+.1f}%"


def quantile(metric, run, w, q, extra=""):
    """Latency quantile in seconds. Runs exported before base-time-unit=seconds carry *_milliseconds series; both are read."""
    _, end, span = w
    base = metric.removesuffix("_seconds")
    v = prom_query(f'histogram_quantile({q}, sum by (le) (increase({base}_seconds_bucket{{run_id="{run}"{extra}}}[{span}s])))', end)
    if v is not None:
        return v
    ms = prom_query(f'histogram_quantile({q}, sum by (le) (increase({base}_milliseconds_bucket{{run_id="{run}"{extra}}}[{span}s])))', end)
    return None if ms is None else ms / 1000.0


def run_metrics(r):
    """All Prometheus-derived figures for one row (None where unavailable)."""
    w = window(r)
    m = {"e2e_p50": None, "e2e_p99": None, "relay_p99": None, "consume_p99": None, "max_lag": None, "applied_per_s": None}
    if not w:
        return m
    run = r["run_id"]
    m["e2e_p50"] = quantile("payflow_events_e2e_latency_seconds", run, w, 0.5, ',poison="false"')
    m["e2e_p99"] = quantile("payflow_events_e2e_latency_seconds", run, w, 0.99, ',poison="false"')
    m["relay_p99"] = quantile("payflow_events_relay_latency_seconds", run, w, 0.99)
    m["consume_p99"] = quantile("payflow_events_consume_latency_seconds", run, w, 0.99)
    m["max_lag"] = prom_query(f'max_over_time(events:max_lag_seconds{{run_id="{run}"}}[{w[2]}s])', w[1])
    m["applied_per_s"] = prom_query(f'sum(increase(payflow_events_processed_total{{result="applied",run_id="{run}"}}[{w[2]}s])) / {w[2]}', w[1])
    return m


def bench_rows(rows):
    return [r for r in rows if r.get("bench")]


def by_bench(rows):
    return {r["bench"]: r for r in rows if r.get("bench")}


# ---------------------------------------------------------------- sections

@section("Correctness")
def correctness(rows, ctx):
    out = ["| RUN_ID | scenario | relay | committed | lost | phantom | duplicate effects | duplicates received | order violations | DLQ | pills | rollup mismatch | outbox clean |",
           "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    keys = ["run_id", "scenario", "relay", "committed", "lost", "phantom", "duplicate_effects", "duplicates_received",
            "order_violations", "dlq_count", "pills_injected", "rollup_mismatch_minor", "outbox_clean"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(k, "n/a")) for k in keys) + " |")
    return out


@section("Benchmarks: latency split, throughput, lag, partition imbalance")
def benchmarks(rows, ctx):
    out = ["| bench | RUN_ID | offered/s | partitions x consumers | key | hot share | e2e p50 (s) | e2e p99 (s) | relay p99 (s) | consume p99 (s) | max lag (s) | partition imbalance | committed/window (events/s) |",
           "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    n = 0
    ctx["metrics"] = {}
    for r in bench_rows(rows):
        m = run_metrics(r)
        ctx["metrics"][r["bench"]] = m
        w = window(r)
        eff = r["committed"] / w[2] if w else None
        out.append(f"| {r['bench']} | {r['run_id']} | {r.get('offered_rate', 'n/a')} | {r.get('partitions', 'n/a')} x {r.get('consumers', 'n/a')} "
                   f"| {r.get('key_strategy', 'n/a')} | {r.get('hot_share', 'n/a')} | {cap(m['e2e_p50'], 5)} | {cap(m['e2e_p99'], 5)} | {cap(m['relay_p99'], 1)} "
                   f"| {cap(m['consume_p99'], 1)} | {fmt(m['max_lag'], digits=1)} | {fmt(r.get('partition_imbalance'), digits=2)} | {fmt(eff, digits=0)} |")
        n += 1
    return out if n else ["No benchmark run recorded yet."]


# Phase 1 vs Phase 2: each entry is (title, before bench, after bench, [(label, getter, lower_is_better)])
COMPARISONS = [
    ("Skew: partition key = merchant_id (naive) -> entity key (aggregate_id), 40% hot merchant", "skew-merchant", "skew-aggregate"),
    ("Skew: partition key = merchant_id (naive) -> salted two-stage rollup, 40% hot merchant", "skew-merchant", "skew-salted"),
    ("Poison pills: healthy-event cost of 0.1% pills (no pills -> with pills, same load)", "pills-off", "pills-on"),
]


@section("Phase 1 -> Phase 2 (percent change, computed from the rows above)")
def improvements(rows, ctx):
    rb = by_bench(rows)
    metrics = ctx.get("metrics", {})
    out = ["| comparison | metric | before | after | change |", "|---|---|---:|---:|---:|"]
    n = 0
    for title, b, a in COMPARISONS:
        if b not in rb or a not in rb:
            continue
        mb, ma = metrics.get(b) or run_metrics(rb[b]), metrics.get(a) or run_metrics(rb[a])
        figures = [("e2e p50 (s)", mb["e2e_p50"], ma["e2e_p50"], 3), ("e2e p99 (s, lower bound when capped at 5)", mb["e2e_p99"], ma["e2e_p99"], 3), ("max lag (s)", mb["max_lag"], ma["max_lag"], 1),
                   ("partition imbalance (max/mean)", rb[b].get("partition_imbalance"), rb[a].get("partition_imbalance"), 2)]
        for label, x, y, d in figures:
            out.append(f"| {title} | {label} | {fmt(x, digits=d)} | {fmt(y, digits=d)} | {fpct(pct(x, y))} |")
            n += 1
    return out if n else ["Needs the skew-* and pills-* benchmark runs."]


@section("Rollup exactness")
def rollup(rows, ctx):
    rs = [r for r in rows if r.get("rollup")]
    if not rs:
        return ["No rollup run recorded yet."]
    out = ["| RUN_ID | bench | committed | rollup mismatch (minor units) | duplicates received |", "|---|---|---:|---:|---:|"]
    for r in rs:
        out.append(f"| {r['run_id']} | {r.get('bench', '')} | {r['committed']} | {r['rollup_mismatch_minor']} | {r['duplicates_received']} |")
    return out


# ---------------------------------------------------------------- main

def main():
    rows = json.loads(RESULTS.read_text(encoding="utf-8")) if RESULTS.exists() and RESULTS.stat().st_size else []
    ctx = {"machine": machine_spec()}
    md = ["# Results", "", f"Generated by `make report` from `docs/results.json` and the Prometheus API at {PROM}.",
          f"Machine: {ctx['machine']}.", "",
          "Every number below is read from `docs/results.json` or Prometheus for the RUN_ID shown, and every percentage is computed from "
          "those figures. `n/a` means the source was not available; nothing is estimated. Runs are single-node (one Kafka broker, "
          "replication factor 1) on one laptop, so they compare configurations against each other and are not cluster benchmarks.", ""]
    for title, fn in SECTIONS:
        md += [f"## {title}", ""] + fn(rows, ctx) + [""]
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"wrote {OUT_MD} ({len(rows)} runs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
