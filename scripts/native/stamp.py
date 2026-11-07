#!/usr/bin/env python3
"""Merge key=value fields into one run's row of docs/results.json (used by the run scripts after the verifier)."""
import json
import sys


def coerce(v):
    if v in ("True", "true"):
        return True
    if v in ("False", "false"):
        return False
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


def main():
    path, run_id, *pairs = sys.argv[1:]
    rows = json.load(open(path, encoding="utf-8"))
    hit = False
    for r in rows:
        if r["run_id"] == run_id:
            r.update({k: coerce(v) for k, v in (p.split("=", 1) for p in pairs)})
            hit = True
    if not hit:
        sys.exit(f"run {run_id} not in {path}")
    json.dump(rows, open(path, "w", encoding="utf-8"), indent=2)


if __name__ == "__main__":
    main()
