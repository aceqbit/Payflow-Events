#!/usr/bin/env python3
"""BACKWARD compatibility check for JSON schemas: a reader on the NEW schema must accept data of the OLD schema.

usage: check-schema-compat.py OLD.json NEW.json   (exit 1 when incompatible)
       check-schema-compat.py --dir DIR --against GIT_REF
"""
import json
import subprocess
import sys
from pathlib import Path


def problems(old, new):
    out = []
    op, np_ = old.get("properties", {}), new.get("properties", {})
    for name in set(new.get("required", [])) - set(old.get("required", [])):
        out.append(f"new required property '{name}'")
    for name, spec in op.items():
        if name not in np_:
            if new.get("additionalProperties") is False:
                out.append(f"property '{name}' removed while additionalProperties=false")
            continue
        if spec.get("type") != np_[name].get("type"):
            out.append(f"property '{name}' type {spec.get('type')} -> {np_[name].get('type')}")
    return out


def main(argv):
    if argv[0] == "--dir":
        d, ref = Path(argv[1]), argv[argv.index("--against") + 1]
        bad = 0
        for f in sorted(d.glob("*.json")):
            try:
                old = subprocess.check_output(["git", "show", f"{ref}:{f.as_posix()}"], stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError:
                continue  # new schema file
            p = problems(json.loads(old), json.loads(f.read_text()))
            for m in p:
                print(f"INCOMPATIBLE {f.name}: {m}")
            bad += len(p)
        return 1 if bad else 0
    p = problems(json.loads(Path(argv[0]).read_text()), json.loads(Path(argv[1]).read_text()))
    for m in p:
        print("INCOMPATIBLE:", m)
    return 1 if p else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
