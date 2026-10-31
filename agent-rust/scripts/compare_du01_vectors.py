import json
import sys

def python_rows(path):
    with open(path, encoding="utf-8") as stream:
        return {row["ref"]: row for row in map(json.loads, stream) if row.get("ref")}

def rust_rows(path):
    rows = {}
    with open(path, encoding="utf-8") as stream:
        for line in stream:
            marker = "DU01_C01 "
            if marker in line:
                row = json.loads(line.split(marker, 1)[1])
                rows[row["ref"]] = row
    return rows

expected, actual = python_rows(sys.argv[1]), rust_rows(sys.argv[2])
if set(expected) != set(actual):
    raise SystemExit(f"profile refs differ: Python={sorted(expected)} Rust={sorted(actual)}")
for ref in sorted(expected):
    for key in ("raw_sha256", "resolved_sha256", "resolved_preimage", "config"):
        if expected[ref][key] != actual[ref][key]:
            raise SystemExit(f"{ref}: {key} differs\nPython: {expected[ref][key]}\nRust: {actual[ref][key]}")
    print(f"PASS {ref}: raw and resolved identities match")
