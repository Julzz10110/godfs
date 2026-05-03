#!/usr/bin/env bash
# Gate: BenchmarkWriteSegmentSplit max ns/op across repeated runs must stay
# at or below the integer in testdata/bench/client_write_segment_max_nsop.txt
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
LIMIT_FILE="${BENCH_NSOP_LIMIT_FILE:-testdata/bench/client_write_segment_max_nsop.txt}"
OUT="$(mktemp)"
cleanup() { rm -f "$OUT"; }
trap cleanup EXIT

go test ./pkg/client -bench='^BenchmarkWriteSegmentSplit$' -benchtime=15x -count=5 -run='^$' 2>&1 | tee "$OUT"

python3 - "$OUT" "$LIMIT_FILE" <<'PY'
import math
import pathlib
import sys

out_path, limit_path = sys.argv[1], sys.argv[2]
text = pathlib.Path(out_path).read_text(encoding="utf-8", errors="replace")
vals = []
for line in text.splitlines():
    if "BenchmarkWriteSegmentSplit" not in line or "ns/op" not in line:
        continue
    parts = line.split()
    for i, p in enumerate(parts):
        if p == "ns/op" and i > 0:
            try:
                vals.append(float(parts[i - 1]))
            except ValueError:
                pass
            break
if not vals:
    print("bench_gate: no ns/op lines parsed from benchmark output", file=sys.stderr)
    sys.exit(2)
worst = math.ceil(max(vals))
lim = int(pathlib.Path(limit_path).read_text().strip().splitlines()[0].strip())
print(f"bench_gate: BenchmarkWriteSegmentSplit worst ns/op (ceiled) = {worst}, limit = {lim}")
sys.exit(0 if worst <= lim else 1)
PY
