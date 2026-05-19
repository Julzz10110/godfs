#!/usr/bin/env bash
# capture e2e benchmark lines for bench workflow artifacts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
OUT="${1:-bench_E2E.txt}"
go test ./test/e2e -bench='^BenchmarkE2E_' -benchtime=3x -count=3 -run='^$' -timeout=20m 2>&1 | tee "$OUT"
echo "wrote $OUT"
