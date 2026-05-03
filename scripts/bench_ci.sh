#!/usr/bin/env bash
# Micro-benchmark gate for CI (no saved baseline; catches gross regressions via fixed -benchtime).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# Only the smaller split benchmark (avoids 192 MiB alloc of BenchmarkWriteSegmentSplit_64MiBChunks on small runners).
go test ./pkg/client -bench='^BenchmarkWriteSegmentSplit$' -benchtime=10x -run='^$' -count=1
