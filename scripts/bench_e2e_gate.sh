#!/usr/bin/env bash
# Optional nightly gate: E2E bench regression vs baseline.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
bash scripts/bench_e2e_report.sh "$TMP"
bash scripts/benchstat_e2e_report.sh "$TMP"
