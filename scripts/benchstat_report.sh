#!/usr/bin/env bash
# Compare current BenchmarkWriteSegmentSplit output to committed baseline using benchstat.
# Usage: benchstat_report.sh [path-to-new-bench.txt]
# Exits 1 if benchstat output suggests a statistically significant slowdown (grep heuristic).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BASE="${ROOT}/testdata/bench/baseline_ClientWriteSegmentSplit.txt"
OUT="$(mktemp)"
TMP_NEW=""

cleanup() {
  rm -f "${OUT}"
  if [[ -n "${TMP_NEW}" ]]; then
    rm -f "${TMP_NEW}"
  fi
}
trap cleanup EXIT

NEW="${1:-}"
if [[ -z "${NEW}" ]]; then
  TMP_NEW="$(mktemp)"
  NEW="${TMP_NEW}"
  go test ./pkg/client -bench='^BenchmarkWriteSegmentSplit$' -benchtime=20x -count=6 -run='^$' 2>&1 | tee "${NEW}"
fi

if [[ ! -f "${BASE}" ]]; then
  echo "benchstat_report: missing baseline ${BASE}" >&2
  exit 2
fi

go run golang.org/x/perf/cmd/benchstat@latest "${BASE}" "${NEW}" | tee "${OUT}"

if grep -Eiq 'statistically significant' "${OUT}" && grep -Eiq 'slower' "${OUT}"; then
  echo "benchstat_report: significant slowdown vs baseline" >&2
  exit 1
fi
echo "benchstat_report: no significant slowdown"
