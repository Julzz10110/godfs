#!/usr/bin/env bash
# benchstat compare for BenchmarkE2E_* vs committed baseline.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BASE="${ROOT}/testdata/bench/baseline_BenchmarkE2E_SingleChunkWrite_1Replica.txt"
OUT="$(mktemp)"
TMP_NEW=""
trap 'rm -f "${OUT}"; [[ -n "${TMP_NEW}" ]] && rm -f "${TMP_NEW}"' EXIT

NEW="${1:-}"
if [[ -z "${NEW}" ]]; then
  TMP_NEW="$(mktemp)"
  NEW="${TMP_NEW}"
  bash scripts/bench_e2e_report.sh "${NEW}"
fi

if [[ ! -f "${BASE}" ]]; then
  echo "benchstat_e2e_report: missing baseline ${BASE}" >&2
  echo "Capture with: bash scripts/bench_e2e_report.sh testdata/bench/baseline_BenchmarkE2E_SingleChunkWrite_1Replica.txt" >&2
  exit 2
fi

go run golang.org/x/perf/cmd/benchstat@latest "${BASE}" "${NEW}" | tee "${OUT}"

if grep -Eiq 'statistically significant' "${OUT}" && grep -Eiq 'slower' "${OUT}"; then
  echo "benchstat_e2e_report: significant E2E slowdown vs baseline" >&2
  exit 1
fi
echo "benchstat_e2e_report: no significant E2E slowdown"
