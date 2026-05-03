#!/usr/bin/env bash
# CI / local: client micro-benchmark with ns/op gate (see scripts/bench_gate.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$ROOT/scripts/bench_gate.sh"
