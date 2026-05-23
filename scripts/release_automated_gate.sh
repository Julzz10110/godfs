#!/usr/bin/env bash
# Run automated release gates. See docs/RELEASE_CHECKLIST.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== go test (unit + e2e, excluding integration tag) =="
go test $(go list ./... | grep -v '/test/integration$') -count=1 -timeout=15m

echo "== observability (promtool + Helm rules) =="
bash scripts/observability_check.sh

echo "== k8s manifests (kubeconform, no cluster) =="
bash scripts/k8s_verify_manifests.sh

echo ""
echo "Automated gates passed."
echo "Complete manual steps R1–R10 in docs/RELEASE_CHECKLIST.md before tagging."
