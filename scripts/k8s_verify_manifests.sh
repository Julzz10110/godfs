#!/usr/bin/env bash
# Validate Kubernetes manifests without a live cluster (CI and local).
# - kubectl kustomize: render check (no API server)
# - kubectl apply --dry-run=client --validate=false: client dry-run without OpenAPI download
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi

verify_k() {
  local dir=$1
  echo "== kustomize build: $dir =="
  kubectl kustomize "$dir" >/dev/null

  echo "== kubectl apply dry-run: $dir =="
  kubectl apply -k "$dir" --dry-run=client --validate=false
}

verify_k deployments/k8s
verify_k deployments/k8s/overlays/production

echo "k8s manifest verification OK"
