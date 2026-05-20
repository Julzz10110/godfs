#!/usr/bin/env bash
# Validate Kubernetes manifests without a live cluster (CI and local).
# Uses kubectl kustomize only — kubectl apply dry-run still contacts the API for discovery.
# Optional: kubeconform (installed in CI) for schema checks; -ignore-missing-schemas for Prometheus Operator CRDs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi

verify_k() {
  local dir=$1
  echo "== kubectl kustomize: $dir =="
  local manifest count
  manifest="$(kubectl kustomize "$dir")"
  count="$(printf '%s\n' "$manifest" | grep -c '^apiVersion:' || true)"
  if [[ "${count:-0}" -lt 1 ]]; then
    echo "no resources rendered from $dir" >&2
    exit 1
  fi
  echo "rendered ${count} resource(s)"

  if command -v kubeconform >/dev/null 2>&1; then
    echo "== kubeconform: $dir =="
    printf '%s\n' "$manifest" | kubeconform \
      -kubernetes-version 1.29.0 \
      -ignore-missing-schemas \
      -summary
  fi
}

verify_k deployments/k8s
verify_k deployments/k8s/overlays/production

echo "k8s manifest verification OK"
