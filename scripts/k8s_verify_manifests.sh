#!/usr/bin/env bash
# Validate Kubernetes manifests (client-side dry-run). Used in CI and locally.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi

echo "== kustomize base =="
kubectl apply -k deployments/k8s --dry-run=client

echo "== kustomize production overlay =="
kubectl apply -k deployments/k8s/overlays/production --dry-run=client

echo "k8s manifest dry-run OK"
