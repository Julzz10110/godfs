#!/usr/bin/env bash
# Manifest gate + optional live cluster checks (kind/minikube).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== Step 1: manifest validation (no cluster) =="
bash scripts/k8s_verify_manifests.sh

if ! command -v kubectl >/dev/null 2>&1; then
	echo "kubectl not found; skipping live cluster steps (2–8)."
	exit 0
fi

if ! kubectl config current-context >/dev/null 2>&1; then
	echo "No kubectl context; skipping live cluster steps (2–8)."
	exit 0
fi

NS="${GODFS_K8S_NAMESPACE:-godfs}"
echo "== Step 2: namespace and workloads ($NS) =="
kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
kubectl get pods -n "$NS" -o wide || true
kubectl get pdb -n "$NS" || true

echo "== Step 3: optional membership smoke (set MASTER_ADDR, e.g. 127.0.0.1:9090) =="
if [[ -n "${MASTER_ADDR:-}" ]]; then
	bash scripts/k8s_raft_membership_smoke.sh
else
	echo "Skip membership smoke: export MASTER_ADDR after kubectl port-forward svc/godfs-master -n $NS 9090:9090"
fi

echo "Done. Complete steps 4–8 in deployments/k8s/OPERATIONS.md (apply, restart pod, ListMasters)."
