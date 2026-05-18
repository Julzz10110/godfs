#!/usr/bin/env bash
# CI gate: promtool, Helm observability templates, Grafana dashboard shape, Helm/rules sync.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deployments/helm/godfs"
RULES_FILE="$ROOT/deployments/observability/rules/godfs.yaml"
CRD_FILE="$ROOT/deployments/observability/prometheus-rules-godfs.yaml"
DASH="$ROOT/deployments/observability/dashboards/godfs-overview.json"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing $1" >&2; exit 1; }; }
need helm
need yq
need jq

if ! command -v promtool >/dev/null 2>&1; then
  PT_VER="2.55.1"
  curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v${PT_VER}/prometheus-${PT_VER}.linux-amd64.tar.gz" \
    | tar -xz -C "$TMP"
  export PATH="$TMP/prometheus-${PT_VER}.linux-amd64:$PATH"
fi
need promtool

echo "== promtool: rules/godfs.yaml =="
promtool check rules "$RULES_FILE"

echo "== promtool: prometheus-rules-godfs.yaml (extracted groups) =="
yq '{groups: .spec.groups}' "$CRD_FILE" >"$TMP/crd-groups.yaml"
promtool check rules "$TMP/crd-groups.yaml"

echo "== helm template (observability) =="
helm template godfs "$CHART" -n godfs \
  --set prometheus.operator.enabled=true \
  >"$TMP/all.yaml"

helm template godfs "$CHART" -n godfs \
  --set prometheus.operator.enabled=true \
  --show-only templates/observability-prometheusrules.yaml \
  >"$TMP/prometheusrule.yaml"

yq '{groups: .spec.groups}' "$TMP/prometheusrule.yaml" >"$TMP/helm-groups.yaml"
promtool check rules "$TMP/helm-groups.yaml"

echo "== Helm rules match committed rules/godfs.yaml =="
yq -o=json '.groups' "$RULES_FILE" >"$TMP/rules.json"
yq -o=json '.groups' "$TMP/helm-groups.yaml" >"$TMP/helm.json"
diff -u "$TMP/rules.json" "$TMP/helm.json"

echo "== CRD spec.groups match rules/godfs.yaml =="
yq -o=json '.spec.groups' "$CRD_FILE" >"$TMP/crd.json"
diff -u "$TMP/rules.json" "$TMP/crd.json"

echo "== ServiceMonitor manifests present in helm render =="
grep -q 'kind: ServiceMonitor' "$TMP/all.yaml"

echo "== Grafana dashboard panels =="
for title in "Raft leaders" "Under-replicated chunks" "Rebalance queue depth" "REST requests / s" "Latency (REST p95, gRPC p99 max)"; do
  jq -e --arg t "$title" '.panels[] | select(.title == $t)' "$DASH" >/dev/null
done

echo "observability_check: OK"
