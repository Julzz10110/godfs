#!/usr/bin/env bash
# Regenerate plain-manifest Prometheus rules from Helm defaults (SLO thresholds in values.yaml).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deployments/helm/godfs"
RULES_DIR="$ROOT/deployments/observability/rules"
RULES_FILE="$RULES_DIR/godfs.yaml"
CRD_FILE="$ROOT/deployments/observability/prometheus-rules-godfs.yaml"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing $1" >&2; exit 1; }; }
need helm
need yq

helm template godfs "$CHART" -n godfs \
  --set prometheus.operator.enabled=true \
  --show-only templates/observability-prometheusrules.yaml \
  >"$TMP/prometheusrule.yaml"

{
  echo "# Prometheus rule groups for goDFS (promtool + plain PrometheusRule bundle)."
  echo "# SLO thresholds match Helm defaults (deployments/helm/godfs/values.yaml prometheus.slo.*)."
  echo "# After changing Helm SLO values, run: bash scripts/sync_observability_rules.sh"
  yq '{groups: .spec.groups}' "$TMP/prometheusrule.yaml"
} >"$RULES_FILE"

yq -n "
  .apiVersion = \"monitoring.coreos.com/v1\" |
  .kind = \"PrometheusRule\" |
  .metadata.name = \"godfs-rules\" |
  .metadata.namespace = \"godfs\" |
  .spec.groups = load(\"$RULES_FILE\").groups
" >"$CRD_FILE"

echo "synced $RULES_FILE and $CRD_FILE from Helm chart"
