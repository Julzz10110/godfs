# goDFS observability bundle

Prometheus rules, ServiceMonitors, and a starter Grafana dashboard for production SRE.

## Contents

| File | Purpose |
|------|---------|
| `rules/godfs.yaml` | Rule groups (`promtool check rules`); synced from Helm defaults |
| `prometheus-rules-godfs.yaml` | PrometheusRule CR for `kubectl apply` |
| `servicemonitors-godfs.yaml` | ServiceMonitors for plain `deployments/k8s` labels |
| `dashboards/godfs-overview.json` | Grafana: leader, under-replicated, rebalance queue, REST RPS/latency |

Helm chart (`deployments/helm/godfs`): set `prometheus.operator.enabled=true` for PrometheusRule + ServiceMonitor templates and tunable `prometheus.slo.*` thresholds.

## Apply (Kubernetes)

**Helm (recommended for tunable SLO):**

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace \
  --set prometheus.operator.enabled=true
```

**Plain manifests:**

```bash
kubectl apply -f deployments/observability/prometheus-rules-godfs.yaml
kubectl apply -f deployments/observability/servicemonitors-godfs.yaml
```

Import the dashboard: **Dashboards → Import → Upload JSON** (`dashboards/godfs-overview.json`).

## Change SLO thresholds

1. Edit `deployments/helm/godfs/values.yaml` → `prometheus.slo.*` (and dataplane thresholds if needed).
2. Regenerate plain manifests: `bash scripts/sync_observability_rules.sh`
3. Commit Helm values + `rules/godfs.yaml` + `prometheus-rules-godfs.yaml`.

## Validate (CI runs this)

```bash
bash scripts/observability_check.sh
```

Includes: `promtool check rules`, `helm template` with `prometheus.operator.enabled=true`, diff Helm vs committed rules (Ruby YAML parse), Grafana panel check. Requires: `helm`, `ruby`, `jq`, `promtool` (downloaded by the script if missing).

## SLO defaults (lab / starter)

Tune per cluster. Documented in `docs/RUNBOOK.md`.

| Signal | Default threshold | Helm value |
|--------|-------------------|------------|
| gRPC unary p99 (any method) | 2s | `prometheus.slo.grpcUnaryP99MaxSeconds` |
| PrepareWrite p99 | 1s | `prometheus.slo.prepareWriteP99Seconds` |
| REST aggregate p95 | 5s | `prometheus.slo.restP95Seconds` |
| REST 5xx share | 5% | `prometheus.slo.rest5xxRate` |

On a healthy local compose stack, PrepareWrite p99 is typically **&lt; 500ms**; REST small-object GET p95 **&lt; 1s**.

## Histogram buckets

gRPC and REST latency histograms use buckets up to **30s** (see `internal/observability/prometheus.go` and `internal/restgateway/observability.go`) for accurate p95/p99 recording rules.
