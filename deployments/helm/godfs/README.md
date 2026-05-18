# godfs Helm chart

Minimal chart to deploy:

- Raft masters (StatefulSet)
- chunkserver Deployment
- restgateway Deployment

## Install

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace
```

## Prometheus / SLO

Enable Prometheus Operator resources and tunable latency alerts:

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace \
  --set prometheus.operator.enabled=true
```

Thresholds: `values.yaml` → `prometheus.slo.*`. After changes, sync plain manifests: `bash scripts/sync_observability_rules.sh`.

CI validates rules via `bash scripts/observability_check.sh`.

## Notes

- Operations (bootstrap, rolling updates, membership RPCs, PDB): **`deployments/k8s/OPERATIONS.md`**.
- Raft bootstrap: `raft.bootstrap` must be enabled only for the first start of an empty cluster.
- TLS: configure `tls.secretName` and mount paths via `values.yaml`.  `GODFS_TLS_RELOAD=1` enables hot reload.
- Auth/RBAC: configure `auth.secretName` keys.

