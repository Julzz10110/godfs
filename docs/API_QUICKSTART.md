# REST, FUSE, and Helm quick starts

**Index:** one-page navigation for all entry paths — [`GETTING_STARTED.md`](GETTING_STARTED.md).

This document helps **external teams get productive** without reading the source:

- concise **REST** and **FUSE** quick starts,
- copy-paste examples (`curl`, Python),
- a minimal **Helm** chart layout for Kubernetes.

## Quick start (REST, local)

Start a single-node master, one chunk, and the REST gateway (Docker Compose):

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

Upload and download a file via REST:

```bash
curl -X POST "http://127.0.0.1:8080/v1/fs/mkdir" \
  -H "Content-Type: application/json" \
  -d '{"path":"/data"}'

curl -X PUT "http://127.0.0.1:8080/v1/fs/content?path=/data/hello.txt" \
  --data-binary @./local.txt

curl -X GET "http://127.0.0.1:8080/v1/fs/content?path=/data/hello.txt" \
  -o ./out.txt
```

Auth (optional): send `Authorization: Bearer <token>` (API key / JWT) on every REST request.

Use **HTTPS** on the gateway when `GODFS_REST_HTTPS_ENABLED=1` and TLS key material is set; see `docs/EXTERNAL_ACCESS.md` and `docs/SECURITY_COOKBOOK.md`.

## Quick start (Python requests)

```python
import requests

BASE = "http://127.0.0.1:8080/v1"
TOKEN = None  # "..."  # set if auth enabled

def hdr():
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}

requests.post(f"{BASE}/fs/mkdir", json={"path": "/data"}, headers=hdr()).raise_for_status()

with open("local.txt", "rb") as f:
    r = requests.put(f"{BASE}/fs/content", params={"path": "/data/hello.txt"}, data=f, headers=hdr())
    r.raise_for_status()

r = requests.get(f"{BASE}/fs/content", params={"path": "/data/hello.txt"}, headers=hdr(), stream=True)
r.raise_for_status()
with open("out.txt", "wb") as f:
    for chunk in r.iter_content(chunk_size=1024 * 256):
        if chunk:
            f.write(chunk)
```

## Quick start (FUSE, Linux)

Requirements: `fusermount3` / libfuse; Linux only.

```bash
mkdir -p /mnt/godfs
GODFS_MASTER=127.0.0.1:9090 \
  go run ./cmd/fuse --mountpoint /mnt/godfs --prefix /data --cache-ttl 2s

echo "hello" > /mnt/godfs/hello.txt
cat /mnt/godfs/hello.txt

fusermount3 -u /mnt/godfs
```

Limitations: truncate / `O_TRUNC` is not supported by the metadata backend (`EOPNOTSUPP`).

## Kubernetes deployment (Helm)

The minimal Helm chart lives in `deployments/helm/godfs`.

**Gateway-only** chart (REST without master/chunk in the same release): `deployments/helm/godfs-restgateway` — expects a reachable `GODFS_MASTER` in values.

Typical install (examples, adjust values):

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace
```

See chart `values.yaml` for:

- images/tags,
- enabling Raft masters (StatefulSet),
- TLS + External Secrets wiring,
- metrics listen address.

Note: Helm is not vendored in this repo; install it locally to render/apply the chart.

## References

- REST/FUSE contract and env vars: `docs/EXTERNAL_ACCESS.md`
- Operations runbook: `docs/RUNBOOK.md`
- Security and rotation: `docs/OPERATOR_SECURITY.md`
