# DR manifests (snapshot backup/restore)

Files in this directory are example Job/CronJob manifests for exporting and restoring a snapshot manifest.

## Requirements

- `godfs-client` image (see `deployments/docker/Dockerfile.client`)
- A PVC to store archives:
  - `godfs-backup-pvc` (NFS/CSI volume snapshots/replication are all fine)
- A Secret with a client API key:
  - `godfs-client-auth` with key `api_key`
- Access to the master service:
  - `godfs-master:9090`

## Backup (CronJob)

Apply:

```bash
kubectl apply -f deployments/k8s/dr/snapshot-backup-cronjob.yaml
```

Manifest files will appear under `/backup` inside the container (on the PVC).

## Restore (Job)

Copy the desired `manifest.json` to the PVC (or rename an existing one), then:

```bash
kubectl apply -f deployments/k8s/dr/snapshot-restore-job.yaml
```

By default, the job uses `--force` (it clears metadata before restoring).

