
# md-pipeline – Deployment & Reference Architecture

This guide assumes:
- **OpenShift 4.19.7** with 12 nodes × 16 vCPUs × 64GB RAM.
- **Stackable operators** installed in `stackable-operators` namespace.
- **vSphere CSI** default StorageClass `vsphere-datastore` present.
- **FlashBlade S3** reachable (adjust protocol/port/TLS as appropriate).

## 0) Prereqs on your workstation
- `oc` (or `kubectl`) authenticated against your cluster API
- Python 3.9+ with `pip install -r scripts/requirements.txt`.

## 1) Create namespace and S3 connection
```bash
oc apply -f k8s/00-namespace.yaml
oc apply -f k8s/s3/01-s3-secrets.yaml            # put your FlashBlade S3 keys into this file first
oc apply -f k8s/s3/02-s3-connection.yaml
oc apply -f k8s/s3/03-s3-buckets.yaml
```

## 2) Deploy PostgreSQL (for Hive Metastore)
```bash
oc apply -f k8s/postgres/10-postgres-secret.yaml
oc apply -f k8s/postgres/11-postgres-statefulset.yaml
oc apply -f k8s/postgres/12-postgres-service.yaml
```

## 3) Deploy Hive Metastore (Stackable)
```bash
oc apply -f k8s/hive/20-hive-metastore.yaml
```
kubectl exec -it mdp-hive-metastore-default-0 -n md-pipeline -- /bin/bash
cd /stackable/hive/bin
./schematool -dbType postgres -initSchema -verbose

Wait until the StatefulSet is ready (1/1 or 3/3 if scaled) and the service is up.

## 4) Deploy Trino + Iceberg catalog
```bash
oc apply -f k8s/trino/30-trino-configmap.yaml
oc apply -f k8s/trino/31-trino-catalog.yaml
oc apply -f k8s/trino/32-trino-cluster.yaml
```

## 5) Deploy Spark application ConfigMaps (job code) & submit jobs
```bash
oc apply -f k8s/spark/40-spark-serviceaccount.yaml
oc adm policy add-scc-to-user anyuid system:serviceaccount:md-pipeline:spark-runner

oc apply -f k8s/spark/41-spark-job-scripts-configmap.yaml

oc apply -f k8s/spark/42-bronze-ingest.yaml
oc apply -f k8s/spark/43-silver-build.yaml
oc apply -f k8s/spark/44-gold-finalize.yaml

```

## 6) Validate and benchmark (from your laptop)
```bash
python3 scripts/run_pipeline.py --endpoint http://0.0.0.0   --access-key $S3_ACCESS_KEY --secret-key $S3_SECRET_KEY   --namespace md-pipeline --trino-host trino-coordinator.mdp.svc.cluster.local  --trino-port 80 --catalog iceberg --schema gold
```

Artifacts and metrics are written to `scripts/out/`.

---

## Sizing notes (starting point on 12×16c/64G nodes)

- **Spark (stress)**: 1 driver (2 CPU / 4 GiB), 120 executors × (1 CPU / 4 GiB). Adjust `instances` 96–160.
- **Trino**: 1 coordinator (4 CPU / 8 GiB), 10 workers × (8 CPU / 24 GiB).
- **Hive Metastore**: 1–3 replicas × (2 CPU / 4 GiB).
- **PostgreSQL**: 2 CPU / 8 GiB, 200Gi PVC on `thin-csi` (or Pure FlashArray SC when available).

Tune upward to saturate FlashBlade S3.

See `docs/TUNING.md` for S3A/Trino/Iceberg settings aimed at multi-GB/s throughput.
