
# md-pipeline – Performance Tuning Cheatsheet

## Spark ↔ S3A (FlashBlade) key settings
Set these in `sparkConf` for each job (values are aggressive starting points):

```
spark.hadoop.fs.s3a.endpoint                   = http://0.0.0.0
spark.hadoop.fs.s3a.path.style.access         = true
spark.hadoop.fs.s3a.connection.maximum        = 512
spark.hadoop.fs.s3a.threads.max               = 512
spark.hadoop.fs.s3a.fast.upload               = true
spark.hadoop.fs.s3a.multipart.size            = 134217728         # 128 MiB
spark.hadoop.fs.s3a.committer.name            = magic              # or 'partitioned'/'directory'
spark.hadoop.fs.s3a.committer.staging.conflict-mode = replace
spark.hadoop.fs.s3a.connection.keepalive      = true
spark.hadoop.fs.s3a.connection.timeout        = 200s
spark.sql.parquet.output.committer.class      = org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.files.maxPartitionBytes             = 128m               # adjust upward to create larger files
spark.sql.shuffle.partitions                  = 2000               # scale with cluster size
```

If TLS is enabled on FlashBlade, use `https://` endpoint and provide the CA to the Stackable S3Connection (or set `fs.s3a.ssl.enabled=true`).

## Spark local shuffle on PVC (vSphere CSI / FlashArray)
Use Kubernetes-on-Spark volume integration to mount a PVC per executor as local disk:

```
spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName = OnDemand
spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass = thin-csi
spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit = 100Gi
spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path = /local1
spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly = false
```

## Trino
Trino properties to raise IO parallelism and exchange buffers:

```
exchange.max-buffer-size   = 512MB
exchange.max-response-size = 64MB
trino.s3.path-style-access = true
trino.s3.max-connections   = 512
trino.s3.staging-directory = /data/trino-staging
```

## Iceberg
Prefer larger target file sizes and compaction to reduce small-files:
```
Table properties:
  write.target-file-size-bytes=536870912      # 512 MiB
  write.distribution-mode=hash                # default in newer releases
  commit.manifest.target-size-bytes=8388608
```

## Throughput math
Use the provided Python orchestrator to compute end-to-end throughput:
```
throughput_gbps = (total_bytes_written / duration_seconds) / (1024**3)
```
