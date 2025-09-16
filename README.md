# Apache Spark Lakehouse Medallion Pipeline

Production-ready Apache Spark medallion pipeline (Bronze → Silver → Gold) demonstrating the operational complexity of building lakehouse architectures with Apache Spark, Iceberg, Hive Metastore, and Trino on Kubernetes.

> **Supporting materials for the blog post:** ["Apache Spark in 2025: Handle with Gloves"](https://sillidata.com/2025/09/16/apache-spark-in-2025-handle-with-gloves/)

## Architecture Overview

This implementation showcases a complete Customer 360 data pipeline using:

- **Apache Spark 3.5.6** for distributed data processing
- **Stackable Data Platform** operators for Kubernetes orchestration  
- **Apache Iceberg** for lakehouse table format (Silver/Gold layers)
- **Hive Metastore** for catalog management
- **Trino** for interactive analytics
- **Pure Storage FlashBlade** as S3-compatible object storage
- **OpenShift 4.19** / Kubernetes for container orchestration

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │───▶│   Silver    │───▶│    Gold     │
│Raw Customer │    │Enriched Data│    │Executive KPIs│
│Interactions │    │& Validation │    │& Analytics  │
└─────────────┘    └─────────────┘    └─────────────┘
      │                    │                  │
   Parquet              Iceberg           Iceberg
```

## What This Demonstrates

This repository illustrates why Apache Spark requires "radioactive-proof gloves" in production:

- **25+ Spark configuration parameters** just for basic S3 and Iceberg integration
- **64 executors × 32GB memory** for processing 1TB of customer data
- **Multiple Kubernetes deployments** (PostgreSQL, Hive, Trino, Spark operators)
- **Complex resource tuning** across storage, compute, and networking layers

## Real Implementation Complexity

The repository demonstrates the actual operational overhead:

- **100+ YAML configuration parameters** across multiple Kubernetes manifests
- **Automated monitoring and diagnostics** for multi-stage pipeline execution  
- **Production-grade tuning** for S3, Spark, and Iceberg integration
- **Comprehensive error handling** and rollback procedures

## Deployment Automation

This repository includes production-ready automation scripts:

- **`deploy.sh`** - Interactive deployment script with validation and diagnostics
- **`run_pipeline.py`** - Python orchestrator for monitoring and running the complete pipeline
- **`rollback.sh`** - Clean environment teardown

### Quick Deploy
```bash
# Interactive deployment with validation
./deploy.sh

# Or run the complete pipeline
python3 run_pipeline.py --stage full
```

## Prerequisites

- Kubernetes cluster with 200+ GB available memory
- Stackable Data Platform operators installed
- S3-compatible object storage (FlashBlade or AWS S3)
- vSphere CSI or equivalent for persistent volumes

## Manual Deployment Steps

```bash
git clone https://github.com/AndrewSilliFant/spark-lakehouse-medallion-pipeline
cd spark-lakehouse-medallion-pipeline

# Deploy infrastructure
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/s3/01-s3-secrets.yaml
kubectl apply -f k8s/s3/02-s3-connection.yaml
kubectl apply -f k8s/s3/03-s3-buckets.yaml

# Deploy data services
kubectl apply -f k8s/postgres/10-postgres-secret.yaml
kubectl apply -f k8s/postgres/11-postgres-statefulset.yaml
kubectl apply -f k8s/postgres/12-postgres-service.yaml
kubectl apply -f k8s/hive/20-hive-metastore.yaml

# Deploy analytics layer
kubectl apply -f k8s/trino/28-trino-auth-class.yaml
kubectl apply -f k8s/trino/29-trino-auth-secret.yaml
kubectl apply -f k8s/trino/30-trino-configmap.yaml
kubectl apply -f k8s/trino/31-trino-catalog.yaml
kubectl apply -f k8s/trino/32-trino-cluster.yaml

# Run medallion pipeline
kubectl apply -f k8s/spark/40-spark-serviceaccount.yaml
kubectl apply -f k8s/spark/41-spark-job-scripts-configmap.yaml
kubectl apply -f k8s/spark/42-bronze-ingest.yaml
kubectl apply -f k8s/spark/43-silver-build.yaml
kubectl apply -f k8s/spark/44-gold-finalize.yaml
```

## Performance Configuration Examples

From `TUNING.md` - sample S3A optimization required for production:
```yaml
spark.hadoop.fs.s3a.connection.maximum: 512
spark.hadoop.fs.s3a.threads.max: 512
spark.hadoop.fs.s3a.multipart.size: 134217728
spark.hadoop.fs.s3a.fast.upload: true
spark.hadoop.fs.s3a.committer.name: magic
```

**Resource Scaling by Pipeline Stage:**
- **Bronze** (data ingestion): 64 executors × 32GB = 2TB total memory
- **Silver** (enrichment): 32 executors × 16GB = 512GB total memory  
- **Gold** (aggregation): 24 executors × 6GB = 144GB total memory

## Pipeline Stages

### Bronze Layer: Raw Data Ingestion
- Generates realistic Customer 360 interaction data
- 30+ customer attributes with business logic
- Configurable data volume (default: 1TB)
- Parquet format for performance

### Silver Layer: Data Enrichment
- Email/phone standardization
- Geographic data cleansing
- Customer value segmentation
- Behavioral analytics features
- Iceberg format with schema evolution

### Gold Layer: Executive Analytics
- Daily KPI aggregations
- Revenue analytics by channel
- Customer engagement metrics
- Churn risk indicators
- Business intelligence ready

## Monitoring and Troubleshooting

**Monitor Pipeline Execution:**
```bash
# Watch Spark jobs
kubectl get sparkapplications -n md-pipeline -w

# Follow driver logs
kubectl logs -l spark-role=driver -n md-pipeline -f

# Check resource usage
kubectl top pods -n md-pipeline
```

**Common Issues:**
- Out of memory errors: Increase executor memory or reduce partition size
- S3 connection timeouts: Tune connection pool settings
- Metastore connection failures: Check PostgreSQL resource allocation

## Blog Post Connection

This repository provides the concrete implementation behind the blog post arguments about Spark's operational complexity. Every configuration file demonstrates why organizations choose managed services like Databricks over self-managed Spark deployments.

The automation scripts show exactly what "handling Spark with gloves" means in practice:
- Multi-stage deployment orchestration
- Real-time monitoring and diagnostics
- Error handling and recovery procedures
- Performance tuning across multiple systems

## File Structure

```
├── k8s/                    # Kubernetes manifests
│   ├── 00-namespace.yaml
│   ├── s3/                # S3 connection and buckets
│   ├── postgres/          # Hive Metastore database
│   ├── hive/             # Stackable Hive cluster
│   ├── trino/            # Stackable Trino cluster
│   └── spark/            # Spark jobs and configs
├── scripts/
│   ├── deploy.sh         # Automated deployment
│   ├── run_pipeline.py   # Pipeline orchestration
│   └── rollback.sh       # Environment cleanup
└── docs/
    ├── DEPLOYMENT.md     # Step-by-step guide
    └── TUNING.md         # Performance optimization
```

## Disclaimers

- This implementation uses Pure Storage FlashBlade as S3-compatible storage
- Configurations optimized for demonstration purposes
- Production deployments require proper security hardening
- Resource requirements may vary with different storage backends

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

*"Handle with care. The results are worth it. Just maybe don't plan any weekend trips until your pipeline is stable."*