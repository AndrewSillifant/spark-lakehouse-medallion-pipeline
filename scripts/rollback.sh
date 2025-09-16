#!/bin/bash
set -e

# MD-Pipeline Environment Rollback Script
# This script removes all components of the medallion pipeline in reverse dependency order

# Variables
NAMESPACE="md-pipeline"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# If the script is in a scripts/ subdirectory, go up one level
if [[ "$(basename "$SCRIPT_DIR")" == "scripts" ]]; then
    SCRIPT_DIR="$(dirname "$SCRIPT_DIR")"
fi

echo "=== MD-Pipeline Environment Rollback ==="
echo "Namespace: $NAMESPACE"
echo "Script directory: $SCRIPT_DIR"
echo

# Function to safely delete resource if it exists
safe_delete() {
    local resource_type="$1"
    local resource_name="$2"
    local namespace="${3:-$NAMESPACE}"
    
    if oc get "$resource_type" "$resource_name" -n "$namespace" >/dev/null 2>&1; then
        echo "Deleting $resource_type/$resource_name..."
        oc delete "$resource_type" "$resource_name" -n "$namespace" --timeout=60s
    else
        echo "Resource $resource_type/$resource_name not found (already deleted)"
    fi
}

# Function to delete file-based resources
delete_manifest() {
    local file_path="$1"
    if [[ -f "$file_path" ]]; then
        echo "Deleting resources from: $file_path"
        oc delete -f "$file_path" --ignore-not-found=true --timeout=60s || true
    else
        echo "File not found: $file_path"
    fi
}

echo "1. Stopping and removing Spark Applications..."
# Delete in reverse order of typical execution
safe_delete sparkapplication mdp-gold-finalize
safe_delete sparkapplication mdp-silver-build
safe_delete sparkapplication mdp-bronze-ingest
safe_delete sparkapplication mdp-smoke

echo
echo "2. Removing Spark configurations..."
delete_manifest "$SCRIPT_DIR/k8s/spark/49-smoke.yaml"
delete_manifest "$SCRIPT_DIR/k8s/spark/44-gold-finalize.yaml"
delete_manifest "$SCRIPT_DIR/k8s/spark/43-silver-build.yaml"
delete_manifest "$SCRIPT_DIR/k8s/spark/42-bronze-ingest.yaml"
delete_manifest "$SCRIPT_DIR/k8s/spark/41-spark-job-scripts-configmap.yaml"
delete_manifest "$SCRIPT_DIR/k8s/spark/40-spark-serviceaccount.yaml"

echo
echo "3. Removing Trino cluster and catalogs..."
delete_manifest "$SCRIPT_DIR/k8s/trino/32-trino-cluster.yaml"
delete_manifest "$SCRIPT_DIR/k8s/trino/31-trino-iceberg-catalog.yaml"
delete_manifest "$SCRIPT_DIR/k8s/trino/30-trino-iceberg-metastore-cm.yaml"

echo
echo "4. Removing Hive Metastore..."
delete_manifest "$SCRIPT_DIR/k8s/hive/20-hive-metastore.yaml"

echo
echo "5. Removing PostgreSQL database..."
delete_manifest "$SCRIPT_DIR/k8s/postgres/12-postgres-service.yaml"
delete_manifest "$SCRIPT_DIR/k8s/postgres/11-postgres-statefulset.yaml"
delete_manifest "$SCRIPT_DIR/k8s/postgres/10-postgres-secret.yaml"

echo
echo "6. Removing S3 buckets and connections..."
delete_manifest "$SCRIPT_DIR/k8s/s3/03-s3-buckets.yaml"
delete_manifest "$SCRIPT_DIR/k8s/s3/02-s3-connection.yaml"
delete_manifest "$SCRIPT_DIR/k8s/s3/01-s3-secrets.yaml"

echo
echo "7. Waiting for PVCs to be released..."
# Wait for StatefulSets to clean up their PVCs
sleep 10

# List remaining PVCs for manual cleanup if needed
echo "Checking for remaining PVCs..."
oc get pvc -n "$NAMESPACE" 2>/dev/null || echo "No PVCs found or namespace already deleted"

echo
echo "8. Final cleanup - removing any remaining resources..."
# Clean up any remaining pods, services, etc.
oc delete pods --all -n "$NAMESPACE" --timeout=60s --ignore-not-found=true || true
oc delete services --all -n "$NAMESPACE" --timeout=60s --ignore-not-found=true || true
oc delete configmaps --all -n "$NAMESPACE" --timeout=60s --ignore-not-found=true || true
oc delete secrets --all -n "$NAMESPACE" --timeout=60s --ignore-not-found=true || true
oc delete pvc --all -n "$NAMESPACE" --timeout=60s --ignore-not-found=true || true

echo
echo "9. Removing namespace..."
delete_manifest "$SCRIPT_DIR/k8s/00-namespace.yaml"

echo
echo "=== Rollback Complete ==="
echo "All MD-Pipeline resources have been removed."
echo
echo "To verify cleanup:"
echo "  oc get all -n $NAMESPACE"
echo "  oc get pvc -n $NAMESPACE"
echo "  oc get namespace $NAMESPACE"
echo
echo "Note: If PVCs are stuck in 'Terminating' state, you may need to:"
echo "  oc patch pvc <pvc-name> -n $NAMESPACE --type merge -p '{\"metadata\":{\"finalizers\":null}}'"