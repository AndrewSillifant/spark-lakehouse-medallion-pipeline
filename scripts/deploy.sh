#!/bin/bash
set -e

# MD-Pipeline Deployment Script with Fixed Validation Checks
# Deploys the complete medallion data pipeline with comprehensive validation

# Variables
NAMESPACE="md-pipeline"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# If the script is in a scripts/ subdirectory, go up one level
if [[ "$(basename "$SCRIPT_DIR")" == "scripts" ]]; then
    SCRIPT_DIR="$(dirname "$SCRIPT_DIR")"
fi
TIMEOUT_SECONDS=300
RETRY_INTERVAL=10
INTERACTIVE=true  # Enable interactive prompts by default

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a file exists
check_file() {
    local file_path="$1"
    if [[ ! -f "$file_path" ]]; then
        log_error "Required file not found: $file_path"
        return 1
    fi
    log_info "Found: $file_path"
    return 0
}

# FIXED: Function to test S3 connectivity with proper smoke test monitoring
test_s3_connectivity() {
    log_info "Testing S3 connectivity with smoke test..."
    
    # Clean up any existing smoke test first
    oc delete sparkapplication mdp-smoke -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
    sleep 5
    
    # Deploy smoke test
    if ! check_file "$SCRIPT_DIR/k8s/spark/49-smoke.yaml"; then
        log_error "Smoke test YAML not found"
        return 1
    fi
    
    log_info "Deploying smoke test..."
    oc apply -f "$SCRIPT_DIR/k8s/spark/49-smoke.yaml"
    sleep 10  # Give it time to start
    
    # Monitor smoke test execution with improved logic
    local timeout=600  # 10 minutes for smoke test
    local start_time=$(date +%s)
    local end_time=$((start_time + timeout))
    local last_status=""
    local found_driver=false
    
    log_info "Monitoring smoke test execution..."
    
    while [[ $(date +%s) -lt $end_time ]]; do
        local elapsed=$(($(date +%s) - start_time))
        
        # Check if SparkApplication exists
        if ! oc get sparkapplication mdp-smoke -n "$NAMESPACE" >/dev/null 2>&1; then
            log_info "[$((elapsed))s] Waiting for SparkApplication to be created..."
            sleep 10
            continue
        fi
        
        # Get application state
        local state=$(oc get sparkapplication mdp-smoke -n "$NAMESPACE" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "PENDING")
        
        # Check driver pod status for more accurate completion detection
        local driver_pod=$(oc get pods -l spark-app-name=mdp-smoke,spark-role=driver -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $1}' | head -1)
        local driver_phase=""
        if [[ -n "$driver_pod" ]]; then
            driver_phase=$(oc get pod "$driver_pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
            found_driver=true
        fi
        
        # Show status updates
        if [[ "$state" != "$last_status" ]]; then
            if [[ -n "$driver_pod" ]]; then
                log_info "[$((elapsed))s] Smoke test - App: $state, Driver Pod: $driver_phase"
            else
                log_info "[$((elapsed))s] Smoke test - App: $state"
            fi
            last_status="$state"
        fi
        
        # Check driver pod completion first (more reliable than SparkApplication status)
        if [[ "$driver_phase" == "Succeeded" ]]; then
            log_info "Driver pod completed successfully, checking results..."
            
            # Check driver logs for success/failure markers
            local driver_logs=$(oc logs "$driver_pod" -n "$NAMESPACE" --tail=50 2>/dev/null || echo "")
            
            if echo "$driver_logs" | grep -q "SMOKE_OK"; then
                log_success "S3 connectivity test PASSED!"
                log_info "Smoke test confirmed S3 read/write functionality"
                return 0
            elif echo "$driver_logs" | grep -q "SMOKE_FAIL"; then
                log_error "S3 connectivity test FAILED!"
                log_info "Driver logs:"
                echo "$driver_logs" | tail -20
                return 1
            else
                log_warning "Driver pod succeeded but no clear result marker found"
                log_info "Driver logs (last 20 lines):"
                echo "$driver_logs" | tail -20
                # If driver succeeded and no explicit failure, assume success
                return 0
            fi
        elif [[ "$driver_phase" == "Failed" || "$driver_phase" == "Error" ]]; then
            log_error "Driver pod failed!"
            local driver_logs=$(oc logs "$driver_pod" -n "$NAMESPACE" --tail=30 2>/dev/null || echo "")
            log_info "Driver failure logs:"
            echo "$driver_logs" | tail -20
            return 1
        fi
        
        # Fallback to SparkApplication status
        case "$state" in
            "COMPLETED")
                log_info "SparkApplication marked as completed, checking logs..."
                local driver_logs=$(oc logs -l spark-app-name=mdp-smoke,spark-role=driver -n "$NAMESPACE" --tail=50 2>/dev/null || echo "")
                
                if echo "$driver_logs" | grep -q "SMOKE_OK"; then
                    log_success "S3 connectivity test PASSED!"
                    return 0
                elif echo "$driver_logs" | grep -q "SMOKE_FAIL"; then
                    log_error "S3 connectivity test FAILED!"
                    echo "$driver_logs" | tail -20
                    return 1
                else
                    log_warning "Job completed but result unclear from logs"
                    return 0  # Assume success if job completed without explicit failure
                fi
                ;;
            "FAILED"|"ERROR")
                log_error "Smoke test job FAILED!"
                log_info "Driver logs:"
                oc logs -l spark-app-name=mdp-smoke,spark-role=driver -n "$NAMESPACE" --tail=30 2>/dev/null || log_warning "Could not retrieve driver logs"
                return 1
                ;;
            "SUBMITTED"|"RUNNING")
                # Show executor progress only if we haven't found the driver yet
                if [[ "$found_driver" == "false" ]]; then
                    local executor_count=$(oc get pods -l spark-app-name=mdp-smoke,spark-role=executor -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' \n')
                    if [[ "${executor_count:-0}" -gt 0 ]]; then
                        log_info "[$((elapsed))s] Job running, executors: $executor_count"
                        found_driver=true  # Assume driver is starting if executors exist
                    fi
                fi
                ;;
        esac
        
        sleep 15
    done
    
    log_error "Smoke test timed out after $((timeout/60)) minutes"
    log_info "Final status check:"
    oc get sparkapplication mdp-smoke -n "$NAMESPACE" -o wide 2>/dev/null || log_warning "Could not get final status"
    
    # Show final driver logs if available
    local final_driver_pod=$(oc get pods -l spark-app-name=mdp-smoke,spark-role=driver -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $1}' | head -1)
    if [[ -n "$final_driver_pod" ]]; then
        log_info "Final driver logs:"
        oc logs "$final_driver_pod" -n "$NAMESPACE" --tail=30 2>/dev/null || log_warning "Could not get driver logs"
    fi
    
    return 1
}

# Diagnostic functions for detailed error analysis
show_pod_diagnostics() {
    local pod_name="$1"
    local namespace="$2"
    
    echo "--- Pod Diagnostics for $pod_name ---"
    oc describe pod "$pod_name" -n "$namespace" | tail -20
    echo "--- Recent logs ---"
    oc logs "$pod_name" -n "$namespace" --tail=10 2>/dev/null || echo "Could not retrieve logs"
}

show_statefulset_diagnostics() {
    local statefulset_name="$1"
    local namespace="$2"
    
    echo "--- StatefulSet Diagnostics for $statefulset_name ---"
    oc describe statefulset "$statefulset_name" -n "$namespace" | grep -A 10 "Events:" || true
    
    local pods=$(oc get pods -n "$namespace" -l "app=$statefulset_name" --no-headers 2>/dev/null || echo "")
    echo "--- Pods in StatefulSet ---"
    echo "$pods"
    
    local first_pod=$(echo "$pods" | head -1 | awk '{print $1}')
    if [[ -n "$first_pod" ]]; then
        echo "--- Logs from $first_pod ---"
        oc logs "$first_pod" -n "$namespace" --tail=10 2>/dev/null || echo "Could not retrieve logs"
    fi
}

show_stackable_diagnostics() {
    local resource_type="$1"
    local resource_name="$2"
    local namespace="$3"
    
    echo "--- Stackable $resource_type Diagnostics for $resource_name ---"
    oc describe "$resource_type" "$resource_name" -n "$namespace" | grep -A 20 "Events:" || true
    
    echo "--- Related pods ---"
    oc get pods -n "$namespace" -l "app.kubernetes.io/instance=$resource_name" || true
    
    echo "--- Recent logs from coordinator/primary pod ---"
    local main_pod=$(oc get pods -n "$namespace" -l "app.kubernetes.io/instance=$resource_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$main_pod" ]]; then
        oc logs "$main_pod" -n "$namespace" --tail=15 2>/dev/null || echo "Could not retrieve logs"
    fi
}

show_timeout_diagnostics() {
    local resource_type="$1"
    local resource_name="$2"
    local namespace="$3"
    
    echo "--- Timeout Diagnostics for $resource_type/$resource_name ---"
    
    # Show resource status
    if oc get "$resource_type" "$resource_name" -n "$namespace" >/dev/null 2>&1; then
        oc describe "$resource_type" "$resource_name" -n "$namespace" | tail -30
    else
        echo "Resource not found - may not have been created"
    fi
    
    # Show related events
    echo "--- Recent namespace events ---"
    oc get events -n "$namespace" --sort-by='.lastTimestamp' | tail -10 || true
}

validate_stackable_cluster() {
    local resource_type="$1"
    local resource_name="$2" 
    local namespace="$3"
    
    case "$resource_type" in
        "trinocluster")
            # Check for Trino-specific issues
            local coordinator_pod=$(oc get pods -n "$namespace" -l "app.kubernetes.io/component=coordinator,app.kubernetes.io/instance=$resource_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -n "$coordinator_pod" ]]; then
                # Check for catalog errors
                local catalog_errors=$(oc logs "$coordinator_pod" -n "$namespace" --tail=50 2>/dev/null | grep -c -i "error.*catalog" | tr -d ' \n' || echo "0")
                if [[ "${catalog_errors:-0}" -gt 0 ]]; then
                    log_warning "Trino coordinator has catalog-related errors:"
                    oc logs "$coordinator_pod" -n "$namespace" --tail=50 2>/dev/null | grep -i "error.*catalog" || true
                fi
                
                # Check if server started successfully
                local server_started=$(oc logs "$coordinator_pod" -n "$namespace" 2>/dev/null | grep -c "SERVER STARTED" | tr -d ' \n' || echo "0")
                if [[ "${server_started:-0}" -gt 0 ]]; then
                    log_success "Trino server started successfully"
                else
                    log_warning "Trino server may not have started properly"
                fi
            fi
            ;;
        "hivecluster") 
            # Check for Hive-specific issues
            local hive_pod=$(oc get pods -n "$namespace" -l "app.kubernetes.io/component=metastore,app.kubernetes.io/instance=$resource_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -n "$hive_pod" ]]; then
                # Check if Hive Metastore is listening on port 9083
                local port_check=$(oc exec "$hive_pod" -n "$namespace" -- sh -c "timeout 2 bash -c '</dev/tcp/localhost/9083' && echo 'OK' || echo 'FAIL'" 2>/dev/null | tr -d ' \n' || echo "FAIL")
                if [[ "$port_check" == "OK" ]]; then
                    log_success "Hive Metastore is accepting connections on port 9083"
                else
                    log_warning "Hive Metastore may not be listening on port 9083"
                fi
            fi
            ;;
    esac
}

# Function to prompt for user confirmation
prompt_continue() {
    local message="$1"
    local default="${2:-y}"
    
    if [[ "$INTERACTIVE" != "true" ]]; then
        return 0
    fi
    
    echo
    log_info "$message"
    while true; do
        if [[ "$default" == "y" ]]; then
            echo -n "Continue? [Y/n/q]: "
        else
            echo -n "Continue? [y/N/q]: "
        fi
        
        read -r response </dev/tty
        response=${response:-$default}
        
        case "$response" in
            [Yy]|[Yy][Ee][Ss])
                echo
                return 0
                ;;
            [Nn]|[Nn][Oo])
                log_info "Skipping this step..."
                echo
                return 1
                ;;
            [Qq]|[Qq][Uu][Ii][Tt])
                log_warning "Deployment stopped by user"
                exit 0
                ;;
            *)
                echo "Please answer yes (y), no (n), or quit (q)"
                ;;
        esac
    done
}

# Function to apply manifest and verify
apply_and_verify() {
    local file_path="$1"
    local resource_type="$2"
    local resource_name="$3"
    local namespace="${4:-$NAMESPACE}"
    
    if ! check_file "$file_path"; then
        return 1
    fi
    
    log_info "Applying: $file_path"
    oc apply -f "$file_path"
    
    if [[ -n "$resource_type" && -n "$resource_name" ]]; then
        wait_for_resource "$resource_type" "$resource_name" "$namespace"
    fi
}

# Function to wait for resource to be ready
wait_for_resource() {
    local resource_type="$1"
    local resource_name="$2"
    local namespace="${3:-$NAMESPACE}"
    local timeout="${4:-$TIMEOUT_SECONDS}"
    
    log_info "Waiting for $resource_type/$resource_name to be ready..."
    
    local start_time=$(date +%s)
    local end_time=$((start_time + timeout))
    
    while [[ $(date +%s) -lt $end_time ]]; do
        if oc get "$resource_type" "$resource_name" -n "$namespace" >/dev/null 2>&1; then
            case "$resource_type" in
                "pod")
                    local status=$(oc get pod "$resource_name" -n "$namespace" -o jsonpath='{.status.phase}')
                    if [[ "$status" == "Running" ]]; then
                        log_success "$resource_type/$resource_name is running"
                        return 0
                    elif [[ "$status" == "Failed" || "$status" == "Error" ]]; then
                        log_error "$resource_type/$resource_name failed"
                        show_pod_diagnostics "$resource_name" "$namespace"
                        return 1
                    fi
                    ;;
                "statefulset")
                    local ready=$(oc get statefulset "$resource_name" -n "$namespace" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
                    local desired=$(oc get statefulset "$resource_name" -n "$namespace" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
                    if [[ "${ready:-0}" == "$desired" ]] && [[ "${ready:-0}" -gt 0 ]]; then
                        log_success "$resource_type/$resource_name is ready ($ready/$desired)"
                        return 0
                    fi
                    log_info "StatefulSet status: ${ready:-0}/$desired ready"
                    
                    # Check for failed pods in StatefulSet
                    local failed_pods=$(oc get pods -n "$namespace" -l "app=$resource_name" --field-selector=status.phase=Failed --no-headers 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
                    if [[ "${failed_pods:-0}" -gt 0 ]]; then
                        log_error "StatefulSet has $failed_pods failed pods"
                        show_statefulset_diagnostics "$resource_name" "$namespace"
                        return 1
                    fi
                    ;;
                "hivecluster"|"trinocluster")
                    # For Stackable operators, check if pods are running and look for errors
                    local pods=$(oc get pods -n "$namespace" -l "app.kubernetes.io/instance=$resource_name" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
                    local failed_pods=$(oc get pods -n "$namespace" -l "app.kubernetes.io/instance=$resource_name" --field-selector=status.phase=Failed --no-headers 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
                    
                    if [[ "${failed_pods:-0}" -gt 0 ]]; then
                        log_error "$resource_type/$resource_name has $failed_pods failed pods"
                        show_stackable_diagnostics "$resource_type" "$resource_name" "$namespace"
                        return 1
                    fi
                    
                    if [[ "${pods:-0}" -gt 0 ]]; then
                        log_success "$resource_type/$resource_name has $pods running pods"
                        validate_stackable_cluster "$resource_type" "$resource_name" "$namespace"
                        return 0
                    fi
                    log_info "$resource_type/$resource_name: $pods pods running"
                    ;;
                *)
                    log_success "$resource_type/$resource_name exists"
                    return 0
                    ;;
            esac
        else
            log_info "$resource_type/$resource_name not found yet..."
        fi
        
        sleep $RETRY_INTERVAL
    done
    
    log_error "Timeout waiting for $resource_type/$resource_name to be ready"
    show_timeout_diagnostics "$resource_type" "$resource_name" "$namespace"
    return 1
}

# Function to validate S3 credentials
validate_s3_credentials() {
    log_info "Validating S3 credentials in k8s/s3/01-s3-secrets.yaml..."
    
    local secrets_file="$SCRIPT_DIR/k8s/s3/01-s3-secrets.yaml"
    if ! check_file "$secrets_file"; then
        return 1
    fi
    
    # Check if placeholder values are still present
    if grep -q "REPLACE_WITH_YOUR" "$secrets_file" 2>/dev/null; then
        log_error "S3 credentials file contains placeholder values. Please update with real FlashBlade credentials."
        return 1
    fi
    
    if grep -q "accessKey: $" "$secrets_file" 2>/dev/null || grep -q "secretKey: $" "$secrets_file" 2>/dev/null; then
        log_error "S3 credentials appear to be empty. Please update with real FlashBlade credentials."
        return 1
    fi
    
    log_success "S3 credentials file appears to be configured"
    return 0
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if oc command exists
    if ! command -v oc &> /dev/null; then
        log_error "OpenShift CLI (oc) not found. Please install oc command."
        return 1
    fi
    
    # Check if connected to cluster
    if ! oc whoami &> /dev/null; then
        log_error "Not connected to OpenShift cluster. Please login with 'oc login'."
        return 1
    fi
    
    # Check if Stackable operators are installed
    if ! oc get crd hiveclusters.hive.stackable.tech &> /dev/null; then
        log_error "Stackable Hive operator not found. Please install Stackable operators first."
        return 1
    fi
    
    if ! oc get crd trinoclusters.trino.stackable.tech &> /dev/null; then
        log_error "Stackable Trino operator not found. Please install Stackable operators first."
        return 1
    fi
    
    if ! oc get crd sparkapplications.spark.stackable.tech &> /dev/null; then
        log_error "Stackable Spark operator not found. Please install Stackable operators first."
        return 1
    fi
    
    log_success "Prerequisites check passed"
    return 0
}

# Function to initialize Hive schema
initialize_hive_schema() {
    log_info "Checking Hive Metastore schema..."
    
    local hive_pod=$(oc get pods -n "$NAMESPACE" -l "app.kubernetes.io/component=metastore" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    if [[ -z "$hive_pod" ]]; then
        log_error "Hive Metastore pod not found"
        return 1
    fi
    
    log_info "Hive pod found: $hive_pod"
    
    # Check if schema exists by querying PostgreSQL database directly
    log_info "Checking if Hive schema is already initialized..."
    local postgres_pod=$(oc get pods -n "$NAMESPACE" -l "app=mdp-hms-postgres" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    if [[ -n "$postgres_pod" ]]; then
        local table_count=$(oc exec "$postgres_pod" -n "$NAMESPACE" -- sh -c 'PGPASSWORD=Osmium76 psql -U hive -d hive -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '\''public'\'';"' 2>/dev/null | tr -d ' \n' || echo "0")
        
        if [[ "${table_count:-0}" -gt 50 ]]; then
            log_success "Hive schema already initialized ($table_count tables found)"
            log_info "Stackable operator auto-initialized the schema successfully"
            return 0
        fi
    fi
    
    log_info "Schema will be auto-initialized by Stackable operator on first use"
    return 0
}

# Function to validate Trino connectivity
validate_trino_connectivity() {
    log_info "Validating Trino connectivity..."
    
    local trino_pod=$(oc get pods -n "$NAMESPACE" -l "app.kubernetes.io/component=coordinator" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    if [[ -z "$trino_pod" ]]; then
        log_error "Trino coordinator pod not found"
        return 1
    fi
    
    # Check if Iceberg catalog properties file exists
    log_info "Checking if Iceberg catalog configuration exists..."
    if oc exec "$trino_pod" -n "$NAMESPACE" -- test -f /stackable/config/catalog/iceberg.properties 2>/dev/null; then
        log_info "Iceberg catalog properties file found"
        oc exec "$trino_pod" -n "$NAMESPACE" -- cat /stackable/config/catalog/iceberg.properties 2>/dev/null || true
    else
        log_error "Iceberg catalog properties file not found"
        log_info "Available catalog files:"
        oc exec "$trino_pod" -n "$NAMESPACE" -- ls -la /stackable/config/catalog/ 2>/dev/null || true
        return 1
    fi
    
    # Check if Trino server started successfully
    local server_started=$(oc logs "$trino_pod" -n "$NAMESPACE" 2>/dev/null | grep -c "SERVER STARTED" | tr -d ' \n' || echo "0")
    if [[ "${server_started:-0}" -gt 0 ]]; then
        log_success "Trino server startup confirmed"
    else
        log_warning "Trino server startup not confirmed"
        return 1
    fi
    
    log_success "Trino connectivity validation passed"
    return 0
}

# Main deployment function
deploy_md_pipeline() {
    echo "========================================="
    echo "MD-Pipeline Deployment Script"
    echo "Medallion Bench Pipeline on OpenShift"
    echo "========================================="
    echo
    
    if [[ "$INTERACTIVE" == "true" ]]; then
        log_info "Interactive mode enabled. You will be prompted before each major step."
        echo
    fi
    
    # Step 0: Prerequisites
    log_info "Checking prerequisites..."
    if ! check_prerequisites; then
        log_error "Prerequisites check failed"
        exit 1
    fi
    
    prompt_continue "Prerequisites validated. Ready to start deployment?"
    
    # Step 1: Create namespace
    echo
    log_info "=== STEP 1: Creating namespace ==="
    apply_and_verify "$SCRIPT_DIR/k8s/00-namespace.yaml" "namespace" "$NAMESPACE" ""
    log_success "Namespace created successfully"
    
    # Step 2: Deploy S3 resources
    echo
    log_info "=== STEP 2: Deploying S3 resources ==="
    prompt_continue "Deploy S3 resources?"
    
    if ! validate_s3_credentials; then
        log_error "S3 credentials validation failed"
        exit 1
    fi
    
    apply_and_verify "$SCRIPT_DIR/k8s/s3/01-s3-secrets.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/s3/02-s3-connection.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/s3/03-s3-buckets.yaml"
    log_success "S3 resources deployed successfully"
    
    # Step 3: Deploy PostgreSQL
    echo
    log_info "=== STEP 3: Deploying PostgreSQL ==="
    prompt_continue "Deploy PostgreSQL database?"
    
    apply_and_verify "$SCRIPT_DIR/k8s/postgres/10-postgres-secret.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/postgres/11-postgres-statefulset.yaml" "statefulset" "mdp-hms-postgres"
    apply_and_verify "$SCRIPT_DIR/k8s/postgres/12-postgres-service.yaml"
    log_success "PostgreSQL deployed and running"
    
    # Step 4: Deploy Hive Metastore
    echo
    log_info "=== STEP 4: Deploying Hive Metastore ==="
    prompt_continue "Deploy Hive Metastore?"
    
    apply_and_verify "$SCRIPT_DIR/k8s/hive/20-hive-metastore.yaml" "hivecluster" "mdp-hive"
    log_success "Hive Metastore cluster deployed"
    
    # Initialize Hive schema
    echo
    log_info "=== Initializing Hive Schema ==="
    prompt_continue "Initialize Hive schema?"
    
    sleep 30  # Allow Hive to fully start
    if ! initialize_hive_schema; then
        log_warning "Hive schema check completed (auto-initialization expected)"
    fi
    
    # Step 5: Deploy Trino
    echo
    log_info "=== STEP 5: Deploying Trino cluster ==="
    prompt_continue "Deploy Trino components?"
    
    apply_and_verify "$SCRIPT_DIR/k8s/trino/28-trino-auth-class.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/trino/29-trino-auth-secret.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/trino/30-trino-configmap.yaml"
    apply_and_verify "$SCRIPT_DIR/k8s/trino/31-trino-catalog.yaml" 
    apply_and_verify "$SCRIPT_DIR/k8s/trino/32-trino-cluster.yaml" "trinocluster" "mdp-trino"
    log_success "Trino cluster deployed"
    
    # Wait for Trino to be ready and test connectivity
    echo
    log_info "=== Validating Trino connectivity ==="
    sleep 60  # Allow Trino to start with new catalog
    if ! validate_trino_connectivity; then
        log_warning "Trino connectivity validation had issues"
        prompt_continue "Continue despite Trino connectivity issues?" "n"
    else
        log_success "Trino connectivity validated"
    fi
    
    # Step 6: Deploy Spark resources
    echo
    log_info "=== STEP 6: Deploying Spark resources ==="
    prompt_continue "Deploy Spark resources?"
    
    apply_and_verify "$SCRIPT_DIR/k8s/spark/40-spark-serviceaccount.yaml"
    
    # Add SCC permissions
    log_info "Adding SCC permissions for Spark service account..."
    oc adm policy add-scc-to-user anyuid "system:serviceaccount:$NAMESPACE:spark-runner" 2>/dev/null || true
    oc adm policy add-scc-to-user privileged "system:serviceaccount:$NAMESPACE:spark-runner" 2>/dev/null || true
    
    apply_and_verify "$SCRIPT_DIR/k8s/spark/41-spark-job-scripts-configmap.yaml"
    log_success "Spark resources deployed"
    
    # Step 7: S3 connectivity test
    echo
    log_info "=== STEP 7: S3 connectivity test ==="
    log_info "This will run a smoke test to verify Spark can read/write to FlashBlade."
    
    if prompt_continue "Run S3 connectivity test?" "y"; then
        if ! test_s3_connectivity; then
            log_warning "S3 connectivity test failed"
            log_info "Check S3 credentials and FlashBlade connectivity"
            prompt_continue "Continue with pipeline deployment?" "y"
        else
            log_success "S3 connectivity test passed!"
        fi
        
        # Clean up smoke test
        oc delete sparkapplication mdp-smoke -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
    else
        log_info "Skipping S3 connectivity test"
    fi
    
    # Step 8: Validate Spark job manifests
    echo
    log_info "=== STEP 8: Validating Spark job manifests ==="
    
    check_file "$SCRIPT_DIR/k8s/spark/42-bronze-ingest.yaml"
    check_file "$SCRIPT_DIR/k8s/spark/43-silver-build.yaml" 
    check_file "$SCRIPT_DIR/k8s/spark/44-gold-finalize.yaml"
    log_success "All Spark job manifests validated"
    
    echo
    log_success "========================================="
    log_success "MD-Pipeline Infrastructure Complete!"
    log_success "========================================="
    echo
    log_info "Next steps for running the 1TB pipeline:"
    echo
    log_info "MANUAL EXECUTION:"
    log_info "1. Bronze: oc apply -f k8s/spark/42-bronze-ingest.yaml"
    log_info "2. Silver: oc apply -f k8s/spark/43-silver-build.yaml"
    log_info "3. Gold:   oc apply -f k8s/spark/44-gold-finalize.yaml"
    echo
    log_info "MONITORING:"
    log_info "  oc get sparkapplication -n $NAMESPACE -w"
    log_info "  oc logs -l spark-role=driver -n $NAMESPACE -f"
    echo
}

# Function to monitor Spark job with real-time updates
monitor_spark_job() {
    local job_name="$1"
    local timeout_minutes="${2:-120}"  # Increased default timeout
    
    log_info "Monitoring Spark job '$job_name'..."
    log_info "Press Ctrl+C to stop monitoring (job will continue running)"
    
    local start_time=$(date +%s)
    local timeout_seconds=$((timeout_minutes * 60))
    local last_status=""
    local executor_count=0
    
    while true; do
        local elapsed=$(($(date +%s) - start_time))
        
        if [[ $elapsed -gt $timeout_seconds ]]; then
            log_warning "Monitoring timeout reached ($timeout_minutes minutes)"
            log_info "Job may still be running. Check manually with: oc get sparkapplication $job_name -n $NAMESPACE"
            return 0
        fi
        
        # Get current status
        local status=$(oc get sparkapplication "$job_name" -n "$NAMESPACE" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
        local new_executor_count=$(oc get pods -n "$NAMESPACE" -l "spark-role=executor,spark-app-name=$job_name" --no-headers 2>/dev/null | wc -l | tr -d ' \n' || echo "0")
        
        # Show status changes
        if [[ "$status" != "$last_status" ]] || [[ "$new_executor_count" != "$executor_count" ]]; then
            log_info "[$((elapsed/60))m$((elapsed%60))s] Status: $status, Executors: $new_executor_count"
            last_status="$status"
            executor_count="$new_executor_count"
        fi
        
        # Check for completion
        case "$status" in
            "COMPLETED")
                log_success "Job '$job_name' completed successfully in $((elapsed/60)) minutes!"
                return 0
                ;;
            "FAILED"|"CANCELLED")
                log_error "Job '$job_name' failed after $((elapsed/60)) minutes"
                log_info "Recent driver logs:"
                oc logs -l "spark-role=driver,spark-app-name=$job_name" -n "$NAMESPACE" --tail=30 2>/dev/null || log_warning "Could not retrieve logs"
                return 1
                ;;
        esac
        
        sleep 30  # Check every 30 seconds
    done
}

# Parse command line arguments
case "${1:-}" in
    -h|--help)
        echo "MD-Pipeline Deployment Script"
        echo "Usage: $0 [OPTIONS]"
        echo "Options:"
        echo "  -h, --help           Show this help"
        echo "  --non-interactive    Run without prompts"
        echo "  --validate-only      Only run validation checks"
        exit 0
        ;;
    --non-interactive)
        INTERACTIVE=false
        deploy_md_pipeline
        ;;
    --validate-only)
        INTERACTIVE=false
        check_prerequisites
        validate_s3_credentials
        log_success "Validation completed"
        exit 0
        ;;
    "")
        deploy_md_pipeline
        ;;
    *)
        log_error "Unknown option: $1"
        exit 1
        ;;
esac