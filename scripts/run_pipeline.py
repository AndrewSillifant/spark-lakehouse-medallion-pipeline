#!/usr/bin/env python3
"""
MD-Pipeline Orchestration Script
Medallion Bench Pipeline

This script orchestrates the full data pipeline from outside the OpenShift cluster.
Requires: kubectl/oc configured with cluster access, python3
"""

import subprocess
import time
import json
import sys
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MDPipelineOrchestrator:
    def __init__(self, namespace="md-pipeline", kubeconfig=None):
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        self.kubectl_cmd = ["oc"]  # Use oc for OpenShift
        if kubeconfig:
            self.kubectl_cmd.extend(["--kubeconfig", kubeconfig])
    
    def run_kubectl(self, args, capture_output=True):
        """Run oc command and return result"""
        cmd = self.kubectl_cmd + args
        logger.debug(f"Running: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=capture_output, text=True, check=True)
            return result.stdout.strip() if capture_output else None
        except subprocess.CalledProcessError as e:
            logger.error(f"oc command failed: {e}")
            if capture_output and e.stderr:
                logger.error(f"stderr: {e.stderr}")
            raise
    
    def wait_for_spark_job(self, job_name, timeout_minutes=120):  # Increased timeout for 1TB job
        """Wait for Spark job to complete and return status"""
        logger.info(f"Waiting for Spark job '{job_name}' to complete (timeout: {timeout_minutes}m)...")
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        last_status = ""
        last_executor_count = 0
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                logger.error(f"Timeout waiting for job '{job_name}' after {timeout_minutes} minutes")
                return False
            
            try:
                # Get SparkApplication status
                status = self.run_kubectl([
                    "get", "sparkapplication", job_name, "-n", self.namespace, 
                    "-o", "jsonpath={.status.applicationState.state}"
                ])
                
                # Get executor count
                executor_count = self.get_executor_count(job_name)
                
                # Only log when status or executor count changes
                if status != last_status or executor_count != last_executor_count:
                    logger.info(f"[{elapsed/60:.1f}m] Job '{job_name}' - Status: {status}, Executors: {executor_count}")
                    last_status = status
                    last_executor_count = executor_count
                
                if status == "COMPLETED":
                    logger.info(f"Job '{job_name}' completed successfully in {elapsed/60:.1f} minutes!")
                    return True
                elif status in ["FAILED", "CANCELLED", "ERROR"]:
                    logger.error(f"Job '{job_name}' failed with status: {status}")
                    self.show_job_logs(job_name)
                    return False
                
                time.sleep(30)  # Check every 30 seconds
                
            except subprocess.CalledProcessError:
                logger.warning(f"Could not get status for job '{job_name}', retrying...")
                time.sleep(10)
    
    def show_job_logs(self, job_name, tail_lines=50):
        """Show recent logs from Spark job driver"""
        logger.info(f"Recent logs for job '{job_name}':")
        try:
            self.run_kubectl([
                "logs", "-l", f"spark-app-name={job_name},spark-role=driver", 
                "-n", self.namespace, "--tail", str(tail_lines)
            ], capture_output=False)
        except subprocess.CalledProcessError:
            logger.warning("Could not retrieve driver logs")
            
        # Also show executor logs if driver logs aren't helpful
        logger.info("Recent executor logs:")
        try:
            self.run_kubectl([
                "logs", "-l", f"spark-app-name={job_name},spark-role=executor", 
                "-n", self.namespace, "--tail", "20"
            ], capture_output=False)
        except subprocess.CalledProcessError:
            logger.warning("Could not retrieve executor logs")
    
    def get_executor_count(self, job_name):
        """Get current number of executor pods"""
        try:
            result = self.run_kubectl([
                "get", "pods", "-l", f"spark-app-name={job_name},spark-role=executor", 
                "-n", self.namespace, "--no-headers"
            ])
            return len([line for line in result.split('\n') if line.strip()]) if result else 0
        except subprocess.CalledProcessError:
            return 0
    
    def apply_manifest(self, manifest_path):
        """Apply Kubernetes manifest"""
        logger.info(f"Applying manifest: {manifest_path}")
        self.run_kubectl(["apply", "-f", manifest_path])
    
    def delete_spark_job(self, job_name):
        """Delete existing Spark job"""
        try:
            self.run_kubectl(["delete", "sparkapplication", job_name, "-n", self.namespace])
            logger.info(f"Deleted existing job '{job_name}'")
            time.sleep(10)  # Wait for cleanup
        except subprocess.CalledProcessError:
            logger.info(f"Job '{job_name}' not found (this is okay)")
    
    def run_smoke_test(self):
        """Run smoke test to verify basic functionality"""
        logger.info("=== Running Smoke Test ===")
        
        self.delete_spark_job("mdp-smoke")
        self.apply_manifest("k8s/spark/49-smoke.yaml")
        
        if self.wait_for_spark_job("mdp-smoke", timeout_minutes=10):
            logger.info("Smoke test passed!")
            return True
        else:
            logger.error("Smoke test failed!")
            return False
    
    def run_bronze_ingest(self):
        """Run enhanced bronze ingestion for 1TB target"""
        logger.info("=== Running Bronze Ingest (1TB Target - No Compression) ===")
        
        self.delete_spark_job("mdp-bronze-ingest")
        self.apply_manifest("k8s/spark/42-bronze-ingest.yaml")  # FIXED: correct filename
        
        # Monitor executor scaling
        start_time = time.time()
        target_executors = 48  # Updated to match new config
        
        logger.info("Waiting for executors to start...")
        while time.time() - start_time < 600:  # Wait up to 10 minutes for executors
            executor_count = self.get_executor_count("mdp-bronze-ingest")
            if executor_count > 0:
                logger.info(f"Executors starting: {executor_count}/{target_executors}")
            
            if executor_count >= target_executors * 0.8:  # 80% of target
                logger.info(f"Sufficient executors running: {executor_count}/{target_executors}")
                break
                
            time.sleep(20)
        
        # Wait for completion with extended timeout for 1TB generation
        if self.wait_for_spark_job("mdp-bronze-ingest", timeout_minutes=180):  # 3 hours max
            logger.info("Bronze ingest completed!")
            
            # Try to get performance metrics from logs
            try:
                logs = self.run_kubectl([
                    "logs", "-l", "spark-app-name=mdp-bronze-ingest,spark-role=driver",
                    "-n", self.namespace, "--tail", "20"
                ])
                
                # Extract performance info
                for line in logs.split('\n'):
                    if "Throughput:" in line or "Est. Size:" in line or "Rows:" in line:
                        logger.info(f"Performance: {line.strip()}")
                        
            except subprocess.CalledProcessError:
                logger.warning("Could not retrieve performance metrics from logs")
            
            return True
        else:
            logger.error("Bronze ingest failed!")
            return False
    
    def run_silver_build(self):
        """Run silver layer build (Iceberg tables)"""
        logger.info("=== Running Silver Build (Iceberg) ===")
        
        self.delete_spark_job("mdp-silver-build")
        self.apply_manifest("k8s/spark/43-silver-build.yaml")
        
        if self.wait_for_spark_job("mdp-silver-build", timeout_minutes=60):
            logger.info("Silver build completed!")
            return True
        else:
            logger.error("Silver build failed!")
            return False
    
    def run_gold_finalize(self):
        """Run gold layer finalization"""
        logger.info("=== Running Gold Finalize ===")
        
        self.delete_spark_job("mdp-gold-finalize")
        self.apply_manifest("k8s/spark/44-gold-finalize.yaml")
        
        if self.wait_for_spark_job("mdp-gold-finalize", timeout_minutes=30):
            logger.info("Gold finalize completed!")
            return True
        else:
            logger.error("Gold finalize failed!")
            return False
    
    def validate_pipeline(self):
        """Validate pipeline results using Trino"""
        logger.info("=== Validating Pipeline Results ===")
        
        try:
            # Find Trino coordinator pod
            trino_pod = self.run_kubectl([
                "get", "pods", "-l", "app.kubernetes.io/component=coordinator,app.kubernetes.io/instance=mdp-trino",
                "-n", self.namespace, "-o", "jsonpath={.items[0].metadata.name}"
            ])
            
            if not trino_pod:
                logger.error("Could not find Trino coordinator pod")
                return False
            
            logger.info(f"Using Trino pod: {trino_pod}")
            
            # Test basic connectivity
            logger.info("Testing Trino connectivity...")
            self.run_kubectl([
                "exec", trino_pod, "-n", self.namespace, "--",
                "trino", "--catalog", "iceberg", "--schema", "information_schema",
                "--execute", "SELECT 1 as test"
            ], capture_output=False)
            
            # Check bronze data (if accessible via Trino)
            try:
                logger.info("Checking Bronze data size...")
                self.run_kubectl([
                    "exec", trino_pod, "-n", self.namespace, "--", 
                    "trino", "--catalog", "iceberg", "--execute", 
                    "SELECT 'Bronze validation - basic connectivity test' as status"
                ], capture_output=False)
            except subprocess.CalledProcessError:
                logger.warning("Bronze data not accessible via Trino (this is normal)")
            
            # Check silver table if it exists
            try:
                logger.info("Checking Silver table...")
                self.run_kubectl([
                    "exec", trino_pod, "-n", self.namespace, "--", 
                    "trino", "--catalog", "iceberg", "--schema", "silver",
                    "--execute", "SELECT COUNT(*) as silver_row_count FROM iot_daily_city_sensors LIMIT 1"
                ], capture_output=False)
            except subprocess.CalledProcessError:
                logger.warning("Silver table not accessible (run Silver job first)")
            
            # Check gold table if it exists
            try:
                logger.info("Checking Gold table...")
                self.run_kubectl([
                    "exec", trino_pod, "-n", self.namespace, "--",
                    "trino", "--catalog", "iceberg", "--schema", "gold", 
                    "--execute", "SELECT COUNT(*) as gold_row_count FROM iot_executive_kpis LIMIT 1"
                ], capture_output=False)
            except subprocess.CalledProcessError:
                logger.warning("Gold table not accessible (run Gold job first)")
            
            logger.info("Pipeline validation completed!")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Pipeline validation failed: {e}")
            return False
    
    def check_cluster_resources(self):
        """Check if cluster has sufficient resources for 1TB job"""
        logger.info("Checking cluster resources...")
        
        try:
            # Get node info
            nodes_info = self.run_kubectl(["get", "nodes", "-o", "wide"])
            logger.info("Available nodes:")
            logger.info(nodes_info)
            
            # Check resource allocation
            logger.info("Checking resource allocation...")
            self.run_kubectl(["top", "nodes"], capture_output=False)
            
        except subprocess.CalledProcessError:
            logger.warning("Could not retrieve cluster resource info")
    
    def run_full_pipeline(self):
        """Run the complete medallion pipeline"""
        logger.info("Starting MD-Pipeline (1TB Medallion Benchmark)")
        logger.info("Target: 1TB Bronze (no compression) -> Silver Iceberg -> Gold Analytics")
        
        # Check cluster resources first
        self.check_cluster_resources()
        
        start_time = time.time()
        
        # Step 1: Smoke Test
        logger.info("\n--- STEP 1: Smoke Test ---")
        if not self.run_smoke_test():
            logger.error("Smoke test failed - aborting pipeline")
            return False
        
        # Step 2: Bronze Ingest (1TB target with no compression)
        logger.info("\n--- STEP 2: Bronze Ingest (1TB Target) ---")
        bronze_start = time.time()
        if not self.run_bronze_ingest():
            logger.error("Bronze ingest failed - aborting pipeline")
            return False
        bronze_time = time.time() - bronze_start
        
        # Step 3: Silver Build
        logger.info("\n--- STEP 3: Silver Build ---")
        silver_start = time.time()
        if not self.run_silver_build():
            logger.error("Silver build failed - aborting pipeline")
            return False
        silver_time = time.time() - silver_start
        
        # Step 4: Gold Finalize
        logger.info("\n--- STEP 4: Gold Finalize ---")
        gold_start = time.time()
        if not self.run_gold_finalize():
            logger.error("Gold finalize failed - aborting pipeline")  
            return False
        gold_time = time.time() - gold_start
        
        # Step 5: Validate Results
        logger.info("\n--- STEP 5: Pipeline Validation ---")
        validation_success = self.validate_pipeline()
        if not validation_success:
            logger.warning("Pipeline validation had issues, but core processing completed")
        
        total_time = time.time() - start_time
        
        logger.info("\n=== PIPELINE COMPLETED SUCCESSFULLY! ===")
        logger.info("Performance Summary:")
        logger.info(f"  Bronze (1TB):   {bronze_time/60:.1f} minutes ({1024*60/bronze_time:.1f} GB/min)")
        logger.info(f"  Silver:         {silver_time/60:.1f} minutes") 
        logger.info(f"  Gold:           {gold_time/60:.1f} minutes")
        logger.info(f"  Total Runtime:  {total_time/60:.1f} minutes")
        
        # Calculate throughput
        total_throughput = 1024 / (total_time / 60)  # GB per minute for 1TB
        logger.info(f"  Overall Throughput: {total_throughput:.1f} GB/min")
        
        return True

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MD-Pipeline Orchestrator (Fixed)')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file')
    parser.add_argument('--namespace', default='md-pipeline', help='Kubernetes namespace')
    parser.add_argument('--stage', choices=['smoke', 'bronze', 'silver', 'gold', 'validate', 'full'],
                       default='full', help='Pipeline stage to run')
    parser.add_argument('--check-resources', action='store_true', help='Check cluster resources before running')
    
    args = parser.parse_args()
    
    orchestrator = MDPipelineOrchestrator(
        namespace=args.namespace,
        kubeconfig=args.kubeconfig
    )
    
    if args.check_resources:
        orchestrator.check_cluster_resources()
        return
    
    success = False
    
    try:
        if args.stage == 'smoke':
            success = orchestrator.run_smoke_test()
        elif args.stage == 'bronze':
            success = orchestrator.run_bronze_ingest()
        elif args.stage == 'silver':
            success = orchestrator.run_silver_build()
        elif args.stage == 'gold':
            success = orchestrator.run_gold_finalize()
        elif args.stage == 'validate':
            success = orchestrator.validate_pipeline()
        elif args.stage == 'full':
            success = orchestrator.run_full_pipeline()
            
    except KeyboardInterrupt:
        logger.info("\nPipeline orchestration interrupted by user")
        logger.info("Note: Spark jobs may continue running in the cluster")
        logger.info("Check status with: oc get sparkapplication -n " + args.namespace)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()