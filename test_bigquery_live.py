#!/usr/bin/env python3
"""
Live BigQuery integration test.

This script tests actual BigQuery connectivity and DDL generation.
Requires:
- BigQuery access configured (gcloud auth or service account)
- A BigQuery project with accessible tables

Usage:
    python test_bigquery_live.py PROJECT_ID DATASET_ID TABLE_ID

Example:
    python test_bigquery_live.py handshake-production coresignal users
"""

import json
import os
import shlex
import subprocess
import sys
import tempfile


def run_schema_diff_command(cmd: str, description: str) -> tuple[int, str, str]:
    """Run a schema-diff command and return results."""
    print("\n{'='*50}")
    print(f"🧪 {description}")
    print(f"Command: {cmd}")
    print(f"{'='*50}")
    
    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    
    print(f"Exit code: {result.returncode}")
    if result.stdout:
        print("STDOUT:")
        print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    return result.returncode, result.stdout, result.stderr


def run_bq_connectivity_test(project_id: str, dataset_id: str, table_id: str) -> bool:
    """Test basic BigQuery connectivity."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        # Try to get table info
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)
        
        print("✅ BigQuery connectivity successful!")
        print(f"   Table: {table_ref}")
        print(f"   Schema fields: {len(table.schema)}")
        print(f"   Created: {table.created}")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connectivity failed: {e}")
        return False


def create_sample_data(table_schema) -> str:
    """Create sample data that matches the table schema."""
    # This is a simple example - in practice you'd want to match the actual schema
    sample_data = [
        {
            "id": 1,
            "name": "Test User 1",
            "email": "test1@example.com"
        },
        {
            "id": 2, 
            "name": "Test User 2",
            "email": "test2@example.com"
        }
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        for record in sample_data:
            json.dump(record, f)
            f.write('\n')
        return f.name


def run_ddl_generation_test(project_id: str, dataset_id: str, table_id: str):
    """Test DDL generation functionality."""
    table_ref = f"{project_id}:{dataset_id}.{table_id}"
    
    # Test basic DDL generation
    cmd = f"schema-diff ddl {table_ref}"
    exit_code, stdout, stderr = run_schema_diff_command(
        cmd, f"Generate DDL for {table_ref}"
    )
    
    if exit_code == 0:
        print("✅ DDL generation successful!")
        # Check if we got reasonable DDL output
        if "CREATE OR REPLACE TABLE" in stdout:
            print("✅ DDL contains CREATE TABLE statement")
        if project_id in stdout and dataset_id in stdout and table_id in stdout:
            print("✅ DDL contains correct table reference")
    else:
        print("❌ DDL generation failed")
    
    # Test DDL with no constraints
    cmd = f"schema-diff ddl {table_ref} --no-constraints"
    exit_code, stdout, stderr = run_schema_diff_command(
        cmd, f"Generate DDL without constraints for {table_ref}"
    )
    
    if exit_code == 0 and "ALTER TABLE" not in stdout:
        print("✅ No-constraints option working")
    
    # Test DDL output to file
    with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as f:
        temp_sql = f.name
    
    try:
        cmd = f"schema-diff ddl {table_ref} --out {temp_sql}"
        exit_code, stdout, stderr = run_schema_diff_command(
            cmd, f"Generate DDL to file for {table_ref}"
        )
        
        if exit_code == 0 and os.path.exists(temp_sql):
            with open(temp_sql) as f:
                ddl_content = f.read()
            print(f"✅ DDL written to file ({len(ddl_content)} characters)")
            print(f"Preview:\n{ddl_content[:200]}...")
        else:
            print("❌ DDL file output failed")
    finally:
        if os.path.exists(temp_sql):
            os.unlink(temp_sql)


def run_live_table_comparison_test(project_id: str, dataset_id: str, table_id: str):
    """Test comparing data against live BigQuery table."""
    table_ref = f"{project_id}:{dataset_id}.{table_id}"
    
    # Create sample data
    sample_file = create_sample_data(None)
    
    try:
        # Test explicit BigQuery comparison
        cmd = f"schema-diff {sample_file} {table_ref} --right bigquery"
        exit_code, stdout, stderr = run_schema_diff_command(
            cmd, "Compare sample data vs live BigQuery table (explicit)"
        )
        
        if exit_code == 0:
            print("✅ Explicit BigQuery comparison successful!")
            if "Only in" in stdout or "Type mismatches" in stdout:
                print("✅ Comparison shows meaningful differences")
        else:
            print("❌ Explicit BigQuery comparison failed")
        
        # Test auto-detection
        cmd = f"schema-diff {sample_file} {table_ref}"
        exit_code, stdout, stderr = run_schema_diff_command(
            cmd, "Compare sample data vs live BigQuery table (auto-detect)"
        )
        
        if exit_code == 0:
            print("✅ Auto-detection BigQuery comparison successful!")
        else:
            print("❌ Auto-detection BigQuery comparison failed")
        
        # Test with all records
        cmd = f"schema-diff {sample_file} {table_ref} --all-records"
        exit_code, stdout, stderr = run_schema_diff_command(
            cmd, "Compare all records vs live BigQuery table"
        )
        
        if exit_code == 0:
            print("✅ All-records comparison successful!")
        
    finally:
        os.unlink(sample_file)


def run_batch_operations_test(project_id: str, dataset_id: str):
    """Test batch DDL operations (if multiple tables exist)."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref, max_results=3))
        
        if len(tables) >= 2:
            table_names = [table.table_id for table in tables[:2]]
            
            # Test batch DDL
            cmd = f"schema-diff ddl-batch {project_id}:{dataset_id} {' '.join(table_names)}"
            exit_code, stdout, stderr = run_schema_diff_command(
                cmd, f"Batch DDL generation for {len(table_names)} tables"
            )
            
            if exit_code == 0:
                print("✅ Batch DDL generation successful!")
            else:
                print("❌ Batch DDL generation failed")
        else:
            print(f"⚠️ Skipping batch test - only {len(tables)} tables found")
    
    except Exception as e:
        print(f"⚠️ Skipping batch test - error listing tables: {e}")


def main():
    """Main test function."""
    if len(sys.argv) != 4:
        print("Usage: python test_bigquery_live.py PROJECT_ID DATASET_ID TABLE_ID")
        print("\nExample:")
        print("python test_bigquery_live.py handshake-production coresignal users")
        sys.exit(1)
    
    project_id, dataset_id, table_id = sys.argv[1:4]
    
    print("🧪 LIVE BIGQUERY INTEGRATION TEST")
    print("=" * 60)
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_id}")
    print(f"Table: {table_id}")
    print("=" * 60)
    
    # Test 1: Basic connectivity
    if not run_bq_connectivity_test(project_id, dataset_id, table_id):
        print("❌ Cannot continue - BigQuery connectivity failed")
        sys.exit(1)
    
    # Test 2: DDL generation
    print("\n🏗️ Testing DDL Generation")
    run_ddl_generation_test(project_id, dataset_id, table_id)
    
    # Test 3: Live table comparison
    print("\n📊 Testing Live Table Comparison")
    run_live_table_comparison_test(project_id, dataset_id, table_id)
    
    # Test 4: Batch operations
    print("\n📦 Testing Batch Operations")
    run_batch_operations_test(project_id, dataset_id)
    
    print("\n🎉 LIVE BIGQUERY TEST COMPLETED!")
    print("If all tests passed, your BigQuery integration is working correctly!")


if __name__ == "__main__":
    main()
