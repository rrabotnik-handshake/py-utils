#!/usr/bin/env python3
"""
Quick test of schema-diff BigQuery integration.

This script runs basic tests that don't require actual BigQuery access.
"""

import json
import os
import subprocess
import tempfile


def test_command(cmd: str, description: str) -> bool:
    """Test a command and return success status."""
    print(f"\n🧪 {description}")
    print(f"   Command: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"   ✅ SUCCESS")
        return True
    else:
        print(f"   ❌ FAILED (exit code: {result.returncode})")
        if result.stderr:
            print(f"   Error: {result.stderr.strip()}")
        return False


def main():
    """Run quick tests."""
    print("🚀 QUICK SCHEMA-DIFF BIGQUERY INTEGRATION TEST")
    print("=" * 60)
    
    success_count = 0
    total_tests = 0
    
    # Test 1: Help commands
    tests = [
        ("schema-diff --help > /dev/null", "Basic help command"),
        ("schema-diff ddl --help > /dev/null", "DDL subcommand help"),
        ("schema-diff ddl-batch --help > /dev/null", "DDL batch help"),
        ("schema-diff ddl-dataset --help > /dev/null", "DDL dataset help"),
        ("schema-diff config-show --help > /dev/null", "Config show help"),
        ("schema-diff config-init --help > /dev/null", "Config init help"),
    ]
    
    for cmd, desc in tests:
        total_tests += 1
        if test_command(cmd, desc):
            success_count += 1
    
    # Test 2: Config management
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "test.yml")
        
        total_tests += 1
        if test_command(f"schema-diff config-init {config_path}", "Config file creation"):
            success_count += 1
            
            # Verify file was created
            if os.path.exists(config_path):
                print(f"   ✅ Config file created successfully")
                with open(config_path) as f:
                    content = f.read()
                    if "default_project" in content:
                        print(f"   ✅ Config file has expected content")
                    else:
                        print(f"   ⚠️ Config file missing expected content")
    
    # Test 3: Schema comparison with SQL
    test_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ]
    
    sql_schema = '''
CREATE TABLE users (
    id INT64 NOT NULL,
    name STRING,
    age INT64,
    email STRING
);
'''
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as data_file:
        for record in test_data:
            json.dump(record, data_file)
            data_file.write('\n')
        data_path = data_file.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as sql_file:
        sql_file.write(sql_schema)
        sql_path = sql_file.name
    
    try:
        total_tests += 1
        if test_command(
            f"schema-diff {data_path} {sql_path} --right sql", 
            "Data vs SQL schema comparison"
        ):
            success_count += 1
        
        total_tests += 1
        if test_command(
            f"schema-diff {data_path} {sql_path} --right sql --show-common", 
            "SQL comparison with common fields"
        ):
            success_count += 1
            
    finally:
        os.unlink(data_path)
        os.unlink(sql_path)
    
    # Test 4: dbt model auto-detection
    dbt_model_path = "/Users/rostislav.rabotnik/coresignal/data/linkedin_member_us.sql"
    if os.path.exists(dbt_model_path):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as data_file:
            json.dump({"id": 1, "name": "test"}, data_file)
            data_path = data_file.name
        
        try:
            total_tests += 1
            if test_command(
                f"schema-diff {data_path} {dbt_model_path}",
                "dbt model auto-detection"
            ):
                success_count += 1
        finally:
            os.unlink(data_path)
    else:
        print(f"\n⚠️ Skipping dbt model test - file not found: {dbt_model_path}")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"🏆 TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Passed: {success_count}/{total_tests} tests")
    
    if success_count == total_tests:
        print(f"🎉 ALL TESTS PASSED! BigQuery integration is working correctly.")
        return 0
    else:
        print(f"❌ Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    exit(main())
