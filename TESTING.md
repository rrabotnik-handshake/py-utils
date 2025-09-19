# Testing BigQuery Integration

This directory contains comprehensive test suites for the new BigQuery integration features in `schema-diff`.

## Test Files

### 1. `quick_test.py` - Basic Integration Test
**Run this first** - Tests core functionality without requiring BigQuery access.

```bash
python quick_test.py
```

**What it tests:**
- ✅ All subcommand help systems work
- ✅ Configuration management (create/show config files)
- ✅ SQL schema parsing and comparison
- ✅ dbt model auto-detection
- ✅ Enhanced comparison features (--show-common, --all-records)

**Expected result:** All 10 tests should pass.

### 2. `test_bigquery_integration.py` - Comprehensive Test Suite
Tests all features including error handling and edge cases.

```bash
python test_bigquery_integration.py
```

**What it tests:**
- 🔍 Basic functionality preservation
- 🏗️ DDL generation subcommands (syntax/help)
- ⚙️ Configuration management
- 🔍 BigQuery type detection and parsing
- 📊 Enhanced schema comparison features
- 🔧 dbt model auto-detection
- 🚨 Error handling and edge cases

**Note:** This will attempt BigQuery operations but gracefully handles failures.

### 3. `test_bigquery_live.py` - Live BigQuery Test
**Requires actual BigQuery access** - Tests real DDL generation and live table comparisons.

```bash
# Replace with your actual BigQuery project/dataset/table
python test_bigquery_live.py handshake-production coresignal users
```

**Prerequisites:**
- BigQuery authentication configured (`gcloud auth` or service account)
- Access to a BigQuery project with readable tables
- `google-cloud-bigquery` package installed

**What it tests:**
- 🔗 Live BigQuery connectivity
- 🏗️ Real DDL generation from live tables
- 📊 Live table schema comparisons
- 📦 Batch DDL operations
- 💾 File output functionality

## Running Tests

### Option 1: Quick Verification (Recommended)
```bash
# Verify basic integration works
python quick_test.py
```

### Option 2: Full Test Suite
```bash
# Run comprehensive tests (handles BQ failures gracefully)
python test_bigquery_integration.py
```

### Option 3: Live BigQuery Testing
```bash
# Only if you have BigQuery access
python test_bigquery_live.py YOUR_PROJECT YOUR_DATASET YOUR_TABLE
```

## Example Test Commands

Here are some manual commands you can run to test specific functionality:

### DDL Generation
```bash
# Generate DDL for a table (will fail without BQ access, but tests parsing)
schema-diff ddl my-project:dataset.table

# Test DDL help
schema-diff ddl --help
schema-diff ddl-batch --help
schema-diff ddl-dataset --help
```

### Configuration
```bash
# Show current config
schema-diff config-show

# Create a config file
schema-diff config-init my-config.yml --project my-project

# Show config with custom file
schema-diff config-show --config my-config.yml
```

### Schema Comparisons
```bash
# Create test data
echo '{"id": 1, "name": "test"}' > test.json

# Compare with dbt model (auto-detected)
schema-diff test.json data/linkedin_member_us.sql

# Compare with explicit BigQuery table (will fail without access)
schema-diff test.json my-project:dataset.table --right bigquery

# Enhanced comparisons
schema-diff test.json data/linkedin_member_us.sql --show-common --all-records
```

## Troubleshooting

### Installation Issues
```bash
# Reinstall with BigQuery support
pip uninstall schema-diff
pip install -e ".[bigquery]"
```

### BigQuery Authentication
```bash
# Setup gcloud auth
gcloud auth application-default login

# Or set service account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Import Errors
If you see import errors, ensure you have all dependencies:
```bash
pip install google-cloud-bigquery pygments pyyaml
```

## Expected Test Results

### ✅ All Tests Pass
If all tests pass, your BigQuery integration is working correctly and you can:
- Generate DDL from live BigQuery tables
- Compare data files against live BigQuery schemas
- Use enhanced configuration management
- Leverage all existing schema-diff features with BigQuery

### ⚠️ Partial Failures
Some tests may fail if:
- BigQuery access not configured (expected for `test_bigquery_live.py`)
- Network connectivity issues
- Missing optional dependencies

This is normal - the core functionality should still work.

### ❌ Major Failures
If basic tests fail, check:
1. Is `schema-diff` properly installed? (`which schema-diff`)
2. Are you in the correct virtual environment?
3. Did you install with BigQuery support? (`pip install -e ".[bigquery]"`)

## Next Steps

After testing passes:
1. Try the DDL generation with your own BigQuery tables
2. Set up configuration files for your projects
3. Integrate with your data pipeline workflows
4. Use in CI/CD for schema validation

The BigQuery integration makes `schema-diff` a powerful tool for data engineering workflows! 🚀
