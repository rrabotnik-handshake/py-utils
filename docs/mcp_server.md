# schema-diff MCP Server

This MCP (Model Context Protocol) server exposes `schema-diff` functionality to AI assistants and other MCP clients, enabling them to compare schemas, analyze data structures, and generate DDL statements.

## Features

The MCP server provides four main tools:

### 1. **compare_schemas**

Compare two schemas or data files to identify differences in structure, types, and nullability.

**Supported formats:**

- Data files: JSON, NDJSON, compressed (.gz)
- Schema files: JSON Schema, SQL DDL, Spark, dbt, Protobuf
- BigQuery: Live tables (`project:dataset.table`) or API JSON exports
- Google Cloud Storage: `gs://` paths

**Key parameters:**

- `file1`, `file2` - Files, tables, or GCS paths to compare
- `left_format`, `right_format` - Override auto-detection (e.g., `bq:api-json`, `spark:tree`)
- `show_common` - Include common fields in output
- `fields` - Compare only specific fields
- `sample_size` - Number of records to sample (default: 1000)

### 2. **generate_schema**

Generate schemas from data files in various formats.

**Output formats:**

- `json_schema` - JSON Schema (Draft-07)
- `sql_ddl` - SQL CREATE TABLE statement
- `bigquery_ddl` - BigQuery DDL with nested types
- `spark` - Spark StructType schema
- `openapi` - OpenAPI schema

**Key parameters:**

- `data_file` - Path to data file
- `format` - Output schema format
- `table_name` - Table name for DDL outputs
- `required_fields` - Fields to mark as NOT NULL

### 3. **analyze_schema**

Analyze schemas for complexity, patterns, and field categorization.

**Analysis types:**

- `complexity` - Nesting depth, type distribution, field counts
- `patterns` - Repeated structures, naming conventions
- `suggestions` - Improvement recommendations
- `dimensional` - Dimensional modeling analysis
- `field_categories` - Categorize fields (audit, metrics, identifiers, etc.)

**Key parameters:**

- `schema_file` - File or table to analyze
- `schema_type` - Override format detection
- `category` - Filter to specific field category
- `format` - Output format: `text`, `json`, `markdown`

### 4. **generate_bigquery_ddl**

Generate BigQuery DDL from live tables.

**Key parameters:**

- `table_ref` - BigQuery table: `project:dataset.table`
- `output` - Save to file (optional)

## Installation

### 1. Install with MCP support

```bash
cd /Users/rostislav.rabotnik/coresignal
pip install -e '.[mcp,bigquery,gcs]'
```

### 2. Configure your MCP client

#### For Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

**Option A: Using system Python**

```json
{
  "mcpServers": {
    "schema-diff": {
      "command": "python3",
      "args": ["-m", "mcp_server"],
      "cwd": "/Users/rostislav.rabotnik/coresignal",
      "env": {
        "PYTHONPATH": "/Users/rostislav.rabotnik/coresignal"
      }
    }
  }
}
```

**Option B: Using virtual environment Python (recommended)**

```json
{
  "mcpServers": {
    "schema-diff": {
      "command": "/Users/rostislav.rabotnik/coresignal/coresignal/bin/python",
      "args": ["-m", "mcp_server"],
      "cwd": "/Users/rostislav.rabotnik/coresignal"
    }
  }
}
```

#### For other MCP clients

The project includes three MCP configuration files:

- **`mcp.json`** - Standard MCP project metadata (for registries/discovery)
- **`mcp-config.json`** - Example configuration template
- **`mcp_server.py`** - The server implementation

Use `mcp-config.json` as a template and adjust paths for your specific client.

### 3. Authentication (for BigQuery features)

```bash
# Authenticate with Google Cloud
gcloud auth application-default login

# Set default project
gcloud config set project YOUR_PROJECT_ID
```

## Usage Examples

Once configured, you can ask your AI assistant to use schema-diff:

### Compare BigQuery API JSON schemas

```
"Compare the schemas in src_schema.json and tgt_schema.json
and tell me what fields were added or removed"
```

### Analyze data structure

```
"Analyze the schema complexity of my_data.json and
identify any audit or tracking fields"
```

### Generate DDL from data

```
"Generate BigQuery DDL for the data in users.json
with table name 'users_table'"
```

### Compare live BigQuery tables

```
"Compare the schemas of project:dataset.table1 and
project:dataset.table2, showing only the differences"
```

### Generate DDL from live table

```
"Generate the DDL for handshake-production:coresignal.linkedin_member_us"
```

## Testing the Server

### Manual testing with stdio

```bash
cd /Users/rostislav.rabotnik/coresignal
python3 -m mcp_server
```

The server will start and listen on stdin/stdout. You can test by sending MCP protocol messages.

### Testing with MCP Inspector

```bash
# Install MCP Inspector
npm install -g @modelcontextprotocol/inspector

# Run inspector
mcp-inspector python3 -m mcp_server
```

This opens a web UI for testing MCP tools interactively.

## Supported File Formats

The MCP server supports all `schema-diff` formats:

**Data Formats:**

- JSON, NDJSON, compressed (.gz)
- Google Cloud Storage (`gs://`)

**Schema Formats:**

- JSON Schema (Draft-07)
- Spark Schema (tree or JSON)
- SQL DDL (PostgreSQL, BigQuery)
- BigQuery Live Tables
- **BigQuery API JSON** (from `bq show --format=json`)
- dbt (manifest, schema.yml, model.sql)
- Protobuf (.proto)

## Troubleshooting

### Server not appearing in Claude Desktop

1. Check the config file location and syntax
2. Verify Python path is correct
3. Restart Claude Desktop
4. Check Claude Desktop logs: `~/Library/Logs/Claude/`

### Import errors

```bash
# Ensure schema-diff is installed
pip install -e '.[mcp,bigquery,gcs]'

# Verify installation
python3 -c "from schema_diff.cli import main; print('OK')"
```

### BigQuery authentication errors

```bash
# Check authentication
gcloud auth application-default print-access-token

# Re-authenticate if needed
gcloud auth application-default login
```

### GCS access errors

```bash
# Set default project
gcloud config set project YOUR_PROJECT_ID

# Check permissions
gsutil ls gs://your-bucket/
```

## Development

### Running tests

```bash
# Run with test data
python3 -c "
from mcp_server import call_tool
import asyncio

result = asyncio.run(call_tool('compare_schemas', {
    'file1': 'test1.json',
    'file2': 'test2.json'
}))
print(result)
"
```

### Adding new tools

Edit `mcp_server.py` and:

1. Add tool definition in `list_tools()`
2. Add handler in `call_tool()`
3. Update this README

## Architecture

```
┌─────────────────┐
│   MCP Client    │  (Claude Desktop, etc.)
│  (AI Assistant) │
└────────┬────────┘
         │ MCP Protocol
         │ (stdio)
┌────────▼────────┐
│   mcp_server.py │
│   MCP Server    │
└────────┬────────┘
         │
    ┌────▼────┬────────┬────────┐
    │ compare │ analyze│ generate│ ddl
    └─────────┴────────┴─────────┘
              │
         schema-diff
         (Python CLI)
```

## Resources

- **schema-diff documentation:** [README.md](./src/schema_diff/README.md)
- **MCP Protocol:** https://modelcontextprotocol.io/
- **MCP Python SDK:** https://github.com/modelcontextprotocol/python-sdk

## License

Same as schema-diff main package.
