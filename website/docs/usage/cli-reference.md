---
id: cli-reference
title: CLI Reference
sidebar_label: CLI Reference
---

# CLI Reference

This page provides a comprehensive reference for all Wvlet CLI commands.

## Global Options

These options can be used with any command:

```bash
wvlet [global options] <command> [command options]
```

| Option | Description |
|--------|-------------|
| `-h, --help` | Display help message |
| `--version` | Display version |
| `--debug` | Enable debug logging |
| `-l <level>` | Set log level (ERROR, WARN, INFO, DEBUG, TRACE) |
| `-L <pattern>=<level>` | Set log level for specific class patterns |
| `--profile <name>` | Use a specific connection profile |
| `-w <path>` | Set working directory |

## Commands

### `wvlet compile`

Compile Wvlet queries to SQL.

```bash
wvlet compile [options] [query]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --file <path>` | Compile queries from a file |
| `-t, --target <type>` | Target database type (duckdb, trino) |
| `--catalog <name>` | Use specific catalog |
| `--schema <name>` | Use specific schema |
| `--use-static-catalog` | Use static catalog for compilation |
| `--static-catalog-path <path>` | Path to static catalog files |

**Examples:**

```bash
# Compile inline query
wvlet compile "from users select * limit 10"

# Compile from file
wvlet compile -f queries/analysis.wv

# Compile for specific target
wvlet compile -f query.wv --target trino

# Compile with static catalog
wvlet compile -f query.wv --use-static-catalog --catalog mydb
```

### `wvlet run`

Run Wvlet queries and display results.

```bash
wvlet run [options] [query]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-f, --file <path>` | Run queries from a file |
| `-t, --target <type>` | Target database type |
| `--catalog <name>` | Use specific catalog |
| `--schema <name>` | Use specific schema |
| `--format <type>` | Output format (table, json, csv, tsv) |
| `--limit <n>` | Limit result rows |

**Examples:**

```bash
# Run inline query
wvlet run "from users select count(*)"

# Run from file
wvlet run -f queries/report.wv

# Run with specific format
wvlet run -f query.wv --format json

# Run with row limit
wvlet run "from large_table select *" --limit 1000
```

### `wvlet ui`

Start the Wvlet Web UI server.

```bash
wvlet ui [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-p, --port <port>` | Server port (default: 8080) |
| `--host <host>` | Server host (default: localhost) |
| `--no-browser` | Don't open browser automatically |

**Examples:**

```bash
# Start UI on default port
wvlet ui

# Start on custom port
wvlet ui --port 9000

# Start without opening browser
wvlet ui --no-browser
```

### `wvlet catalog`

Manage static catalog metadata.

```bash
wvlet catalog <subcommand> [options]
```

#### `catalog import`

Import catalog metadata from a database.

```bash
wvlet catalog import [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-p, --path <path>` | Catalog storage path (default: ./catalog) |
| `-t, --type <type>` | Database type (duckdb, trino) |
| `-n, --name <name>` | Catalog name to import |
| `-s, --schema <name>` | Schema to import (default: all) |
| `--profile <name>` | Connection profile to use |

**Examples:**

```bash
# Import from DuckDB
wvlet catalog import --name mydb

# Import from Trino with profile
wvlet catalog import --type trino --name prod --profile production

# Import specific schema
wvlet catalog import --name mydb --schema sales

# Import to custom location
wvlet catalog import --path /data/catalogs --name mydb
```

#### `catalog list`

List available static catalogs.

```bash
wvlet catalog list [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-p, --path <path>` | Catalog directory path (default: ./catalog) |

**Examples:**

```bash
# List catalogs in default location
wvlet catalog list

# List catalogs in custom location
wvlet catalog list --path /data/catalogs
```

#### `catalog show`

Show details of a specific catalog.

```bash
wvlet catalog show [options] <catalog-spec>
```

**Arguments:**

- `<catalog-spec>`: Catalog specification in format `dbtype/name`

**Options:**

| Option | Description |
|--------|-------------|
| `-p, --path <path>` | Catalog directory path (default: ./catalog) |

**Examples:**

```bash
# Show catalog details
wvlet catalog show duckdb/mydb

# Show from custom location
wvlet catalog show --path /data/catalogs trino/prod
```

#### `catalog refresh`

Refresh catalog metadata from database.

```bash
wvlet catalog refresh [options]
```

**Options:**

Same as `catalog import`.

**Examples:**

```bash
# Refresh existing catalog
wvlet catalog refresh --name mydb

# Refresh with different profile
wvlet catalog refresh --name prod --profile production_readonly
```

## Configuration

### Profile Configuration

Create profiles in `~/.wvlet/config.yml`:

```yaml
profiles:
  default:
    type: duckdb
    database: ":memory:"
  
  local_duckdb:
    type: duckdb
    database: /path/to/database.db
  
  production:
    type: trino
    host: trino.example.com
    port: 8080
    catalog: production
    schema: analytics
    user: myuser
```

Use profiles with `--profile` option:

```bash
wvlet run -f query.wv --profile production
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `WVLET_HOME` | Wvlet home directory (default: ~/.wvlet) |
| `WVLET_LOG_LEVEL` | Default log level |
| `WVLET_STATIC_CATALOG_PATH` | Default static catalog path |
| `WVLET_DB_TYPE` | Default database type |

## Command Shortcuts

The `wv` command is a shortcut for the Wvlet REPL:

```bash
# Start REPL
wv

# Run REPL command
wv "from users select count(*)"

# Access catalog commands through wv
wv catalog list
```

## Examples

### Typical Development Workflow

```bash
# 1. Import catalog for offline development
wvlet catalog import --name dev_db

# 2. Write and test queries
wvlet compile -f query.wv --use-static-catalog --catalog dev_db

# 3. Run queries against actual database
wvlet run -f query.wv

# 4. Start UI for interactive development
wvlet ui
```

### CI/CD Pipeline Example

```bash
#!/bin/bash
# ci-validate-queries.sh

# Use static catalog for validation
find queries -name "*.wv" | while read file; do
  echo "Validating: $file"
  wvlet compile -f "$file" \
    --use-static-catalog \
    --static-catalog-path ./catalog \
    --catalog prod_db
done
```

### Batch Processing Example

```bash
# Process multiple queries
for query in daily/*.wv; do
  output="${query%.wv}.sql"
  wvlet compile -f "$query" > "$output"
  echo "Generated: $output"
done

# Run all reports
for report in reports/*.wv; do
  name=$(basename "$report" .wv)
  wvlet run -f "$report" --format csv > "output/${name}.csv"
done
```

## See Also

- [Installation Guide](./install.md)
- [Catalog Management](./catalog-management.md)
- [REPL Usage](./repl.md)
- [Web UI](./ui.md)