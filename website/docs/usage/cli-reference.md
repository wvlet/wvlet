---
id: cli-reference
title: CLI Reference
sidebar_label: CLI Reference
---

# CLI Reference

This page provides a comprehensive reference for all Wvlet CLI commands.

`wvlet` is the single command of every distribution (npm, Homebrew, and the release packages). `wvlet` with no arguments on a terminal opens the [REPL](repl.md); in a pipe or a script it prints the version. `wv` is an alias of `wvlet repl`. Commands marked *JVM CLI* need the [Homebrew or release-package `wvlet`](install.md#homebrew-recommended); the npm `wvlet` doesn't have them yet.

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
| `-l, --loglevel <level>` | Set log level (error, warn, info, debug, trace) |
| `-L <pattern>=<level>` | Set log level for specific class patterns |

`compile`, `to_wvlet`, `run`, and `repl` share these options:

| Option | Description |
|--------|-------------|
| `-w <path>` | Working folder (default: current folder) |
| `-f, --file <path>` | Read the query from a file |
| `-t, --target <type>` | Engine: `wvlet` (default for `run` and `repl`), `duckdb` (default dialect for `compile`), `trino`, `snowflake`, ... |
| `-p, --profile <name>` | Use a connection profile from `~/.wvlet/profiles.json` |
| `--catalog <name>`, `--schema <name>` | Override the default engine's catalog and schema |
| `--strict` | Fail compilation on typing errors (`compile`, `to_wvlet`, `run`) |

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
| `--strict` | Fail compilation on typing errors |

**Examples:**

```bash
# Compile inline query
wvlet compile "from users select * limit 10"

# Compile from file
wvlet compile -f queries/analysis.wv

# Compile for specific target
wvlet compile -f query.wv --target trino

```

### `wvlet catalog import`

*JVM CLI.* Import database table schemas as Wvlet type definitions, and engine functions as Wvlet
function definitions, for offline query validation.
See [Importing Database Catalogs](./catalog-import.md).

```bash
wvlet catalog import [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--profile <name>` | Profile to connect with |
| `--catalog <name>` | Catalog to import (default: profile catalog) |
| `--schema <name>` | Import only the specified schema |
| `--path <folder>` | Output folder (default: `catalog`) |
| `--no-functions` | Skip importing engine functions |

**Examples:**

```bash
# Import all schemas of the profile's catalog
wvlet catalog import --profile production

# Import a single schema
wvlet catalog import --profile production --schema sales
```

### `wvlet skill`

Print or install the AI-agent skill embedded in this wvlet version, so coding agents read syntax that matches the installed compiler. `install`, `list`, and `remove` accept `-a, --agent` (`claude`, `codex`, `pi`; default all), `-g, --global`, `--dir`, `--force`, and `--json`; `remove` also takes `--yes` and `--dry-run`. See [Wvlet for AI Agents](../for-ai-agents.md).

```bash
wvlet skill                          # SKILL.md
wvlet skill syntax                   # references/syntax.md (also models, testing, cli)
wvlet skill files                    # list the files
wvlet skill install                  # install for Claude Code, Codex, and Pi in this project
wvlet skill install -a claude -g     # install for Claude Code in ~/.claude/skills
wvlet skill install --dir tools      # install into tools/wvlet/
wvlet skill list                     # show installs and whether they are current
wvlet skill remove -a codex --yes    # remove what wvlet installed (--dry-run to preview)
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
| `-t, --target <type>` | Engine to run on: `wvlet` (the in-process [wvlet engine](../engines/wvlet-engine.md), default without `--profile`), duckdb, trino |
| `-p, --profile <name>` | Run with a connection profile |
| `--catalog <name>`, `--schema <name>` | Override the default engine's catalog and schema |
| `--format <type>` | Output format: `box` (default) or `csv` (the last result) |
| `--host`, `--port`, `--user`, `--https` | Connect to a Trino coordinator, or with `-t wvlet` to a remote [wvlet server](remote-server.md). Passwords and tokens come from the profile only |

A failed `test` assertion or statement makes `wvlet run` exit with status 1 after printing all results.

**Examples:**

```bash
# Run inline query
wvlet run "from users select count(*)"

# Run from file
wvlet run -f queries/report.wv

# Print the result as CSV
wvlet run -f query.wv --format csv
```

### `wvlet repl`

Start the interactive [REPL](repl.md). A bare `wvlet` on a terminal does the same.

```bash
wvlet repl [-p <profile>] [-t <engine>] [-c <command>] [-f <file>]
```

`-c` runs a command and exits, `-f` runs the commands in a file and exits.

### `wvlet build`

Build materialized models in dependency order. The npm and Native `wvlet` build one model at a time; see [Model Pipelines](../syntax/model-pipelines.md) for what only the JVM CLI does.

```bash
wvlet build [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-w <folder>` | Project folder with .wv files (default: current folder) |
| `-s, --select <selectors>` | Models to build, comma-separated: `m`, `m+` (with downstream), `+m` (with upstream) |
| `--dry-run` | Print the build order and SQL without running anything |
| `-p, --profile <name>` | Profile to use |
| `-t, --target <engine>` | Engine to use without a profile: `wvlet` (default), `duckdb`, ... |
| `--catalog <name>`, `--schema <name>` | Context catalog and schema |
| `--run-store <type>` | *JVM CLI.* Store of run records: `file` (default), `sqlite`, or `postgres` |

**Examples:**

```bash
# Build every materialized model
wvlet build

# Build a model and everything downstream of it
wvlet build -s stg_orders+
```

### `wvlet ui`

*JVM CLI.* Start the Wvlet Web UI server.

```bash
wvlet ui [options]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-p, --port <port>` | Server port (default: 8080) |
| `--host <host>` | Server host (default: localhost) |
| `--no-browser` | Don't open browser automatically |
| `--max-running-queries <n>` | Queries running at once; others wait in the queue (default: half the CPUs) |
| `--max-queued-queries <n>` | Queries waiting to run; submissions past it fail with `RESOURCE_EXHAUSTED` (default: 1000). See [Remote Wvlet Server](./remote-server.md#query-queue-and-logs) |
| `--max-query-memory <size>` | Memory one wvlet-engine query may hold, e.g. `2GB`, before it fails with `RESOURCE_EXHAUSTED` (default: no limit) |
| `--memory-pool-size <size>` | Memory wvlet-engine queries may hold together before queued queries wait (default: 70% of the heap) |
| `--low-memory-killer-delay <duration>` | How long wvlet-engine queries may stay over the memory pool size, with more than one running, before the one holding the most memory fails with `RESOURCE_EXHAUSTED`, e.g. `10s` or `off` (default: `5s`) |

**Examples:**

```bash
# Start UI on default port
wvlet ui

# Start on custom port
wvlet ui --port 9000

# Start without opening browser
wvlet ui --no-browser
```


## Configuration

### Profile Configuration

Create profiles in `~/.wvlet/profiles.json` (parsed as JSONC: comments and trailing
commas are allowed, and `${ENV_VAR}` references are expanded from the environment).
A profile describes a working environment with one or more named connectors; the
connector marked `"default": true` (or the first one) is the engine queries run on:

```jsonc
{
  "profiles": [
    {
      "name": "local",
      "connectors": [
        { "name": "duckdb", "type": "duckdb", "catalog": "memory", "schema": "main" }
      ]
    },
    {
      "name": "production",
      "connectors": [
        {
          "name": "trino",
          "type": "trino",
          "default": true,
          "host": "trino.example.com:8080",
          "catalog": "production",
          "schema": "analytics",
          "user": "myuser",
          "password": "${TRINO_PASSWORD}"
        },
        { "name": "local", "type": "duckdb", "catalog": "memory", "schema": "main" }
      ]
    }
  ]
}
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
| `WVLET_DB_TYPE` | Default database type |

## The `wv` alias

`wv` is an alias of `wvlet repl`, kept for existing scripts. It takes the same options:

```bash
# Start the REPL (same as a bare `wvlet` on a terminal)
wv

# Run one command and exit
wv -c "from users select count(*)"
```

## Examples

### Typical Development Workflow

```bash
# 1. Write and test queries
wvlet compile -f query.wv

# 2. Run queries against database
wvlet run -f query.wv

# 3. Start UI for interactive development
wvlet ui
```

### CI/CD Pipeline Example

```bash
#!/bin/bash
# ci-validate-queries.sh

# Validate query syntax
find queries -name "*.wv" | while read file; do
  echo "Validating: $file"
  wvlet compile -f "$file"
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
- [REPL Usage](./repl.md)
- [Web UI](./ui.md)
