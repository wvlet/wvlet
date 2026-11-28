---
sidebar_label: Web UI (wvlet server)
---

# Web UI (wvlet server)

`wvlet server` starts the wvlet server, which runs queries for [remote clients](remote-server.md) and serves a web UI on `http://localhost:9090` for writing and running Wvlet queries. Add `--open` to open it in a browser (macOS). The server's [monitoring API](remote-server.md#monitoring-api) lists running, queued, and recent queries with their plans and memory, and reports the server's load.

To start it with a sample TPC-H dataset, add `--tpch`. It sets `prepareTPCH` on the profile's connector: the in-process engine generates the tables in memory (see [TPC-H tables](../engines/wvlet-engine-tables.md#tpc-h-tables)), and a DuckDB profile runs DuckDB's `dbgen`:

```bash
$ wvlet server --tpch --open
```

![Web UI](./wvlet-ui-screenshot.png)

## Usage

```bash
$ wvlet server --help
usage: server
  Start the wvlet server and its web UI

[global options]
 -h, --help             Display help message
 --debug                Enable debug log
 -l [LOGLEVEL]          log level
 -L [LOGLEVELPATTERNS]  log level for a class pattern
[options]
 -p, --port:[PORT]    Web UI server port. default:9090
 -w [WORKDIR]         Working directory
 --profile:[PROFILE]  Profile to use
 --catalog:[CATALOG]  Context database catalog to use
 --schema:[SCHEMA]    Context database schema to use
 --open               Open the web UI in a browser (macOS)
 --quit-immediately   Quit the server immediately after starting. Only for boot testing
 --tpch               Load small demo TPC-H tables (scale factor 0.01, or tpchScaleFactor)
 --auth-token:[AUTHTOKEN]  Require this bearer token on RPC requests
 --max-running-queries:[MAXRUNNINGQUERIES]  Queries running at once; others wait in the queue. default: half the CPUs
 --max-queued-queries:[MAXQUEUEDQUERIES]  Queries waiting to run; submissions past it are rejected. default:1000
 --max-query-memory:[MAXQUERYMEMORY]  Memory one wvlet-engine query may hold (e.g. 2GB) before it fails. default: no limit
 --memory-pool-size:[MEMORYPOOLSIZE]  Memory wvlet-engine queries may hold together (e.g. 8GB) before queued queries wait. default: 70% of the heap
 --low-memory-killer-delay:[LOWMEMORYKILLERDELAY]  How long wvlet-engine queries may stay over the memory pool size before the largest one fails (e.g. 10s, or off). default: 5s
```

`wvlet ui` was removed; use `wvlet server --open`.

## Shortcut Keys

- `ctrl-enter`: Execute the sub query at the current line
- `shift-enter`: Describe the schema at the line
- `ctrl-j ctrl-d`: Describe the schema at the line
- `⌘-enter`: Run the current query
- `ctrl-j ctrl-r`: Run the current query
- `⌘-shift-enter`: Run the current query with production mode (e.g., skip debug/test expressions)
