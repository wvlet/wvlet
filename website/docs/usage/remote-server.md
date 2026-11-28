# Remote Wvlet Server

The `wvlet` CLI can run queries on a central wvlet server instead of a local engine. The server holds the profiles, catalogs, and database credentials; thin clients — including the native `wvlet` binary and the Node.js CLI (`npx @wvlet/cli`) — send the original Wvlet query text and receive structured results, with no local database engine or credentials required.

## Starting a server

```bash
# Start a wvlet server on the default port 9090
wvlet server --port 9090 --profile your-profile
```

The server compiles and executes queries with its own profile (`~/.wvlet/profiles.json` on the server host), so clients never need direct access to the underlying database.

## Running queries remotely

Use `-t wvlet` with the server's host (and port, when it is not the default 9090 for HTTP or 443 for HTTPS). Without a host, `-t wvlet` runs the [in-process wvlet engine](../engines/wvlet-engine.md) instead:

```bash
# Run a query on a remote wvlet server
wvlet run -t wvlet --host wvlet.example.com --port 9090 "from lineitem select count(*)"

# Run a query file
wvlet run -t wvlet --host wvlet.example.com -f daily_report.wv

# Use HTTPS (default port 443)
wvlet run -t wvlet --host wvlet.example.com --https "from orders limit 10"
```

Each CLI invocation holds one server-side session, so multi-statement scripts keep their state — `use` statements and temporary tables work across statements within a run.

A profile entry can hold the connection settings so you only pass `-p`:

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "remote",
      "connectors": [
        {
          "name": "wvlet",
          "type": "wvlet",
          "default": true,
          "host": "wvlet.example.com",
          "port": 9090,
          "properties": {
            // Optional: run under this profile name defined on the SERVER
            "remoteProfile": "production"
          }
        }
      ]
    }
  ]
}
```

```bash
wvlet run --profile remote "from lineitem select count(*)"
```

## Query queue and logs

The server runs a bounded number of queries at once and queues the rest; a client's statements run one at a time, in order.

- `--max-running-queries N` sets how many queries run at once (default: half the CPUs). Others wait as `QUEUED`.
- `--max-queued-queries N` sets how many queries can wait (default: 1000). Past it, a submission fails with `RESOURCE_EXHAUSTED`; retry after running queries finish.
- `--max-query-memory SIZE` (e.g. `2GB`) fails a query on the in-process wvlet engine with `RESOURCE_EXHAUSTED` once its join, aggregation, sort, window, and CTE tables and buffered results exceed it (default: no limit).
- `--memory-pool-size SIZE` (e.g. `8GB`) is how much memory the in-process engine's queries may hold together (default: 70% of the JVM heap). While they hold more, queued queries wait for running ones to finish instead of starting. Sizes take `KB`, `MB`, `GB`, or `TB` (powers of 1024); memory is estimated from the engine's vectors, so leave headroom below the heap. Other engines (DuckDB, Trino) are not counted.
- `--low-memory-killer-delay DURATION` (e.g. `10s`, default `5s`) protects the server when running queries keep growing past the pool: once they stay over `--memory-pool-size` that long with more than one running, the query holding the most memory fails with `RESOURCE_EXHAUSTED` ("Query was killed to free memory"), so the others can finish. Its client sees the error and can retry later. `off` never kills; a single running query is never killed, since `--max-query-memory` bounds it.
- Each query gets an id like `20261127_155437_00042_x7k2p`: the UTC creation time, a per-server sequence number, and a server instance id, so ids sort by time and neighboring queries differ in the sequence.
- Finished queries stay readable by id for an hour (at most 1000 are kept).
- Query events (`CREATED`, `STARTED`, `COMPLETED`, with status, times, row counts, and errors) are appended as JSON lines to `query-events-yyyy-MM-dd.jsonl` in the `logs` folder next to the server log; the server prints the folder at startup.

```bash
wvlet server --port 9090 --max-running-queries 4 --max-queued-queries 100 --max-query-memory 2GB
```

## Monitoring API

The server's `FrontendApi` (uni RPC: `POST /wvlet.lang.api.v1.frontend.FrontendApi/<method>` with a JSON `{"request": {...}}` body) reports what it runs; the [web UI](./ui.md) polls it about once a second:

- `listQueries` lists running, queued, and kept finished queries, newest first. Filter by `statuses`, `sessionId`, `profile`, `createdAfter` / `createdBefore`, and `text` (case-insensitive); page with `limit` (default 100, at most 1000) and the response's `nextPageToken`. Each entry has the status, session, profile, the first 200 characters of the query, times, queued and run milliseconds (up to now while waiting or running), result rows, the first error, and memory.
- `getQueryDetail` returns one query's full text, the SQL it sent to DuckDB or Trino, the in-process engine's physical plan of each statement, its errors, and memory: bytes held now and the peak of one statement. `memory.tracked` is false for queries on other engines, which the server does not count. Result rows stay in `getQueryInfo`.
- `getClusterInfo` reports running, queued, and kept finished query counts against `--max-running-queries` / `--max-queued-queries`, the engine memory pool (held bytes, capacity, per-query limit, open queries, queries the low-memory killer failed), the load of query workers and engine task threads, the JVM heap, and the uptime.

Every token holder can list every session's queries, including their text.

## Authentication

To require a bearer token for all query (RPC) requests, start the server with a token. Prefer the environment variable so the secret stays out of the process list:

```bash
WVLET_SERVER_TOKEN=your-secret-token wvlet server --port 9090
```

(`--auth-token your-secret-token` also works.)

Clients supply the token through the profile's `token` property. Environment variables are interpolated with `${VAR}`, so the secret does not need to be written into the file:

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "remote",
      "connectors": [
        {
          "name": "wvlet",
          "type": "wvlet",
          "default": true,
          "host": "wvlet.example.com",
          "port": 9090,
          "useHttps": true,
          "properties": {
            "token": "${WVLET_TOKEN}"
          }
        }
      ]
    }
  ]
}
```

The token is sent as an `Authorization: Bearer` header on every request. Requests without a valid token are rejected with an authentication error; the static Web UI assets remain reachable without a token. Always combine token authentication with HTTPS (for example, behind a TLS-terminating reverse proxy) so the token is not sent in clear text.
