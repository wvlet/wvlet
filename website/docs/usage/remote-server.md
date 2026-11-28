# Remote Wvlet Server

The `wvlet` CLI can run queries on a central wvlet server instead of a local engine. The server holds the profiles, catalogs, and database credentials; thin clients — including the native `wvlet` binary and the Node.js CLI (`npx @wvlet/cli`) — send the original Wvlet query text and receive structured results, with no local database engine or credentials required.

## Starting a server

```bash
# Start a wvlet server on the default port 9090
wvlet ui --port 9090 --profile your-profile
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
- Each query gets an id like `20261127_155437_00042_x7k2p`: the UTC creation time, a per-server sequence number, and a server instance id, so ids sort by time and neighboring queries differ in the sequence.
- Finished queries stay readable by id for an hour (at most 1000 are kept).
- Query events (`CREATED`, `STARTED`, `COMPLETED`, with status, times, row counts, and errors) are appended as JSON lines to `query-events-yyyy-MM-dd.jsonl` in the `logs` folder next to the server log; the server prints the folder at startup.

```bash
wvlet ui --port 9090 --max-running-queries 4 --max-queued-queries 100 --max-query-memory 2GB
```

## Authentication

To require a bearer token for all query (RPC) requests, start the server with a token. Prefer the environment variable so the secret stays out of the process list:

```bash
WVLET_SERVER_TOKEN=your-secret-token wvlet ui --port 9090
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
