# Remote Wvlet Server

The `wvlet` CLI can run queries on a central wvlet server instead of a local engine. The server holds the profiles, catalogs, and database credentials; thin clients — including the native `wv` binary and the Node.js CLI (`npx @wvlet/cli`) — send the original Wvlet query text and receive structured results, with no local database engine or credentials required.

## Starting a server

```bash
# Start a wvlet server on the default port 9090
wvlet ui --port 9090 --profile your-profile
```

The server compiles and executes queries with its own profile (`~/.wvlet/profiles.json` on the server host), so clients never need direct access to the underlying database.

## Running queries remotely

Use `-t wvlet` with the server's host (and port, when it is not the default 9090 for HTTP or 443 for HTTPS):

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
wvlet run -p remote "from lineitem select count(*)"
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
