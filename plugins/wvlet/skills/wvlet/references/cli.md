# Running Wvlet

## Installing

- `npm install -g @wvlet/cli` (Node.js 22+): `wvlet version`, `compile`, `to_wvlet`, `run`
  with DuckDB included and no JDK.
- `brew install wvlet/wvlet/wvlet` or a release package: the full JVM CLI, which adds the `wv`
  REPL, `wvlet build`, `flow`, `catalog`, and `ui`.

## Commands

| Command | Purpose |
|---------|---------|
| `wvlet compile "<query>"`, `wvlet compile -f q.wv` | Print the SQL; checks syntax and types |
| `wvlet compile -f q.wv --target trino` | SQL for another engine (`duckdb`, `trino`, `snowflake`, `hive`) |
| `wvlet compile -f q.wv --strict` | Fail on typing errors too |
| `wvlet run "<query>"`, `wvlet run -f q.wv` | Execute and print the result |
| `wvlet to_wvlet "<sql>"` | Convert SQL to Wvlet |
| `wvlet build [-s sel] [--dry-run]` | Build materialized models in dependency order |
| `wvlet catalog import` | Generate `table` declarations from a live database |
| `wvlet flow run <flow>` | Run a `flow` pipeline |
| `wv`, `wv -c "<query>"` | REPL, or run one query and exit |

Global options: `--profile <name>`, `-w <folder>` (working folder), `--catalog`, `--schema`,
`-l debug` for debug logs.

## Engines

- DuckDB is the default and needs no setup. `from 'file.parquet'` reads local files.
- `-t wvlet` runs the in-process columnar engine on Parquet, JSON, CSV, and Iceberg files with
  no DuckDB.
- Trino, Snowflake, and other engines are profiles in `~/.wvlet/profiles.json`:

```json
{
  "profiles": [
    {
      "name": "dev",
      "connectors": [
        { "name": "td", "type": "trino", "default": true, "host": "example.com",
          "user": "${TD_API_KEY}", "catalog": "td", "schema": "sample_datasets" },
        { "name": "local", "type": "duckdb", "catalog": "memory", "schema": "main" }
      ]
    }
  ]
}
```

Use `${ENV_VAR}` for secrets; never write credentials into a profile. Select a profile with
`--profile dev`. Address a connector's tables with `from td.www_access`, and switch engines in a
script with `use local`.

## Flows

A `flow` groups `stage`s into a pipeline with retries, timeouts, triggers, and schedules
(experimental):

```wvlet
flow my_pipeline = {
  stage extract = from source | select *
  stage transform = from extract | where valid = true
  stage report = from transform | group by kind | agg _.count as n
}
```

`|` may separate operators on one line. Run with `wvlet flow run my_pipeline`; list with
`wvlet flow list`. See https://wvlet.org/wvlet/docs/syntax/flow for triggers and scheduling.

## More documentation

- All docs as plain text: https://wvlet.org/wvlet/llms-full.txt (index: `llms.txt`)
- Agent guide: https://wvlet.org/wvlet/docs/for-ai-agents
- Syntax reference: https://wvlet.org/wvlet/docs/syntax/
