# Running Wvlet

## Installing

- `npm install -g @wvlet/cli` (Node.js 22+): `wvlet version`, `compile`, `to_wvlet`, `run`
  with DuckDB included and no JDK.
- `brew install wvlet/wvlet/wvlet` or a release package: the JVM CLI, which adds the `wv`
  REPL, `wvlet build`, `flow`, `catalog`, and `ui`. DuckDB (`-t duckdb`) and Snowflake on the
  JVM CLI need the full package `wvlet-cli-full-<version>.tar.gz` from the GitHub release; the
  default package reports this when they are selected.

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
| `wvlet skill [file]`, `wvlet skill files` | Print this skill (or a reference) as shipped with the installed version |
| `wvlet skill install [--agent claude,codex,pi] [--global]` | Install or update this skill for coding agents; `list` and `remove --yes` manage it |
| `wv`, `wv -c "<query>"` | REPL, or run one query and exit |

Global options: `--profile <name>`, `-w <folder>` (working folder), `--catalog`, `--schema`,
`-l debug` for debug logs.

## Engines

- The in-process wvlet engine is the default for `wvlet run` and `wv`: it reads Parquet, JSON,
  CSV, and Iceberg files (`from 'file.parquet'`) with no setup and no DuckDB. A `wvlet`
  connector with `"prepareTPCH": true` (and `"tpchScaleFactor"`, default 0.01) generates the
  TPC-H tables in memory; values differ from `dbgen`'s, so don't compare answers with official
  ones. `wvlet ui --tpch` does the same.
- To give the wvlet engine the keys of Parquet files (for join order), write a sidecar
  `<name>.keys.json` next to `<name>.parquet`, or `_keys.json` in a folder, e.g.
  `{"primaryKey": ["l_orderkey", "l_linenumber"], "uniqueKeys": [["l_comment"]]}`. Keys never
  change results; there is no DDL for them.
- `-t duckdb` (also `wv -t duckdb`) runs on DuckDB: needed for `sql"..."` / `execute`,
  `.duckdb` files, extensions, and `read_csv(...)`-style table functions. `wvlet compile` emits
  DuckDB SQL by default. `wvlet build` and `wvlet flow` run on the wvlet engine, keeping tables
  in the project's `target/warehouse`; `view` models need `-t duckdb`.
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
