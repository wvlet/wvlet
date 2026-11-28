# Running Wvlet

## Installing

- `wvlet` is the single command of every distribution, with the same options everywhere.
- `brew install wvlet/wvlet/wvlet` (recommended; Homebrew sets up the Java runtime) or a release
  package: `wvlet` and its `wv` alias of `wvlet repl`, with `run`, `build` (parallel, with run
  records), `flow`, `catalog`, `ui`, and the JDBC connectors. DuckDB (`-t duckdb`) and Snowflake
  need the full package `wvlet-cli-full-<version>.tar.gz` from the GitHub release; the default
  package reports this when they are selected.
- `npm install -g @wvlet/cli` (Node.js 22+): `wvlet version`, `compile`, `to_wvlet`, `run`,
  `repl`, `build`, `skill`, with no JDK. Its `build` runs one model at a time, doesn't enforce
  `timeout`, and records no runs. `ui`, `flow`, and `catalog` there exit with a message naming the
  JVM CLI.

## Commands

| Command | Purpose |
|---------|---------|
| `wvlet compile "<query>"`, `wvlet compile -f q.wv` | Print the SQL; checks syntax and types |
| `wvlet compile -f q.wv --target trino` | SQL for another engine (`duckdb`, `trino`, `snowflake`, `hive`) |
| `wvlet compile -f q.wv --strict` | Fail on typing errors too |
| `wvlet run "<query>"`, `wvlet run -f q.wv` | Execute and print the result; a failed `test` exits 1 |
| `wvlet run -f q.wv --format csv` | Print the last result as CSV |
| `wvlet to_wvlet "<sql>"` | Convert SQL to Wvlet |
| `wvlet build [-s sel] [--dry-run]` | Build materialized models in dependency order |
| `wvlet catalog import` | Generate `table` declarations from a live database |
| `wvlet flow run <flow>` | Run a `flow` pipeline |
| `wvlet skill [file]`, `wvlet skill files` | Print this skill (or a reference) as shipped with the installed version |
| `wvlet skill install [--agent claude,codex,pi] [--global]` | Install or update this skill for coding agents; `list` and `remove --yes` manage it |
| `wvlet repl`, `wvlet repl -c "<query>"` | REPL, or run one query and exit; `wv` is an alias on the JVM CLI |
| `wvlet ui [--max-running-queries n] [--max-queued-queries n] [--max-query-memory 2GB] [--memory-pool-size 8GB]` | Start the server and Web UI (JVM CLI); past the queue limit, submissions fail with `RESOURCE_EXHAUSTED`; a wvlet-engine query over `--max-query-memory` fails with `RESOURCE_EXHAUSTED`, and queued queries wait while engine queries hold more than `--memory-pool-size` (default 70% of the heap) |

`compile`, `to_wvlet`, `run`, and `repl` share `-p, --profile <name>`, `-t <engine>`,
`-w <folder>` (working folder), `--catalog`, and `--schema`; `-l debug` gives debug logs. A bare
`wvlet` opens the REPL only on a terminal; otherwise it prints the version.

## Engines

- The in-process wvlet engine is the default for `wvlet run` and the REPL: it reads Parquet, JSON,
  CSV, and Iceberg files (`from 'file.parquet'`) with no setup and no DuckDB. A `wvlet`
  connector with `"prepareTPCH": true` (and `"tpchScaleFactor"`, default 0.01) generates the
  TPC-H tables in memory, and `"prepareTPCDS": true` (`"tpcdsScaleFactor"`, up to 10) the
  TPC-DS tables; values differ from `dbgen` / `dsdgen`'s, so don't compare answers with
  official ones. `wvlet ui --tpch` does the same for TPC-H. With no setting, `from tpch.sf1.lineitem`
  or `tpcds.tiny.store_sales` reads them too (schema `tiny` is scale factor 0.01, `sf<n>` is n).
- To give the wvlet engine the keys of Parquet files (for join order), write a sidecar
  `<name>.keys.json` next to `<name>.parquet`, or `_keys.json` in a folder, e.g.
  `{"primaryKey": ["l_orderkey", "l_linenumber"], "uniqueKeys": [["l_comment"]]}`. Keys never
  change results; there is no DDL for them.
- `-t duckdb` (also `wvlet repl -t duckdb`) runs on DuckDB: needed for `sql"..."` / `execute`,
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
