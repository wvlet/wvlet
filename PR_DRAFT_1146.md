Title: Auto-switch to DuckDB when a query reads local files (Fixes #1146)

Summary
- When a query’s base relation is a local file (e.g., 'data.parquet' or 'sample.json'), execute it on DuckDB automatically instead of the default profile (e.g., Trino). This makes simple local-file queries work out of the box on engines that don’t support local file reads.

Rationale
- Trino cannot read local filesystem paths directly. Users frequently run into this when prototyping: “save to 'out.parquet'” succeeds (via the existing DuckDB handoff), but a subsequent “from 'out.parquet'” fails on Trino. This change completes the local workflow by transparently using DuckDB for such reads.

Scope of Change
- Runner only; no SQL or AST changes.
- QueryExecutor detects simple file-only inputs and chooses DuckDB for execution. All other queries continue to use the default connector.

Details
- Detection rule: prefer DuckDB when all of the following are true:
  - The relation contains at least one FileRef/FileScan.
  - All file paths are local (do not start with s3:// or https://).
  - The plan contains no TableRef/TableFunctionCall/RawSQL/ModelScan inputs (avoids cross-engine joins/mixed sources).
- Execution: generate SQL as usual, then choose connector:
  - DuckDB connector if detection passes.
  - Default profile connector otherwise.
- Logging: prints a single info line: “Auto-switched to DuckDB for local file read.”

Tests
- Added LocalFileReadHandoffTest:
  - Creates a small local Parquet via DuckDB.
  - Uses a Trino default profile with WvletScriptRunner.
  - Runs: select count(*) as c from 'out.parquet'.
  - Asserts success and c = 2.
- Existing TrinoLocalSaveTest continues to validate the prior “save to local file via DuckDB” handoff.

Non-Goals (Follow-ups)
- Mixed-source queries (local file + remote table) still run on the default connector and will fail on engines that don’t support local paths. A later enhancement could stage data or add an explicit opt-in.
- Remote file reads (s3://, https://) are not auto-switched in this PR.

Config
- No new flags in this PR. If needed, we can add `profile.properties.autoDuckDBForLocalRead=false` to disable in a future patch.

Risk & Compatibility
- Low risk: affects only queries whose only inputs are local files. Other queries are executed unchanged on the default engine.

Manual QA
- wvlet repl: run a query that reads 'sample.parquet' created locally; verify successful execution and see the “Auto-switched …” log line.

Changelog
- runner: auto-switch to DuckDB for queries that read local files.

