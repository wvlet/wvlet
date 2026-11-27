# Docs: align the introduction and quick start with the Wvlet engine

## Goals

- The docs entry pages tell the same story as the redesigned top page (#2510): a flow-style language with its own engine, a choice of engines, SQL generation for DuckDB / Trino / Snowflake / Hive, and SQL accepted as input.
- A new reader can copy every quick-start example into `wv` with no flags and get the shown result.
- The top page's "Reusable, composable queries" link lands on a finished page.

## Background

- `wv` and `wvlet run` run on the in-process Wvlet engine by default. The quick start still opens with `execute sql"call dbgen(sf=0.01)"`, which the engine rejects (`execute` is unsupported), so the walk-through fails at step one.
- Since #2478, every engine session can read generated TPC-H tables as `tpch.tiny.<table>` with no setup. `use tpch.tiny` does not make bare names resolve, and `show tables in tpch.tiny` returns no rows (checked with a CLI built from main).
- The engine's TPC-H values follow the spec but aren't `dbgen`'s, so every printed result in the quick start and intro must be regenerated.
- The intro's architecture image shows only SQL generation, plus boxes that don't exist as features (differentially private SQL, Embulk import/export, Python extension).
- `data-models.md` covers models, `table`, and `trait`, but not model parameters or `def` functions (scalar functions and partial queries). Both appear only in passing elsewhere (an `index.md` example, `model-pipelines.md`), with no page that explains them.
- `wv` is in the JVM CLI only; the install page recommends npm, whose `wvlet run` also defaults to the engine. DuckDB (`-t duckdb`) ships with npm and the full JVM package, not the Homebrew one.

## Design

- Lane: **Express** — docs only, no code or always-gate area.
- `index.md`:
  - New opening and `description` matching the top page: language first, its own columnar engine (JVM, Node.js, browser, native), compiles to SQL for the four engines, reads `.sql` files and converts SQL with `wvlet to_wvlet`.
  - Keep the motivation (semantic order, pipe-syntax references, Trino Summit slides); reword lines that say queries always become SQL.
  - Replace the architecture image with a short Mermaid diagram: `.wv` / `.sql` → compiler → logical plan → Wvlet engine or SQL generator → DuckDB / Trino / Snowflake / Hive. Only parts that exist today.
  - Feature examples query `tpch.tiny.*` with outputs re-run on the engine.
- `quick-start.md`:
  - Start with `wv` and `from tpch.tiny.customer`; explain the `tpch` catalog in one paragraph (scale factor in the schema name, generated on scan, nothing written to disk) and link the engine page.
  - Use qualified names throughout instead of a profile, so nothing needs setup; mention `-t duckdb` and `dbgen` in one tip for readers who want DuckDB's data.
  - Show `wvlet run "..."` once at the start for npm users; the rest stays in `wv`.
  - The DuckDB/`dbgen` tip says it needs the npm or full JVM package.
  - Regenerate every output by running it; also fix stale text ("backed by DuckDB in-memory database", the description) and an existing mismatch (the `where` example's output didn't match its `select`).
- `data-models.md`: drop the "work in progress" warning; rewrap to one line per paragraph (AGENTS.md); add "Model parameters" and "Functions" (scalar `def`, partial queries `def name = where ...`) sections with examples verified on the engine. Keep the top page link as is.
- `for-ai-agents.md` and the bundled skill: no update. Neither mentions the `dbgen` quick start, the skill already covers `def`, and this PR changes no syntax or CLI.
- Verification: run every example on the engine with a CLI built from this branch; `pnpm --filter website run build`.
- Out of scope: the top page hero result is `dbgen` data under a "wvlet engine" badge; it stays for the styling / demo items.

## Alternatives and Why Not?

- Quick start with a `prepareTPCH` profile so examples read `from customer`: adds a setup step before the first query.
- Keep `dbgen` with `wv -t duckdb`: contradicts the engine-first positioning and the default the reader gets.
- Point the top page link at `model-pipelines.md` instead: it's about building, not reuse; finishing data-models is small.
