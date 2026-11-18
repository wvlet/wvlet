# dbt-style Model Pipelines

## Goals

- Let a folder of `.wv` files work as a dbt-style project: declare models once, then build every model, or a selected slice of the graph, in dependency order with one command.
- Record which dbt features Wvlet already covers and which are missing, so the follow-up PRs follow one roadmap instead of piecemeal additions.

## Background

- Wvlet already has most of the building blocks, but they are not connected into a model-centric workflow:
  - `model` defines reusable, parameterized queries, but a model is always inlined (dbt's `ephemeral`); nothing persists it.
  - `save to`, `create view`, and `append to t on keys` (MERGE) persist results, but the user writes one statement per model and orders them by hand.
  - `flow` / `stage` has a DAG scheduler with parallelism, retries, timeouts, schedules, backfill, and recorded runs (`wvlet flow run`, `session list`), but the DAG comes from hand-written stages, not from model references.
- dbt feature parity today:

| dbt feature | Wvlet today | Gap |
|---|---|---|
| Models + `ref()` | `model` + `from model_name` | None for references; no persistence |
| Materializations (view/table/incremental/ephemeral) | Only ephemeral (inlined) | **Missing** |
| `dbt build/run --select a+ +b` | `wvlet flow run` on hand-written flows | **Missing** graph-from-models + selectors |
| Incremental models (`is_incremental()`, `this`, `unique_key`) | `append to t on keys`; no watermark helper | **Missing** |
| Sources (`source()`, freshness) | `table ... in catalog.schema` declarations | Freshness missing |
| Seeds | `save to t if not exists` from `'file.csv'` | Covered |
| Snapshots (SCD2) | None | **Missing** |
| Singular data tests | `test` assertions in `.wv` | Covered |
| Generic tests (unique, not_null, relationships) | Column constraints on `table` declarations | Not checked against data |
| Macros (Jinja) | `def` partial queries, scalar `def` functions, parameterized models, `for` loops, ``s`...` `` table names, local `import` | Mostly covered, and type-checked |
| `adapter.dispatch` | Engine-specific stdlib folders and `trait ... in <dialect>` | Covered |
| `var()` / `env_var()` | Flow parameters only | **Missing** for models |
| Targets / profiles | Profiles + connectors | Covered |
| Hooks (pre/post), grants | `execute` statements | Missing as model config |
| Docs + lineage | `---` doc comments | Generated docs/lineage missing |
| Packages | Local `import` only (GitHub import still planned) | No package manager (out of scope) |

- Macros are the strongest point already: dbt macros are untyped Jinja text templates. Wvlet's `def` and parameterized models are type-checked and run on every engine. We do not plan a text-template layer.

## Design

Lane: Standard — new user-facing syntax and a new CLI command.

User flow (this PR):

- A user keeps models in `models/*.wv` and adds a materialization to the ones that should persist: `model orders_daily with { materialized: 'table' } = { from orders ... }`.
- They run `wvlet build` in the project folder. It finds every materialized model, derives the dependency graph from model references, and builds the models in order. Independent models run in parallel.
- `wvlet build --select orders_daily+` builds a model and everything downstream. `+orders_daily` builds it and everything upstream. A bare name builds only that model.
- Output shows one row per model with its state, like `wvlet flow run`. A failure gives a non-zero exit code, and downstream models are skipped.
- To recover, they fix the model and rerun `wvlet build`, or rerun a selection. `wvlet build --dry-run` prints the order and the generated SQL without running anything.

Decisions:

- Model config reuses the flow `with { key: value }` block, so models and stages share one configuration syntax. Unknown keys are compile errors.
- Materializations in this PR: `view` (`save as view`, create-or-replace), `table` (`save to`, replaced atomically when the engine supports it), and `ephemeral` (today's inline behavior, also the default). The default keeps every existing model unchanged.
- Inside `wvlet build`, a model that references a materialized model reads the persisted relation, as dbt's `ref()` does. Ephemeral models are still inlined.
- Outside a build (`wvlet run`, the REPL, spec tests), references keep today's inlining, so existing queries never hit a missing or stale table.
- Either way the reference is typed from the model definition, never from the live catalog.
- A selection whose materialized upstream models have never been built fails before running, with an error naming the models to build first.
- `wvlet build` compiles the graph into an internal flow, one stage per model, and runs it on the existing flow executor. Builds get parallelism, retries, and recorded runs (`wvlet flow session list`) without a second scheduler. Builds are recorded under the flow name `build`, so `wvlet flow session list` shows them next to other flows.
- Optional `schema:` and `alias:` keys set the target location; otherwise the model's name in the profile's current schema is used. If a `table` declaration binds the same name to another location, compilation fails, because declarations already pin where writes land.
- Model config also accepts the flow stage keys `retries:`, `timeout:`, and `engine:`, passed through to the model's stage.
- Parameterized models cannot be materialized. That is a compile error, because there is no single result to persist.
- Tests cover config parsing and errors, graph derivation and selectors, and DuckDB runner specs that build a small project end to end.
- Docs: a new "Model Pipelines" page with the parity table, plus updates to `data-models.md`, the CLI reference, and `for-ai-agents.md`.

Roadmap (follow-up PRs, in this order):

1. `incremental` materialization: `unique_key:` lowers to the existing keyed `append to`, plus `is_incremental` and `this` for watermark filters. This also replaces the old "planned for 2025" incremental section on the docs front page.
2. Generic data tests from `table`/model column constraints (`unique`, `not null`, a `references` relationship check), run by `wvlet build` after each model.
3. `var('name', default)` with `wvlet build --var k=v`, and `pre_hook:` / `post_hook:` config.
4. Snapshots (SCD2) as `materialized: 'snapshot'` with `unique_key:` and `updated_at:`.
5. `wvlet docs`: a lineage graph and model docs from `---` comments in the existing web UI. Source freshness checks.

## Alternatives and Why Not?

- **A dbt-style `ref('name')` function and a Jinja-like template layer.** Wvlet already resolves model names with types. Text templating would lose type checking and cross-engine compilation, which are Wvlet's main advantages over dbt.
- **A YAML project/config file (`dbt_project.yml`, `schema.yml`).** Keeping config next to the query in `.wv` files lets the compiler validate it. A project-level default (e.g. default materialization per folder) can come later if users need it.
- **Generate a `flow` source file instead of building the graph internally.** Generated code would drift from the models; deriving the graph on every build keeps one source of truth.
- **Start with incremental models.** They depend on materialization and on the build graph, so the build command comes first.

## Preferences learned

- Context: how `from materialized_model` resolves outside `wvlet build`. Asked: inline vs read the persisted table. User chose: "use static type definition as much as possible". Applied: a reference always keeps the model's static type, never a live-catalog lookup. Persisted relations are read only inside a build, and are typed from the model definition.
