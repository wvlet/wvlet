# Custom functions (UDF/UDTF) for queries and flows

Design note for user-extensible functions in Wvlet: invoking local TypeScript/Node scripts, shell
commands, and JVM plugin jars from queries and flow stages.

## Goals

- Let users extend Wvlet with their own logic without touching the compiler: scoring, enrichment,
  calling a local model, running a script, calling an internal API.
- Make this first-class in **flows**: a stage should be able to hand its rows to custom code and
  continue the pipeline with the rows that come back, with the usual retries, timeouts, heartbeat,
  materialization, and `session resume` semantics.
- Support three implementation vehicles with **one** language surface and **one** runtime
  contract: TypeScript/Node scripts, arbitrary shell commands, and functions packaged in a jar.
- Keep it safe to share `.wv` files: query text must never be able to name an arbitrary command.

Non-goals: engine-side UDF registration (DuckDB/Trino), Scala.js / Native execution, and
isolation of the executed code (left to a future container-based execution engine).

## Background

What exists today (paths relative to the repo root):

- `def f(...) = native` (`WvletParser.scala:365`, `FunctionInliner.scala:371`) is the only "the
  body lives outside the SQL text" hatch. `NativeFunction.scala` is a hard-coded one-entry map
  (`ulid_string`) with **no arguments** and compile-time folding only. `native.wv` even carries a
  TODO deferring `env(key)` as "shell only for safety".
- `def f = sql"..."` bodies are inlined textually, dialect-aware (`FunctionInliner.variantRank`).
  This covers SQL-expressible functions well and is not the gap.
- Connectors (`Connector.scala`) unify three capabilities: `catalog` (tables), `engine` (SQL), and
  `tools` (MCP-shaped, `call slack.post_message(...)`). Tool invocation is one-way: the result is a
  single summary row (`QueryExecutor.runEmbeddedToolCalls`), never a relation. `ConnectorProvider`
  is a literal factory list, explicitly "no classpath scanning".
- Flows hand rows between stages only through physical tables (`FlowExecutor.materializeStage`,
  `create table __wv_flow_<run>_<stage> as <sql>`). `activate('<connector>', tool: ...)` streams
  a stage's rows *out* (as JSON lines) but nothing flows back in. `FlowStageRunner` exists and its
  doc says "runners of long-running non-SQL work should invoke `heartbeat()`", but production
  never injects one.
- `TableFunctionCall` resolves only to parameterized models; there is no UDTF path.
- The TypeScript SDK is compiler-only (Scala.js). There is no host bridge or IPC protocol.
- The 2026-08-18 agent-flow brief envisioned `agent(...)` as a stdlib table function backed by an
  `AgentRunner` SPI (ServiceLoader, like `ActivationSink`). A general custom-function primitive
  is the substrate that design needs; `agent()` becomes one connector function among others.

Constraints that shape the design:

- Engines cannot call out: DuckDB JDBC offers no scalar-UDF registration, Trino/Snowflake need
  server-side deployment. So custom code must run **at a materialization boundary** in the
  runner: rows out of the engine, through the function, back into the engine as a table. That
  is a UDTF shape (relation in, relation out); a scalar UDF is a special case (one row in, one
  row out, columns appended).
- Leo's spelling rule: SQL-looking syntax keeps SQL semantics. Hive's `TRANSFORM ... USING 'cmd'`
  looks close but has tab-separated, string-only semantics, so we should not reuse that spelling.

## Design

### 1. Declaration: `def` is the signature, the body says where the code lives

```wvlet
type scored = { id: int, score: double }

-- Code-backed functions: implemented in a JVM plugin jar or a TypeScript module,
-- found by name on the plugin path. Usable anywhere a function is usable.
def geocode(addr: string): string = native
def score(threshold: double): scored = native

-- Ad-hoc shell command: rows in on stdin, rows out on stdout (table-shaped only)
def dedupe: scored = sh"python3 scripts/dedupe.py"
```

- **`= native`** keeps its spelling ("implemented by the runtime") and gains arguments,
  per-row evaluation, and row streaming. Today's hard-coded one-entry `NativeFunction` map
  becomes a **function registry** resolved at execution time, in order: built-ins
  (`ulid_string`) → JVM `FunctionProvider`s (ServiceLoader, plugin jars) → TypeScript/JS
  modules. The `.wv` declaration is language-agnostic; the same `def` works whether the
  implementation is Scala or TypeScript. An unresolved `native` def stays what it is today:
  an engine-native function passed through to SQL (`#1896`), so engine catalogs keep working.
- **`sh"..."`** is a new interpolated-string body, symmetrical with `sql"..."`. `${arg}`
  splices are shell-quoted; every argument is also exported as `WVLET_ARG_<name>` and all of
  them as JSON in `WVLET_FUNCTION_ARGS` (triple-quoted strings do not interpolate `${...}`
  in the scanner today, so the environment variables are the way to pass arguments there).
- **The return type decides the shape.** A scalar type (`string`, `double`, …) is a scalar
  function: one value per input row. A row type (a declared `type`/`trait` name) is a table
  function: rows in, one **result object** out (section 4). `sh` bodies are table functions
  only.
- Typing is static from the `def` signature, so downstream columns type-check and the LSP
  completes them. Compile never executes anything and never needs the implementation; the
  Scala.js compiler (playground, LSP) is unaffected.

### 2. Invocation: regular functions, no new syntax

```wvlet
-- scalar, in any expression position
from customers
select id, geocode(address) as geo
where geocode(address) != ''

-- table function in pipe position (already parses as PartialQueryApply today)
from orders where status = 'paid'
| score(threshold: 0.5)
| where score > 0.8

from dedupe()          -- no input: runs with an empty input relation
```

- Scalar code-backed functions are ordinary `FunctionApply` expressions; the typer resolves
  them through `FunctionInliner` like any `def` (dialect scoring untouched) and tags the
  symbol as *external*.
- `from t | f(args)` already resolves user `def`s (partial queries). One new case: a resolved
  `def` with a row return type and an external body becomes `ExternalApply(child, fn, args)`
  instead of being inlined. `from f(args)` is the existing `TableFunctionCall` extended the
  same way. No operator keyword.
- Arguments use the regular function-call syntax: positional or `name = value`, with `def`
  defaults filled in. Table function arguments must be literals; scalar function arguments
  are any expression.

### 3. Execution: lowering to materialization boundaries (JVM runner)

Engines cannot call out (no scalar-UDF API through DuckDB JDBC; Trino/Snowflake need
server-side deployment), so custom code runs in the runner between two SQL fragments. A new
runner-side pass, `ExternalFunctionLowering`, runs before SQL generation in both
`QueryExecutor.executeQuery` and `FlowExecutor.materializeStage`:

1. **Scalar calls in expressions.** For each `select` / `add` / `where` operator whose
   expressions contain external scalar calls, the pass inserts under the operator an
   `ExternalApply` (scalar-map shape) over the operator's input that computes each call as an
   appended column (`__wv_fn_1`, …), with the call's argument expressions projected as
   engine-computed helper columns first. The original expression becomes a column reference,
   and helper columns are excluded again above operators that pass all columns through.
   Nested calls `f(g(x))` lower inside-out over repeated passes. Other operators (group by,
   order by, join conditions) report a clear error asking to compute the value with `add`
   first. Only a row id and the argument values leave the engine; the result is joined back
   by row id, so passthrough columns keep their exact values and types.
2. **Each `ExternalApply` is executed** as: generate SQL for its child; stream input rows
   with `DBConnector.streamJsonRows`; feed them to the implementation (JVM plugin
   in-process, TS module through the Node harness, or the `sh` subprocess); receive one
   result object; write its rows as JSON lines under `target/`; load them into the engine
   with the declared schema (DuckDB `read_json`, or the DuckDB-handoff staging for
   Trino/Snowflake, the `activate('file')` route from #2028); replace the node with a
   `TableRef` (query path) or write into the stage table (flow path). The result's metadata
   fields are logged and, in flows, stored with the stage record in the run store (file,
   SQLite, Postgres; `wvlet flow session show` displays them). Output is loaded with the
   declared column types rather than inferred ones, and decimal inputs are handed to functions
   as doubles, because JSON rows carry decimals as strings.
3. Input rows are streamed through one process per query, not one process per row; scalar
   TS functions are called per row inside the harness.

In flows this rides everything already there: the stage is a normal stage, so `retries`,
`timeout` (the running process is registered as the attempt's cancellable handle and
killed), `heartbeat` (ticked every second while a function runs), materialization, and
`session resume` need no special casing. The boundary currently requires DuckDB as the
engine receiving the rows (the same limit as cross-connector staging).

Non-zero exit or a thrown exception fails the attempt with the stderr tail in the message.

### 4. Implementations and the result object

Every table function returns **one JSON object**, not a row stream. Functions usually need
to return a single value carrying metadata (model used, token counts, elapsed time, a
verdict) that may also contain rows, and one object is what agents, tools, and HTTP calls
naturally produce. Contract:

- `rows` (optional array of objects): becomes the relation, typed by the `def` return type.
  For very large outputs `rows_path` (a JSON-lines file) is accepted instead, so a function
  can spool to disk without holding everything in memory.
- Any other top-level field is **metadata**: logged, stored with the flow stage attempt, and
  shown by `wvlet flow show`; never part of the relation.
- An object with neither `rows` nor `rows_path` is itself a **one-row relation**: its fields
  are the columns. This is how a function that returns a verdict or a summary composes with
  `where`/`route`/`test`, and it matches today's `call` summary-row behavior.

**JVM (plugin jar).**

```scala
trait FunctionProvider:            // registered in META-INF/services
  def functions: Seq[ExternalFunction]
trait ExternalFunction { def name: String }          // in wvlet-lang, package wvlet.lang.ext
trait ScalarFunction extends ExternalFunction { def eval(args: Seq[Any]): Any }
trait TableFunction  extends ExternalFunction {
  def apply(args: JSONObject, input: Iterator[JSONObject]): JSONObject }   // result object
```

Discovered with `ServiceLoader` (the `ActivationSink` pattern) from the application
classpath plus jars in the plugin folders: `<workdir>/plugins` by convention, plus the
directories in `WVLET_PLUGIN_PATH` (child classloader). A name provided twice is an error,
never first-wins. The compile-time built-in `ulid_string` is left as it is.

**TypeScript / JavaScript.** A module on the plugin path exports functions by name:

```ts
// plugins/geo.ts
export function geocode(addr: string): string { return lookup(addr) }
export async function score(rows: AsyncIterable<Row>, args: { threshold: number }) {
  const out = []
  for await (const r of rows) out.push({ id: r.id, score: model(r) })
  return { rows: out, model: 'v3', elapsed_ms: 12 }      // result object
}
```

The runner ships a small harness (`wvlet-udf-host.mjs`, a resource in `wvlet-runner`) and
launches `node [--experimental-strip-types] harness.mjs <module> <fn> <scalar|table>`.
Table mode hands the async iterable of input rows to the export and writes the returned
object to stdout; scalar mode calls the export per row (awaiting promises). Node 22+ is
already a project requirement. No SDK is needed; a typed helper package (`@wvlet/udf`) is a
follow-up.

**Shell (`sh"..."`).** stdin: one JSON object per input row, closed at end of input. stdout:
the result object as JSON; as a convenience, newline-delimited row objects are also accepted
and treated as `rows` with no metadata, so `cat` is a valid identity and `jq`-style filters
work. stderr: logged at the stage's log level. env: `WVLET_FUNCTION_ARGS`,
`WVLET_INPUT_COLUMNS`. Exit 0 = success.

### 5. Trust model

Running a `.wv` that declares `sh"..."` or `native` functions runs code on the host, the
same trust as running `make` in a checkout; a TS/JVM plugin only runs code already installed
on the plugin path. No allow-flag is added: isolation is the job of a future
**container-based execution engine** (functions run in a container the runner controls),
which this design leaves room for by keeping every implementation behind the same boundary
(`ExternalApply` → implementation → result object). Compile is always side-effect free, so
the LSP and playground never execute anything.

### 6. Delivery (single PR)

- Lang: `ExternalApply` plan node; resolution of external `def`s in pipe (`PartialQueryApply`)
  and `from f()` (`TableFunctionCall`) positions with argument binding; SQL generation guards
  so external code never leaks into SQL text; `wvlet.lang.ext` SPI traits; status codes
  `FUNCTION_NOT_FOUND` and `EXTERNAL_FUNCTION_FAILED`.
- Runner (JVM): `ExternalFunctionRegistry` (ServiceLoader + plugin folders + Node export
  listing), `ExternalProcess`, `ExternalFunctionExecutor` (result-object handling, typed load,
  scalar join-back), `ExternalFunctionLowering`, the Node host `wvlet-udf-host.mjs`, hooks in
  `QueryExecutor.executeQuery` and `FlowExecutor.materializeStage`, stage metadata in the run
  record and all three run stores, `flow session show` output.
- Specs and tests: `spec/basic/external-function.wv`, `spec/basic/flow-external-function.wv`,
  three `spec/neg` cases, and `ExternalFunctionTest` (JVM provider through the real
  ServiceLoader path, JavaScript and TypeScript modules, scalar lowering incl. nesting, error
  paths, flow metadata, stage timeout killing a running function).
- Docs: `website/docs/syntax/custom-functions.md`, linked from `stdlib.md` and `flow.md`.

Follow-ups: `--plugin-dir` CLI flag and a profile `plugins:` key, `@wvlet/udf` typed helper,
`agent()` as a table function, container-based execution engine for isolation, non-DuckDB
result engines (DuckDB handoff), execution on the Native/Node CLIs (`wvc`, `@wvlet/cli`),
inline `{ ... }` return types on `def`, `${...}` interpolation in triple-quoted strings,
batching several scalar calls of one operator into one round trip, constant folding of
literal-only scalar calls, typed access to result metadata from queries.

## Alternatives and Why Not?

- **An operator word for the pipe form** (`call`, `apply`, `pipe`, `exec`, Hive's
  `transform using`). Plain application needs no keyword, matches how partial-query `def`s
  are already used, and keeps one model: a `def` is invoked the same way whatever its body is.
- **Language-tagged bodies** (`ts"./geo.ts#geocode"`, `jvm"com.example.Geo"`). Ties the query
  to an implementation location; `= native` plus a plugin path lets the same `.wv` run with a
  Scala implementation on the JVM today and a TS one elsewhere later, and keeps the LSP and
  Scala.js compiler implementation-free.
- **Binding commands in the profile** (`process` connector, `| call scorer.score(...)`).
  Splits one function across two files and gives the compiler no signature. Isolation is
  better solved by the container execution engine than by where the command is written.
- **Engine-side registration** (DuckDB Java UDF, Trino plugins). Not reachable through DuckDB
  JDBC; Trino/Snowflake need server-side deployment. SQL-expressible cases are already
  covered by `def f = sql"..."` inlining.
- **Streaming row iterators as the return value.** Rejected: functions usually return one
  value with metadata that may contain rows; an iterator cannot carry metadata and forces
  agents/tools/HTTP calls into an unnatural shape. Streaming stays on the input side, and
  `rows_path` covers large outputs.
- **MCP servers as the vehicle.** Too heavy for a 10-line function. Its request/response
  shape now matches the result-object contract, so an MCP wrapper can sit on it later.
- **A flow-only `FlowOp`.** Would make custom code flow-only; in the relation algebra it works
  in ad-hoc queries, `test` statements, and models, and flows inherit it.

## Decisions taken in review

- Single result object (metadata plus optional rows) instead of a row iterator as the return
  value of table functions.
- No `--allow-shell` gate; isolation is deferred to the container-based execution engine.
- `sh"..."` as the shell body prefix; no operator keyword for invocation.
- Scalar-in-expression support ships in the same PR as table functions.
