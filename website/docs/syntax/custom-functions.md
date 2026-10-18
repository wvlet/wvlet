# Custom Functions

Wvlet functions usually compile to SQL. When the logic you need cannot be written in SQL, such as calling a model, an internal API, or a script you already have, declare a **custom function**: the `.wv` file holds the signature, and the code runs outside the database engine, in TypeScript, on the JVM, or as a shell command. Custom functions work in ad-hoc queries, models, and [flow](./flow.md) stages alike.

:::info Experimental
Custom functions are **experimental**. They run with the JVM runner (`wvlet run`, the `wv` REPL, `wvlet flow`, and the server) and currently need DuckDB as the engine that receives their results.
:::

## A First Example

Put a TypeScript module into the `plugins` folder of your working directory:

```ts title="plugins/scoring.ts"
type Order = { id: number; amount: number }

// A table function: receives the input rows, returns a result object
export async function score(rows: AsyncIterable<Order>, args: { threshold: number }) {
  const out = []
  for await (const r of rows) {
    const s = r.amount / 1000
    if (s >= args.threshold) out.push({ id: r.id, score: s })
  }
  return { rows: out, model: 'linear-v1' }
}

// A scalar function: evaluated once per row
export function mask(email: string): string {
  return email.replace(/^[^@]+/, '***')
}
```

Declare the functions in Wvlet with `= native` and use them like any other function:

```wvlet
type scored = {
  id: int
  score: double
}

def score(threshold: double): scored = native
def mask(email: string): string = native

-- A table function applies to a relation with the pipe
from orders
| score(threshold = 0.5)
| where score > 0.8

-- A scalar function is called in expressions
from customers
select id, mask(email) as email
```

The declaration is language-agnostic. `= native` means "implemented by the runtime": Wvlet looks the function up by name among the installed plugins when the query runs, so the same `.wv` file works whether `score` is written in TypeScript or Scala.

## Two Kinds of Functions

The return type of the `def` decides how a function is used:

| Return type | Kind | Usage | Receives | Returns |
|---|---|---|---|---|
| A row type (`type scored = {...}`) | Table function | `from t \| f(args)` or `from f(args)` | All input rows and the call arguments | A result object |
| A value type (`string`, `long`, ...) | Scalar function | `f(x)` in `select`, `add`, `where` | The argument values of one row | One value |

Arguments are passed positionally or by name (`score(0.5)`, `score(threshold = 0.5)`), and parameters may declare defaults (`def score(threshold: double = 0.5)`). Arguments of table functions must be literal values; scalar functions take any expression.

### The Result Object

A table function returns a single object rather than a stream of rows, because functions usually have more to report than rows: the model that was used, token counts, a verdict.

- `rows`, an array of objects, becomes the output relation. Its columns and types come from the declared return type.
- `rows_path` may name a JSON-lines file instead of `rows`, so a function producing a large output can write it to disk.
- Every other top-level field is **metadata**. It is logged, recorded with the stage when the function runs in a flow (`wvlet flow session show <run_id>`), and never becomes part of the relation.
- An object with neither `rows` nor `rows_path` is itself a **one-row relation** whose fields are the columns. This suits functions that return one verdict or summary.

```wvlet
type verdict = {
  outcome: string
  reason: string
}

def review(topic: string): verdict = native

-- One row: { outcome: 'approved', reason: '...' }
from review('release notes')
| where outcome = 'approved'
```

## Implementing Functions

### TypeScript and JavaScript

Any `.ts`, `.mts`, `.js`, or `.mjs` module in a plugin folder provides its exported functions by name. Node.js must be on the `PATH` (Node 22.6 or later for TypeScript; set `WVLET_NODE` to use a specific binary).

- A table function has the shape `(rows: AsyncIterable<Row>, args: Record<string, unknown>) => result`, and may be `async`.
- A scalar function receives the call arguments positionally and returns a value or a promise of one.

Rows are streamed to the function, so iterate over them with `for await` instead of collecting them first when the input is large. Anything the module writes to stderr shows up in the Wvlet log.

### JVM (Plugin Jars)

Implement `wvlet.lang.ext.FunctionProvider` and register the class in `META-INF/services/wvlet.lang.ext.FunctionProvider` of your jar:

```scala
import wvlet.lang.ext.*
import wvlet.uni.json.JSON.*

class MyFunctions extends FunctionProvider:
  override def functions: Seq[ExternalFunction] = Seq(
    new ScalarFunction:
      override def name: String = "mask"
      override def eval(args: Seq[Any]): Any =
        args.head.toString.replaceAll("^[^@]+", "***")
    ,
    new TableFunction:
      override def name: String = "score"
      override def apply(args: JSONObject, input: Iterator[JSONObject]): JSONObject =
        val rows = input.filter(_.get("amount").isDefined).toIndexedSeq
        JSONObject(Seq("rows" -> JSONArray(rows), "model" -> JSONString("jvm-v1")))
  )
```

Drop the jar into a plugin folder. JVM functions run inside the Wvlet process, which makes them the fastest option for per-row scalar functions.

### Shell Commands

For a script or a command-line tool, write the command as the function body with `sh"..."`. A shell function is always a table function:

```wvlet
type scored = {
  id: int
  score: double
}

-- The input rows arrive on stdin as JSON lines
def rescore: scored = sh"python3 scripts/rescore.py"

def first_rows(n: int): scored = sh"head -n ${n}"

from orders
| rescore
| first_rows(100)
```

- **stdin**: one JSON object per input row.
- **stdout**: the result object as JSON. As a convenience, newline-delimited row objects are accepted as well and become the rows, so line-oriented tools (`cat`, `grep`, `head`, `jq -c`) work unchanged.
- **Arguments**: `${param}` splices an argument into the command, shell-quoted. Every argument is also available as the environment variable `WVLET_ARG_<name>`, and all of them as a JSON object in `WVLET_FUNCTION_ARGS`. Use the environment variables in triple-quoted (`sh"""..."""`) bodies, where `${...}` is not interpolated. `WVLET_INPUT_COLUMNS` lists the input column names.
- **stderr** goes to the Wvlet log. A non-zero exit code fails the query, with the last lines of stderr in the error message.
- The command runs in the working folder (`-w`).

## Plugin Folders

Wvlet looks for plugin jars and modules in:

1. the `plugins` folder of the working directory
2. every directory listed in the `WVLET_PLUGIN_PATH` environment variable (separated by `:`; `;` on Windows)

A function name must be provided only once. If two plugins export the same name, the query fails rather than picking one silently. A `= native` declaration with no matching plugin is treated as a function of the database engine and is passed through to SQL, which is how the bundled [engine function catalogs](./stdlib.md#engine-specific-functions) work.

## Custom Functions in Flows

A stage that applies a custom function is an ordinary stage: the function's rows are materialized like any stage output, so `retries`, `timeout`, `heartbeat`, triggers, and `session resume` behave as usual. A failing function fails the stage, a timeout or cancellation stops the running process, and a running function keeps reporting liveness to the stage's `heartbeat` watchdog.

```wvlet
flow ScoreOrders = {
  stage orders = from raw_orders where status = 'paid'

  stage scored with {
    retries: 2
    timeout: 10m
  } = from orders | score(threshold = 0.5)

  stage fallback if scored.failed =
    from orders | select id, 0.0 as score

  stage report = from scored | where score > 0.8
}
```

## How It Works

Database engines cannot call out to your code, so Wvlet runs a custom function at a **materialization boundary**: the query up to the function is executed on the engine, its rows are handed to the function, and the returned rows are loaded back into a temporary table that the rest of the query reads from. For a scalar function, only a row id and the argument values leave the engine, and the result is joined back by row id, so other columns keep their exact values and types.

Keep in mind:

- Each function call in a query is one such round trip. Filter before applying a function (`from t | where ... | f()`), so that less data crosses the boundary.
- Rows cross the boundary as JSON. Decimal values arrive as numbers, timestamps and dates as strings.
- `wvlet compile` produces SQL only, so a query that uses a custom function cannot be compiled to a single SQL statement; run it with `wvlet run`, the REPL, or a flow.

## Trust Model

A `.wv` file that declares an `sh"..."` function, or uses a plugin, runs code on your machine with your permissions, just like running `make` in a checked-out repository. Review functions from sources you do not trust before running them. Isolated, container-based execution of custom functions is planned.
