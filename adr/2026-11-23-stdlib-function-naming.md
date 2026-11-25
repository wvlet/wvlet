# ADR: Stdlib function names are Wvlet's engine-neutral contract

Date: 2026-11-23 · PR #2322

## Context

- The `wvlet` engine is becoming the default local engine, and it lacks many functions users reach for: `sha1`, base64, `corr`, hyperbolic math, timestamp parsing, JSON builders, and more (`EngineStdlibCoverageCheck`, `EngineFunctionCoverageCheck`, PR #2320).
- Function names are hard to change once queries depend on them. Wvlet already has an engine-neutral standard: the stdlib's dot-syntax functions (`name.upper`, `price.round(2)`, `s.sha256`), which compile to each engine's SQL through `in duckdb` / `in trino` / ... variants (`wvlet-stdlib/module/standard/*.wv`).
- The engine follows the DuckDB dialect (`DBType.dialectContextName`, ADR 2026-11-21-wvlet-engine-connector). That made it tempting to port DuckDB's catalog (786 functions) by name. Doing so would make DuckDB's names, aliases, and quirks de facto Wvlet API: `argmax`/`arg_max`/`max_by` for one aggregate, `list_*`/`array_*` duplicates, `date_sub` that returns a difference, `to_days(n)` that builds an interval, and `array_unique` that returns a count.
- Engines disagree on names and on meaning. The comparison over ANSI, DuckDB, Trino, PostgreSQL, Spark SQL, Snowflake, and BigQuery is in `plans/2026-11-23-function-name-comparison.md`. Examples:
  - Parsing a timestamp is `strptime`, `date_parse`, or `to_timestamp`, with three format dialects.
  - `cast(2.5e0 as bigint)` is 2 on DuckDB/PostgreSQL (half even) and Spark (truncation), but 3 on Trino/Snowflake/BigQuery.
  - Day-of-week numbering has four schemes.

## Decision

### The contract
- The public function surface is the stdlib: receiver-first methods (`x.f(args)`) in snake_case, one name per capability, no aliases. Aggregations keep the column as the receiver (ADR 2026-10-27).
- A bare SQL function call (`select corr(a, b)`, `sql"..."`, `.sql` files) is engine SQL, not part of the contract. It works where that engine has the function.

### Choosing a name
Apply these rules in order:
1. **Existing Wvlet conventions win.**
   - `to_*` converts the receiver's value: `to_long`, `to_json`, `to_base64`. It never builds a different kind of value, so DuckDB's `to_days(n)` interval constructor is not adopted.
   - `from_*` converts back (`from_unixtime`, `from_base64`), and `try_*` returns NULL instead of failing.
   - `diff_*` / `add_*` for date math, `is_*` for predicates, `max_by` / `min_by` for value-at-extreme.
   - A function on the same kind of input as an existing one follows it. `s.split(separator)` returns an array, so the regex form is `s.regexp_split(pattern)`, not PostgreSQL/DuckDB's `regexp_split_to_array`.
2. **The ANSI SQL name, when there is one**, e.g. `normalize`, `json_object`, `json_array`.
3. **The name most of the seven sources share**, counting ANSI and the six engines, e.g. `corr`, `covar_samp`, `regr_slope`, `translate`, `sinh`, `sha1`, `regexp_extract_all`, `url_encode`.
4. **Otherwise a descriptive name in Wvlet style** (`round_half_even`, `day_name`), never an engine-specific abbreviation.

### Semantics are part of the name
- A stdlib function means one thing on every engine. Each dialect variant implements that meaning, wrapping the engine's function as needed. Where an engine cannot, the function is absent for that engine (a compile error), never different.
- The meaning is written in the function's doc comment and checked by the engine conformance probes. The recurring decisions:
  - Indexes and positions are 1-based.
  - Regex functions search for a match, except functions named `*_full_match`. `regexp_extract_all` defaults to the whole match (group 0).
  - `regexp_extract` returns NULL when the pattern does not match, as Trino, Snowflake, and BigQuery do; a match keeps its text, even an empty one. DuckDB and Hive return `''`, so their variants return the extract only on a match (PR for the 2026-11-23 dbt gap report).
  - `day_of_week` is ISO: 1 = Monday through 7. It already is today.
  - Day and month names are full English names.
  - `diff_*` counts unit boundaries crossed, as the DuckDB, Snowflake, and BigQuery `DATE_DIFF` variants do today. Hive's `months_between`-based variants truncate to complete months and are a follow-up.
  - `url_encode` encodes a space as `%20`. Trino and Spark encode it as `+`, so their variants wrap it.
  - Hex output is lowercase: digests (`md5`, `sha1`, `sha256`) and `to_hex`. `from_hex` accepts either case. Encoding functions work on a string's UTF-8 bytes, and decoders return a string (PR #2325).
  - NULL in gives NULL out unless the doc says otherwise. String concatenation is the existing exception (ADR 2026-11-18-string-concat-null).
  - Stdlib timestamps are UTC wall-clock times: `from_unixtime` reads epoch seconds in UTC, and `to_unixtime` floors to a whole second on every engine (DuckDB and Trino used to round half even).
  - Format patterns for parsing and formatting dates use strftime `%` codes. Variants for engines with Java- or Oracle-style patterns translate them. Today's `date.format(pattern)` passes the pattern through unchanged and is the first follow-up.
- Engine-only capabilities with no shared name (`mad`, `kahan_sum`, `damerau_levenshtein`, `list_grade_up`, `json_merge_patch`, ...) are added only on request, through the same rules.

### First names (tier 1)
- **Aggregates** (column receiver):
  - `y.corr(x)`, `y.covar_pop(x)`, `y.covar_samp(x)`
  - `y.regr_slope(x)`, `y.regr_intercept(x)`, `y.regr_r2(x)`, `y.regr_count(x)`
  - `x.kurtosis` and `x.skewness`, both sample statistics
  - `x.bit_and`, `x.bit_or`, `x.bit_xor`
  - Existing `max_by` / `min_by`; no `arg_max`.
- **Strings:**
  - `s.translate(from, to)`, `s.normalize` (NFC), `s.octet_length`
  - `s.regexp_extract_all(pattern)` / `(pattern, group)`, `s.regexp_split(pattern)`
  - `s.url_encode` / `s.url_decode` (`%20` for a space)
  - `s.hamming_distance(other)`, `s.jaro_winkler_similarity(other)` (0 to 1, case-sensitive)
- **Encoding:** `s.to_base64` / `s.from_base64`, `s.to_hex` / `s.from_hex`, `s.sha1`, and a top-level `uuid` that returns a string.
- **Math:**
  - `x.gcd(y)`, `x.lcm(y)`, `n.factorial`
  - `x.sinh`, `x.cosh`, `x.tanh`, `x.asinh`, `x.acosh`, `x.atanh`, `x.cot`
  - `x.round_half_even(digits)`
- **Date/time:**
  - `d.day_name`, `d.month_name`, `d.iso_year`
  - `s.to_timestamp(format)` and `s.try_to_timestamp(format)`, which returns NULL on failure. These are format overloads of the existing `s.to_timestamp`, with strftime codes; `to_timestamp(s, fmt)` is the name PostgreSQL, Spark, and Snowflake share.
  - `current_date`, `current_timestamp`, written bare: today's date and the current time in UTC on every engine, not the session time zone (PR for the date/time family)
  - `ts.to_time_zone(zone)` and `ts.from_time_zone(zone)` convert a UTC wall-clock time to an IANA zone's and back (user-approved addition, date/time PR). Spark's `from_utc_timestamp` / `to_utc_timestamp` were not adopted: rule 1's `to_*` / `from_*` reads the direction more clearly
- **Casts:**
  - `to_int` / `to_long` on a fractional value truncate toward zero on every engine (2.5 → 2, -2.5 → -2), like Scala/Java `toLong` and Spark. `round` stays the way to round.
  - Numeric `to_boolean` means non-zero is true.

Later families (JSON, lists, maps, structs) add their rows to this ADR in the PR that introduces them, each with a comparison row.

### The engine's own SQL names
- The engine binds every function name that its dialect's stdlib variants expand to, so the stdlib works. Beyond that it adds a bare name only when the naming rules above would pick it, so `.sql` input on the engine reads like portable SQL.
- DuckDB-only aliases are not added.
- `DBType.Wvlet` gets its own `wvlet` dialect context.
  - It inherits the hand-written `in duckdb` stdlib variants (`type wvlet extends duckdb`), because the engine runs their DuckDB SQL.
  - It does not inherit the generated DuckDB catalog (`standard/duckdb/functions.wv`). Engine queries see a generated catalog of the names the engine binds (`EngineFunctions`), so a function the engine lacks fails at compile time with the stdlib name to use, not at run time with "Unsupported function in the engine".
  - The DuckDB connector keeps the full DuckDB catalog.

## Consequences

- Coverage is measured on the stdlib first (`EngineStdlibCoverageCheck`). The DuckDB-catalog count (`EngineFunctionCoverageCheck`) is a compatibility measure, not a target.
- Each new function is a stdlib definition with dialect variants, an engine implementation of what the DuckDB variant expands to, and a conformance probe. The stdlib reference page and the engine catalogs regenerate as usual.
- Existing definitions that break these rules are fixed in follow-ups, not renamed silently:
  - The probe found that the engine's double `to_long` rounds 2.5 to 3, but DuckDB's gives 2. Both change to truncation, as do the variants for engines that round: Trino, Snowflake, BigQuery, DuckDB, and PostgreSQL wrap the cast in `trunc`. The other candidates were half away from zero (Trino/Snowflake/BigQuery and plain `round`) and half even (DuckDB/PostgreSQL). Truncation was chosen to match Scala/Java `toLong`, which Wvlet's users expect, with `round` available when rounding is wanted.
  - `date.format(pattern)` passes engine-specific patterns through.
  - `type_of` returns engine type names (`INTEGER` on DuckDB, `int` on the engine).
- The `wvlet` dialect context lands with the CLI default-engine switch: one PR that adds the context, generates the engine catalog from `EngineFunctions`, and keeps the `in duckdb` variants. Queries that call a DuckDB-only name on the engine then fail at compile time and must use the stdlib name or the DuckDB connector; the release notes list this.
