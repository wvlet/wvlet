# Stdlib date/time functions and time zones on the engine

## Goals
- Add the ADR's tier-1 date/time names (`adr/2026-11-23-stdlib-function-naming.md`): `d.day_name`, `d.month_name`, `d.iso_year`, `s.to_timestamp(format)`, `s.try_to_timestamp(format)`, `current_date`, `current_timestamp`.
- Give the engine the time primitives that a later TD time compatibility layer needs: `AT TIME ZONE`, timestamps with offsets, and epoch conversions with fractional seconds.
- Fix the engine's 3-argument `date_sub`, which it binds Trino-style although DuckDB's `date_sub(part, start, end)` is a difference.

## Background
- The engine has one timestamp type: UTC microseconds with no zone. DuckDB's `now()`, `to_timestamp(double)`, `strptime` with `%z`, and `AT TIME ZONE` produce or consume `TIMESTAMP WITH TIME ZONE` (TIMESTAMPTZ) instead.
- DuckDB's TIMESTAMPTZ results depend on the session time zone, which defaults to the machine's zone (the conformance JDBC connection runs in America/Los_Angeles).
- The engine's binder drops `AT TIME ZONE`, so `ts AT TIME ZONE 'Asia/Tokyo'` returns `ts` unchanged.
- Casting `'2024-01-01 10:00:00+09'` to a plain TIMESTAMP drops the offset in DuckDB 2.0.1 too, so the engine's `parseTimestamp` already matches DuckDB there. The offset matters only for TIMESTAMPTZ.
- The engine's `epoch(ts)` returns whole seconds, but DuckDB's returns a DOUBLE with the fraction. The stdlib's `to_unixtime` casts that DOUBLE to bigint, so it rounds half even (`-0.5` → 0) instead of flooring.
- The compiler inlines a bare top-level stdlib name only for `= native` defs, so `uuid` needs `()` today. `current_date` is also a plain identifier to the parser, so it fails on the engine with "Column not found".
- The TD time functions (`TD_TIME_ADD` and the rest) are out of scope and have their own item.

## Design
- Lane: Standard. The names come from the approved ADR, and the user listed the prerequisites and the `date_sub` fix. The user decided the three points that changed a meaning or added names (Decisions below).
- **Time zones on the engine:**
  - Add a TIMESTAMPTZ type to the engine. It is stored like TIMESTAMP, as UTC microseconds, but its type is kept so `AT TIME ZONE` knows which direction to convert.
  - The engine's session time zone is UTC. TIMESTAMPTZ values print with `+00`, as DuckDB's do under `SET TimeZone='UTC'`. The conformance tests set DuckDB to UTC.
  - `ts AT TIME ZONE z` follows DuckDB: a plain timestamp is read as wall-clock time in `z` and becomes an instant, and an instant becomes the wall-clock time in `z`. Times that fall in a DST gap or overlap resolve as DuckDB's do.
  - Zone rules: on the JVM, `java.time` with the full IANA database; on Scala.js, the platform's `Intl`; on Scala Native, UTC only (other zones report that they are unsupported, as `normalize` does for non-ASCII text there).
  - Casting a string to TIMESTAMPTZ applies its `+09`, `+09:30`, `Z`, or zone-name suffix. Casting to plain TIMESTAMP keeps dropping it, as DuckDB does.
  - `now()`, `current_timestamp`, `to_timestamp(double)`, and `strptime` with `%z` return TIMESTAMPTZ, as in DuckDB.
- **Epochs:**
  - The engine's `epoch` returns a DOUBLE with the fraction, as DuckDB's does. No new bare engine names: each engine name is one a stdlib variant expands to.
  - `x.from_unixtime` also accepts a double, keeping fractional seconds to the microsecond: DuckDB `make_timestamp(cast(x * 1000000 as bigint))`, BigQuery `timestamp_micros(...)`, Snowflake `to_timestamp_ntz(x)`, Trino `from_unixtime(x)` read in UTC. Hive is left out.
- **New stdlib functions:**
  - `d.day_name` / `d.month_name`: full English names, on dates and timestamps.
  - `d.iso_year`: the ISO-8601 week-numbering year.
  - `s.to_timestamp(format)` / `s.try_to_timestamp(format)` use strftime codes (`%Y %m %d %H %M %S %f %y %b %B %a %A %j %p %I`). The pattern must match the whole string, and the try form returns NULL where the plain form fails. Variants: DuckDB `strptime` / `try_strptime`; Trino `date_parse` (the codes that differ, `%M` `%B` `%A`, are rewritten to MySQL's) and `try(date_parse(...))`; BigQuery `parse_timestamp` / `safe.parse_timestamp`. Snowflake (format models, not `%` codes) and Hive are left out. A pattern with `%z` is not part of the stdlib meaning, because the result would depend on the session time zone.
  - `current_date` / `current_timestamp` return a plain DATE / TIMESTAMP (in UTC). In a `.wv` query the bare name resolves to the stdlib def. The DuckDB variant is `now() AT TIME ZONE 'UTC'`, so the engine binds `now()` (TIMESTAMPTZ) and `AT TIME ZONE`. SQL input's own `current_timestamp` keeps the engine's existing meaning.
  - They work bare (no `()`). Name lookup does not change: the identifier already resolves to the def by the precedence in `adr/2026-11-18-name-lookup-precedence.md`, and a column still shadows it. What changes is the expansion: the compiler inlines a resolved zero-argument def's body, as it does today only for `= native` defs. This covers user defs too and makes `uuid` work bare. The Lean model covers lookup, not inlining, so it does not change.
- **`date_sub` fix:** the engine's 3-argument `date_sub(part, start, end)` returns the number of complete parts between the two values (month ends included), as DuckDB does. The Trino-style subtraction it replaces is not Trino SQL either: Trino has no `date_sub`.
- Tests compare every function against DuckDB through duckdb_jdbc 2.0.1 (in UTC): DuckDB is the reference because the engine runs the DuckDB variants. No Trino is available locally, so the Trino, BigQuery, and Snowflake variants are compiled but not run. The tests cover NULLs, pre-1970 fractions, month-end and leap-day differences, DST gaps and overlaps, and strptime failures. Shared unit tests run on Scala.js.

## Decisions (user, 2026-11-23)
- `ts.to_unixtime` floors on every engine, in this PR.
- `current_date` / `current_timestamp` are UTC on every engine.
- `ts.to_time_zone(zone)` / `ts.from_time_zone(zone)` are added in this PR, with an ADR row.

## Alternatives and Why Not?
- Keep one timestamp type and treat `AT TIME ZONE` as a shift: without the type, `ts AT TIME ZONE z` cannot tell which way to convert, and it would differ from DuckDB in one of the two directions.
- Use the machine's zone as the engine's session zone, as DuckDB does: query results would then depend on where they run.
- Require `current_date()` with parentheses: SQL and every engine spell it bare, and the ADR lists it bare.
- Make `to_unixtime` return a double: that changes an existing function's type. Fractional seconds come in through `from_unixtime` on a double.
- Spark's `from_utc_timestamp` / `to_utc_timestamp` for the time zone pair: the names read backwards to most users, and ADR rule 1 (`to_*` / `from_*`) gives clearer ones.
