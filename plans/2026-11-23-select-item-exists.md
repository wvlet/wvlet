# EXISTS as a select item

## Goals

- Let `exists { ... }` (Wvlet) and `exists (select ...)` (SQL) appear as select-item expressions, so a query can return the EXISTS flag as a boolean column
- Replay the Lean decorrelation model's mark flags (`markJoin`) as select items against the engine and DuckDB, closing the P3 follow-up recorded under M5 PR 12

## Background

- Both parsers already parse EXISTS as a primary expression (WHERE, ON, CASE); only the select-item lists stopped at a reserved keyword before trying an expression, so `select exists ...` and `select not exists ...` ended the item list
- The engine already decorrelates a correlated EXISTS in a select item through `decorrelateExists` (the select-item subquery rewrite of PR 16, #2342), as a left join with the distinct correlation keys
- `markJoin_sound` proves the mark join keeps every outer row once with SQL's EXISTS as its flag (never unknown), so the flags are the expected select-item values

## Design

- Lane: Express — a parser fix plus tests, with merge authorized in the launch prompt; no engine code changes
- `EXISTS` and `NOT` may start a select item in both parsers (`expressionStartKeywords`), next to the literal-start keywords
- `Exists` gets `BooleanType` at construction, like the other boolean expressions, so the typer resolves a select list holding it
- The Decorrelate traces add each mark case's outer rows with their flag; `DecorrelateConformanceTest` checks `select a, b, exists {...} as e` and `not exists {...}` against them on the engine and DuckDB, with the mark's left join
- Spec files in spec/basic and spec/sql/basic cover correlated, NOT, and uncorrelated EXISTS select items

## Alternatives and Why Not?

- Add EXISTS to `literalStartKeywords`: that list also feeds the keyword tables and names literal starts, which EXISTS is not
