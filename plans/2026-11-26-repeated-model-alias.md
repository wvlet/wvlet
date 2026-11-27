# Repeated model references without a duplicate alias

## Goals

- `from m join m on true` and `from m, m` run again on DuckDB, Trino, and the wvlet engine, as they did before #2532.
- Model-name qualifiers from #2532 (`stg_orders.customer_id` after a join) keep working.

## Background

- #2532 wraps every inlined, unaliased model body in `AliasedRelation(body, <model name>)` inside `GenSQL.expandRelation`, so the model name works as a column qualifier like a table name.
- A model referenced twice in one join tree now gets the same alias twice, `(...) as m join (...) as m`, which DuckDB and Trino reject. Before #2532 both subqueries were unnamed, so the query ran.
- SQL generation and the wvlet engine both consume this expansion, so one fix in `expandRelation` covers both.

## Design

- Lane: Express. The diagnosis is clear, the change is a small non-UI fix in one function, and it touches no always-gate area.
- Scope boundary: one join tree, meaning a `Join` and its nested `Join` children down to the leaves. A FROM list `from a, b` is a join tree of implicit joins. Subqueries and other operators below a leg start their own scope.
- Within a scope, only the first unaliased reference to a model gets the model name as its alias. Later unaliased references to the same model stay unnamed, as before #2532.
- An unaliased model whose name is already used as a user alias in the same scope also stays unnamed, e.g. `from m as m join m`.
- User aliases are never changed. A self-join that needs both sides qualified still uses explicit aliases (`from m as a join m as b`).
- Tests: a spec/basic case with a self-joined model on DuckDB and the engine, and ModelNameQualifierTest cases for `join` and the comma form.

## Alternatives and Why Not?

- Generate unique aliases (`m`, `m_2`): a user could start depending on an invented name. Unnamed repeats match the pre-#2532 behavior.
- Drop the model-name alias whenever a model repeats: this would break `m.x` for the first reference, which is unambiguous in SQL terms anyway.
