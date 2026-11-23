# M5: Carry filters across join-key equalities

## Goals

- At SF1, TPC-DS q78 runs 11x slower than DuckDB (engine 444 ms vs 40 ms), the largest absolute gap in the suite. The goal is to bring it well below that.
- A filter over the join keys of one join input should also filter the other input when that can't change the result, and a filter over one input of an inner join should reach that input.

## Background

- q78 filters `ss_sold_year = 2000` above `ss left join ws on ws_sold_year = ss_sold_year ...` (and the same for `cs`). Each of `ss`, `ws`, and `cs` aggregates `sales × date_dim` by `d_year`.
- In the runtime plan before `PushDownFilters`, the aggregation inputs are already `Join[Inner]` nodes: the binder optimizes and orders an aggregation's input while binding it.
- `PushDownFilters` pushes a conjunct into the preserved side of a LEFT/RIGHT join only. It stops at an ordered `Join[Inner]`; only an `InnerJoins` graph takes conditions.
- So the year filter reaches the `ss` aggregation and stops above `store_sales × date_dim`. The runtime join filter on `ss_sold_date_sk` then gets every `date_dim` key.
- `ws` and `cs` get no year filter. They aggregate every year, and their large builds are copied on probe (`LongVec.gather` is 24% of engine CPU).
- `InnerJoins` already infers `b = k` from `a = b` and `a = k` (same type) for the conditions that `ReorderJoins` places.

## Design

- Lane: Express. An internal optimizer change behind a new toggle, with no SQL or API change. The launch prompt says not to merge, so the PR stays open for the user.
- New rule in `PushDownFilters`, under the toggle `join_key_filters`.
- **Carry across keys.** A conjunct that moves into the left input of a LEFT, semi, or anti join, and reads only left key columns, is also pushed into the right input. Its key columns are replaced by their right key partners. RIGHT joins are handled the same way with the sides swapped.
  - This is sound: a surviving left row makes the conjunct TRUE, and a right row matches it only on equal non-NULL keys, so the copy over the right row is TRUE as well. A right row that fails the copy (FALSE or NULL) can only match left rows that the conjunct drops anyway.
  - The rule works for any deterministic conjunct over key columns (`= 2000`, `IN`, ranges, `IS NULL`), not only `= constant`.
  - It applies only to key pairs that are plain columns of the same type, and not to float/double keys, where key equality and `=` can disagree (`-0.0`, NaN).
- **Ordered inner joins.** A conjunct over one input of an ordered `Join[Inner]` / cross join goes into that input. A conjunct over the key columns of one input is also carried to the other input, as above.
- For q78, `ws_sold_year = 2000` then passes through the two projections and the `HashAgg` group key (`d_year`) to the `date_dim` scan of `ws`. The `ss` filter reaches its `date_dim` scan too.
- **Lean model.** A new `formal/WvletFormal/JoinKeyFilter/` model of bag joins with 3VL predicates (inner, left, right, semi, anti) proves that filtering the other input by the carried conjunct keeps the filtered join's rows. `lake exe join-key-filter-traces` exports cases. A conformance test runs each case as an engine query with the toggle on and off, and checks the rows and where the copy lands.
- Golden plans are regenerated, and any plan that changes is listed in the PR. TPC-DS 99/99 conformance passes. SF1 A/B through the toggle, with unchanged-plan queries as controls. An ADR records the rule.

## Alternatives and Why Not?

- **Only `a = constant`, as the item first proposed.** The soundness argument is the same for every conjunct over the keys, and the general rule costs no extra code.
- **Turn ordered inner joins back into `InnerJoins` graphs so `ReorderJoins` places the filter.** It would re-order joins that the binder ordered, and widens the change to join ordering.
- **Carry FULL-join filters.** Today no conjunct passes through a FULL join at all, and no benchmark plan needs it.
