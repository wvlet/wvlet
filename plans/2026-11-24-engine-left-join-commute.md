# Engine: move a LEFT join above the inner joins of its preserved input

## Goals

- Second PR for the outer-join gap (after #2390): in TPC-DS q80 and q40 the engine runs `store_sales ⟕ store_returns` (`catalog_sales ⟕ catalog_returns`) on the whole fact table before the selective date/item/promotion joins; DuckDB joins the dimensions first (store_sales → 33K rows at SF10) and then builds the outer join on the small side.

## Background

- The binder binds `from ss left join sr on ..., date_dim, item, ... where ...` as an inner join tree whose input is the LEFT join; `ReorderJoins` orders the tree but treats the LEFT join as an opaque input.
- `(A ⟕ₚ B) ⋈_q C = (A ⋈_q C) ⟕ₚ B` (as bags) when `q` reads no column of `B`; `p` keeps reading `A` and `B` only.
- `ReorderJoins` already flips an outer join's build side by estimated rows (`swap_build_side`, ratio 4), so a LEFT join over a small joined tree builds on the tree (DuckDB's LEFT→RIGHT flip).
- A tree condition over `B` that rejects NULLs already made the join inner in #2390 (`outer_to_inner`); one that doesn't (q78's `sr_ticket_number is null`) blocks the move.

## Design

Lane: Standard (join planning, Lean model required); direction pre-approved by the work item; merge authorized by the user via the coordinator once CI is green and the SF10 suite totals show no regression.

- Lean model first: `WvletFormal/LeftJoinCommute` proves the identity as a permutation (the two plans list rows in different orders), for inputs whose rows are NULL outside their columns, and exports traces over the pushdown fixture's tables plus `t(e)`.
- Engine, behind a new toggle `commute_left_joins`, in `ReorderJoins.order`: an input that is a LEFT join whose right input no tree condition reads is replaced by its left input; the tree is ordered (recursively, for further LEFT joins), and the LEFT join joins on top with its build side chosen over the tree's estimated rows.
- No cost guard: the move is taken whenever it is legal. Inner joins of a star schema reduce the fact side, and the SF10 A/B on the suites checks for regressions.
- The tree's output columns put `B` last, which `ReorderJoins` already allows (its trees output the node's columns in join order).
- Tests: the conformance test replays the fixture through the full optimizer (placement and rows); golden plans change for q40 and q80; SF10 A/B on q80/q40 and on the full TPC-DS suite.

## Alternatives and Why Not?

- **Full DuckDB-style conflict rules inside the DP enumerator.** Larger change to `JoinOrderOptimizer`; placing the LEFT join on top covers the TPC shapes (a fact table with its returns, then dimensions).
- **Do it in PushDownFilters.** That pass must keep each node's output columns in order; `ReorderJoins` already may reorder them.
