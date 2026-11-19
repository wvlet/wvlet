/-
Licensed under the Apache License, Version 2.0.

Which refusals of `accepts` are necessary. Each theorem below takes a block holding one later
clause, shows that `accepts` refuses the earlier operator, and gives rows where merging it anyway
would change the result. Rows are natural numbers.

Refusals without a counterexample here are not about rows. Each one keeps the SQL well formed:
- One clause per slot: `limit | limit`, `offset | offset`, `order by | order by`,
  `group by | group by`, two projections, an aggregation under a projection.
- SQL validity: ORDER BY under GROUP BY or SELECT DISTINCT may name a column the block no longer
  outputs.
- HAVING only comes right before its GROUP BY, so it is refused exactly when that GROUP BY would
  be, and refusals that look at a pending HAVING are never reached with another operator.
-/
import WvletFormal.SqlFold.Model

namespace Wvlet.SqlFold.Necessity

open Wvlet.SqlFold

/-- `accepts` refuses `op` under `b`, and merging it would change the rows of `xs`. -/
abbrev Necessary (b : Block Nat) (op : Op Nat) (xs : List Nat) : Prop :=
  accepts b.shape op.kind = false ∧ (b.add op).eval xs ≠ b.eval (op.step xs)

def atLeastOne : Nat → Bool := fun r => decide (1 ≤ r)
/-- An aggregation that counts its input rows. -/
def countRows : List Nat → List Nat := fun l => [l.length]
/-- An aggregation whose result depends on row order. -/
def firstRow : List Nat → List Nat := fun l => l.take 1

/-! ### LIMIT must not cross a later clause in an earlier slot -/

/-- `limit 1 | where r ≥ 1` keeps no row of `[0, 1]`; WHERE before LIMIT keeps `[1]`. -/
theorem limit_where : Necessary { wh := [(atLeastOne, true)] } (.limit 1) [0, 1] := by decide
theorem limit_group : Necessary { grp := some countRows } (.limit 1) [0, 1] := by decide
theorem limit_having : Necessary { hav := [atLeastOne] } (.limit 1) [0, 1] := by decide
theorem limit_distinct : Necessary { dist := true } (.limit 2) [0, 0, 1] := by decide
theorem limit_orderBy : Necessary { ord := some id } (.limit 1) [1, 0] := by decide
/-- `limit 1 | offset 1` is empty, while SQL `LIMIT 1 OFFSET 1` skips first. -/
theorem limit_offset : Necessary { off := some 1 } (.limit 1) [0, 1, 2] := by decide

/-! ### OFFSET likewise; a later LIMIT is fine since SQL applies OFFSET first -/

theorem offset_where : Necessary { wh := [(atLeastOne, true)] } (.offset 1) [0, 1] := by decide
theorem offset_group : Necessary { grp := some countRows } (.offset 1) [0, 1] := by decide
theorem offset_having : Necessary { hav := [atLeastOne] } (.offset 1) [0, 1] := by decide
theorem offset_distinct : Necessary { dist := true } (.offset 1) [0, 0, 1] := by decide
theorem offset_orderBy : Necessary { ord := some id } (.offset 1) [1, 0] := by decide

/-! ### ORDER BY -/

theorem sort_group : Necessary { grp := some firstRow } (.sort id true) [1, 0] := by decide
/-- A key that the later projection overwrites (`order by b | select a as b`). -/
theorem sort_unsafeSelect :
    Necessary { sel := some (fun r => 1 - r) } (.sort id false) [0, 1] := by decide

/-! ### Projections must not cross a later filter they rename, nor a later grouping -/

/-- `select a as b | where b ≥ 1`: WHERE would read the input column. -/
theorem project_unsafeWhere :
    Necessary { wh := [(atLeastOne, false)] } (.project (· + 1)) [0] := by decide
theorem project_group : Necessary { grp := some countRows } (.project (· + 1)) [5] := by
  decide
theorem project_having : Necessary { hav := [atLeastOne] } (.project (· + 1)) [0] := by decide

theorem distinct_unsafeWhere :
    Necessary { wh := [(atLeastOne, false)] } (.distinct (· + 1)) [0] := by decide
theorem distinct_group : Necessary { grp := some countRows } (.distinct (· + 1)) [5] := by
  decide

/-! ### Duplicate elimination -/

/-- `dedup | select a`: the later projection must not become `select distinct a`. -/
theorem dedup_select : Necessary { sel := some (fun _ => 0) } .dedup [0, 1] := by decide
theorem dedup_group : Necessary { grp := some countRows } .dedup [0, 0] := by decide

/-! ### Grouping and aggregation must not cross a later filter (it becomes HAVING or nests) -/

theorem group_where : Necessary { wh := [(atLeastOne, true)] } (.group countRows) [0] := by
  decide
theorem agg_where : Necessary { wh := [(atLeastOne, true)] } (.agg countRows) [0] := by decide

end Wvlet.SqlFold.Necessity
