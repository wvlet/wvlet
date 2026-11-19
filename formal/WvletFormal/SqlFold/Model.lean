/-
Licensed under the Apache License, Version 2.0.

A model of how `SqlGenerator` folds a pipeline of relational operators into SQL SELECT blocks.

Rows are an abstract type `R`; predicates, projections, sort keys, and aggregations are opaque
functions. A pipeline runs its operators one after another over a list of rows. A SELECT block
evaluates its clauses in SQL order: WHERE, GROUP BY (with its aggregation), HAVING, SELECT,
DISTINCT, ORDER BY, OFFSET, LIMIT. ORDER BY sees the projected row, as SQL resolves output names
first.

`fold` mirrors `SqlGenerator.relation`: it walks the pipeline from the last operator to the first,
merges an operator into the current block when `accepts` allows it, and otherwise opens a nested
block that becomes the current block's FROM source. The decision reads only the block's `Shape`
(which clauses it holds) and the operator's `Kind`, which is what the Scala code can observe.

Alias scope is the one property the decision cannot see in the functions themselves: a filter or
sort key may name a column that a projection computes or renames. The `safe` flag on a filter or
sort says it names no such column of the projection it would cross; the generator derives it
from column names. `Scoped` states that every flag the fold relies on is truthful.
-/

namespace Wvlet.SqlFold

/-- Keep the first occurrence of each row; `seen` holds the rows already emitted. -/
def dedupFrom {R : Type} [DecidableEq R] (seen : List R) : List R → List R
  | [] => []
  | x :: xs => if x ∈ seen then dedupFrom seen xs else x :: dedupFrom (x :: seen) xs

/-- Duplicate elimination (`dedup`, `select distinct`), keeping first occurrences. -/
def dedup {R : Type} [DecidableEq R] (xs : List R) : List R := dedupFrom [] xs

/-- Insert into a list sorted by `k`, before the first row whose key is not smaller. -/
def insertBy {R : Type} (k : R → Nat) (x : R) : List R → List R
  | [] => [x]
  | y :: ys => if k x ≤ k y then x :: y :: ys else y :: insertBy k x ys

/-- Sort by a key (insertion sort). -/
def sortBy {R : Type} (k : R → Nat) : List R → List R
  | [] => []
  | x :: xs => insertBy k x (sortBy k xs)

/-- A pipeline operator. -/
inductive Op (R : Type) where
  /-- `where p`; `safe`: `p` names no column the nearest earlier projection computes or renames. -/
  | filter (p : R → Bool) (safe : Bool)
  /-- A filter directly over a bare `group by` (rendered as HAVING). -/
  | having (p : R → Bool)
  /-- `select` and the other column-level operators. -/
  | project (f : R → R)
  /-- `select distinct`. -/
  | distinct (f : R → R)
  /-- `dedup`: duplicate elimination over whole rows. -/
  | dedup
  /-- A bare `group by` without its own select list. -/
  | group (g : List R → List R)
  /-- An aggregation together with its select list: `group by … agg`, a global `agg`, `count`. -/
  | agg (g : List R → List R)
  /-- `order by`; `safe`: the key names no column the nearest later projection computes or
  renames. -/
  | sort (k : R → Nat) (safe : Bool)
  | limit (n : Nat)
  | offset (n : Nat)

/-- What the folding decision observes of an operator. -/
inductive Kind where
  | filter (safe : Bool)
  | having
  | project
  | distinct
  | dedup
  | group
  | agg
  | sort (safe : Bool)
  | limit
  | offset
  deriving DecidableEq, Repr

def Op.kind {R : Type} : Op R → Kind
  | .filter _ s => .filter s
  | .having _ => .having
  | .project _ => .project
  | .distinct _ => .distinct
  | .dedup => .dedup
  | .group _ => .group
  | .agg _ => .agg
  | .sort _ s => .sort s
  | .limit _ => .limit
  | .offset _ => .offset

/-- Run one operator. -/
def Op.step {R : Type} [DecidableEq R] : Op R → List R → List R
  | .filter p _, xs => xs.filter p
  | .having p, xs => xs.filter p
  | .project f, xs => xs.map f
  | .distinct f, xs => SqlFold.dedup (xs.map f)
  | .dedup, xs => SqlFold.dedup xs
  | .group g, xs => g xs
  | .agg g, xs => g xs
  | .sort k _, xs => sortBy k xs
  | .limit n, xs => xs.take n
  | .offset n, xs => xs.drop n

/-- Run a pipeline, first operator first. -/
def run {R : Type} [DecidableEq R] (ops : List (Op R)) (xs : List R) : List R :=
  ops.foldl (fun acc op => op.step acc) xs

/-- The clauses of one SELECT block. -/
structure Block (R : Type) where
  /-- WHERE conjuncts with their scope flags. -/
  wh : List ((R → Bool) × Bool) := []
  grp : Option (List R → List R) := none
  /-- The grouping carries its own select list (`agg`), so the SELECT slot is taken. -/
  aggSel : Bool := false
  hav : List (R → Bool) := []
  sel : Option (R → R) := none
  dist : Bool := false
  ord : Option (R → Nat) := none
  off : Option Nat := none
  lim : Option Nat := none

/-- Which clauses a block holds, as `SQLBlock` exposes them. -/
structure Shape where
  where_ : Bool := false
  /-- Every WHERE conjunct is scope-safe. -/
  whereSafe : Bool := true
  group : Bool := false
  having : Bool := false
  select : Bool := false
  distinct : Bool := false
  orderBy : Bool := false
  offset : Bool := false
  limit : Bool := false
  deriving DecidableEq, Repr

def allWhere {R : Type} : List ((R → Bool) × Bool) → R → Bool
  | [], _ => true
  | q :: qs, r => q.1 r && allWhere qs r

def allSafe {R : Type} : List ((R → Bool) × Bool) → Bool
  | [] => true
  | q :: qs => q.2 && allSafe qs

def allHaving {R : Type} : List (R → Bool) → R → Bool
  | [], _ => true
  | p :: ps, r => p r && allHaving ps r

def Block.shape {R : Type} (b : Block R) : Shape where
  where_ := !b.wh.isEmpty
  whereSafe := allSafe b.wh
  group := b.grp.isSome
  having := !b.hav.isEmpty
  select := b.sel.isSome || b.aggSel
  distinct := b.dist
  orderBy := b.ord.isSome
  offset := b.off.isSome
  limit := b.lim.isSome

/-- The clauses after SELECT: DISTINCT, ORDER BY, OFFSET, LIMIT. -/
def Block.finish {R : Type} [DecidableEq R] (b : Block R) (zs : List R) : List R :=
  let z1 := if b.dist then dedup zs else zs
  let z2 := match b.ord with
    | none => z1
    | some k => sortBy k z1
  let z3 := match b.off with
    | none => z2
    | some n => z2.drop n
  match b.lim with
  | none => z3
  | some n => z3.take n

/-- The clauses up to SELECT: WHERE, GROUP BY, HAVING, SELECT. -/
def Block.select {R : Type} (b : Block R) (xs : List R) : List R :=
  let x1 := xs.filter (allWhere b.wh)
  let x2 := match b.grp with
    | none => x1
    | some g => g x1
  let x3 := x2.filter (allHaving b.hav)
  match b.sel with
  | none => x3
  | some f => x3.map f

/-- Evaluate one SELECT block over its FROM source. -/
def Block.eval {R : Type} [DecidableEq R] (b : Block R) (xs : List R) : List R :=
  b.finish (b.select xs)

/-- Evaluate nested blocks, outermost first; the last block reads the source. -/
def evalBlocks {R : Type} [DecidableEq R] (bs : List (Block R)) (xs : List R) : List R :=
  bs.foldr (fun b acc => b.eval acc) xs

/-- Put an operator's clause into a block. -/
def Block.add {R : Type} (b : Block R) : Op R → Block R
  | .filter p s => { b with wh := (p, s) :: b.wh }
  | .having p => { b with hav := p :: b.hav }
  | .project f => { b with sel := some f }
  | .distinct f => { b with sel := some f, dist := true }
  | .dedup => { b with dist := true }
  | .group g => { b with grp := some g }
  | .agg g => { b with grp := some g, aggSel := true }
  | .sort k _ => { b with ord := some k }
  | .limit n => { b with lim := some n }
  | .offset n => { b with off := some n }

def Shape.add (s : Shape) : Kind → Shape
  | .filter safe => { s with where_ := true, whereSafe := safe && s.whereSafe }
  | .having => { s with having := true }
  | .project => { s with select := true }
  | .distinct => { s with select := true, distinct := true }
  | .dedup => { s with distinct := true }
  | .group => { s with group := true }
  | .agg => { s with group := true, select := true }
  | .sort _ => { s with orderBy := true }
  | .limit => { s with limit := true }
  | .offset => { s with offset := true }

/--
May an operator join a block whose clauses all come from operators applied after it?

It may when none of those later clauses sits in an earlier SQL slot, unless the two commute:
filters commute with projections of scope-safe predicates, with sorting, and with duplicate
elimination; LIMIT and OFFSET commute with a projection; OFFSET runs before LIMIT in SQL.
-/
def accepts (s : Shape) : Kind → Bool
  | .filter _ => true
  | .having => !s.where_ && !s.group
  | .group => !s.where_ && !s.group
  | .agg => !s.where_ && !s.group && !s.select
  | .project => !s.select && !s.group && !s.having && (!s.where_ || s.whereSafe)
  | .distinct => !s.select && !s.distinct && !s.group && !s.having && (!s.where_ || s.whereSafe)
  | .dedup => !s.select && !s.distinct && !s.group && !s.having
  | .sort safe => !s.orderBy && !s.group && !s.distinct && (!s.select || safe)
  | .limit =>
    !s.where_ && !s.group && !s.having && !s.distinct && !s.orderBy && !s.offset && !s.limit
  | .offset => !s.where_ && !s.group && !s.having && !s.distinct && !s.orderBy && !s.offset

/--
Fold operators, given last operator first, into nested blocks, outermost first. A refused
operator opens a fresh block, which becomes the FROM source of the current one.
-/
def fold {R : Type} (b : Block R) : List (Op R) → List (Block R)
  | [] => [b]
  | op :: rest =>
    if accepts b.shape op.kind then fold (b.add op) rest
    else b :: fold (({} : Block R).add op) rest

/-- The same fold over shapes and kinds, which the conformance traces export. -/
def foldShapes (s : Shape) : List Kind → List Shape
  | [] => [s]
  | k :: rest =>
    if accepts s k then foldShapes (s.add k) rest
    else s :: foldShapes (({} : Shape).add k) rest

/-- The scope flags a merge relies on are truthful. -/
def Block.scopeOk {R : Type} (b : Block R) : Op R → Prop
  | .project f | .distinct f =>
    ∀ q ∈ b.wh, q.2 = true → ∀ r, q.1 (f r) = q.1 r
  | .sort k true => ∀ f, b.sel = some f → ∀ r, k (f r) = k r
  | _ => True

/-- Every merge that `fold` makes relies only on truthful scope flags. -/
def Scoped {R : Type} : Block R → List (Op R) → Prop
  | _, [] => True
  | b, op :: rest =>
    if accepts b.shape op.kind then b.scopeOk op ∧ Scoped (b.add op) rest
    else Scoped (({} : Block R).add op) rest

end Wvlet.SqlFold
