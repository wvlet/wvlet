/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the name-lookup model, exported as JSON so that a Scala test
(`NameLookupConformanceTest`) can replay them against the shipped `Scope`, `GlobalSymbolIndex`,
`Context.findSymbolByName`, `TyperRules.identifierRules`, and `RelationRefResolver.resolveTableRef`.
-/
import WvletFormal.NameLookup.Resolve

namespace Wvlet.NameLookup.Traces

open Wvlet.NameLookup

private def jstr (s : String) : String := "\"" ++ s ++ "\""
private def jbool (b : Bool) : String := if b then "true" else "false"
private def jnum (n : Nat) : String := toString n
private def jopt (o : Option Nat) : String := (o.map jnum).getD "null"
private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

/-! ## Scope programs -/

/-- Names used in scope programs. -/
def scopeNames : List Name := [0, 1]

/-- At most this many scope handles. -/
def maxHandles : Nat := 3

def opStr : Op → String
  | .child h => s!"child {h}"
  | .fresh => "fresh"
  | .add h n => s!"add {h} {nameStr n}"
  | .enter h n => s!"enter {h} {nameStr n}"

/-- The operations available with `c` handles, and the handle count after each. -/
def opsFor (c : Nat) : List (Op × Nat) :=
  let create :=
    if c < maxHandles then (List.range c).map (fun h => (Op.child h, c + 1)) ++ [(Op.fresh, c + 1)]
    else []
  let bind := (List.range c).flatMap fun h =>
    scopeNames.flatMap fun n => [(Op.add h n, c), (Op.enter h n, c)]
  create ++ bind

/-- Every program of exactly `d` operations, starting with `c` handles. -/
def programsOf : Nat → Nat → List (List Op)
  | 0, _ => [[]]
  | d + 1, c => (opsFor c).flatMap fun (op, c') => (programsOf d c').map (op :: ·)

/-- Every program of one to three operations, starting from the root scope. -/
def programs : List (List Op) := [1, 2, 3].flatMap fun d => programsOf d 1

def scopeCase (ops : List Op) : String :=
  let st := run ops
  let table := st.scopes.map fun s => jarr (scopeNames.map fun n => jopt (s.lookup n))
  jobj [("ops", jarr (ops.map (jstr ∘ opStr))), ("entered", jarr (st.entered.map jnum)),
    ("lookup", jarr table)]

/-! ## Global lookup -/

def pkgStr : Option Pkg → String
  | none => "null"
  | some p => jstr (".".intercalate p)

/-- Unit shapes: two basenames, the preset / default / `p` / `q.r` buckets, three definition lists. -/
def unitDomain : List Unit :=
  [0, 1].flatMap fun file =>
    [none, some [], some ["p"], some ["q", "r"]].flatMap fun pkg =>
      [[0], [1], [0, 0]].map fun defs => { file, pkg, defs }

/-- One unit, or two units where the second defines `n0` once (both orders occur). -/
def unitSets : List (List Unit) :=
  unitDomain.map ([·]) ++
    unitDomain.flatMap fun a => (unitDomain.filter (·.defs == [0])).map fun b => [a, b]

/-- The unit's package and its imports (newest first). -/
def contexts : List (Pkg × List Pkg) :=
  [([], []), ([], [["q"]]), ([], [["q", "r"]]), (["p"], []), ([], [["s", "n0"]])]

def unitJson (u : Unit) : String :=
  jarr [jstr s!"f{u.file}.wv", pkgStr u.pkg, jarr (u.defs.map (jstr ∘ nameStr))]

def entryStr (e : GEntry) : String := jstr s!"{e.unit}.{e.idx}"

def bindingStr : Binding → String
  | .local x => jstr s!"local {x}"
  | .global e => entryStr e

/-- The symbol id of the local binding in the global cases that have one. -/
def localSym : Sym := 99

def globalCase (units : List Unit) (cur : Pkg) (imports : List Pkg) (isLocal : Bool) : String :=
  let scope := if isLocal then Scope.empty.add 0 localSym else Scope.empty
  let env : Env := { scope, units, pkg := cur, imports }
  jobj [("units", jarr (units.map unitJson)), ("package", jstr (".".intercalate cur)),
    ("imports", jarr (imports.map fun r => jstr (".".intercalate r))),
    ("local", jbool isLocal),
    ("visible", jarr ((visibleEntries units 0 cur imports).map entryStr)),
    ("found", jarr ((findSymbol env 0).map bindingStr))]

def globalCases : List String :=
  (unitSets.flatMap fun us => contexts.map fun (cur, imports) => globalCase us cur imports false) ++
  (unitDomain.map fun u => globalCase [u] [] [] true)

/-! ## Identifiers and relation names -/

def kindStr : Kind → String
  | .function => "function"
  | .value => "value"
  | .tableValue => "tableValue"
  | .model => "model"
  | .alias => "alias"
  | .other => "other"

def kinds : List Kind := [.function, .value, .tableValue, .model, .alias, .other]

def infos : List (Option Info) :=
  none :: kinds.flatMap fun kind => [false, true].map fun completing => some { kind, completing }

def infoFields : Option Info → List (String × String)
  | none => [("symbol", "null")]
  | some i => [("symbol", jstr (kindStr i.kind)), ("completing", jbool i.completing)]

def identStr : IdentResult Info → String
  | .column => jstr "column"
  | .symbol _ => jstr "symbol"
  | .unresolved => jstr "unresolved"

def identifierCases : List String :=
  infos.flatMap fun found => [false, true].map fun column =>
    jobj (infoFields found ++ [("column", jbool column),
      ("result", identStr (identify found id column))])

def relStr : RelResult → String
  | .localRelation => "local"
  | .keep => "keep"
  | .modelScan => "modelScan"
  | .values => "values"
  | .aliasRelation => "alias"
  | .table => "table"

/-- Local relation names live in `Typer.prepare`, outside the replayed resolver. -/
def relationCases : List String :=
  infos.map fun found => jobj (infoFields found ++ [("result", jstr (relStr (resolveRelation false found)))])

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe name-lookup-traces (do not edit by hand)\",\n" ++
  "  \"localSymbol\": " ++ jnum localSym ++ ",\n" ++
  "  \"scopes\": " ++ jarrayLines (programs.map scopeCase) ++ ",\n" ++
  "  \"globals\": " ++ jarrayLines globalCases ++ ",\n" ++
  "  \"identifiers\": " ++ jarrayLines identifierCases ++ ",\n" ++
  "  \"relations\": " ++ jarrayLines relationCases ++ "\n" ++
  "}\n"

end Wvlet.NameLookup.Traces
