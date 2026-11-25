/-
Licensed under the Apache License, Version 2.0.

Name resolution in the Typer:

- `findSymbol` is `Context.findSymbolByName`: the scope, then the visible top-level definitions, then
  (after an import of the bare name) a definition in any package.
- `identify` is `TyperRules.identifierRules`: a bare identifier denotes the symbol `findSymbol`
  returns, except that an input column shadows a function definition that is not completing.
- `resolveRelation` is `RelationRefResolver.resolveTableRef` behind the CTE and flow-stage names of
  `Typer.prepare`: what `from x` denotes.
-/
import WvletFormal.NameLookup.Global

namespace Wvlet.NameLookup

/-- What a bare name is looked up in. -/
structure Env where
  scope : Scope
  units : List Unit
  /-- The package of the unit being typed. -/
  pkg : Pkg
  /-- Import references, as `Context.importDefs` lists them (newest first). -/
  imports : List Pkg

/-- A resolved symbol: a scope binding or a top-level definition. -/
inductive Binding where
  | local (x : Sym)
  | global (e : GEntry)
deriving Repr, DecidableEq

/-- The fixture spelling of a name. -/
def nameStr (n : Name) : String := "n" ++ toString n

/-- An `import` whose last segment is the name (`Import.importRef.leafName == name`). -/
def importsByName (imports : List Pkg) (n : Name) : Bool :=
  imports.any fun r => r.getLast? == some (nameStr n)

/--
`Context.findSymbolByName`, as the list of symbols it may return: empty when it returns `None`, a
single symbol for the scope and the visible definitions, and every candidate of `findFirstAnywhere`
for the import fallback (it picks one of them in hash-map order).
-/
def findSymbol (env : Env) (n : Name) : List Binding :=
  match env.scope.lookup n with
  | some x => [.local x]
  | none =>
    match visibleEntries env.units n env.pkg env.imports with
    | w :: _ => [.global w]
    | [] =>
      if importsByName env.imports n then (anywhereCandidates env.units n).map .global else []

/-- A local binding wins over every global definition and import. -/
theorem findSymbol_local (env : Env) (n : Name) (x : Sym) (h : env.scope.lookup n = some x) :
    findSymbol env n = [.local x] := by
  simp [findSymbol, h]

/--
Re-running lookup after a new local binding (the multi-pass comment in `identifierRules`): the next
lookup returns the new binding, whatever the global index and the imports hold.
-/
theorem findSymbol_add (env : Env) (n : Name) (x : Sym) :
    findSymbol { env with scope := env.scope.add n x } n = [.local x] :=
  findSymbol_local _ _ _ (Scope.lookup_add_self _ _ _)

/-- A new local binding changes the lookup of no other name. -/
theorem findSymbol_add_ne (env : Env) {n m : Name} (x : Sym) (h : m ≠ n) :
    findSymbol { env with scope := env.scope.add n x } m = findSymbol env m := by
  simp [findSymbol, Scope.lookup_add_ne _ _ h]

/-- Without a local binding, the visible definition with the strictly smallest basename wins. -/
theorem findSymbol_global (env : Env) (n : Name) (w : GEntry) (hs : env.scope.lookup n = none)
    (hw : w ∈ entriesIn env.units n (visiblePackages env.pkg env.imports))
    (hmin : ∀ e ∈ entriesIn env.units n (visiblePackages env.pkg env.imports), e ≠ w →
      w.file < e.file) :
    findSymbol env n = [.global w] := by
  have hh := visibleEntries_unique_min env.units n env.pkg env.imports w hw hmin
  unfold findSymbol; rw [hs]; simp only
  cases h : visibleEntries env.units n env.pkg env.imports with
  | nil => simp [h] at hh
  | cons v vs => simp [h] at hh; simp [hh]

/--
The import fallback is the last tier: it applies only when no local binding and no visible
definition exists, and only after an import of the bare name.
-/
theorem findSymbol_anywhere (env : Env) (n : Name) (b : Binding) (hb : b ∈ findSymbol env n)
    (hs : env.scope.lookup n = none) (hv : visibleEntries env.units n env.pkg env.imports = []) :
    importsByName env.imports n = true ∧ ∃ e ∈ anywhereCandidates env.units n, b = .global e := by
  unfold findSymbol at hb; rw [hs, hv] at hb; simp only at hb
  split at hb
  · rename_i hi
    simp only [List.mem_map] at hb
    obtain ⟨e, he, rfl⟩ := hb
    exact ⟨hi, e, he, rfl⟩
  · simp at hb

/-! ## Bare identifiers -/

/-- The kinds of term symbols the lookup distinguishes. -/
inductive Kind where
  | function   -- MethodSymbolInfo
  | value      -- ValSymbolInfo of a non-relation value
  | tableValue -- ValSymbolInfo of a table value constant (`val t(a, b) = [[...]]`)
  | model      -- ModelSymbolInfo
  | alias      -- RelationAliasSymbolInfo (`... as name`)
  | other      -- any other term symbol, e.g. SavedRelationSymbolInfo
deriving Repr, DecidableEq

/-- A symbol's kind, and whether its lazy completion is running. -/
structure Info where
  kind : Kind
  completing : Bool
deriving Repr, DecidableEq

/-- `isFunctionDef` in `identifierRules`. -/
def Info.isLiveFunction (i : Info) : Bool := i.kind == .function && !i.completing

inductive IdentResult (α : Type) where
  | column
  | symbol (b : α)
  | unresolved
deriving Repr, DecidableEq

/-- `identifierRules` for an `Identifier`, given what `findSymbolByName` returned. -/
def identify {α : Type} (found : Option α) (info : α → Info) (column : Bool) : IdentResult α :=
  match found with
  | some b => if (info b).isLiveFunction && column then .column else .symbol b
  | none => if column then .column else .unresolved

/--
The precedence of a bare identifier: the column wins exactly when the input relation has it and
the lookup found nothing or a function definition that is not completing.
-/
theorem identify_column_iff {α : Type} (found : Option α) (info : α → Info) (column : Bool) :
    identify found info column = .column ↔
      column = true ∧ ∀ b, found = some b → (info b).isLiveFunction = true := by
  cases found with
  | none => cases column <;> simp [identify]
  | some b =>
    simp only [identify]
    cases h : (info b).isLiveFunction <;> cases column <;> simp [h]

/-- Any other symbol the lookup finds wins over a same-named column. -/
theorem identify_symbol {α : Type} (b : α) (info : α → Info) (column : Bool)
    (h : (info b).isLiveFunction = false) : identify (some b) info column = .symbol b := by
  simp [identify, h]

/-- An identifier is unresolved exactly when the lookup and the input relation both miss. -/
theorem identify_unresolved_iff {α : Type} (found : Option α) (info : α → Info) (column : Bool) :
    identify found info column = .unresolved ↔ found = none ∧ column = false := by
  cases found with
  | none => cases column <;> simp [identify]
  | some b =>
    simp only [identify]
    cases (info b).isLiveFunction <;> cases column <;> simp

/--
Multi-pass consistency for identifiers: after a pass adds a local binding `x`, the identifier
denotes `x`, unless `x` is a function definition that a same-named column shadows.
-/
theorem identify_after_add (env : Env) (n : Name) (x : Sym) (info : Binding → Info)
    (column : Bool) :
    identify (findSymbol { env with scope := env.scope.add n x } n).head? info column =
      if (info (.local x)).isLiveFunction && column then .column else .symbol (.local x) := by
  rw [findSymbol_add]; rfl

/--
Lambda parameters (`Typer.typeLambda`): the Typer binds each parameter of `xs.transform(x -> ...)`
as a value with `add` in a child scope of the enclosing expression, so inside the lambda body the
parameter wins over a same-named input column, an outer local binding, and every top-level
definition or import.
-/
theorem identify_lambda_param (env : Env) (n : Name) (x : Sym) (info : Binding → Info)
    (column : Bool) (hv : (info (.local x)).kind = .value) :
    identify (findSymbol { env with scope := env.scope.child.add n x } n).head? info column =
      .symbol (.local x) := by
  have h : findSymbol { env with scope := env.scope.child.add n x } n = [.local x] :=
    findSymbol_local _ _ _ (Scope.lookup_add_self _ _ _)
  rw [h]
  simp [identify, Info.isLiveFunction, hv]

/-! ## Relation names -/

inductive RelResult where
  | localRelation -- a CTE alias or flow stage of the enclosing query
  | keep          -- left unresolved: a recursive reference, or a plain value
  | modelScan
  | values        -- a table value constant
  | aliasRelation -- the aliased query
  | table         -- a table declaration or the catalog
deriving Repr, DecidableEq

/-- `from x`: the local relation names, then `findSymbolByName`, then tables. -/
def resolveRelation (isLocal : Bool) (found : Option Info) : RelResult :=
  if isLocal then .localRelation else
  match found with
  | none => .table
  | some i =>
    if i.completing then .keep else
    match i.kind with
    | .model => .modelScan
    | .tableValue => .values
    | .value => .keep
    | .alias => .aliasRelation
    | .function | .other => .table

/-- Local relation names shadow every symbol and table. -/
theorem resolveRelation_local (found : Option Info) :
    resolveRelation true found = .localRelation := rfl

/--
Tables are the last tier: `from x` reaches a table only when no local relation and no relation
symbol has the name. A plain value or a completing symbol stops the lookup before the tables.
-/
theorem resolveRelation_table_iff (isLocal : Bool) (found : Option Info) :
    resolveRelation isLocal found = .table ↔
      isLocal = false ∧
        ∀ i, found = some i → i.completing = false ∧ (i.kind = .function ∨ i.kind = .other) := by
  cases isLocal with
  | true => simp [resolveRelation]
  | false =>
    cases found with
    | none => simp [resolveRelation]
    | some i =>
      simp only [resolveRelation]
      cases hc : i.completing <;> cases hk : i.kind <;> simp [hc, hk]

end Wvlet.NameLookup
