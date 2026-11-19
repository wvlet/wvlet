/-
Licensed under the Apache License, Version 2.0.

Local scopes (`Scope` in wvlet-lang/src/main/scala/wvlet/lang/compiler/Scope.scala): `add` binds a
name and shadows any earlier binding, `enter` binds it only when it is not bound yet, and a child
scope starts from a snapshot of its parent's bindings. `Scope.lookupEntry` reads a name-keyed map
that `add` updates, which is the newest binding of the entry list modelled here.
-/

namespace Wvlet.NameLookup

/-- Term names of the model, written `n0`, `n1`, ... in the fixture. -/
abbrev Name := Nat

/-- Symbols bound in a scope, identified by number. -/
abbrev Sym := Nat

/-- A scope: its bindings, newest first. -/
structure Scope where
  entries : List (Name × Sym)
deriving Repr, DecidableEq

namespace Scope

/-- `Scope.newScope` and `newFreshScope`: no bindings. -/
def empty : Scope := ⟨[]⟩

/-- `Scope.lookupSymbol`: the newest binding of the name. -/
def lookup (s : Scope) (n : Name) : Option Sym := (s.entries.find? (·.1 == n)).map (·.2)

/-- `Scope.add`: bind the name, shadowing any earlier binding. -/
def add (s : Scope) (n : Name) (x : Sym) : Scope := ⟨(n, x) :: s.entries⟩

/--
`Scope.enter`: bind the name only when it is not bound yet. Returns the scope and the symbol the
name is bound to afterwards (the Scala method's return value).
-/
def enter (s : Scope) (n : Name) (x : Sym) : Scope × Sym :=
  match s.lookup n with
  | some y => (s, y)
  | none => (s.add n x, x)

/-- `Scope.newChildScope`: a snapshot of the parent's bindings. -/
def child (s : Scope) : Scope := ⟨s.entries⟩

/-! ## Shadowing -/

/-- The newest `add` wins: last-entered wins for `add`. -/
@[simp] theorem lookup_add_self (s : Scope) (n : Name) (x : Sym) : (s.add n x).lookup n = some x := by
  simp [add, lookup]

/-- Binding one name never changes the lookup of another name. -/
@[simp] theorem lookup_add_ne (s : Scope) {n m : Name} (x : Sym) (h : m ≠ n) :
    (s.add n x).lookup m = s.lookup m := by
  have : (n == m) = false := beq_false_of_ne (Ne.symm h)
  simp [add, lookup, this]

/-- `add` of different names commutes: the lookup does not depend on the entry order. -/
theorem lookup_add_comm (s : Scope) {n m : Name} (x y : Sym) (h : n ≠ m) (k : Name) :
    ((s.add n x).add m y).lookup k = ((s.add m y).add n x).lookup k := by
  by_cases hk : k = m
  · subst hk; simp [Ne.symm h]
  · by_cases hn : k = n
    · subst hn; simp [hk]
    · simp [hk, hn]

/-- `enter` never shadows: a bound name keeps its binding, and `enter` returns it. -/
theorem enter_bound (s : Scope) (n : Name) (x y : Sym) (h : s.lookup n = some y) :
    s.enter n x = (s, y) := by
  simp [enter, h]

/-- `enter` of an unbound name binds it to the new symbol. -/
theorem enter_free (s : Scope) (n : Name) (x : Sym) (h : s.lookup n = none) :
    s.enter n x = (s.add n x, x) := by
  simp [enter, h]

/-- After `enter`, the name is bound to the symbol `enter` returned. -/
theorem lookup_enter (s : Scope) (n : Name) (x : Sym) :
    (s.enter n x).1.lookup n = some (s.enter n x).2 := by
  unfold enter
  cases h : s.lookup n <;> simp [h]

/-- `enter` never changes the lookup of another name. -/
theorem lookup_enter_ne (s : Scope) {n m : Name} (x : Sym) (h : m ≠ n) :
    (s.enter n x).1.lookup m = s.lookup m := by
  unfold enter
  cases s.lookup n <;> simp [h]

/-- A child scope sees exactly its parent's bindings at the time it is created. -/
@[simp] theorem lookup_child (s : Scope) (n : Name) : s.child.lookup n = s.lookup n := rfl

/-- Inner scopes shadow outer ones: an `add` in a child wins over the parent's binding. -/
theorem lookup_child_add (s : Scope) (n : Name) (x : Sym) :
    (s.child.add n x).lookup n = some x := lookup_add_self _ _ _

/-- A fresh scope sees nothing. -/
@[simp] theorem lookup_empty (n : Name) : empty.lookup n = none := rfl

end Scope

/-! ## Scope programs

A replayable program over scope handles: handle 0 is a root scope (`Scope.newScope(0)`); `child h`
and `fresh` create a new handle; `add` and `enter` bind a name in one handle. The symbol bound by
the operation at position `i` of a program is `i`.
-/

inductive Op where
  | child (h : Nat)
  | fresh
  | add (h : Nat) (n : Name)
  | enter (h : Nat) (n : Name)
deriving Repr, DecidableEq

/-- The scopes by handle, and the symbols that each `enter` returned. -/
structure State where
  scopes : List Scope
  entered : List Sym
deriving Repr

def State.init : State := ⟨[Scope.empty], []⟩

def State.get (st : State) (h : Nat) : Scope := st.scopes.getD h Scope.empty

/-- One operation; `x` is the symbol it binds. Operations on missing handles are skipped. -/
def step (st : State) (x : Sym) : Op → State
  | .child h => if h < st.scopes.length then { st with scopes := st.scopes ++ [(st.get h).child] } else st
  | .fresh => { st with scopes := st.scopes ++ [Scope.empty] }
  | .add h n =>
    if h < st.scopes.length then { st with scopes := st.scopes.set h ((st.get h).add n x) } else st
  | .enter h n =>
    if h < st.scopes.length then
      let r := (st.get h).enter n x
      { scopes := st.scopes.set h r.1, entered := st.entered ++ [r.2] }
    else st

def runFrom (st : State) (i : Nat) : List Op → State
  | [] => st
  | op :: ops => runFrom (step st i op) (i + 1) ops

def run (ops : List Op) : State := runFrom State.init 0 ops

/--
Every operation on handle `h` leaves every other existing handle unchanged. With `lookup_child`,
this is the snapshot semantics: a child scope never sees a binding made in its parent after it was
created.
-/
theorem step_frame (st : State) (x : Sym) (op : Op) (k : Nat) (hk : k < st.scopes.length)
    (hne : ∀ h n, (op = .add h n ∨ op = .enter h n) → h ≠ k) :
    (step st x op).scopes[k]? = st.scopes[k]? := by
  cases op with
  | child h =>
    simp only [step]; split
    · rw [List.getElem?_append_left hk]
    · rfl
  | fresh => simp only [step]; rw [List.getElem?_append_left hk]
  | add h n =>
    simp only [step]; split
    · exact List.getElem?_set_ne (hne h n (Or.inl rfl))
    · rfl
  | enter h n =>
    simp only [step]; split
    · exact List.getElem?_set_ne (hne h n (Or.inr rfl))
    · rfl

end Wvlet.NameLookup
