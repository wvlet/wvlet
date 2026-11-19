/-
Licensed under the Apache License, Version 2.0.

The phase leaves no Wvlet-only form behind (no string `+`, no `s"..."`, no `if(...)` call), and it
is idempotent. Idempotence matters because `RewriteExpr.rewriteOnly` runs the rules once for every
enclosing `Query`: `transformUpExpressions` descends into child plans, so a subquery or CTE nested
under k queries is rewritten k times.
-/
import WvletFormal.Rewrite.Sound

namespace Wvlet.Rewrite

/-! ## The nodes each rule rewrites -/

def concatFires : Expr → Bool
  | .add l r => isStrAdd (typeOf l) (typeOf r)
  | _ => false

def interpFires : Expr → Bool
  | .interp pfx _ => pfx = "s"
  | _ => false

def ifFires : Expr → Bool
  | .call f as => f = "if" && (as.length == 2 || as.length == 3)
  | _ => false

theorem concatRule_of_not_fires {x : Expr} (h : concatFires x = false) : concatRule x = x := by
  cases x <;> simp_all [concatRule, concatFires]

theorem interpRule_of_not_fires {x : Expr} (h : interpFires x = false) : interpRule x = x := by
  cases x <;> simp_all [interpRule, interpFires]

theorem ifRule_of_not_fires {x : Expr} (h : ifFires x = false) : ifRule x = x := by
  match x with
  | .call f (.cons c (.cons t .nil)) =>
    simp only [ifFires, Args.length] at h
    simp [ifRule, args2]; intro hf; simp [hf] at h
  | .call f (.cons c (.cons t (.cons e .nil))) =>
    simp only [ifFires, Args.length] at h
    simp [ifRule]; intro hf; simp [hf] at h
  | .call _ .nil | .call _ (.cons _ .nil) | .call _ (.cons _ (.cons _ (.cons _ (.cons _ _)))) => rfl
  | .lit _ | .col _ _ | .add _ _ | .interp _ _ | .part _ | .ifExpr _ _ _ | .cast _ => rfl

/-! ## Expressions with no node a predicate accepts -/

mutual
/-- No node of the expression satisfies `p`. -/
def Clean (p : Expr → Bool) : Expr → Prop
  | .lit v => p (.lit v) = false
  | .col n t => p (.col n t) = false
  | .part s => p (.part s) = false
  | .add l r => p (.add l r) = false ∧ Clean p l ∧ Clean p r
  | .interp pfx ps => p (.interp pfx ps) = false ∧ CleanArgs p ps
  | .call f as => p (.call f as) = false ∧ CleanArgs p as
  | .ifExpr c t e => p (.ifExpr c t e) = false ∧ Clean p c ∧ Clean p t ∧ Clean p e
  | .cast e => p (.cast e) = false ∧ Clean p e

def CleanArgs (p : Expr → Bool) : Args → Prop
  | .nil => True
  | .cons e rest => Clean p e ∧ CleanArgs p rest
end

/-- No proper subexpression satisfies `p`. -/
def Below (p : Expr → Bool) : Expr → Prop
  | .lit _ | .col _ _ | .part _ => True
  | .add l r => Clean p l ∧ Clean p r
  | .interp _ ps => CleanArgs p ps
  | .call _ as => CleanArgs p as
  | .ifExpr c t e => Clean p c ∧ Clean p t ∧ Clean p e
  | .cast e => Clean p e

theorem clean_iff {p : Expr → Bool} (e : Expr) : Clean p e ↔ p e = false ∧ Below p e := by
  cases e <;> simp [Clean, Below]

/-- `p` looks only at a node's head, its children's types, and its arity. -/
structure Shallow (p : Expr → Bool) : Prop where
  add : ∀ l r l' r', typeOf l = typeOf l' → typeOf r = typeOf r' → p (.add l r) = p (.add l' r')
  interp : ∀ pfx ps ps', p (.interp pfx ps) = p (.interp pfx ps')
  call : ∀ f as as', as.length = as'.length → p (.call f as) = p (.call f as')
  ifExpr : ∀ c t e c' t' e', p (.ifExpr c t e) = p (.ifExpr c' t' e')
  cast : ∀ e e', p (.cast e) = p (.cast e')

/-- `p` accepts none of the nodes the rules build. -/
structure Helpers (p : Expr → Bool) : Prop where
  lit : ∀ v, p (.lit v) = false
  cast : ∀ e, p (.cast e) = false
  concat : ∀ as, p (.call "concat" as) = false
  coalesce : ∀ as, p (.call "coalesce" as) = false
  ifExpr : ∀ c t e, p (.ifExpr c t e) = false

theorem concatFires_shallow : Shallow concatFires :=
  ⟨fun _ _ _ _ h1 h2 => by simp [concatFires, h1, h2], fun _ _ _ => rfl, fun _ _ _ _ => rfl,
    fun _ _ _ _ _ _ => rfl, fun _ _ => rfl⟩

theorem interpFires_shallow : Shallow interpFires :=
  ⟨fun _ _ _ _ _ _ => rfl, fun _ _ _ => rfl, fun _ _ _ _ => rfl, fun _ _ _ _ _ _ => rfl,
    fun _ _ => rfl⟩

theorem ifFires_shallow : Shallow ifFires :=
  ⟨fun _ _ _ _ _ _ => rfl, fun _ _ _ => rfl, fun _ _ _ h => by simp [ifFires, h],
    fun _ _ _ _ _ _ => rfl, fun _ _ => rfl⟩

theorem concatFires_helpers : Helpers concatFires :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => rfl, fun _ => rfl, fun _ _ _ => rfl⟩

theorem interpFires_helpers : Helpers interpFires :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => rfl, fun _ => rfl, fun _ _ _ => rfl⟩

theorem ifFires_helpers : Helpers ifFires :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => by simp [ifFires], fun _ => by simp [ifFires],
    fun _ _ _ => rfl⟩

/-! ## The helpers the rules build keep an expression clean -/

section Helpers

variable {p : Expr → Bool} (hp : Helpers p)
include hp

theorem clean_asVarchar {e : Expr} (h : Clean p e) : Clean p (asVarchar e) := by
  unfold asVarchar
  split
  · exact h
  · exact ⟨hp.cast e, h⟩

theorem clean_toText {e : Expr} (h : Clean p e) : Clean p (toText e) := by
  unfold toText
  split
  · exact clean_asVarchar hp h
  · exact ⟨hp.coalesce _, clean_asVarchar hp h, hp.lit _, trivial⟩

theorem clean_quotePart {e : Expr} (h : Clean p e) : Clean p (quotePart e) := by
  cases e <;> first | exact hp.lit _ | exact clean_toText hp h

theorem clean_concat2 {a b : Expr} (ha : Clean p a) (hb : Clean p b) : Clean p (concat2 a b) :=
  ⟨hp.concat _, ha, hb, trivial⟩

theorem clean_foldRest : ∀ {rest : Args} {acc : Expr}, CleanArgs p rest → Clean p acc →
    Clean p (foldRest acc rest)
  | .nil, _, _, h => h
  | .cons _ _, _, ⟨h1, h2⟩, h => by
    simp only [foldRest]
    exact clean_foldRest h2 (clean_concat2 hp h (clean_quotePart hp h1))

theorem clean_foldParts : ∀ {parts : Args}, CleanArgs p parts → Clean p (foldParts parts)
  | .nil, _ => hp.lit _
  | .cons _ _, ⟨h1, h2⟩ => clean_foldRest hp h2 (clean_quotePart hp h1)

theorem clean_interpRule {x : Expr} (h : Below p x) (hx : p x = false) :
    Clean p (interpRule x) := by
  cases x with
  | interp pfx ps =>
    simp only [interpRule]
    split
    · exact clean_foldParts hp h
    · exact (clean_iff _).mpr ⟨hx, h⟩
  | _ => exact (clean_iff _).mpr ⟨hx, h⟩

theorem clean_ifRule {x : Expr} (h : Below p x) (hx : p x = false) : Clean p (ifRule x) := by
  match x with
  | .call f (.cons c (.cons t .nil)) =>
    simp only [ifRule]
    split
    · exact ⟨hp.ifExpr _ _ _, h.1, h.2.1, hp.lit _⟩
    · exact (clean_iff _).mpr ⟨hx, h⟩
  | .call f (.cons c (.cons t (.cons e .nil))) =>
    simp only [ifRule]
    split
    · exact ⟨hp.ifExpr _ _ _, h.1, h.2.1, h.2.2.1⟩
    · exact (clean_iff _).mpr ⟨hx, h⟩
  | .call _ .nil | .call _ (.cons _ .nil) | .call _ (.cons _ (.cons _ (.cons _ (.cons _ _))))
  | .lit _ | .col _ _ | .add _ _ | .interp _ _ | .part _ | .ifExpr _ _ _ | .cast _ =>
    exact (clean_iff _).mpr ⟨hx, h⟩

end Helpers

/-! ## Generic facts about bottom-up passes -/

theorem length_bottomUpArgs (r : Expr → Expr) : ∀ as, (bottomUpArgs r as).length = as.length
  | .nil => rfl
  | .cons _ rest => by simp [bottomUpArgs, Args.length, length_bottomUpArgs r rest]

mutual
theorem typeOf_bottomUp {r : Expr → Expr} (ht : ∀ x, typeOf (r x) = typeOf x) :
    ∀ e, typeOf (bottomUp r e) = typeOf e
  | .lit v => ht _
  | .col _ _ => ht _
  | .part _ => ht _
  | .add l rr => by
    simp only [bottomUp, ht, typeOf, typeOf_bottomUp ht l, typeOf_bottomUp ht rr]
  | .interp _ _ => by simp only [bottomUp, ht, typeOf]
  | .call f as => by simp only [bottomUp, ht, typeOf, argTypes_bottomUpArgs ht as]
  | .ifExpr c t e => by
    simp only [bottomUp, ht, typeOf, typeOf_bottomUp ht t, typeOf_bottomUp ht e]
  | .cast _ => by simp only [bottomUp, ht, typeOf]

theorem argTypes_bottomUpArgs {r : Expr → Expr} (ht : ∀ x, typeOf (r x) = typeOf x) :
    ∀ as, argTypes (bottomUpArgs r as) = argTypes as
  | .nil => rfl
  | .cons e rest => by
    simp only [bottomUpArgs, argTypes, typeOf_bottomUp ht e, argTypes_bottomUpArgs ht rest]
end

mutual
/-- A pass is the identity on an expression none of whose nodes the rule rewrites. -/
theorem bottomUp_id {r : Expr → Expr} {p : Expr → Bool} (hr : ∀ x, p x = false → r x = x) :
    ∀ e, Clean p e → bottomUp r e = e
  | .lit v, h => hr _ h
  | .col _ _, h => hr _ h
  | .part _, h => hr _ h
  | .add l rr, ⟨h0, h1, h2⟩ => by
    simp only [bottomUp, bottomUp_id hr l h1, bottomUp_id hr rr h2, hr _ h0]
  | .interp _ ps, ⟨h0, h1⟩ => by simp only [bottomUp, bottomUpArgs_id hr ps h1, hr _ h0]
  | .call _ as, ⟨h0, h1⟩ => by simp only [bottomUp, bottomUpArgs_id hr as h1, hr _ h0]
  | .ifExpr c t e, ⟨h0, h1, h2, h3⟩ => by
    simp only [bottomUp, bottomUp_id hr c h1, bottomUp_id hr t h2, bottomUp_id hr e h3, hr _ h0]
  | .cast e, ⟨h0, h1⟩ => by simp only [bottomUp, bottomUp_id hr e h1, hr _ h0]

theorem bottomUpArgs_id {r : Expr → Expr} {p : Expr → Bool} (hr : ∀ x, p x = false → r x = x) :
    ∀ as, CleanArgs p as → bottomUpArgs r as = as
  | .nil, _ => rfl
  | .cons e rest, ⟨h1, h2⟩ => by
    simp only [bottomUpArgs, bottomUp_id hr e h1, bottomUpArgs_id hr rest h2]
end

mutual
/-- A pass whose rule cleans every node with clean children leaves a clean expression. -/
theorem bottomUp_establishes {r : Expr → Expr} {p : Expr → Bool}
    (hr : ∀ x, Below p x → Clean p (r x)) : ∀ e, Clean p (bottomUp r e)
  | .lit _ => hr _ trivial
  | .col _ _ => hr _ trivial
  | .part _ => hr _ trivial
  | .add l rr => hr _ ⟨bottomUp_establishes hr l, bottomUp_establishes hr rr⟩
  | .interp _ ps => hr _ (bottomUpArgs_establishes hr ps)
  | .call _ as => hr _ (bottomUpArgs_establishes hr as)
  | .ifExpr c t e =>
    hr _ ⟨bottomUp_establishes hr c, bottomUp_establishes hr t, bottomUp_establishes hr e⟩
  | .cast e => hr _ (bottomUp_establishes hr e)

theorem bottomUpArgs_establishes {r : Expr → Expr} {p : Expr → Bool}
    (hr : ∀ x, Below p x → Clean p (r x)) : ∀ as, CleanArgs p (bottomUpArgs r as)
  | .nil => trivial
  | .cons e rest => ⟨bottomUp_establishes hr e, bottomUpArgs_establishes hr rest⟩
end

mutual
/-- A type-preserving pass whose rule keeps clean expressions clean keeps a clean expression
clean, for a predicate that looks only at node heads. -/
theorem bottomUp_preserves {r : Expr → Expr} {p : Expr → Bool} (hsh : Shallow p)
    (ht : ∀ x, typeOf (r x) = typeOf x) (hr : ∀ x, Clean p x → Clean p (r x)) :
    ∀ e, Clean p e → Clean p (bottomUp r e)
  | .lit _, h => hr _ h
  | .col _ _, h => hr _ h
  | .part _, h => hr _ h
  | .add l rr, ⟨h0, h1, h2⟩ =>
    hr _ ⟨(hsh.add _ _ _ _ (typeOf_bottomUp ht l) (typeOf_bottomUp ht rr)).trans h0,
      bottomUp_preserves hsh ht hr l h1, bottomUp_preserves hsh ht hr rr h2⟩
  | .interp _ ps, ⟨h0, h1⟩ =>
    hr _ ⟨(hsh.interp _ _ _).trans h0, bottomUpArgs_preserves hsh ht hr ps h1⟩
  | .call _ as, ⟨h0, h1⟩ =>
    hr _ ⟨(hsh.call _ _ _ (length_bottomUpArgs r as)).trans h0,
      bottomUpArgs_preserves hsh ht hr as h1⟩
  | .ifExpr c t e, ⟨h0, h1, h2, h3⟩ =>
    hr _ ⟨(hsh.ifExpr _ _ _ _ _ _).trans h0, bottomUp_preserves hsh ht hr c h1,
      bottomUp_preserves hsh ht hr t h2, bottomUp_preserves hsh ht hr e h3⟩
  | .cast e, ⟨h0, h1⟩ => hr _ ⟨(hsh.cast _ _).trans h0, bottomUp_preserves hsh ht hr e h1⟩

theorem bottomUpArgs_preserves {r : Expr → Expr} {p : Expr → Bool} (hsh : Shallow p)
    (ht : ∀ x, typeOf (r x) = typeOf x) (hr : ∀ x, Clean p x → Clean p (r x)) :
    ∀ as, CleanArgs p as → CleanArgs p (bottomUpArgs r as)
  | .nil, _ => trivial
  | .cons e rest, ⟨h1, h2⟩ =>
    ⟨bottomUp_preserves hsh ht hr e h1, bottomUpArgs_preserves hsh ht hr rest h2⟩
end

/-! ## The phase -/

/-- A row of NULLs, to read off engine-independent facts (types) from the soundness theorems. -/
def nullRow : Row := fun _ _ => .null

theorem nullRow_wellTyped : nullRow.WellTyped := fun _ _ => rfl

theorem typeOf_concatRule (x : Expr) : typeOf (concatRule x) = typeOf x :=
  (concatRule_sound (eng := .duckdb) nullRow_wellTyped x).1

theorem typeOf_interpRule (x : Expr) : typeOf (interpRule x) = typeOf x :=
  (interpRule_sound (eng := .duckdb) nullRow_wellTyped x).1

theorem typeOf_ifRule (x : Expr) : typeOf (ifRule x) = typeOf x :=
  (ifRule_sound (eng := .duckdb) (row := nullRow) rfl x).1

theorem clean_concatRule {x : Expr} (h : Below concatFires x) :
    Clean concatFires (concatRule x) := by
  cases x with
  | add l r =>
    simp only [concatRule]
    split
    · exact clean_concat2 concatFires_helpers (clean_toText concatFires_helpers h.1)
        (clean_toText concatFires_helpers h.2)
    · rename_i hs
      exact ⟨by simpa [concatFires] using hs, h⟩
  | _ => exact (clean_iff _).mpr ⟨rfl, h⟩

/-- After the phase, no string `+`, no `s"..."`, and no `if(...)` call is left: every remaining
node has a SQL meaning. -/
theorem phase_clean (e : Expr) :
    Clean concatFires (phase e) ∧ Clean interpFires (phase e) ∧ Clean ifFires (phase e) := by
  have c1 : Clean concatFires (bottomUp concatRule e) :=
    bottomUp_establishes (fun _ h => clean_concatRule h) e
  have c2 : Clean concatFires (bottomUp interpRule (bottomUp concatRule e)) :=
    bottomUp_preserves concatFires_shallow typeOf_interpRule
      (fun x hx => clean_interpRule concatFires_helpers ((clean_iff x).mp hx).2
        ((clean_iff x).mp hx).1) _ c1
  have i2 : Clean interpFires (bottomUp interpRule (bottomUp concatRule e)) :=
    bottomUp_establishes (fun x h => by
      cases x with
      | interp pfx ps =>
        simp only [interpRule]
        split
        · exact clean_foldParts interpFires_helpers h
        · rename_i hp; exact ⟨by simpa [interpFires] using hp, h⟩
      | _ => exact (clean_iff _).mpr ⟨rfl, h⟩) _
  refine ⟨?_, ?_, ?_⟩
  · exact bottomUp_preserves concatFires_shallow typeOf_ifRule
      (fun x hx => clean_ifRule concatFires_helpers ((clean_iff x).mp hx).2
        ((clean_iff x).mp hx).1) _ c2
  · exact bottomUp_preserves interpFires_shallow typeOf_ifRule
      (fun x hx => clean_ifRule interpFires_helpers ((clean_iff x).mp hx).2
        ((clean_iff x).mp hx).1) _ i2
  · exact bottomUp_establishes (fun x h => by
      match x with
      | .call f (.cons c (.cons t .nil)) =>
        simp only [ifRule]
        split
        · exact ⟨rfl, h.1, h.2.1, rfl⟩
        · rename_i hf; exact ⟨by simp [ifFires, hf], h⟩
      | .call f (.cons c (.cons t (.cons e .nil))) =>
        simp only [ifRule]
        split
        · exact ⟨rfl, h.1, h.2.1, h.2.2.1⟩
        · rename_i hf; exact ⟨by simp [ifFires, hf], h⟩
      | .call _ .nil | .call _ (.cons _ .nil)
      | .call _ (.cons _ (.cons _ (.cons _ (.cons _ _)))) =>
        exact (clean_iff _).mpr ⟨by simp [ifRule, ifFires, Args.length], h⟩
      | .lit _ | .col _ _ | .add _ _ | .interp _ _ | .part _ | .ifExpr _ _ _ | .cast _ =>
        exact (clean_iff _).mpr ⟨rfl, h⟩) _

/-- Running the phase again changes nothing, so rewriting a nested query once per enclosing query
is harmless. -/
theorem phase_idem (e : Expr) : phase (phase e) = phase e := by
  obtain ⟨c, i, f⟩ := phase_clean e
  unfold phase at c i f ⊢
  rw [bottomUp_id (fun _ h => concatRule_of_not_fires h) _ c,
    bottomUp_id (fun _ h => interpRule_of_not_fires h) _ i,
    bottomUp_id (fun _ h => ifRule_of_not_fires h) _ f]

end Wvlet.Rewrite
