/-
Licensed under the Apache License, Version 2.0.

The rewrite rules preserve values: on every engine and every well-typed row, each rule, each
bottom-up pass, and the whole phase leave the value (and the static type) of an expression
unchanged, NULL inputs included.
-/
import WvletFormal.Rewrite.Rules

namespace Wvlet.Rewrite

variable {eng : Engine} {row : Row}

/-! ## Typing: evaluated values fit their static types -/

/-- Each evaluated argument fits its type. -/
def AllFit : List (Option Value) → List Ty → Prop
  | [], [] => True
  | o :: os, t :: ts => (∀ v, o = some v → v.fits t = true) ∧ AllFit os ts
  | _, _ => False

theorem concatValues_fits {vs : List Value} {v : Value} (h : concatValues eng vs = some v) :
    v.fits .string = true := by
  unfold concatValues at h
  split at h
  · cases h
  · split at h
    · cases h; rfl
    · split at h <;> (cases h; rfl)

theorem addInt_fits {a b v : Value} (h : addInt a b = some v) : v.fits .int = true := by
  cases a <;> cases b <;> simp [addInt] at h <;> subst h <;> rfl

theorem coalesce_fits :
    ∀ {os : List (Option Value)} {ts : List Ty} {vs : List Value},
      AllFit os ts → seq os = some vs → (coalesceValues vs).fits (ts.foldr Ty.join .null) = true
  | [], [], vs, _, h => by simp [seq] at h; subst h; rfl
  | [], _ :: _, _, h, _ => h.elim
  | _ :: _, [], _, h, _ => h.elim
  | o :: os, t :: ts, vs, ⟨ho, hos⟩, h => by
    cases o with
    | none => simp [seq] at h
    | some v =>
      simp only [seq, Option.bind_some, Option.map_eq_some_iff] at h
      obtain ⟨vs', hs, rfl⟩ := h
      have hv := ho v rfl
      simp only [List.foldr]
      cases v with
      | null => simp only [coalesceValues]; exact Value.fits_join_right _ (coalesce_fits hos hs)
      | str _ => exact Value.fits_join_left _ hv
      | int _ => exact Value.fits_join_left _ hv
      | bool _ => exact Value.fits_join_left _ hv

theorem ite3_fits {c : Value} {t e : Option Value} {ty : Ty} {v : Value}
    (ht : ∀ w, t = some w → w.fits ty = true) (he : ∀ w, e = some w → w.fits ty = true)
    (h : ite3 c t e = some v) : v.fits ty = true := by
  rcases ite3_cases c t e with h' | h' | h' <;> rw [h'] at h
  · exact ht v h
  · exact he v h
  · cases h

theorem callEval_fits {f : String} {os : List (Option Value)} {ts : List Ty} {v : Value}
    (hf : AllFit os ts) (h : callEval eng f os = some v) : v.fits (callType f ts) = true := by
  by_cases h1 : f = "concat"
  · simp only [callEval, callType, h1, ite_true, Option.bind_eq_some_iff] at h ⊢
    obtain ⟨vs, _, hc⟩ := h
    exact concatValues_fits hc
  by_cases h2 : f = "coalesce"
  · simp [callEval, callType, h2] at h ⊢
    cases hs : seq os with
    | none => simp [hs] at h
    | some vs =>
      simp [hs] at h
      subst h
      exact coalesce_fits hf hs
  by_cases h3 : f = "if"
  · simp [callEval, callType, h3] at h ⊢
    rcases os with _ | ⟨c, _ | ⟨t, _ | ⟨e, _ | ⟨x, os⟩⟩⟩⟩ <;> simp at h
    · rcases ts with _ | ⟨tc, _ | ⟨tt, _ | ⟨te, ts⟩⟩⟩ <;> simp [AllFit] at hf
      cases c with
      | none => simp at h
      | some cv => exact ite3_fits hf.2 (fun w hw => by cases hw; rfl) h
    · rcases ts with _ | ⟨tc, _ | ⟨tt, _ | ⟨te, _ | ⟨tx, ts⟩⟩⟩⟩ <;> simp [AllFit] at hf
      cases c with
      | none => simp at h
      | some cv =>
        exact ite3_fits (fun w hw => Value.fits_join_left _ (hf.2.1 w hw))
          (fun w hw => Value.fits_join_right _ (hf.2.2 w hw)) h
  · simp [callEval, h1, h2, h3] at h

mutual
theorem eval_fits (hrow : row.WellTyped) :
    ∀ (e : Expr) (v : Value), eval eng row e = some v → v.fits (typeOf e) = true
  | .lit w, v, h => by simp [eval] at h; subst h; simp [typeOf]
  | .col n t, v, h => by simp [eval] at h; subst h; exact hrow t n
  | .add l r, v, h => by
    simp only [eval, typeOf] at h ⊢
    split at h
    · simp only [Option.bind_eq_some_iff, Option.map_eq_some_iff] at h
      obtain ⟨_, _, _, _, rfl⟩ := h
      simp [*, Value.fits]
    · simp only [Option.bind_eq_some_iff] at h
      obtain ⟨_, _, _, _, hv⟩ := h
      simp only [*]
      exact addInt_fits hv
  | .interp pfx parts, v, h => by
    simp only [eval] at h
    split at h
    · simp only [Option.map_eq_some_iff] at h
      obtain ⟨_, _, rfl⟩ := h
      rfl
    · cases h
  | .part s, v, h => by simp [eval] at h; subst h; rfl
  | .call f args, v, h => by
    simp only [eval, typeOf] at h ⊢
    exact callEval_fits (evalEach_fits hrow args) h
  | .ifExpr c t e, v, h => by
    simp only [eval, typeOf] at h ⊢
    split at h
    · simp only [Option.bind_eq_some_iff] at h
      obtain ⟨_, _, hi⟩ := h
      exact ite3_fits (fun w hw => Value.fits_join_left _ (eval_fits hrow t w hw))
        (fun w hw => Value.fits_join_right _ (eval_fits hrow e w hw)) hi
    · cases h
  | .cast e, v, h => by
    simp only [eval, typeOf, Option.map_eq_some_iff] at h ⊢
    obtain ⟨_, _, rfl⟩ := h
    simp

theorem evalEach_fits (hrow : row.WellTyped) :
    ∀ (as : Args), AllFit (evalEach eng row as) (argTypes as)
  | .nil => trivial
  | .cons e rest => ⟨eval_fits hrow e, evalEach_fits hrow rest⟩
end


/-! ## Expressions `nonNull` accepts are never NULL -/

theorem seq_cons {o : Option Value} {os : List (Option Value)} {vs : List Value}
    (h : seq (o :: os) = some vs) : ∃ v vs', o = some v ∧ seq os = some vs' ∧ vs = v :: vs' := by
  cases o with
  | none => simp [seq] at h
  | some v =>
    cases hs : seq os with
    | none => simp [seq, hs] at h
    | some vs' => simp [seq, hs] at h; exact ⟨v, vs', rfl, rfl, h.symm⟩

theorem seq_mem :
    ∀ {os : List (Option Value)} {vs : List Value}, seq os = some vs → ∀ v ∈ vs, some v ∈ os
  | [], vs, h, v, hv => by simp [seq] at h; subst h; simp at hv
  | o :: os, vs, h, v, hv => by
    obtain ⟨w, vs', rfl, hs, rfl⟩ := seq_cons h
    rcases List.mem_cons.mp hv with rfl | hv
    · simp
    · exact List.mem_cons_of_mem _ (seq_mem hs v hv)

theorem coalesce_ne_null : ∀ {vs : List Value}, (∃ v ∈ vs, v ≠ .null) → coalesceValues vs ≠ .null
  | [], ⟨_, hv, _⟩ => by simp at hv
  | w :: vs, ⟨v, hv, hn⟩ => by
    cases w with
    | null =>
      simp only [coalesceValues]
      rcases List.mem_cons.mp hv with rfl | hv
      · exact absurd rfl hn
      · exact coalesce_ne_null ⟨v, hv, hn⟩
    | str _ => simp [coalesceValues]
    | int _ => simp [coalesceValues]
    | bool _ => simp [coalesceValues]

theorem concatValues_ne_null {vs : List Value} {v : Value} (h : ∀ w ∈ vs, w ≠ .null)
    (hc : concatValues eng vs = some v) : v ≠ .null := by
  unfold concatValues at hc
  split at hc
  · cases hc
  · split at hc
    · cases hc; simp
    · split at hc
      · rename_i hn
        obtain ⟨w, hw, hnull⟩ := List.any_eq_true.mp hn
        have : w = .null := by cases w <;> simp_all [Value.isNull]
        subst this
        cases hc
        exact h _ hw
      · cases hc; simp

mutual
theorem nonNull_sound :
    ∀ (e : Expr), nonNull e = true → ∀ v, eval eng row e = some v → v ≠ .null
  | .lit w, h, v, he => by
    simp [eval] at he; subst he; cases w <;> simp_all [nonNull, Value.isNull]
  | .col _ _, h, _, _ => by simp [nonNull] at h
  | .add _ _, h, _, _ => by simp [nonNull] at h
  | .interp _ _, h, _, _ => by simp [nonNull] at h
  | .part _, h, _, _ => by simp [nonNull] at h
  | .ifExpr _ _ _, h, _, _ => by simp [nonNull] at h
  | .cast e, h, v, he => by
    simp only [nonNull] at h
    simp only [eval] at he
    cases hw : eval eng row e with
    | none => simp [hw] at he
    | some w =>
      simp [hw] at he; subst he
      simpa using nonNull_sound e h w hw
  | .call f args, h, v, he => by
    simp only [nonNull] at h
    simp only [eval] at he
    by_cases h1 : f = "concat"
    · simp only [h1, ite_true] at h
      simp only [callEval, h1, ite_true, Option.bind_eq_some_iff] at he
      obtain ⟨vs, hs, hc⟩ := he
      exact concatValues_ne_null (fun w hw =>
        allNonNull_sound args h _ (seq_mem hs w hw) w rfl) hc
    by_cases h2 : f = "coalesce"
    · simp [h2] at h
      simp [callEval, h2] at he
      cases hs : seq (evalEach eng row args) with
      | none => simp [hs] at he
      | some vs =>
        simp [hs] at he; subst he
        exact coalesce_ne_null (anyNonNull_sound args h vs hs)
    · simp [h1, h2] at h

theorem allNonNull_sound :
    ∀ (as : Args), allNonNull as = true →
      ∀ o ∈ evalEach eng row as, ∀ v, o = some v → v ≠ .null
  | .nil, _, o, ho, _, _ => by simp [evalEach] at ho
  | .cons e rest, h, o, ho, v, hv => by
    simp only [allNonNull, Bool.and_eq_true] at h
    simp only [evalEach, List.mem_cons] at ho
    rcases ho with rfl | ho
    · exact nonNull_sound e h.1 v hv
    · exact allNonNull_sound rest h.2 o ho v hv

theorem anyNonNull_sound :
    ∀ (as : Args), anyNonNull as = true →
      ∀ vs, seq (evalEach eng row as) = some vs → ∃ v ∈ vs, v ≠ .null
  | .nil, h, _, _ => by simp [anyNonNull] at h
  | .cons e rest, h, vs, hs => by
    simp only [evalEach] at hs
    obtain ⟨w, vs', hw, hs', rfl⟩ := seq_cons hs
    simp only [anyNonNull, Bool.or_eq_true] at h
    rcases h with h | h
    · exact ⟨w, by simp, nonNull_sound e h w hw⟩
    · obtain ⟨v, hv, hn⟩ := anyNonNull_sound rest h vs' hs'
      exact ⟨v, List.mem_cons_of_mem _ hv, hn⟩
end

/-! ## Texts: `toText` and `quotePart` evaluate to the text of their operand -/

theorem typeOf_asVarchar (e : Expr) : typeOf (asVarchar e) = .string := by
  unfold asVarchar; split <;> simp_all [typeOf]

theorem typeOf_toText (e : Expr) : typeOf (toText e) = .string := by
  unfold toText
  split
  · exact typeOf_asVarchar e
  · simp [typeOf, argTypes, args2, callType, typeOf_asVarchar, Ty.join, Value.ty]

theorem typeOf_quotePart (p : Expr) : typeOf (quotePart p) = .string := by
  cases p <;> first | rfl | exact typeOf_toText _

theorem asVarchar_eval (hrow : row.WellTyped) (e : Expr) :
    eval eng row (asVarchar e) = (eval eng row e).map Value.toVarchar := by
  unfold asVarchar
  split
  · rename_i ht
    cases hv : eval eng row e with
    | none => rfl
    | some v =>
      have := eval_fits hrow e v hv
      rw [ht] at this
      simp [Value.toVarchar_of_fits_string this]
  · rfl

theorem toText_eval (hrow : row.WellTyped) (e : Expr) :
    eval eng row (toText e) = (eval eng row e).map fun v => .str v.text := by
  unfold toText
  split
  · rename_i hn
    rw [asVarchar_eval hrow]
    cases hv : eval eng row e with
    | none => rfl
    | some v => simp [Value.toVarchar_of_ne_null (nonNull_sound e hn v hv)]
  · simp only [eval, evalEach, args2, callEval, asVarchar_eval hrow]
    cases hv : eval eng row e with
    | none => simp [seq]
    | some v => cases v <;> simp [seq, coalesceValues, Value.toVarchar, Value.text]

theorem quotePart_eval (hrow : row.WellTyped) (p : Expr) :
    eval eng row (quotePart p) = (eval eng row p).map fun v => .str v.text := by
  cases p <;> first | exact toText_eval hrow _ | simp [quotePart, eval, Value.text]

theorem concatValues_two_str (a b : String) :
    concatValues eng [.str a, .str b] = some (.str (a ++ b)) := by
  unfold concatValues
  cases eng.concatSkipsNull <;> simp [Value.fits, Value.isNull, Value.text, cat]

/-- Evaluates to a string (or an error). -/
def IsStr (o : Option Value) : Prop := ∀ v, o = some v → ∃ s, v = .str s

theorem concat2_eval {a b : Expr} (ha : IsStr (eval eng row a)) (hb : IsStr (eval eng row b)) :
    eval eng row (concat2 a b) =
      (eval eng row a).bind fun va => (eval eng row b).map fun vb => .str (va.text ++ vb.text) := by
  simp only [concat2, eval, evalEach, args2, callEval, ite_true]
  cases hva : eval eng row a with
  | none => simp [seq]
  | some va =>
    cases hvb : eval eng row b with
    | none => simp [seq]
    | some vb =>
      obtain ⟨s, rfl⟩ := ha va hva
      obtain ⟨t, rfl⟩ := hb vb hvb
      simp [seq, concatValues_two_str, Value.text]

theorem isStr_map_text (o : Option Value) : IsStr (o.map fun v => .str v.text) := by
  intro v hv
  cases o with
  | none => cases hv
  | some w => simp at hv; exact ⟨w.text, hv.symm⟩

/-! ## Each rule preserves types and values -/

/-- Same static type and same value on `eng` and `row`. -/
def Same (eng : Engine) (row : Row) (a b : Expr) : Prop :=
  typeOf a = typeOf b ∧ eval eng row a = eval eng row b

/-- `RewriteStringConcat` keeps the value of every expression, on every engine. -/
theorem concatRule_sound (hrow : row.WellTyped) : ∀ x, Same eng row (concatRule x) x
  | .add l r => by
    simp only [concatRule]
    split
    · rename_i hs
      refine ⟨by simp [concat2, typeOf, callType, hs], ?_⟩
      rw [concat2_eval (by rw [toText_eval hrow]; exact isStr_map_text _)
        (by rw [toText_eval hrow]; exact isStr_map_text _)]
      simp only [toText_eval hrow, eval, hs, ite_true]
      cases eval eng row l <;> cases eval eng row r <;> simp [Value.text]
    · exact ⟨rfl, rfl⟩
  | .lit _ | .col _ _ | .interp _ _ | .part _ | .call _ _ | .ifExpr _ _ _ | .cast _ => ⟨rfl, rfl⟩

theorem typeOf_foldRest : ∀ (rest : Args) (acc : Expr), typeOf acc = .string →
    typeOf (foldRest acc rest) = .string
  | .nil, _, h => h
  | .cons _ rest, _, _ => typeOf_foldRest rest _ (by simp [concat2, typeOf, callType])

theorem foldRest_eval (hrow : row.WellTyped) : ∀ (rest : Args) (acc : Expr),
    IsStr (eval eng row acc) →
    eval eng row (foldRest acc rest) = (eval eng row acc).bind fun va =>
      (seq (evalEach eng row rest)).map fun vs => .str (va.text ++ cat (vs.map Value.text))
  | .nil, acc, h => by
    simp only [foldRest, evalEach, seq]
    cases hv : eval eng row acc with
    | none => rfl
    | some v => obtain ⟨s, rfl⟩ := h v hv; simp [cat]
  | .cons p rest, acc, h => by
    have hq : IsStr (eval eng row (quotePart p)) := by
      rw [quotePart_eval hrow]; exact isStr_map_text _
    have hc := concat2_eval h hq
    simp only [foldRest]
    rw [foldRest_eval hrow rest _ (by
      rw [hc]; intro v hv
      cases hva : eval eng row acc <;> simp_all
      obtain ⟨_, _, rfl⟩ := hv; exact ⟨_, rfl⟩)]
    rw [hc, quotePart_eval hrow]
    simp only [evalEach]
    cases eval eng row acc with
    | none => rfl
    | some va =>
      cases eval eng row p with
      | none => simp [seq]
      | some vp =>
        cases hs : seq (evalEach eng row rest) with
        | none => simp [seq, hs]
        | some vs => simp [seq, hs, cat, String.append_assoc]

theorem foldParts_eval (hrow : row.WellTyped) (parts : Args) :
    eval eng row (foldParts parts) =
      (seq (evalEach eng row parts)).map fun vs => .str (cat (vs.map Value.text)) := by
  cases parts with
  | nil => simp [foldParts, eval, evalEach, seq, cat]
  | cons p rest =>
    simp only [foldParts]
    rw [foldRest_eval hrow rest _ (by rw [quotePart_eval hrow]; exact isStr_map_text _),
      quotePart_eval hrow]
    simp only [evalEach]
    cases eval eng row p with
    | none => simp [seq]
    | some vp =>
      cases hs : seq (evalEach eng row rest) with
      | none => simp [seq, hs]
      | some vs => simp [seq, hs, cat]

theorem typeOf_foldParts (parts : Args) : typeOf (foldParts parts) = .string := by
  cases parts with
  | nil => rfl
  | cons p rest => exact typeOf_foldRest rest _ (typeOf_quotePart p)

/-- `RewriteStringInterpolation` keeps the value of every expression, on every engine. -/
theorem interpRule_sound (hrow : row.WellTyped) : ∀ x, Same eng row (interpRule x) x
  | .interp pfx parts => by
    simp only [interpRule]
    split
    · rename_i hp
      exact ⟨typeOf_foldParts parts, by rw [foldParts_eval hrow]; simp [eval, hp]⟩
    · exact ⟨rfl, rfl⟩
  | .lit _ | .col _ _ | .add _ _ | .part _ | .call _ _ | .ifExpr _ _ _ | .cast _ => ⟨rfl, rfl⟩

/-- `RewriteIfExpr` keeps the value of every expression on every engine with an `if` function. -/
theorem ifRule_sound (hif : eng.hasIf = true) : ∀ x, Same eng row (ifRule x) x
  | .call f (.cons c (.cons t .nil)) => by
    simp only [ifRule]
    split
    · rename_i hf
      exact ⟨by simp [typeOf, argTypes, callType, hf, Value.ty],
        by simp [eval, evalEach, callEval, hf, hif]⟩
    · exact ⟨rfl, rfl⟩
  | .call f (.cons c (.cons t (.cons e .nil))) => by
    simp only [ifRule]
    split
    · rename_i hf
      exact ⟨by simp [typeOf, argTypes, callType, hf],
        by simp [eval, evalEach, callEval, hf, hif]⟩
    · exact ⟨rfl, rfl⟩
  | .call _ .nil | .call _ (.cons _ .nil) | .call _ (.cons _ (.cons _ (.cons _ (.cons _ _)))) =>
    ⟨rfl, rfl⟩
  | .lit _ | .col _ _ | .add _ _ | .interp _ _ | .part _ | .ifExpr _ _ _ | .cast _ => ⟨rfl, rfl⟩

/-! ## Bottom-up passes and the phase -/

mutual
theorem bottomUp_same {r : Expr → Expr} (h : ∀ x, Same eng row (r x) x) :
    ∀ e, Same eng row (bottomUp r e) e
  | .lit v => h (.lit v)
  | .col n t => h (.col n t)
  | .part s => h (.part s)
  | .add l rr => by
    obtain ⟨t1, e1⟩ := bottomUp_same h l
    obtain ⟨t2, e2⟩ := bottomUp_same h rr
    obtain ⟨t0, e0⟩ := h (.add (bottomUp r l) (bottomUp r rr))
    simp only [bottomUp]
    exact ⟨t0.trans (by simp only [typeOf, t1, t2]), e0.trans (by simp only [eval, t1, t2, e1, e2])⟩
  | .interp p ps => by
    obtain ⟨_, e1⟩ := bottomUpArgs_same h ps
    obtain ⟨t0, e0⟩ := h (.interp p (bottomUpArgs r ps))
    simp only [bottomUp]
    exact ⟨t0, e0.trans (by simp only [eval, e1])⟩
  | .call f as => by
    obtain ⟨t1, e1⟩ := bottomUpArgs_same h as
    obtain ⟨t0, e0⟩ := h (.call f (bottomUpArgs r as))
    simp only [bottomUp]
    exact ⟨t0.trans (by simp only [typeOf, t1]), e0.trans (by simp only [eval, e1])⟩
  | .ifExpr c t e => by
    obtain ⟨_, e1⟩ := bottomUp_same h c
    obtain ⟨t2, e2⟩ := bottomUp_same h t
    obtain ⟨t3, e3⟩ := bottomUp_same h e
    obtain ⟨t0, e0⟩ := h (.ifExpr (bottomUp r c) (bottomUp r t) (bottomUp r e))
    simp only [bottomUp]
    exact ⟨t0.trans (by simp only [typeOf, t2, t3]), e0.trans (by simp only [eval, e1, e2, e3])⟩
  | .cast e => by
    obtain ⟨_, e1⟩ := bottomUp_same h e
    obtain ⟨t0, e0⟩ := h (.cast (bottomUp r e))
    simp only [bottomUp]
    exact ⟨t0, e0.trans (by simp only [eval, e1])⟩

theorem bottomUpArgs_same {r : Expr → Expr} (h : ∀ x, Same eng row (r x) x) :
    ∀ as, argTypes (bottomUpArgs r as) = argTypes as ∧
      evalEach eng row (bottomUpArgs r as) = evalEach eng row as
  | .nil => ⟨rfl, rfl⟩
  | .cons e rest => by
    obtain ⟨t1, e1⟩ := bottomUp_same h e
    obtain ⟨t2, e2⟩ := bottomUpArgs_same h rest
    exact ⟨by simp only [bottomUpArgs, argTypes, t1, t2], by simp only [bottomUpArgs, evalEach, e1, e2]⟩
end

/-- The `rewrite-expr` phase keeps the static type and the value of every expression, NULL inputs
included, on every well-typed row and every engine with an `if` function. -/
theorem phase_sound (hrow : row.WellTyped) (hif : eng.hasIf = true) (e : Expr) :
    Same eng row (phase e) e := by
  obtain ⟨t1, e1⟩ := bottomUp_same (concatRule_sound (eng := eng) hrow) e
  obtain ⟨t2, e2⟩ := bottomUp_same (interpRule_sound (eng := eng) hrow) (bottomUp concatRule e)
  obtain ⟨t3, e3⟩ := bottomUp_same (ifRule_sound (row := row) hif)
    (bottomUp interpRule (bottomUp concatRule e))
  exact ⟨t3.trans (t2.trans t1), e3.trans (e2.trans e1)⟩

theorem phase_eval (hrow : row.WellTyped) (hif : eng.hasIf = true) (e : Expr) :
    eval eng row (phase e) = eval eng row e :=
  (phase_sound hrow hif e).2

end Wvlet.Rewrite
