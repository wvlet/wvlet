/-
Licensed under the Apache License, Version 2.0.

Why the rule carries a conjunct only from the preserved input of an outer join. A conjunct over the
NULL-supplying input's key reads NULL on the padded rows, so filtering the preserved input by it
changes the result: here `rk IS NULL` above `l LEFT JOIN r` keeps the padded row for `lk = 1`,
while filtering `l` by `lk IS NULL` drops it.
-/
import WvletFormal.JoinKeyFilter.Sound

namespace Wvlet.JoinKeyFilter

/-- `k IS NULL`. -/
def isNullP : Pred := fun v => if v.isNone then .tt else .ff

theorem carry_from_null_supplying_side_unsound :
    (join (fun _ _ => true) .left [⟨some 1, 0⟩] []).filter (fun o => holds isNullP (rightKey o)) ≠
      join (fun _ _ => true) .left (keep isNullP [⟨some 1, 0⟩]) (keep isNullP []) := by
  decide

end Wvlet.JoinKeyFilter
