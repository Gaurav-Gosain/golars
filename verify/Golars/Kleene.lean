/-!
# Three-valued (Kleene) logic

A boolean column value is `Option Bool`: `none` is null. polars (and
golars) use Kleene logic: `false` absorbs `and`, `true` absorbs `or`, and
every other combination with a null is null. A filter keeps a row only
when the predicate is `some true`, so null counts as false.
-/

namespace Golars.Kleene

abbrev KBool := Option Bool

def kand : KBool → KBool → KBool
  | some false, _ => some false
  | _, some false => some false
  | some true, some true => some true
  | _, _ => none

def kor : KBool → KBool → KBool
  | some true, _ => some true
  | _, some true => some true
  | some false, some false => some false
  | _, _ => none

def knot : KBool → KBool
  | some b => some (!b)
  | none => none

/-- The filter rule: a row is kept only when the predicate is true. -/
def keep (p : KBool) : Bool := p == some true

/-- The truth tables observed from polars 1.39.3 for `a & b`, `a | b` and
`~a`, listed as `(a, b, a & b, a | b)` over all nine input pairs. -/
def polarsTable : List (KBool × KBool × KBool × KBool) :=
  [ (some true,  some true,  some true,  some true)
  , (some false, some true,  some false, some true)
  , (none,       some true,  none,       some true)
  , (some true,  some false, some false, some true)
  , (some false, some false, some false, some false)
  , (none,       some false, some false, none)
  , (some true,  none,       none,       some true)
  , (some false, none,       some false, none)
  , (none,       none,       none,       none) ]

def polarsNot : List (KBool × KBool) :=
  [(some true, some false), (some false, some true), (none, none)]

def all3 : List KBool := [some true, some false, none]

theorem and_matches_polars : ∀ r ∈ polarsTable, kand r.1 r.2.1 = r.2.2.1 := by decide
theorem or_matches_polars : ∀ r ∈ polarsTable, kor r.1 r.2.1 = r.2.2.2 := by decide
theorem not_matches_polars : ∀ r ∈ polarsNot, knot r.1 = r.2 := by decide
theorem table_complete (a b : KBool) : ∃ r ∈ polarsTable, r.1 = a ∧ r.2.1 = b := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> decide

theorem kand_comm (a b : KBool) : kand a b = kand b a := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl
theorem kor_comm (a b : KBool) : kor a b = kor b a := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl
theorem kand_assoc (a b c : KBool) : kand (kand a b) c = kand a (kand b c) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rcases c with _ | _ | _ <;> rfl
theorem kor_assoc (a b c : KBool) : kor (kor a b) c = kor a (kor b c) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rcases c with _ | _ | _ <;> rfl
theorem demorgan_and (a b : KBool) : knot (kand a b) = kor (knot a) (knot b) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl
theorem demorgan_or (a b : KBool) : knot (kor a b) = kand (knot a) (knot b) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl
theorem knot_knot (a : KBool) : knot (knot a) = a := by
  rcases a with _ | _ | _ <;> rfl
theorem kand_distrib (a b c : KBool) : kand a (kor b c) = kor (kand a b) (kand a c) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rcases c with _ | _ | _ <;> rfl
theorem kor_distrib (a b c : KBool) : kor a (kand b c) = kand (kor a b) (kor a c) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rcases c with _ | _ | _ <;> rfl

/-- A conjunction keeps exactly the rows both predicates keep. This is
why consecutive filters may be fused into one `and` predicate. -/
theorem keep_kand (a b : KBool) : keep (kand a b) = (keep a && keep b) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl

theorem keep_kor (a b : KBool) : keep (kor a b) = (keep a || keep b) := by
  rcases a with _ | _ | _ <;> rcases b with _ | _ | _ <;> rfl

/-- Null is not true, so the filter rule drops it. -/
theorem keep_null : keep none = false := rfl

/-- `filter(~p)` is not the complement of `filter(p)`: a null row is
dropped by both. Rewrites must not turn one into the other. -/
theorem keep_not_not_complement : ∃ a : KBool, keep (knot a) ≠ !keep a := ⟨none, by decide⟩

/-- The law of excluded middle fails for null. -/
theorem excluded_middle_fails : kor none (knot none) ≠ some true := by decide

end Golars.Kleene
