import Golars.Agg

/-!
# Joins

Left rows have type `α` and right rows type `β`; `kl` and `kr` give the
(nullable) join key. As in polars (default `nulls_equal=False`) a null
key matchRows nothing.

The specification is the nested-loop join. The hash join builds a table
of right rows grouped by key (the hash group-by of `Golars.Agg`) and
probes it with each left row.
-/

namespace Golars.Join

open List Golars.Agg

variable {α β K : Type} [DecidableEq K] (kl : α → Option K) (kr : β → Option K)

/-- Right rows whose key equals the (non-null) key of `a`. -/
def matchRows (r : List β) (a : α) : List β :=
  match kl a with
  | none => []
  | some k => r.filter (fun b => decide (kr b = some k))

/-- Nested-loop inner join: the specification. -/
def innerNL (l : List α) (r : List β) : List (α × β) :=
  l.flatMap (fun a => (matchRows kl kr r a).map (fun b => (a, b)))

/-- Hash table: right rows with a non-null key, grouped by key. -/
def build (r : List β) : List (Option K × List β) :=
  hashGroupBy kr (r.filter (fun b => (kr b).isSome))

def lookup (t : List (Option K × List β)) (k : Option K) : List β :=
  match t.find? (fun g => g.1 == k) with
  | some g => g.2
  | none => []

/-- Hash inner join: probe the table with every non-null left key. -/
def innerHash (l : List α) (r : List β) : List (α × β) :=
  let t := build kr r
  l.flatMap (fun a =>
    match kl a with
    | none => []
    | some k => (lookup t (some k)).map (fun b => (a, b)))

theorem find_groupsOver {γ : Type} (key : γ → Option K) (s : List γ) (ks : List (Option K))
    (k : Option K) :
    (groupsOver key s ks).find? (fun g => g.1 == k) =
      if k ∈ ks then some (k, group key s k) else none := by
  induction ks with
  | nil => rfl
  | cons h t ih =>
    rw [groupsOver, map_cons, find?_cons]
    by_cases e : h = k
    · subst e; simp
    · have : (h == k) = false := by simp [e]
      simp only [this]
      rw [← groupsOver, ih]
      by_cases hk : k ∈ t <;> simp [hk, Ne.symm e]

theorem lookup_build (r : List β) (k : K) :
    lookup (build kr r) (some k) = r.filter (fun b => decide (kr b = some k)) := by
  unfold lookup build
  rw [hashGroupBy_eq, find_groupsOver]
  have eg : group kr (r.filter (fun b => (kr b).isSome)) (some k) = r.filter (fun b => decide (kr b = some k)) := by
    unfold group; rw [filter_filter]; apply filter_congr; intro b _
    cases kr b with
    | none => simp
    | some v =>
      by_cases e : v = k
      · subst e; simp
      · simp [e]
  by_cases hm : some k ∈ firstKeys kr (r.filter (fun b => (kr b).isSome))
  · simp only [hm, ite_true]; exact eg
  · simp only [hm, ite_false]
    rw [← eg]
    apply (filter_eq_nil_iff.mpr _).symm
    intro b hb e
    exact hm ((mem_firstKeys kr _ _).mpr ⟨b, hb, by simpa using e⟩)

/-- **Hash inner join equals the nested-loop join**, row for row (and so
also as a multiset). -/
theorem innerHash_eq (l : List α) (r : List β) : innerHash kl kr l r = innerNL kl kr l r := by
  unfold innerHash innerNL matchRows
  apply congrArg (fun f => l.flatMap f); funext a
  cases kl a with
  | none => rfl
  | some k => simp only [lookup_build]

/-- Left join: every left row, with its matchRows or a single null match. -/
def leftJoin (l : List α) (r : List β) : List (α × Option β) :=
  l.flatMap (fun a =>
    match matchRows kl kr r a with
    | [] => [(a, none)]
    | ms => ms.map (fun b => (a, some b)))

/-- **Left join keeps every left row.** -/
theorem leftJoin_keeps (l : List α) (r : List β) (a : α) (ha : a ∈ l) :
    ∃ o, (a, o) ∈ leftJoin kl kr l r := by
  unfold leftJoin
  cases h : matchRows kl kr r a with
  | nil => exact ⟨none, mem_flatMap.mpr ⟨a, ha, by simp [h]⟩⟩
  | cons b bs => exact ⟨some b, mem_flatMap.mpr ⟨a, ha, by simp [h]⟩⟩

/-- The matched rows of a left join are exactly the inner join. -/
theorem leftJoin_inner (l : List α) (r : List β) :
    (leftJoin kl kr l r).filterMap (fun p => p.2.map (fun b => (p.1, b))) = innerNL kl kr l r := by
  unfold leftJoin innerNL
  rw [filterMap_flatMap]
  apply congrArg (fun f => l.flatMap f); funext a
  cases h : matchRows kl kr r a with
  | nil => rfl
  | cons b bs =>
    simp only [map_cons, filterMap_cons, Option.map_some, cons.injEq, true_and]
    clear h
    induction bs with
    | nil => rfl
    | cons c cs ih => simp only [map_cons, filterMap_cons, Option.map_some]; rw [ih]

/-- Every left row appears at least once in a left join. -/
theorem leftJoin_length (l : List α) (r : List β) : l.length ≤ (leftJoin kl kr l r).length := by
  unfold leftJoin
  induction l with
  | nil => simp
  | cons a l ih =>
    simp only [flatMap_cons, length_append, length_cons]
    have : 1 ≤ (match matchRows kl kr r a with
        | [] => [(a, none)]
        | ms => ms.map (fun b => (a, some b))).length := by
      cases matchRows kl kr r a <;> simp
    omega

def semiJoin (l : List α) (r : List β) : List α := l.filter (fun a => !(matchRows kl kr r a).isEmpty)
def antiJoin (l : List α) (r : List β) : List α := l.filter (fun a => (matchRows kl kr r a).isEmpty)

/-- **Semi and anti joins partition the left side.** -/
theorem semi_anti_partition (l : List α) (r : List β) :
    (semiJoin kl kr l r ++ antiJoin kl kr l r).Perm l := by
  unfold semiJoin antiJoin
  have := filter_append_perm (fun a => !(matchRows kl kr r a).isEmpty) l
  simpa using this

theorem semi_anti_disjoint (l : List α) (r : List β) (a : α) :
    ¬ (a ∈ semiJoin kl kr l r ∧ a ∈ antiJoin kl kr l r) := by
  simp [semiJoin, antiJoin]
  intro _ h1 _ h2; simp [h2] at h1

/-- A semi join keeps exactly the left rows the inner join uses. -/
theorem semi_mem (l : List α) (r : List β) (a : α) :
    a ∈ semiJoin kl kr l r ↔ a ∈ l ∧ ∃ b, (a, b) ∈ innerNL kl kr l r := by
  simp only [semiJoin, mem_filter, innerNL, mem_flatMap, mem_map]
  constructor
  · rintro ⟨ha, hm⟩
    cases h : matchRows kl kr r a with
    | nil => simp [h] at hm
    | cons b bs => exact ⟨ha, b, a, ha, b, by simp [h], rfl⟩
  · rintro ⟨ha, b, a', _, b', hb, e⟩
    simp only [Prod.mk.injEq] at e
    obtain ⟨rfl, rfl⟩ := e
    refine ⟨ha, ?_⟩
    cases h : matchRows kl kr r a' with
    | nil => simp [h] at hb
    | cons _ _ => rfl

end Golars.Join
