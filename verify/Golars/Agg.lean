import Golars.Sort

/-!
# Aggregation

* Partial aggregation: sum (ints), count, min, max and mean as a
  `(sum, count)` pair are monoid homomorphisms from list concatenation,
  so merging per-chunk (or per-thread) partial results equals aggregating
  the concatenation. Float sum is not associative; golars only promises a
  tolerance there and nothing about it is proved here.
* Float min and max follow polars: NaN is ignored unless every non-null
  value is NaN.
* Hash group-by (groups in first-occurrence order) and sort group-by
  (groups in key order) produce the same `(key, group)` pairs up to
  order, and each group keeps its rows in input order.
-/

namespace Golars.Agg

open List

abbrev Val := Option Int

/-! ## Monoid homomorphisms -/

/-- A reduction `h` that maps concatenation to `op` with unit `e`. -/
structure Hom {α β : Type} (h : List α → β) (op : β → β → β) (e : β) : Prop where
  nil : h [] = e
  append : ∀ a b, h (a ++ b) = op (h a) (h b)
  unit : ∀ x, op e x = x

/-- Merging per-chunk partial results equals reducing the concatenation. -/
theorem Hom.chunks {α β : Type} {h : List α → β} {op : β → β → β} {e : β} (H : Hom h op e) :
    ∀ cs : List (List α), h cs.flatten = (cs.map h).foldr op e
  | [] => H.nil
  | c :: cs => by simp [flatten_cons, H.append, H.chunks cs]

def sumI (l : List Val) : Int := l.foldr (fun v s => v.getD 0 + s) 0
def countI (l : List Val) : Nat := (l.filter Option.isSome).length

def mergeMin : Val → Val → Val
  | none, b => b
  | a, none => a
  | some x, some y => some (min x y)

def mergeMax : Val → Val → Val
  | none, b => b
  | a, none => a
  | some x, some y => some (max x y)

def minI (l : List Val) : Val := l.foldr mergeMin none
def maxI (l : List Val) : Val := l.foldr mergeMax none

theorem sum_hom : Hom sumI (· + ·) 0 :=
  ⟨rfl, fun a b => by induction a <;> simp_all [sumI]; omega, fun x => by simp⟩

theorem count_hom : Hom countI (· + ·) 0 :=
  ⟨rfl, fun a b => by simp [countI, filter_append], fun x => by simp⟩

theorem mergeMin_assoc (a b c : Val) : mergeMin (mergeMin a b) c = mergeMin a (mergeMin b c) := by
  cases a <;> cases b <;> cases c <;> simp [mergeMin, Int.min_assoc]

theorem mergeMax_assoc (a b c : Val) : mergeMax (mergeMax a b) c = mergeMax a (mergeMax b c) := by
  cases a <;> cases b <;> cases c <;> simp [mergeMax, Int.max_assoc]

theorem min_hom : Hom minI mergeMin none :=
  ⟨rfl, fun a b => by
    induction a with
    | nil => simp [minI, mergeMin]
    | cons x a ih => simp only [minI, cons_append, foldr_cons] at *; rw [ih, mergeMin_assoc],
   fun x => by cases x <;> rfl⟩

theorem max_hom : Hom maxI mergeMax none :=
  ⟨rfl, fun a b => by
    induction a with
    | nil => simp [maxI, mergeMax]
    | cons x a ih => simp only [maxI, cons_append, foldr_cons] at *; rw [ih, mergeMax_assoc],
   fun x => by cases x <;> rfl⟩

/-- Mean is carried as `(sum, count)`; the final value is `sum / count`
(null when the count is 0). -/
def meanState (l : List Val) : Int × Nat := (sumI l, countI l)

theorem mean_hom : Hom meanState (fun a b => (a.1 + b.1, a.2 + b.2)) (0, 0) :=
  ⟨rfl, fun a b => by simp [meanState, sum_hom.append, count_hom.append], fun x => by simp⟩

/-! ## Float min and max with NaN -/

/-- A float value for min/max purposes: NaN or an ordered number. -/
inductive FV where
  | nan
  | num (x : Int)
  deriving DecidableEq, Repr

def fMerge (isMin : Bool) : Option FV → Option FV → Option FV
  | none, b => b
  | a, none => a
  | some .nan, b => b
  | a, some .nan => a
  | some (.num x), some (.num y) => some (.num (if isMin then min x y else max x y))

def fAgg (isMin : Bool) (l : List (Option FV)) : Option FV := l.foldr (fMerge isMin) none

def nums (l : List (Option FV)) : List Int :=
  l.filterMap (fun v => match v with | some (.num x) => some x | _ => none)

/-- The polars rule: the min (max) of the non-NaN values; NaN when every
non-null value is NaN; null when there is no non-null value. -/
def fSpec (isMin : Bool) (l : List (Option FV)) : Option FV :=
  match nums l with
  | x :: xs => some (.num (xs.foldl (fun a b => if isMin then min a b else max a b) x))
  | [] => if l.any (· == some .nan) then some .nan else none

theorem fMerge_assoc (m : Bool) (a b c : Option FV) :
    fMerge m (fMerge m a b) c = fMerge m a (fMerge m b c) := by
  rcases a with _ | _ | x <;> rcases b with _ | _ | y <;> rcases c with _ | _ | z <;>
    cases m <;> simp [fMerge, Int.min_assoc, Int.max_assoc]

theorem fAgg_hom (m : Bool) : Hom (fAgg m) (fMerge m) none :=
  ⟨rfl, fun a b => by
    induction a with
    | nil => simp [fAgg, fMerge]
    | cons x a ih => simp only [fAgg, cons_append, foldr_cons] at *; rw [ih, fMerge_assoc],
   fun x => by cases x <;> rfl⟩

theorem foldl_minmax_cons (m : Bool) (x y : Int) (ys : List Int) :
    (y :: ys).foldl (fun a b => if m then min a b else max a b) x =
      (if m then min x ((ys).foldl (fun a b => if m then min a b else max a b) y)
       else max x ((ys).foldl (fun a b => if m then min a b else max a b) y)) := by
  induction ys generalizing x y with
  | nil => simp
  | cons z zs ih =>
    simp only [foldl_cons] at *
    rw [ih, ih]
    cases m <;> simp [Int.min_assoc, Int.max_assoc]

/-- **NaN rule.** The chunk-mergeable reduction equals the polars rule. -/
theorem fAgg_eq_spec (m : Bool) (l : List (Option FV)) : fAgg m l = fSpec m l := by
  induction l with
  | nil => rfl
  | cons v l ih =>
    simp only [fAgg, foldr_cons] at *
    rw [ih]
    unfold fSpec
    rcases v with _ | _ | x
    · simp [nums, fMerge]
    · simp only [nums, filterMap_cons, any_cons]
      cases h : filterMap _ l with
      | nil => cases l.any (· == some .nan) <;> simp [fMerge]
      | cons y ys => simp [fMerge]
    · simp only [nums, filterMap_cons]
      cases h : filterMap (fun v => match v with | some (.num x) => some x | _ => none) l with
      | nil => cases l.any (· == some .nan) <;> simp [fMerge]
      | cons y ys => simp only [fMerge]; rw [foldl_minmax_cons]

/-! ## Group-by -/

section GroupBy

variable {α K : Type} [DecidableEq K] (key : α → K)

def group (l : List α) (k : K) : List α := l.filter (fun x => key x == k)

/-! ### Hash group-by -/

def hashInsert (acc : List (K × List α)) (x : α) : List (K × List α) :=
  if acc.any (fun g => g.1 == key x) then
    acc.map (fun g => if g.1 == key x then (g.1, g.2 ++ [x]) else g)
  else acc ++ [(key x, [x])]

/-- Hash group-by: one pass, groups in first-occurrence order. -/
def hashGroupBy (l : List α) : List (K × List α) := l.foldl (hashInsert key) []

def fkStep (ks : List K) (x : α) : List K := if key x ∈ ks then ks else ks ++ [key x]

/-- Distinct keys in first-occurrence order. -/
def firstKeys (l : List α) : List K := l.foldl (fkStep key) []

theorem firstKeys_snoc (l : List α) (x : α) :
    firstKeys key (l ++ [x]) = fkStep key (firstKeys key l) x := by
  simp [firstKeys, foldl_append]

theorem mem_foldl_fkStep (l : List α) : ∀ (ks : List K) (k : K),
    k ∈ l.foldl (fkStep key) ks ↔ k ∈ ks ∨ ∃ y ∈ l, key y = k := by
  induction l with
  | nil => intro ks k; simp
  | cons x l ih =>
    intro ks k
    rw [foldl_cons, ih]
    unfold fkStep
    by_cases hx : key x ∈ ks
    · simp only [hx, ite_true, mem_cons]
      constructor
      · rintro (h | ⟨y, hy, e⟩)
        · exact Or.inl h
        · exact Or.inr ⟨y, Or.inr hy, e⟩
      · rintro (h | ⟨y, rfl | hy, e⟩)
        · exact Or.inl h
        · exact Or.inl (by rw [← e]; exact hx)
        · exact Or.inr ⟨y, hy, e⟩
    · simp only [hx, ite_false, mem_append, mem_singleton, mem_cons, not_mem_nil, or_false]
      constructor
      · rintro ((h | h) | ⟨y, hy, e⟩)
        · exact Or.inl h
        · exact Or.inr ⟨x, Or.inl rfl, by first | exact h | exact h.symm⟩
        · exact Or.inr ⟨y, Or.inr hy, e⟩
      · rintro (h | ⟨y, rfl | hy, e⟩)
        · exact Or.inl (Or.inl h)
        · exact Or.inl (Or.inr (by first | exact e | exact e.symm))
        · exact Or.inr ⟨y, hy, e⟩

theorem mem_firstKeys (l : List α) (k : K) : k ∈ firstKeys key l ↔ ∃ y ∈ l, key y = k := by
  unfold firstKeys; rw [mem_foldl_fkStep]; simp

theorem nodup_foldl_fkStep (l : List α) : ∀ ks : List K, ks.Nodup → (l.foldl (fkStep key) ks).Nodup := by
  induction l with
  | nil => intro ks h; exact h
  | cons x l ih =>
    intro ks h
    rw [foldl_cons]; apply ih
    unfold fkStep
    split
    · exact h
    · rename_i hx
      exact nodup_append.mpr ⟨h, by simp, by
        intro a ha b hb; simp at hb; subst hb; intro e; subst e; exact hx ha⟩

theorem firstKeys_nodup (l : List α) : (firstKeys key l).Nodup :=
  nodup_foldl_fkStep key l [] nodup_nil

/-- The canonical form of a grouping over the key list `ks`. -/
def groupsOver (l : List α) (ks : List K) : List (K × List α) := ks.map (fun k => (k, group key l k))

theorem any_groupsOver (l : List α) (ks : List K) (k : K) :
    (groupsOver key l ks).any (fun g => g.1 == k) = decide (k ∈ ks) := by
  induction ks with
  | nil => rfl
  | cons h t ih =>
    simp only [groupsOver, map_cons, any_cons] at ih ⊢
    rw [ih]
    by_cases e : h = k
    · subst e; simp
    · simp [e, Ne.symm e]

theorem hashInsert_step (l : List α) (x : α) :
    hashInsert key (groupsOver key l (firstKeys key l)) x =
      groupsOver key (l ++ [x]) (firstKeys key (l ++ [x])) := by
  rw [firstKeys_snoc]
  unfold hashInsert fkStep
  rw [any_groupsOver]
  by_cases hm : key x ∈ firstKeys key l
  · simp only [hm, decide_true, ite_true, groupsOver, map_map]
    apply map_congr_left; intro k _
    simp only [Function.comp, group, filter_append, filter_cons, filter_nil]
    by_cases e : k = key x
    · subst e; simp
    · simp [e, Ne.symm e]
  · simp only [hm, decide_false, Bool.false_eq_true, ite_false, groupsOver, map_append, map_cons,
      map_nil]
    congr 1
    · apply map_congr_left; intro k hk
      have : k ≠ key x := by intro e; subst e; exact hm hk
      simp [group, filter_append, Ne.symm this]
    · have : group key l (key x) = [] := by
        apply filter_eq_nil_iff.mpr; intro y hy
        simp only [beq_iff_eq]; intro e
        exact hm ((mem_firstKeys key l _).mpr ⟨y, hy, e⟩)
      simp [group, filter_append] at this ⊢; exact this

/-- **Hash group-by characterisation.** Groups appear in first-occurrence
order and each group holds its rows in input order. -/
theorem hashGroupBy_eq (l : List α) :
    hashGroupBy key l = groupsOver key l (firstKeys key l) := by
  have gen : ∀ pre, (l.foldl (hashInsert key) (groupsOver key pre (firstKeys key pre))) =
      groupsOver key (pre ++ l) (firstKeys key (pre ++ l)) := by
    induction l with
    | nil => intro pre; simp
    | cons x l ih =>
      intro pre
      rw [foldl_cons, hashInsert_step, ih]
      simp
  have := gen []
  simpa [groupsOver, firstKeys, hashGroupBy] using this

/-! ### Sort group-by -/

variable (le : K → K → Bool)

/-- Split a key-sorted list into runs of equal keys. -/
def runs : List α → List (K × List α)
  | [] => []
  | x :: xs =>
    match runs xs with
    | (k, g) :: rest => if key x = k then (k, x :: g) :: rest else (key x, [x]) :: (k, g) :: rest
    | [] => [(key x, [x])]

/-- Sort group-by: stable sort by key, then cut into runs. -/
def sortGroupBy (l : List α) : List (K × List α) :=
  runs key (l.mergeSort (fun a b => le (key a) (key b)))

theorem runs_cons (x : α) (xs : List α) : runs key (x :: xs) =
    match runs key xs with
    | (k, g) :: rest => if key x = k then (k, x :: g) :: rest else (key x, [x]) :: (k, g) :: rest
    | [] => [(key x, [x])] := rfl

/-- Invariant of `runs` on a key-sorted list. -/
def RunsInv (s : List α) : Prop :=
  runs key s = groupsOver key s ((runs key s).map (·.1)) ∧
  ((runs key s).map (·.1)).Nodup ∧
  (∀ k, k ∈ (runs key s).map (·.1) ↔ ∃ y ∈ s, key y = k) ∧
  (∀ x xs, s = x :: xs → ((runs key s).map (·.1)).head? = some (key x))

theorem group_cons_ne (x : α) (l : List α) (k : K) (h : key x ≠ k) :
    group key (x :: l) k = group key l k := by
  simp [group, filter_cons, h]

theorem group_cons_eq (x : α) (l : List α) :
    group key (x :: l) (key x) = x :: group key l (key x) := by
  simp [group, filter_cons]

theorem runs_spec (antisymm : ∀ a b, le a b = true → le b a = true → a = b)
    (total : ∀ a b, (le a b || le b a) = true) :
    ∀ s : List α, s.Pairwise (fun a b => le (key a) (key b) = true) → RunsInv key s
  | [], _ => by
    refine ⟨rfl, nodup_nil, fun k => by simp [runs], fun x xs h => by simp at h⟩
  | x :: xs, hs => by
    have hx := (pairwise_cons.mp hs).1
    obtain ⟨e1, n1, m1, h1⟩ := runs_spec antisymm total xs (pairwise_cons.mp hs).2
    unfold RunsInv
    rw [runs_cons]
    cases hr : runs key xs with
    | nil =>
      have : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          have := (m1 (key y)).mpr ⟨y, mem_cons_self, rfl⟩
          rw [hr] at this; simp at this
      subst this
      refine ⟨by simp [groupsOver, group], by simp, fun k => by simp [eq_comm], ?_⟩
      intro a as h; simp at h; simp [h.1]
    | cons g rest =>
      obtain ⟨k, gs⟩ := g
      rw [hr] at e1 n1 m1 h1
      obtain ⟨y, ys, rfl⟩ : ∃ y ys, xs = y :: ys := by
        cases xs with
        | nil => simp [runs] at hr
        | cons y ys => exact ⟨y, ys, rfl⟩
      have hk : k = key y := by have := h1 y ys rfl; simpa using this
      subst hk
      have egs : gs = group key (y :: ys) (key y) := by
        have := congrArg (fun l => l.head?) e1
        simp [groupsOver] at this; exact this
      subst egs
      have erest : rest = groupsOver key (y :: ys) (rest.map (·.1)) := by
        have := congrArg (fun l => l.tail) e1
        simpa [groupsOver] using this
      have n1' : key y ∉ rest.map (·.1) ∧ (rest.map (·.1)).Nodup := by
        simpa using n1
      dsimp only
      by_cases hxk : key x = key y
      · simp only [hxk, ↓reduceIte]
        refine ⟨?_, n1, ?_, ?_⟩
        · simp only [map_cons, groupsOver]
          congr 1
          · rw [← hxk, group_cons_eq]
          · conv => lhs; rw [erest]
            simp only [groupsOver]
            apply map_congr_left; intro k hk
            have : key x ≠ k := by intro e; rw [← e, hxk] at hk; exact n1'.1 hk
            rw [group_cons_ne key x _ k this]
        · intro k
          have hm : ((key y, x :: group key (y :: ys) (key y)) :: rest).map (·.1) =
              ((key y, group key (y :: ys) (key y)) :: rest).map (·.1) := rfl
          rw [hm, m1]
          constructor
          · rintro ⟨z, hz, e⟩; exact ⟨z, mem_cons_of_mem _ hz, e⟩
          · rintro ⟨z, hz, e⟩
            rcases mem_cons.mp hz with rfl | hz
            · exact ⟨y, mem_cons_self, by rw [← e, hxk]⟩
            · exact ⟨z, hz, e⟩
        · intro a as h; simp at h; simp [← h.1, hxk]
      · simp only [hxk, ↓reduceIte]
        have notin : ∀ z ∈ y :: ys, key z ≠ key x := by
          intro z hz e
          have a1 : le (key x) (key y) = true := hx y mem_cons_self
          have a2 : le (key y) (key z) = true := by
            rcases mem_cons.mp hz with h | h
            · rw [h]; have := total (key y) (key y); simpa using this
            · exact (pairwise_cons.mp (pairwise_cons.mp hs).2).1 z h
          rw [e] at a2
          exact hxk (antisymm _ _ a1 a2)
        refine ⟨?_, ?_, ?_, ?_⟩
        · simp only [map_cons, groupsOver]
          congr 1
          · rw [group_cons_eq]
            have : group key (y :: ys) (key x) = [] :=
              filter_eq_nil_iff.mpr (fun z hz e => notin z hz (by simpa using e))
            rw [this]
          · congr 1
            · rw [group_cons_ne key x _ _ hxk]
            · conv => lhs; rw [erest]
              simp only [groupsOver]
              apply map_congr_left; intro k hk
              have : key x ≠ k := by
                intro e
                obtain ⟨z, hz, ez⟩ := (m1 k).mp (by simp [hk])
                exact notin z hz (by rw [ez, e])
              rw [group_cons_ne key x _ k this]
        · refine nodup_cons.mpr ⟨?_, n1⟩
          intro hin
          obtain ⟨z, hz, ez⟩ := (m1 (key x)).mp hin
          exact notin z hz ez
        · intro k
          rw [map_cons, mem_cons, m1]
          constructor
          · rintro (rfl | ⟨z, hz, e⟩)
            · exact ⟨x, mem_cons_self, rfl⟩
            · exact ⟨z, mem_cons_of_mem _ hz, e⟩
          · rintro ⟨z, hz, e⟩
            rcases mem_cons.mp hz with rfl | hz
            · exact Or.inl e.symm
            · exact Or.inr ⟨z, hz, e⟩
        · intro a as h; simp at h; simp [← h.1]

theorem equiv_key (antisymm : ∀ a b, le a b = true → le b a = true → a = b)
    (h : Golars.Sort.TotalPreorder le) (x y : α) :
    Golars.Sort.Equiv (fun a b : α => le (key a) (key b)) x y = (key y == key x) := by
  simp only [Golars.Sort.Equiv]
  by_cases e : key y = key x
  · rw [e]; simp [h.refl (key x)]
  · have : ¬ (le (key x) (key y) = true ∧ le (key y) (key x) = true) :=
      fun ⟨a, b⟩ => e (antisymm _ _ b a)
    rw [Bool.eq_iff_iff]; simp only [Bool.and_eq_true, beq_iff_eq]
    constructor
    · intro hh; exact absurd hh this
    · intro hh; exact absurd hh e

theorem sortGroupBy_eq (antisymm : ∀ a b, le a b = true → le b a = true → a = b)
    (h : Golars.Sort.TotalPreorder le) (l : List α) :
    sortGroupBy key le l = groupsOver key l ((sortGroupBy key le l).map (·.1)) ∧
      ((sortGroupBy key le l).map (·.1)).Nodup ∧
      (∀ k, k ∈ (sortGroupBy key le l).map (·.1) ↔ ∃ y ∈ l, key y = k) := by
  have hp : Golars.Sort.TotalPreorder (fun a b : α => le (key a) (key b)) :=
    ⟨fun a b c => h.trans _ _ _, fun a b => h.total _ _⟩
  have st := Golars.Sort.mergeSort_isStable hp l
  obtain ⟨e, n, m, _⟩ := runs_spec key le antisymm h.total _ st.2.1
  have grp : ∀ k, group key (l.mergeSort (fun a b => le (key a) (key b))) k = group key l k := by
    intro k
    by_cases hk : ∃ x ∈ l, key x = k
    · obtain ⟨x, _, rfl⟩ := hk
      unfold group
      rw [← filter_congr (fun y _ => equiv_key key le antisymm h x y),
        ← filter_congr (fun y _ => equiv_key key le antisymm h x y)]
      exact st.2.2 x
    · have e1 : group key l k = [] :=
        filter_eq_nil_iff.mpr (fun y hy e => hk ⟨y, hy, by simpa using e⟩)
      have e2 : group key (l.mergeSort (fun a b => le (key a) (key b))) k = [] :=
        filter_eq_nil_iff.mpr (fun y hy e => hk ⟨y, (mergeSort_perm _ _).subset hy, by simpa using e⟩)
      rw [e1, e2]
  unfold sortGroupBy
  refine ⟨?_, n, ?_⟩
  · conv => lhs; rw [e]
    simp only [groupsOver]
    apply map_congr_left; intro k _; rw [grp k]
  · intro k; rw [m]
    constructor
    · rintro ⟨y, hy, e⟩; exact ⟨y, (mergeSort_perm _ _).subset hy, e⟩
    · rintro ⟨y, hy, e⟩; exact ⟨y, (mergeSort_perm _ _).symm.subset hy, e⟩

/-- **Hash group-by equals sort group-by** as a multiset of
`(key, group)` pairs, and therefore of `(key, aggregate)` pairs for any
aggregate. -/
theorem hash_perm_sort (antisymm : ∀ a b, le a b = true → le b a = true → a = b)
    (h : Golars.Sort.TotalPreorder le) (l : List α) {β : Type} (agg : List α → β) :
    ((hashGroupBy key l).map (fun g => (g.1, agg g.2))).Perm
      ((sortGroupBy key le l).map (fun g => (g.1, agg g.2))) := by
  apply Perm.map
  obtain ⟨e, n, m⟩ := sortGroupBy_eq key le antisymm h l
  rw [hashGroupBy_eq, e]
  apply Perm.map
  apply (perm_ext_iff_of_nodup (firstKeys_nodup key l) n).mpr
  intro k; rw [mem_firstKeys, m]

end GroupBy

end Golars.Agg
