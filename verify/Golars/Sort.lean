/-!
# Stable sorting, LSD radix sort, null ordering and multi-key sorts

The specification of a stable sort is `List.mergeSort` from Lean core,
which is stable. `IsStableSortOf le l r` characterises it without
indices: `r` is a permutation of `l`, sorted by `le`, and every class of
tied elements appears in `r` in the same order as in `l`. Theorem
`eq_mergeSort` shows that this characterisation determines the result, so
any algorithm meeting it (radix sort, sort with nulls moved, successive
single-key sorts) equals the specification.
-/

namespace Golars.Sort

open List

variable {α : Type}

/-- A Bool comparator that is a total preorder. -/
structure TotalPreorder (le : α → α → Bool) : Prop where
  trans : ∀ a b c, le a b = true → le b c = true → le a c = true
  total : ∀ a b, (le a b || le b a) = true

theorem TotalPreorder.refl {le : α → α → Bool} (h : TotalPreorder le) (a : α) : le a a = true := by
  have := h.total a a; simpa using this

/-- Two elements tie under `le`. -/
def Equiv (le : α → α → Bool) (a b : α) : Bool := le a b && le b a

/-- `r` is the stable sort of `l` by `le`. -/
def IsStableSortOf (le : α → α → Bool) (l r : List α) : Prop :=
  r.Perm l ∧ r.Pairwise (fun a b => le a b = true) ∧
    ∀ x, r.filter (Equiv le x) = l.filter (Equiv le x)

theorem pairwise_of_forall_mem {R : α → α → Prop} :
    ∀ {l : List α}, (∀ a ∈ l, ∀ b ∈ l, R a b) → l.Pairwise R
  | [], _ => Pairwise.nil
  | a :: l, h => by
    refine pairwise_cons.mpr ⟨fun b hb => h a mem_cons_self b (mem_cons_of_mem _ hb), ?_⟩
    exact pairwise_of_forall_mem (fun x hx y hy => h x (mem_cons_of_mem _ hx) y (mem_cons_of_mem _ hy))

theorem filter_eq_of_sublist {p : α → Bool} {c m : List α} (hs : c.Sublist m)
    (hp : ∀ y ∈ c, p y = true) (hl : c.length = (m.filter p).length) : c = m.filter p := by
  have h1 : c.filter p = c := filter_eq_self.mpr hp
  have h2 := hs.filter p
  rw [h1] at h2
  exact h2.eq_of_length hl

/-- `List.mergeSort` meets the stable sort characterisation. -/
theorem mergeSort_isStable {le : α → α → Bool} (h : TotalPreorder le) (l : List α) :
    IsStableSortOf le l (l.mergeSort le) := by
  refine ⟨mergeSort_perm l le, pairwise_mergeSort h.trans h.total l, ?_⟩
  intro x
  have hc : (l.filter (Equiv le x)).Pairwise (fun a b => le a b = true) := by
    apply pairwise_of_forall_mem
    intro a ha b hb
    simp only [mem_filter, Equiv, Bool.and_eq_true] at ha hb
    exact h.trans _ _ _ ha.2.2 hb.2.1
  have hs := sublist_mergeSort h.trans h.total hc (filter_sublist (p := Equiv le x) (l := l))
  have hl : (l.filter (Equiv le x)).length = ((l.mergeSort le).filter (Equiv le x)).length :=
    (((mergeSort_perm l le).filter (Equiv le x)).length_eq).symm
  exact (filter_eq_of_sublist hs (fun y hy => (mem_filter.mp hy).2) hl).symm

/-- Two stable sorts of the same input are equal. -/
theorem stable_unique {le : α → α → Bool} (h : TotalPreorder le) :
    ∀ {r1 r2 : List α}, r1.Perm r2 → r1.Pairwise (fun a b => le a b = true) →
      r2.Pairwise (fun a b => le a b = true) →
      (∀ x, r1.filter (Equiv le x) = r2.filter (Equiv le x)) → r1 = r2
  | [], r2, hp, _, _, _ => (Perm.nil_eq hp)
  | a :: r1, r2, hp, s1, s2, hf => by
    cases r2 with
    | nil => exact absurd hp.symm (by intro h'; have := h'.length_eq; simp at this)
    | cons b r2 =>
      have ha : a ∈ b :: r2 := hp.subset mem_cons_self
      have hb : b ∈ a :: r1 := hp.symm.subset mem_cons_self
      have lba : le b a = true := by
        rcases mem_cons.mp ha with e | e
        · rw [e]; exact h.refl b
        · exact (pairwise_cons.mp s2).1 a e
      have lab : le a b = true := by
        rcases mem_cons.mp hb with e | e
        · rw [e]; exact h.refl a
        · exact (pairwise_cons.mp s1).1 b e
      have eaa : Equiv le a a = true := by simp [Equiv, h.refl a]
      have eab : Equiv le a b = true := by simp [Equiv, lab, lba]
      have hfa := hf a
      simp only [filter_cons, eaa, eab, ite_true] at hfa
      have hab : a = b := (cons.inj hfa).1
      subst hab
      have tail := stable_unique h (hp.cons_inv) (pairwise_cons.mp s1).2 (pairwise_cons.mp s2).2
        (fun x => by
          have := hf x
          simp only [filter_cons] at this
          split at this
          · exact (cons.inj this).2
          · exact this)
      rw [tail]

/-- Any list meeting the characterisation is the `mergeSort` result. -/
theorem eq_mergeSort {le : α → α → Bool} (h : TotalPreorder le) {l r : List α}
    (hr : IsStableSortOf le l r) : r = l.mergeSort le := by
  have hm := mergeSort_isStable h l
  exact stable_unique h (hr.1.trans hm.1.symm) hr.2.1 hm.2.1 (fun x => by rw [hr.2.2 x, hm.2.2 x])

theorem mergeSort_congr {r s : α → α → Bool} {l : List α}
    (h : ∀ a ∈ l, ∀ b ∈ l, r a b = s a b) : l.mergeSort r = l.mergeSort s := by
  have := map_mergeSort (f := id) (r := r) (s := s) (l := l) (fun a ha b hb => h a ha b hb)
  simpa using this

/-- Filtering commutes with a stable sort. -/
theorem filter_mergeSort {le : α → α → Bool} (h : TotalPreorder le) (p : α → Bool) (l : List α) :
    (l.mergeSort le).filter p = (l.filter p).mergeSort le := by
  apply eq_mergeSort h
  have hm := mergeSort_isStable h l
  refine ⟨hm.1.filter p, hm.2.1.filter p, ?_⟩
  intro x
  rw [filter_filter, filter_filter]
  have e : ∀ l' : List α, l'.filter (fun y => Equiv le x y && p y) =
      (l'.filter (Equiv le x)).filter p := by
    intro l'; rw [filter_filter]; apply filter_congr; intro y _; exact Bool.and_comm _ _
  rw [e, e, hm.2.2 x]

/-! ## LSD radix sort -/

/-- Digit `p` of `k` in base `B`. -/
def digit (B p k : Nat) : Nat := k / B ^ p % B

/-- One counting-sort pass: a stable partition by digit `p`, buckets in
ascending digit order. -/
def pass (key : α → Nat) (B p : Nat) (l : List α) : List α :=
  (List.range B).flatMap (fun d => l.filter (fun x => digit B p (key x) == d))

/-- LSD radix sort with `P` passes, least significant digit first. -/
def lsd (key : α → Nat) (B P : Nat) (l : List α) : List α :=
  (List.range P).foldl (fun acc p => pass key B p acc) l

/-- The pass structure golars uses: a pass whose digit is the same for
every key is skipped (Go: `counts[pass][digit(src[0])] == n`). -/
def passOrSkip (key : α → Nat) (B p : Nat) (l : List α) : List α :=
  match l with
  | [] => []
  | x :: _ =>
    if l.all (fun y => digit B p (key y) == digit B p (key x)) then l else pass key B p l

def lsdSkip (key : α → Nat) (B P : Nat) (l : List α) : List α :=
  (List.range P).foldl (fun acc p => passOrSkip key B p acc) l

/-- The parallel pass: every worker scatters its chunk, and within a
digit the workers' outputs follow worker order. -/
def passChunks (key : α → Nat) (B p : Nat) (chunks : List (List α)) : List α :=
  (List.range B).flatMap (fun d => chunks.flatMap (fun c => c.filter (fun x => digit B p (key x) == d)))

theorem flatMap_range_single {g : Nat → List α} {c : Nat} (hc : c < n)
    (h : ∀ d, d ≠ c → g d = []) : (List.range n).flatMap g = g c := by
  induction n with
  | zero => omega
  | succ n ih =>
    rw [range_succ, flatMap_append]
    by_cases e : c = n
    · subst e
      have : (List.range c).flatMap g = [] := by
        rw [flatMap_eq_nil_iff]; intro d hd; exact h d (by simp at hd; omega)
      simp [this]
    · rw [ih (by omega)]
      simp only [flatMap_cons, flatMap_nil, h n (Ne.symm e), append_nil]

theorem digit_lt (B p k : Nat) (hB : 0 < B) : digit B p k < B := Nat.mod_lt _ hB

/-- A pass whose digit is constant is the identity, so skipping it is
sound. -/
theorem pass_const {key : α → Nat} {B p c : Nat} {l : List α} (hc : c < B)
    (h : ∀ x ∈ l, digit B p (key x) = c) : pass key B p l = l := by
  unfold pass
  rw [flatMap_range_single hc]
  · exact filter_eq_self.mpr (fun x hx => by simp [h x hx])
  · intro d hd
    exact filter_eq_nil_iff.mpr (fun x hx => by simp [h x hx]; omega)

theorem passOrSkip_eq {key : α → Nat} {B p : Nat} (hB : 0 < B) (l : List α) :
    passOrSkip key B p l = pass key B p l := by
  unfold passOrSkip
  split
  · simp [pass]
  · rename_i x xs
    split
    · rename_i hall
      symm; apply pass_const (digit_lt B p _ hB)
      intro y hy
      have := (all_eq_true.mp hall) y hy
      simpa using this
    · rfl

theorem lsdSkip_eq {key : α → Nat} {B P : Nat} (hB : 0 < B) (l : List α) :
    lsdSkip key B P l = lsd key B P l := by
  unfold lsdSkip lsd
  congr 1
  funext acc p
  exact passOrSkip_eq hB acc

theorem passChunks_eq (key : α → Nat) (B p : Nat) (chunks : List (List α)) :
    passChunks key B p chunks = pass key B p chunks.flatten := by
  unfold passChunks pass
  congr 1; funext d
  rw [filter_flatten, flatMap_def]

theorem pass_perm {key : α → Nat} {B p : Nat} (hB : 0 < B) (l : List α) :
    (pass key B p l).Perm l := by
  unfold pass
  have gen : ∀ n, ((List.range n).flatMap (fun d => l.filter (fun x => digit B p (key x) == d))).Perm
      (l.filter (fun x => decide (digit B p (key x) < n))) := by
    intro n
    induction n with
    | zero => simp
    | succ n ih =>
      rw [range_succ, flatMap_append]
      simp only [flatMap_cons, flatMap_nil, append_nil]
      refine (ih.append_right _).trans ?_
      have hp := filter_append_perm (fun x => decide (digit B p (key x) < n))
        (l.filter (fun x => decide (digit B p (key x) < n + 1)))
      rw [filter_filter, filter_filter] at hp
      have e1 : l.filter (fun x => decide (digit B p (key x) < n) && decide (digit B p (key x) < n + 1)) =
          l.filter (fun x => decide (digit B p (key x) < n)) := by
        apply filter_congr; intro x _; rw [Bool.eq_iff_iff]
        simp only [Bool.and_eq_true, decide_eq_true_eq]; omega
      have e2 : l.filter (fun x => !decide (digit B p (key x) < n) && decide (digit B p (key x) < n + 1)) =
          l.filter (fun x => digit B p (key x) == n) := by
        apply filter_congr; intro x _; rw [Bool.eq_iff_iff]
        simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not, decide_eq_true_eq,
          beq_iff_eq]; omega
      rw [e1, e2] at hp
      exact hp
  refine (gen B).trans ?_
  rw [filter_eq_self.mpr (fun x _ => by simp [digit_lt B p _ hB])]

/-- Order on the low `p` digits. -/
def lowLE (key : α → Nat) (B p : Nat) (a b : α) : Bool := decide (key a % B ^ p ≤ key b % B ^ p)

theorem lowLE_preorder (key : α → Nat) (B p : Nat) : TotalPreorder (lowLE key B p) where
  trans a b c h1 h2 := by simp [lowLE] at *; omega
  total a b := by simp [lowLE]; omega

theorem low_succ (B p k : Nat) : k % B ^ (p + 1) = k % B ^ p + B ^ p * digit B p k :=
  Nat.mod_pow_succ

/-- Comparing `u + Q*d` numbers with `u < Q` is lexicographic in `(d, u)`. -/
theorem lex_digit (Q u1 u2 d1 d2 : Nat) (h1 : u1 < Q) (h2 : u2 < Q) :
    u1 + Q * d1 ≤ u2 + Q * d2 ↔ (d1 < d2 ∨ (d1 = d2 ∧ u1 ≤ u2)) := by
  constructor
  · intro h
    by_cases e : d1 < d2
    · exact Or.inl e
    · right
      by_cases e2 : d1 = d2
      · subst e2; exact ⟨rfl, by omega⟩
      · have := Nat.mul_le_mul_left Q (show d2 + 1 ≤ d1 by omega)
        rw [Nat.mul_succ] at this; omega
  · intro h
    rcases h with h | ⟨rfl, h⟩
    · have := Nat.mul_le_mul_left Q (show d1 + 1 ≤ d2 by omega)
      rw [Nat.mul_succ] at this; omega
    · omega

theorem lowLE_succ {key : α → Nat} {B p : Nat} (hB : 0 < B) (x y : α) :
    lowLE key B (p + 1) x y = true ↔
      (digit B p (key x) < digit B p (key y) ∨
        (digit B p (key x) = digit B p (key y) ∧ lowLE key B p x y = true)) := by
  have hBp : 0 < B ^ p := Nat.pow_pos hB
  simp only [lowLE, decide_eq_true_eq, low_succ]
  exact lex_digit _ _ _ _ _ (Nat.mod_lt _ hBp) (Nat.mod_lt _ hBp)

theorem equiv_succ {key : α → Nat} {B p : Nat} (hB : 0 < B) (x y : α) :
    Equiv (lowLE key B (p + 1)) x y =
      (Equiv (lowLE key B p) x y && digit B p (key y) == digit B p (key x)) := by
  rw [Bool.eq_iff_iff]
  simp only [Equiv, Bool.and_eq_true, beq_iff_eq]
  rw [lowLE_succ hB, lowLE_succ hB]
  constructor
  · rintro ⟨h1 | ⟨e1, l1⟩, h2 | ⟨e2, l2⟩⟩
    · omega
    · omega
    · omega
    · exact ⟨⟨l1, l2⟩, e2⟩
  · rintro ⟨⟨l1, l2⟩, e⟩
    exact ⟨Or.inr ⟨e.symm, l1⟩, Or.inr ⟨e, l2⟩⟩

theorem pass_step {key : α → Nat} {B p : Nat} (hB : 0 < B) {l m : List α}
    (hm : IsStableSortOf (lowLE key B p) l m) :
    IsStableSortOf (lowLE key B (p + 1)) l (pass key B p m) := by
  refine ⟨(pass_perm hB m).trans hm.1, ?_, ?_⟩
  · unfold pass
    rw [pairwise_flatMap]
    constructor
    · intro d _
      apply (hm.2.1.filter _).imp_of_mem
      intro a b ha hb hab
      simp only [mem_filter, beq_iff_eq] at ha hb
      rw [lowLE_succ hB]; right; exact ⟨by rw [ha.2, hb.2], hab⟩
    · apply (pairwise_lt_range (n := B)).imp
      intro d1 d2 hd x hx y hy
      simp only [mem_filter, beq_iff_eq] at hx hy
      rw [lowLE_succ hB]; left; rw [hx.2, hy.2]; exact hd
  · intro x
    unfold pass
    rw [filter_flatMap]
    rw [flatMap_range_single (c := digit B p (key x)) (digit_lt B p _ hB)]
    · rw [filter_filter]
      have e1 : m.filter (fun y => Equiv (lowLE key B (p + 1)) x y && (digit B p (key y) == digit B p (key x))) =
          (m.filter (Equiv (lowLE key B p) x)).filter (fun y => digit B p (key y) == digit B p (key x)) := by
        rw [filter_filter]; apply filter_congr; intro y _; rw [equiv_succ hB]
        cases Equiv (lowLE key B p) x y <;> cases (digit B p (key y) == digit B p (key x)) <;> rfl
      have e2 : l.filter (Equiv (lowLE key B (p + 1)) x) =
          (l.filter (Equiv (lowLE key B p) x)).filter (fun y => digit B p (key y) == digit B p (key x)) := by
        rw [filter_filter]; apply filter_congr; intro y _; rw [equiv_succ hB]
        cases Equiv (lowLE key B p) x y <;> cases (digit B p (key y) == digit B p (key x)) <;> rfl
      rw [e1, e2, hm.2.2 x]
    · intro d hd
      apply filter_eq_nil_iff.mpr
      intro y hy
      simp only [mem_filter, beq_iff_eq] at hy
      rw [equiv_succ hB, hy.2]; simp; omega

theorem lsd_succ (key : α → Nat) (B P : Nat) (l : List α) :
    lsd key B (P + 1) l = pass key B P (lsd key B P l) := by
  simp [lsd, range_succ, foldl_append]

theorem lsd_isStable {key : α → Nat} {B : Nat} (hB : 0 < B) (l : List α) :
    ∀ P, IsStableSortOf (lowLE key B P) l (lsd key B P l)
  | 0 => by
    refine ⟨by simp [lsd], ?_, fun x => by simp [lsd]⟩
    simp only [lsd, range_zero, foldl_nil]
    apply pairwise_of_forall_mem; intro a _ b _; simp [lowLE, Nat.mod_one]
  | P + 1 => by
    rw [lsd_succ]
    exact pass_step hB (lsd_isStable hB l P)

/-- **Radix sort correctness.** With enough passes to cover every key,
LSD radix sort is exactly the stable sort by key. -/
theorem lsd_eq_mergeSort {key : α → Nat} {B P : Nat} (hB : 0 < B) (l : List α)
    (hk : ∀ x ∈ l, key x < B ^ P) :
    lsd key B P l = l.mergeSort (fun a b => decide (key a ≤ key b)) := by
  rw [eq_mergeSort (lowLE_preorder key B P) (lsd_isStable hB l P)]
  apply mergeSort_congr
  intro a ha b hb
  simp [lowLE, Nat.mod_eq_of_lt (hk a ha), Nat.mod_eq_of_lt (hk b hb)]

/-- The golars pass structure (skipped constant passes) sorts correctly. -/
theorem lsdSkip_eq_mergeSort {key : α → Nat} {B P : Nat} (hB : 0 < B) (l : List α)
    (hk : ∀ x ∈ l, key x < B ^ P) :
    lsdSkip key B P l = l.mergeSort (fun a b => decide (key a ≤ key b)) := by
  rw [lsdSkip_eq hB, lsd_eq_mergeSort hB l hk]

/-- The result is sorted and a permutation of the input. -/
theorem lsd_sorted_perm {key : α → Nat} {B P : Nat} (hB : 0 < B) (l : List α)
    (hk : ∀ x ∈ l, key x < B ^ P) :
    (lsd key B P l).Perm l ∧ (lsd key B P l).Pairwise (fun a b => key a ≤ key b) := by
  rw [lsd_eq_mergeSort hB l hk]
  have h : TotalPreorder (fun a b : α => decide (key a ≤ key b)) :=
    ⟨fun a b c h1 h2 => by simp at *; omega, fun a b => by simp; omega⟩
  refine ⟨mergeSort_perm _ _, ?_⟩
  exact (pairwise_mergeSort h.trans h.total l).imp (by simp)

/-! ### Go digit extraction -/

theorem goDigit8 (v : UInt64) (p : Nat) (hp : p < 8) :
    ((v >>> (UInt64.ofNat (8 * p))) &&& 255).toNat = digit 256 p v.toNat := by
  rw [UInt64.toNat_and, UInt64.toNat_shiftRight]
  have h1 : (UInt64.ofNat (8 * p)).toNat % 64 = 8 * p := by
    rw [UInt64.toNat_ofNat_of_lt' (by simp [UInt64.size]; omega)]; omega
  have h2 : (255 : UInt64).toNat = 2 ^ 8 - 1 := by decide
  rw [h1, h2, Nat.and_two_pow_sub_one_eq_mod, Nat.shiftRight_eq_div_pow, Nat.pow_mul]
  rfl

theorem goDigit11 (v : UInt64) (p : Nat) (hp : p < 6) :
    ((v >>> (UInt64.ofNat (11 * p))) &&& (if p = 5 then 511 else 2047)).toNat = digit 2048 p v.toNat := by
  rw [UInt64.toNat_and, UInt64.toNat_shiftRight]
  have h1 : (UInt64.ofNat (11 * p)).toNat % 64 = 11 * p := by
    rw [UInt64.toNat_ofNat_of_lt' (by simp [UInt64.size]; omega)]; omega
  rw [h1, Nat.shiftRight_eq_div_pow, Nat.pow_mul]
  unfold digit
  have hv := v.toNat_lt
  by_cases h5 : p = 5
  · subst h5
    have e : (if (5 : Nat) = 5 then (511 : UInt64) else 2047).toNat = 2 ^ 9 - 1 := by decide
    rw [e, Nat.and_two_pow_sub_one_eq_mod]
    show v.toNat / 36028797018963968 % 512 = v.toNat / 36028797018963968 % 2048
    omega
  · have e : (if p = 5 then (511 : UInt64) else 2047).toNat = 2 ^ 11 - 1 := by simp [h5]
    rw [e, Nat.and_two_pow_sub_one_eq_mod]

/-! ## Null ordering -/

/-- The null-aware comparator of `compute.singleKeyCompare`: nulls go
first or last independent of direction; values compare by `le`, reversed
when descending. -/
def nullLE (le : β → β → Bool) (desc nullsLast : Bool) : Option β → Option β → Bool
  | none, none => true
  | none, some _ => !nullsLast
  | some _, none => nullsLast
  | some a, some b => if desc then le b a else le a b

/-- For every choice of direction and null placement the comparator is a
total preorder. -/
theorem nullLE_preorder {β : Type} {le : β → β → Bool} (h : TotalPreorder le) (desc nullsLast : Bool) :
    TotalPreorder (nullLE le desc nullsLast) where
  trans a b c h1 h2 := by
    cases a <;> cases b <;> cases c <;> cases desc <;> cases nullsLast <;>
      simp_all [nullLE] <;> first | exact h.trans _ _ _ h1 h2 | exact h.trans _ _ _ h2 h1
  total a b := by
    cases a with
    | none => cases b <;> cases nullsLast <;> simp [nullLE]
    | some x =>
      cases b with
      | none => cases nullsLast <;> simp [nullLE]
      | some y =>
        have t := h.total x y
        simp only [Bool.or_eq_true] at t
        cases desc <;> simp only [nullLE, Bool.or_eq_true, Bool.false_eq_true, ite_false, ite_true] <;>
          first | exact t | exact Or.comm.mp t

/-- `argSortFloat64Asc` and `argSortInt64Asc` in `series/arg_radix.go`
sort the valid rows and append the null rows in input order. This equals
the stable sort with nulls last. -/
theorem nullsLast_compact {ρ β : Type} {le : β → β → Bool} (h : TotalPreorder le)
    (key : ρ → Option β) (l : List ρ) :
    (l.filter (fun r => (key r).isSome)).mergeSort (fun a b => nullLE le false true (key a) (key b)) ++
      l.filter (fun r => (key r).isNone) =
    l.mergeSort (fun a b => nullLE le false true (key a) (key b)) := by
  have hp : TotalPreorder (fun a b : ρ => nullLE le false true (key a) (key b)) :=
    ⟨fun a b c => (nullLE_preorder h false true).trans (key a) (key b) (key c),
     fun a b => (nullLE_preorder h false true).total (key a) (key b)⟩
  apply eq_mergeSort hp
  have hv := mergeSort_isStable hp (l.filter (fun r => (key r).isSome))
  refine ⟨?_, ?_, ?_⟩
  · refine (hv.1.append_right _).trans ?_
    have := filter_append_perm (fun r => (key r).isSome) l
    refine Perm.trans ?_ this
    apply Perm.of_eq; congr 1; apply filter_congr; intro x _; cases key x <;> rfl
  · rw [pairwise_append]
    refine ⟨hv.2.1, ?_, ?_⟩
    · apply pairwise_of_forall_mem; intro a ha b hb
      simp only [mem_filter] at ha hb
      cases e1 : key a <;> cases e2 : key b <;> simp_all [nullLE]
    · intro a ha b hb
      have ha' := (hv.1.subset ha); simp only [mem_filter] at ha' hb
      cases e1 : key a <;> cases e2 : key b <;> simp_all [nullLE]
  · intro x
    rw [filter_append, hv.2.2 x, filter_filter, filter_filter]
    cases ex : key x
    · have e1 : l.filter (fun r => Equiv (fun a b => nullLE le false true (key a) (key b)) x r &&
          (key r).isSome) = [] := by
        apply filter_eq_nil_iff.mpr; intro r _; cases e : key r <;> simp [Equiv, nullLE, ex, e]
      rw [e1, nil_append]
      apply filter_congr; intro r _; cases e : key r <;> simp [Equiv, nullLE, ex, e]
    · have e2 : l.filter (fun r => Equiv (fun a b => nullLE le false true (key a) (key b)) x r &&
          (key r).isNone) = [] := by
        apply filter_eq_nil_iff.mpr; intro r _; cases e : key r <;> simp [Equiv, nullLE, ex, e]
      rw [e2, append_nil]
      apply filter_congr; intro r _; cases e : key r <;> simp [Equiv, nullLE, ex, e]

/-! ## Multi-key sorts -/

/-- Lexicographic combination: order by `le1`, break ties by `le2`. This
is the comparator chain of `compute.SortIndicesMulti`. -/
def lexLE (le1 le2 : α → α → Bool) (a b : α) : Bool :=
  le1 a b && (!le1 b a || le2 a b)

theorem lexLE_preorder {le1 le2 : α → α → Bool} (h1 : TotalPreorder le1) (h2 : TotalPreorder le2) :
    TotalPreorder (lexLE le1 le2) where
  trans a b c hab hbc := by
    simp only [lexLE, Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true'] at *
    refine ⟨h1.trans _ _ _ hab.1 hbc.1, ?_⟩
    by_cases hca : le1 c a = true
    · right
      have hba : le1 b a = true := h1.trans _ _ _ hbc.1 hca
      have hcb : le1 c b = true := h1.trans _ _ _ hca hab.1
      have e1 : le2 a b = true := by
        rcases hab.2 with h | h
        · simp [hba] at h
        · exact h
      have e2 : le2 b c = true := by
        rcases hbc.2 with h | h
        · simp [hcb] at h
        · exact h
      exact h2.trans _ _ _ e1 e2
    · left; simpa using hca
  total a b := by
    simp only [lexLE]
    have t1 := h1.total a b; have t2 := h2.total a b
    cases e1 : le1 a b <;> cases e2 : le1 b a <;> cases e3 : le2 a b <;> cases e4 : le2 b a <;>
      simp_all

theorem pairwise_lex {le1 le2 : α → α → Bool} (h1 : TotalPreorder le1) {r : List α}
    (s1 : r.Pairwise (fun a b => le1 a b = true))
    (s2 : ∀ x, (r.filter (Equiv le1 x)).Pairwise (fun a b => le2 a b = true)) :
    r.Pairwise (fun a b => lexLE le1 le2 a b = true) := by
  rw [pairwise_iff_forall_sublist] at *
  intro a b hab
  have l1 := s1 hab
  simp only [lexLE, l1, Bool.true_and, Bool.or_eq_true, Bool.not_eq_true']
  by_cases hba : le1 b a = true
  · right
    have ea : Equiv le1 a a = true := by simp [Equiv, h1.refl a]
    have eb : Equiv le1 a b = true := by simp [Equiv, l1, hba]
    have := (s2 a)
    rw [pairwise_iff_forall_sublist] at this
    apply this
    have := hab.filter (Equiv le1 a)
    simpa [filter_cons, ea, eb] using this
  · left; simpa using hba

/-- Sorting stably by the second key and then stably by the first key is
the lexicographic stable sort. This is the int64 multi-key fast path of
`compute.SortIndicesMulti` (arg-radix by the last key first). -/
theorem two_pass_eq_lex {le1 le2 : α → α → Bool} (h1 : TotalPreorder le1) (h2 : TotalPreorder le2)
    (l : List α) :
    (l.mergeSort le2).mergeSort le1 = l.mergeSort (lexLE le1 le2) := by
  apply eq_mergeSort (lexLE_preorder h1 h2)
  have m2 := mergeSort_isStable h2 l
  have m1 := mergeSort_isStable h1 (l.mergeSort le2)
  refine ⟨m1.1.trans m2.1, ?_, ?_⟩
  · apply pairwise_lex h1 m1.2.1
    intro x; rw [m1.2.2 x]; exact m2.2.1.filter _
  · intro x
    have e : ∀ l' : List α, l'.filter (Equiv (lexLE le1 le2) x) =
        (l'.filter (Equiv le1 x)).filter (Equiv le2 x) := by
      intro l'; rw [filter_filter]; apply filter_congr; intro y _
      simp only [Equiv, lexLE]
      cases le1 x y <;> cases le1 y x <;> cases le2 x y <;> cases le2 y x <;> rfl
    have e' : ∀ l' : List α, (l'.filter (Equiv le1 x)).filter (Equiv le2 x) =
        (l'.filter (Equiv le2 x)).filter (Equiv le1 x) := by
      intro l'; rw [filter_filter, filter_filter]; apply filter_congr; intro y _
      exact Bool.and_comm _ _
    rw [e, e, m1.2.2 x, e', e', m2.2.2 x]

/-! ## Packed (value, row) keys -/

/-- `series.packInt64Keys` packs `(v - lo) << idxBits | i`. For rows below
`2^idxBits` the packed order is the order by value, then by row, so an
unsigned sort of packed words is a stable argsort. -/
theorem packed_le_iff (u1 u2 i1 i2 ib : Nat) (h1 : i1 < 2 ^ ib) (h2 : i2 < 2 ^ ib) :
    u1 * 2 ^ ib + i1 ≤ u2 * 2 ^ ib + i2 ↔ u1 < u2 ∨ (u1 = u2 ∧ i1 ≤ i2) := by
  constructor
  · intro h
    by_cases e : u1 < u2
    · exact Or.inl e
    · right
      by_cases e2 : u1 = u2
      · subst e2; exact ⟨rfl, by omega⟩
      · have : u2 + 1 ≤ u1 := by omega
        have := Nat.mul_le_mul_right (2 ^ ib) this
        rw [Nat.succ_mul] at this; omega
  · intro h
    rcases h with h | ⟨rfl, h⟩
    · have := Nat.mul_le_mul_right (2 ^ ib) (show u1 + 1 ≤ u2 by omega)
      rw [Nat.succ_mul] at this; omega
    · omega

theorem packed_or (u i ib : Nat) (h : i < 2 ^ ib) : u <<< ib ||| i = u * 2 ^ ib + i := by
  rw [← Nat.shiftLeft_add_eq_or_of_lt h, Nat.shiftLeft_eq]

end Golars.Sort
