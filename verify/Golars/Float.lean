/-!
# Float total order and radix keys

An IEEE-754 binary64 value is modelled as its 64-bit pattern. The
standard field interpretation is used: bit 63 is the sign, bits 52..62
the biased exponent and bits 0..51 the trailing significand. No
arithmetic is needed, only the order.

The polars sort order (`cmp`) is defined from the fields alone:

* NaN equals NaN and is greater than every other value.
* `-0.0` equals `0.0`. polars 1.39 compares floats with a total order
  that treats the two zeros as equal, and its sort is stable, so the two
  zeros keep their input order.
* Otherwise negative values are below positive ones, and two values of
  the same sign compare by magnitude. For binary64 the magnitude order is
  the lexicographic order on `(exponent, significand)`; this holds for
  subnormals (exponent 0) and for infinity (exponent 2047, significand 0)
  alike and is the standard property of the encoding.

The key transforms are written with the same bit operations as the Go
code, and the theorems relate them to `cmp`.
-/

namespace Golars.Float

/-- A binary64 bit pattern. -/
abbrev Bits := UInt64

/-- Decoded fields of a binary64 pattern. -/
structure Fields where
  neg : Bool
  exp : Nat
  mant : Nat
  deriving DecidableEq, Repr

@[reducible] def fields (b : Bits) : Fields :=
  { neg := !decide (b.toNat < 9223372036854775808)
    exp := b.toNat / 4503599627370496 % 2048
    mant := b.toNat % 4503599627370496 }

def isNaN (b : Bits) : Bool := (fields b).exp == 2047 && (fields b).mant != 0

def isZero (b : Bits) : Bool := (fields b).exp == 0 && (fields b).mant == 0

/-- Strict magnitude order: lexicographic on (exponent, significand). -/
def magLt (x y : Fields) : Bool :=
  x.exp < y.exp || (x.exp == y.exp && x.mant < y.mant)

/-- Strict order on two non-NaN values. The two zeros are equal. -/
def numLt (a b : Bits) : Bool :=
  if isZero a && isZero b then false
  else match (fields a).neg, (fields b).neg with
    | true, false => true
    | false, true => false
    | false, false => magLt (fields a) (fields b)
    | true, true => magLt (fields b) (fields a)

/-- The polars total order on float64 (ascending). -/
def cmp (a b : Bits) : Ordering :=
  match isNaN a, isNaN b with
  | true, true => .eq
  | true, false => .gt
  | false, true => .lt
  | false, false => if numLt a b then .lt else if numLt b a then .gt else .eq

/-- `a` sorts before or ties with `b`. -/
def le (a b : Bits) : Bool := cmp a b != .gt

/-- `a` sorts strictly before `b`. -/
def lt (a b : Bits) : Bool := cmp a b == .lt

/-- The two values tie under the sort order. -/
def equiv (a b : Bits) : Bool := cmp a b == .eq

/-! ## Key transforms used by golars -/

/-- The IEEE key used by `compute.sortValuesFast` (float64, "needsIEEE"
path) after NaN values are removed: set the sign bit of non-negative
patterns, complement negative ones. Go:
`if bits>>63 == 0 { bits |= 1 << 63 } else { bits = ^bits }`. -/
def ieeeKey (b : Bits) : UInt64 :=
  if b >>> 63 == 0 then b ||| ((1 : UInt64) <<< 63) else ~~~b

/-- Inverse of `ieeeKey`, used to write the sorted values back. Go:
`if k>>63 == 1 { k &^= 1 << 63 } else { k = ^k }`. -/
def ieeeKeyInv (k : UInt64) : Bits :=
  if k >>> 63 == 1 then k &&& ~~~((1 : UInt64) <<< 63) else ~~~k

/-- The key used by `series.floatRank` (float64 `ArgSort`): zeros share
one key, every NaN maps to one key above all numbers. Go tests
`v == 0` and `v != v`, which hold exactly for the two zeros and for
NaN; this model uses the decoded fields for the same tests. -/
def floatRank (b : Bits) : UInt64 :=
  if isZero b then (1 : UInt64) <<< 63
  else if isNaN b then 0xFFF8000000000000
  else ieeeKey b

/-! ## Arithmetic views -/

theorem toNat_lt (b : UInt64) : b.toNat < 18446744073709551616 := b.toNat_lt

theorem shift63_eq_zero_iff (b : UInt64) :
    (b >>> 63 == 0) = decide (b.toNat < 9223372036854775808) := by
  have h := toNat_lt b
  have : (b >>> 63).toNat = b.toNat / 9223372036854775808 := by
    rw [UInt64.toNat_shiftRight, Nat.shiftRight_eq_div_pow]; rfl
  by_cases hb : b.toNat < 9223372036854775808
  · have : b >>> 63 = 0 := UInt64.toNat_inj.mp (by rw [this]; simp; omega)
    simp [this, hb]
  · have hne : b >>> 63 ≠ 0 := by
      intro h0; have := congrArg UInt64.toNat h0; simp at this; omega
    simp [hne, hb]

/-- Arithmetic form of `ieeeKey`. -/
def keyNat (x : Nat) : Nat :=
  if x < 9223372036854775808 then x + 9223372036854775808 else 18446744073709551615 - x

/-- `keyNat` with the two zeros identified: the order of this rank is the
polars order on non-NaN values. -/
def rankNat (x : Nat) : Nat :=
  if x % 9223372036854775808 = 0 then 9223372036854775808 else keyNat x

/-- The rank of NaN in `floatRank`. -/
def nanRank : Nat := 18444492273895866368

theorem ieeeKey_toNat (b : Bits) : (ieeeKey b).toNat = keyNat b.toNat := by
  unfold ieeeKey keyNat
  rw [shift63_eq_zero_iff]
  by_cases hb : b.toNat < 9223372036854775808
  · simp only [hb, decide_true, ite_true]
    rw [UInt64.toNat_or]
    have : ((1 : UInt64) <<< 63).toNat = 2 ^ 63 := by decide
    rw [this, Nat.or_two_pow_eq_add_of_lt hb]
  · simp only [hb, decide_false, ite_false, Bool.false_eq_true]
    rw [UInt64.toNat_not]; rfl

/-! ## Field views

With `m = x % 2^63` (the pattern without its sign bit) the exponent is
`m / 2^52` and the significand `m % 2^52`, so the lexicographic magnitude
order is the order on `m`, NaN means `m > 0x7FF0000000000000` and zero
means `m = 0`. -/

theorem isNaN_eq (b : Bits) :
    isNaN b = decide (9218868437227405312 < b.toNat % 9223372036854775808) := by
  have := toNat_lt b
  simp only [isNaN, fields]
  by_cases h : 9218868437227405312 < b.toNat % 9223372036854775808 <;> simp [h] <;> omega

theorem isZero_eq (b : Bits) : isZero b = decide (b.toNat % 9223372036854775808 = 0) := by
  have := toNat_lt b
  simp only [isZero, fields]
  by_cases h : b.toNat % 9223372036854775808 = 0 <;> simp [h] <;> omega

theorem magLt_eq (a b : Bits) :
    magLt (fields a) (fields b) =
      decide (a.toNat % 9223372036854775808 < b.toNat % 9223372036854775808) := by
  have := toNat_lt a; have := toNat_lt b
  simp only [magLt, fields]
  by_cases h : a.toNat % 9223372036854775808 < b.toNat % 9223372036854775808 <;> simp [h] <;> omega

theorem neg_eq (b : Bits) : (fields b).neg = !decide (b.toNat < 9223372036854775808) := rfl

theorem numLt_eq (a b : Bits) : numLt a b =
    if a.toNat % 9223372036854775808 = 0 ∧ b.toNat % 9223372036854775808 = 0 then false
    else if a.toNat < 9223372036854775808 then
      (if b.toNat < 9223372036854775808 then
        decide (a.toNat % 9223372036854775808 < b.toNat % 9223372036854775808) else false)
    else
      (if b.toNat < 9223372036854775808 then true
       else decide (b.toNat % 9223372036854775808 < a.toNat % 9223372036854775808)) := by
  unfold numLt
  rw [isZero_eq, isZero_eq, magLt_eq, magLt_eq, neg_eq, neg_eq]
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
    simp [h1, h2]

theorem numLt_iff_rank (a b : Bits) : numLt a b = true ↔ rankNat a.toNat < rankNat b.toNat := by
  have hx := toNat_lt a; have hy := toNat_lt b
  rw [numLt_eq]; unfold rankNat keyNat
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
  by_cases z1 : a.toNat % 9223372036854775808 = 0 <;>
  by_cases z2 : b.toNat % 9223372036854775808 = 0 <;>
    simp only [h1, h2, z1, z2, ite_true, ite_false, and_self, and_false, false_and, and_true,
      true_and, Bool.false_eq_true, decide_eq_true_eq, iff_true, iff_false, false_iff, true_iff,
      Nat.lt_irrefl, not_false_eq_true] <;> omega

theorem numLt_eq_rank (a b : Bits) :
    numLt a b = decide (rankNat a.toNat < rankNat b.toNat) := by
  by_cases h : rankNat a.toNat < rankNat b.toNat
  · simp only [h, decide_true]; exact (numLt_iff_rank a b).mpr h
  · simp only [h, decide_false]
    cases e : numLt a b
    · rfl
    · exact absurd ((numLt_iff_rank a b).mp e) h

theorem rank_lt_nan (b : Bits) (hb : isNaN b = false) : rankNat b.toNat < nanRank := by
  have := toNat_lt b
  rw [isNaN_eq] at hb
  simp only [decide_eq_false_iff_not] at hb
  unfold rankNat keyNat nanRank
  by_cases h1 : b.toNat < 9223372036854775808 <;>
  by_cases z1 : b.toNat % 9223372036854775808 = 0 <;>
    simp only [h1, z1, ite_true, ite_false] <;> omega

theorem cmp_num (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false) :
    cmp a b = if rankNat a.toNat < rankNat b.toNat then .lt
      else if rankNat b.toNat < rankNat a.toNat then .gt else .eq := by
  unfold cmp; rw [ha, hb, numLt_eq_rank, numLt_eq_rank]
  by_cases h1 : rankNat a.toNat < rankNat b.toNat <;>
  by_cases h2 : rankNat b.toNat < rankNat a.toNat <;> simp [h1, h2]

theorem le_iff_rank (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false) :
    le a b = true ↔ rankNat a.toNat ≤ rankNat b.toNat := by
  unfold le; rw [cmp_num a b ha hb]
  by_cases h1 : rankNat a.toNat < rankNat b.toNat <;>
  by_cases h2 : rankNat b.toNat < rankNat a.toNat <;> simp [h1, h2] <;> omega

theorem lt_iff_rank (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false) :
    lt a b = true ↔ rankNat a.toNat < rankNat b.toNat := by
  unfold lt; rw [cmp_num a b ha hb]
  by_cases h1 : rankNat a.toNat < rankNat b.toNat <;>
  by_cases h2 : rankNat b.toNat < rankNat a.toNat <;> simp [h1, h2]

theorem equiv_iff_rank (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false) :
    equiv a b = true ↔ rankNat a.toNat = rankNat b.toNat := by
  unfold equiv; rw [cmp_num a b ha hb]
  by_cases h1 : rankNat a.toNat < rankNat b.toNat <;>
  by_cases h2 : rankNat b.toNat < rankNat a.toNat <;> simp [h1, h2] <;> omega

theorem floatRank_toNat (b : Bits) :
    (floatRank b).toNat = if isNaN b then nanRank else rankNat b.toNat := by
  have := toNat_lt b
  have hk := ieeeKey_toNat b
  have hm : ((1 : UInt64) <<< 63).toNat = 9223372036854775808 := by decide
  have hn : (0xFFF8000000000000 : UInt64).toNat = 18444492273895866368 := by decide
  unfold floatRank rankNat nanRank
  rw [isZero_eq, isNaN_eq]
  by_cases z : b.toNat % 9223372036854775808 = 0 <;>
  by_cases n : 9218868437227405312 < b.toNat % 9223372036854775808 <;>
    simp only [z, n, Nat.not_lt_zero, decide_true, decide_false, ite_true, ite_false, Bool.false_eq_true, hm, hn,
      hk] <;> omega

/-! ## Main float theorems -/

/-- The canonical rank used by `series.floatRank` is an exact order
embedding of the polars order: `a` sorts before or ties with `b` iff its
rank is not larger. -/
theorem floatRank_le_iff (a b : Bits) : le a b = true ↔ floatRank a ≤ floatRank b := by
  show _ ↔ (floatRank a).toNat ≤ (floatRank b).toNat
  rw [floatRank_toNat, floatRank_toNat]
  cases ha : isNaN a <;> cases hb : isNaN b
  · simp only [Bool.false_eq_true, ite_false]; exact le_iff_rank a b ha hb
  · have := rank_lt_nan a ha
    simp only [le, cmp, ha, hb, Bool.false_eq_true, ite_false, ite_true]
    constructor
    · intro _; omega
    · intro _; decide
  · have := rank_lt_nan b hb
    simp only [le, cmp, ha, hb, Bool.false_eq_true, ite_false, ite_true]
    constructor
    · intro h; exact absurd h (by decide)
    · intro h; omega
  · simp only [le, cmp, ha, hb, ite_true]
    constructor
    · intro _; exact Nat.le_refl _
    · intro _; decide

theorem equiv_iff_le (a b : Bits) : equiv a b = true ↔ (le a b = true ∧ le b a = true) := by
  cases ha : isNaN a <;> cases hb : isNaN b
  · rw [equiv_iff_rank a b ha hb, le_iff_rank a b ha hb, le_iff_rank b a hb ha]; omega
  · simp [equiv, le, cmp, ha, hb]
  · simp [equiv, le, cmp, ha, hb]
  · simp [equiv, le, cmp, ha, hb]

/-- Ranks are equal exactly for tied values: the two zeros tie and all
NaN patterns tie. -/
theorem floatRank_eq_iff (a b : Bits) : equiv a b = true ↔ floatRank a = floatRank b := by
  rw [equiv_iff_le, floatRank_le_iff, floatRank_le_iff]
  constructor
  · intro ⟨h1, h2⟩; exact UInt64.le_antisymm h1 h2
  · intro h; rw [h]; exact ⟨UInt64.le_refl _, UInt64.le_refl _⟩

theorem le_refl (a : Bits) : le a a = true :=
  (floatRank_le_iff a a).mpr (UInt64.le_refl _)

theorem le_total (a b : Bits) : (le a b || le b a) = true := by
  rw [Bool.or_eq_true, floatRank_le_iff, floatRank_le_iff]
  exact UInt64.le_total _ _

theorem le_trans (a b c : Bits) (h1 : le a b = true) (h2 : le b c = true) : le a c = true := by
  rw [floatRank_le_iff] at *
  exact UInt64.le_trans h1 h2

theorem keyNat_lt_of_rank_lt (x y : Nat) (hx : x < 18446744073709551616)
    (hy : y < 18446744073709551616) (h : rankNat x < rankNat y) : keyNat x < keyNat y := by
  unfold rankNat keyNat at *
  by_cases h1 : x < 9223372036854775808 <;> by_cases h2 : y < 9223372036854775808 <;>
  by_cases z1 : x % 9223372036854775808 = 0 <;> by_cases z2 : y % 9223372036854775808 = 0 <;>
    simp only [h1, h2, z1, z2, ite_true, ite_false] at h ⊢ <;> omega

/-- The golars IEEE key is strictly monotone: a value strictly below
another in the polars order gets a strictly smaller key. -/
theorem ieeeKey_strictMono (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false)
    (h : lt a b = true) : ieeeKey a < ieeeKey b := by
  show (ieeeKey a).toNat < (ieeeKey b).toNat
  rw [ieeeKey_toNat, ieeeKey_toNat]
  exact keyNat_lt_of_rank_lt _ _ (toNat_lt a) (toNat_lt b) ((lt_iff_rank a b ha hb).mp h)

/-- Keys never contradict the order: a key not larger than another means
the value sorts before or ties. So a list sorted by key is sorted in the
polars order. -/
theorem ieeeKey_reflects (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false)
    (h : ieeeKey a ≤ ieeeKey b) : le a b = true := by
  have hk : (ieeeKey a).toNat ≤ (ieeeKey b).toNat := h
  rw [ieeeKey_toNat, ieeeKey_toNat] at hk
  rw [le_iff_rank a b ha hb]
  apply Nat.not_lt.mp; intro h'
  have := keyNat_lt_of_rank_lt _ _ (toNat_lt b) (toNat_lt a) h'
  omega

/-- The IEEE key is injective, so it separates -0.0 from 0.0 although the
polars order ties them. A radix sort on this key alone therefore places
every -0.0 before every 0.0, which is not the stable order. -/
theorem ieeeKey_injective (a b : Bits) (h : ieeeKey a = ieeeKey b) : a = b := by
  have h' := congrArg UInt64.toNat h
  rw [ieeeKey_toNat, ieeeKey_toNat] at h'
  apply UInt64.toNat_inj.mp
  have := toNat_lt a; have := toNat_lt b
  unfold keyNat at h'
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
    simp only [h1, h2, ite_true, ite_false] at h' <;> omega

def posZero : Bits := 0
def negZero : Bits := 0x8000000000000000

theorem zeros_equiv : equiv negZero posZero = true := by
  rw [equiv_iff_rank _ _ (by decide) (by decide)]; decide

theorem ieeeKey_splits_zeros : ieeeKey negZero ≠ ieeeKey posZero := by decide

/-- Apart from the two zeros, tied non-NaN values have equal IEEE keys
(they are then equal patterns). -/
theorem ieeeKey_eq_of_equiv (a b : Bits) (ha : isNaN a = false) (hb : isNaN b = false)
    (h : equiv a b = true) (hz : ¬ (isZero a = true ∧ isZero b = true)) :
    ieeeKey a = ieeeKey b := by
  rw [equiv_iff_rank a b ha hb] at h
  rw [isZero_eq, isZero_eq] at hz
  simp only [decide_eq_true_eq] at hz
  apply UInt64.toNat_inj.mp
  rw [ieeeKey_toNat, ieeeKey_toNat]
  have := toNat_lt a; have := toNat_lt b
  unfold rankNat keyNat at *
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
  by_cases z1 : a.toNat % 9223372036854775808 = 0 <;>
  by_cases z2 : b.toNat % 9223372036854775808 = 0 <;>
    simp only [h1, h2, z1, z2, ite_true, ite_false] at h ⊢ <;> omega

/-- `ieeeKeyInv` undoes `ieeeKey`, so the values written back after the
radix pass are the input patterns. -/
theorem ieeeKeyInv_ieeeKey (b : Bits) : ieeeKeyInv (ieeeKey b) = b := by
  apply UInt64.toNat_inj.mp
  have hb := toNat_lt b
  have hk := ieeeKey_toNat b
  unfold keyNat at hk
  have hs : ((ieeeKey b) >>> 63).toNat = (ieeeKey b).toNat / 9223372036854775808 := by
    rw [UInt64.toNat_shiftRight, Nat.shiftRight_eq_div_pow]; rfl
  have hm : ((1 : UInt64) <<< 63).toNat = 2 ^ 63 := by decide
  unfold ieeeKeyInv
  by_cases h : b.toNat < 9223372036854775808
  · simp only [h, ite_true] at hk
    have e : ieeeKey b >>> 63 = 1 := UInt64.toNat_inj.mp (by rw [hs, hk]; simp; omega)
    rw [e]; simp only [beq_self_eq_true, ite_true]
    rw [UInt64.toNat_and, UInt64.toNat_not, hm]
    have : UInt64.size - 1 - 2 ^ 63 = 2 ^ 63 - 1 := by decide
    rw [this, Nat.and_two_pow_sub_one_eq_mod, hk]; omega
  · simp only [h, ite_false] at hk
    have e : (ieeeKey b >>> 63 == 1) = false := by
      have h0 : (ieeeKey b >>> 63).toNat = 0 := by rw [hs, hk]; omega
      cases h1 : (ieeeKey b >>> 63 == 1)
      · rfl
      · simp only [beq_iff_eq] at h1; rw [h1] at h0; simp at h0
    rw [e]; simp only [Bool.false_eq_true, ite_false]
    rw [UInt64.toNat_not, hk]; show 18446744073709551615 - _ = _; omega

/-- Path A of `sortValuesFast`: when no pattern has the sign bit set, the
raw bits already follow the order (positive NaN patterns lie above +Inf). -/
theorem raw_strictMono (a b : Bits) (ha : a.toNat < 9223372036854775808)
    (hb : b.toNat < 9223372036854775808) (h : lt a b = true) : a < b := by
  show a.toNat < b.toNat
  have := toNat_lt a; have := toNat_lt b
  cases na : isNaN a <;> cases nb : isNaN b
  · rw [lt_iff_rank a b na nb] at h
    unfold rankNat keyNat at h
    by_cases z1 : a.toNat % 9223372036854775808 = 0 <;>
    by_cases z2 : b.toNat % 9223372036854775808 = 0 <;>
      simp only [ha, hb, z1, z2, ite_true, ite_false] at h <;> omega
  · rw [isNaN_eq] at na nb
    simp only [decide_eq_true_eq, decide_eq_false_iff_not] at na nb; omega
  · simp [lt, cmp, na, nb] at h
  · simp [lt, cmp, na, nb] at h

theorem raw_reflects (a b : Bits) (ha : a.toNat < 9223372036854775808)
    (hb : b.toNat < 9223372036854775808) (h : a ≤ b) : le a b = true := by
  have h' : a.toNat ≤ b.toNat := h
  have := toNat_lt a; have := toNat_lt b
  cases na : isNaN a <;> cases nb : isNaN b
  · rw [le_iff_rank a b na nb]
    unfold rankNat keyNat
    by_cases z1 : a.toNat % 9223372036854775808 = 0 <;>
    by_cases z2 : b.toNat % 9223372036854775808 = 0 <;>
      simp only [ha, hb, z1, z2, ite_true, ite_false] <;> omega
  · simp [le, cmp, na, nb]
  · rw [isNaN_eq] at na nb
    simp only [decide_eq_true_eq, decide_eq_false_iff_not] at na nb; omega
  · simp [le, cmp, na, nb]

/-! ## Signed int64 key -/

/-- Two's complement value of a pattern (as `BitVec.toInt`). -/
def sval (b : UInt64) : Int :=
  if b.toNat < 9223372036854775808 then b.toNat else (b.toNat : Int) - 18446744073709551616

/-- The key golars uses for int64 radix sorts: `uint64(v) ^ signBit`. -/
def intKey (b : UInt64) : UInt64 := b ^^^ ((1 : UInt64) <<< 63)

theorem testBit_hi (x i : Nat) (hx : x < 2 ^ 63) (hi : 63 < i) : x.testBit i = false :=
  Nat.testBit_lt_two_pow (Nat.lt_trans hx (Nat.pow_lt_pow_right (by decide) hi))

theorem testBit_hi' (x i : Nat) (hx : x < 2 ^ 63) (hi : 63 < i) : (2 ^ 63 + x).testBit i = false :=
  Nat.testBit_lt_two_pow (by
    have := Nat.pow_le_pow_right (n := 2) (by decide) (show 64 ≤ i by omega); omega)

theorem xor_top_lo (r : Nat) (hr : r < 2 ^ 63) : r ^^^ 2 ^ 63 = 2 ^ 63 + r := by
  apply Nat.eq_of_testBit_eq; intro i
  rw [Nat.testBit_xor, Nat.testBit_two_pow]
  by_cases hi : i = 63
  · subst hi; rw [Nat.testBit_two_pow_add_eq, Nat.testBit_lt_two_pow hr]; simp
  · by_cases hlt : i < 63
    · rw [Nat.testBit_two_pow_add_gt hlt]
      have : decide (63 = i) = false := by simp; omega
      simp [this]
    · rw [testBit_hi r i hr (by omega), testBit_hi' r i hr (by omega)]
      have : decide (63 = i) = false := by simp; omega
      simp [this]

theorem xor_top_hi (r : Nat) (hr : r < 2 ^ 63) : (2 ^ 63 + r) ^^^ 2 ^ 63 = r := by
  apply Nat.eq_of_testBit_eq; intro i
  rw [Nat.testBit_xor, Nat.testBit_two_pow]
  by_cases hi : i = 63
  · subst hi; rw [Nat.testBit_two_pow_add_eq, Nat.testBit_lt_two_pow hr]; simp
  · by_cases hlt : i < 63
    · rw [Nat.testBit_two_pow_add_gt hlt]
      have : decide (63 = i) = false := by simp; omega
      simp [this]
    · rw [testBit_hi r i hr (by omega), testBit_hi' r i hr (by omega)]
      have : decide (63 = i) = false := by simp; omega
      simp [this]

theorem intKey_toNat (b : UInt64) : (intKey b).toNat = (sval b + 9223372036854775808).toNat := by
  unfold intKey sval
  rw [UInt64.toNat_xor]
  have : ((1 : UInt64) <<< 63).toNat = 2 ^ 63 := by decide
  rw [this]
  have hb := toNat_lt b
  by_cases h : b.toNat < 9223372036854775808
  · rw [xor_top_lo _ (by omega)]; simp only [h, ite_true]; omega
  · have e : b.toNat = 2 ^ 63 + (b.toNat - 9223372036854775808) := by omega
    have x : b.toNat ^^^ 2 ^ 63 = b.toNat - 9223372036854775808 := by
      conv => lhs; rw [e]
      exact xor_top_hi _ (by omega)
    rw [x]; simp only [h, ite_false]; omega

/-- The int64 key is an order embedding into unsigned order. -/
theorem intKey_le_iff (a b : UInt64) : sval a ≤ sval b ↔ intKey a ≤ intKey b := by
  show _ ↔ (intKey a).toNat ≤ (intKey b).toNat
  rw [intKey_toNat, intKey_toNat]
  have := toNat_lt a; have := toNat_lt b
  unfold sval
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
    simp only [h1, h2, ite_true, ite_false] <;> omega

theorem intKey_injective (a b : UInt64) (h : intKey a = intKey b) : a = b := by
  have h' := congrArg UInt64.toNat h
  rw [intKey_toNat, intKey_toNat] at h'
  apply UInt64.toNat_inj.mp
  have := toNat_lt a; have := toNat_lt b
  unfold sval at h'
  by_cases h1 : a.toNat < 9223372036854775808 <;> by_cases h2 : b.toNat < 9223372036854775808 <;>
    simp only [h1, h2, ite_true, ite_false] at h' <;> omega

end Golars.Float
