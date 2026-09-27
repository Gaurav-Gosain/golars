import Golars.Kleene
import Golars.Sort

/-!
# Logical plans and optimizer rewrites

A small relational algebra over lists of rows with a denotational
semantics. A row is a list of nullable integers addressed by position;
column `i` of the model is the golars column named `c<i>`.

Expressions are evaluated per column (`evalCol`), because `cum_sum`,
`shift` and aggregations read other rows. `isElem` is the model of
`expr.IsElementwise`; `evalCol_elem` shows that an expression it accepts
really is computed row by row.

Following golars, a projection whose expressions are all literals keeps
the input height (polars returns one row there; see docs/verification.md).
-/

namespace Golars.Plan

open Golars.Kleene

abbrev Val := Option Int
abbrev Row := List Val

inductive Expr where
  | col (i : Nat)
  | lit (v : Int)
  | add (a b : Expr)
  | mul (a b : Expr)
  | neg (a : Expr)
  | abs (a : Expr)
  | fillNull (a b : Expr)
  | cumSum (a : Expr)
  | shift (a : Expr)
  | sum (a : Expr)
  deriving Repr, DecidableEq, Inhabited

inductive Pred where
  | gt (a b : Expr)
  | lt (a b : Expr)
  | eq (a b : Expr)
  | and (p q : Pred)
  | or (p q : Pred)
  | not (p : Pred)
  | isNull (a : Expr)
  | lit (b : Bool)
  deriving Repr, DecidableEq, Inhabited

/-! ## Scalar helpers -/

def lift2 (f : Int → Int → Int) : Val → Val → Val
  | some a, some b => some (f a b)
  | _, _ => none

def cmp2 (f : Int → Int → Bool) : Val → Val → KBool
  | some a, some b => some (f a b)
  | _, _ => none

def cumSumAux (acc : Int) : List Val → List Val
  | [] => []
  | none :: t => none :: cumSumAux acc t
  | some x :: t => some (acc + x) :: cumSumAux (acc + x) t

/-- `shift(1)`: the first row becomes null. -/
def shift1 (l : List Val) : List Val :=
  match l with
  | [] => []
  | _ => none :: l.dropLast

/-- `sum` skips nulls; the sum of no values is 0 (polars and golars). -/
def sumVals (l : List Val) : Int := l.foldl (fun s v => s + v.getD 0) 0

/-! ## Semantics -/

def getCol (r : Row) (i : Nat) : Val := r.getD i none

/-- A column value: a unit (scalar) result such as a literal or an
aggregation, or a full column. Units broadcast when combined with a full
column and when a projection materialises them (polars semantics). -/
inductive Colv (γ : Type) where
  | unit (v : γ)
  | full (vs : List γ)
  deriving Repr, DecidableEq

def Colv.isUnit {γ : Type} : Colv γ → Bool
  | .unit _ => true
  | .full _ => false

def Colv.head {γ : Type} (d : γ) : Colv γ → γ
  | .unit v => v
  | .full vs => vs.headD d

def bcast {γ : Type} (n : Nat) : Colv γ → List γ
  | .unit v => List.replicate n v
  | .full vs => vs

def umap {γ δ : Type} (f : γ → δ) : Colv γ → Colv δ
  | .unit v => .unit (f v)
  | .full vs => .full (vs.map f)

def bin {γ δ ε : Type} (n : Nat) (f : γ → δ → ε) : Colv γ → Colv δ → Colv ε
  | .unit x, .unit y => .unit (f x y)
  | a, b => .full (List.zipWith f (bcast n a) (bcast n b))

def evalCol : Expr → List Row → Colv Val
  | .col i, rs => .full (rs.map (getCol · i))
  | .lit v, _ => .unit (some v)
  | .add a b, rs => bin rs.length (lift2 (· + ·)) (evalCol a rs) (evalCol b rs)
  | .mul a b, rs => bin rs.length (lift2 (· * ·)) (evalCol a rs) (evalCol b rs)
  | .neg a, rs => umap (Option.map (fun x => -x)) (evalCol a rs)
  | .abs a, rs => umap (Option.map (fun x => (Int.natAbs x : Int))) (evalCol a rs)
  | .fillNull a b, rs => bin rs.length (fun x y => x.or y) (evalCol a rs) (evalCol b rs)
  | .cumSum a, rs =>
    match evalCol a rs with
    | .unit v => .unit v
    | .full vs => .full (cumSumAux 0 vs)
  | .shift a, rs =>
    match evalCol a rs with
    | .unit _ => .unit none
    | .full vs => .full (shift1 vs)
  | .sum a, rs =>
    match evalCol a rs with
    | .unit v => .unit (some (v.getD 0))
    | .full vs => .unit (some (sumVals vs))

def evalPred : Pred → List Row → Colv KBool
  | .gt a b, rs => bin rs.length (cmp2 (fun x y => decide (x > y))) (evalCol a rs) (evalCol b rs)
  | .lt a b, rs => bin rs.length (cmp2 (fun x y => decide (x < y))) (evalCol a rs) (evalCol b rs)
  | .eq a b, rs => bin rs.length (cmp2 (fun x y => decide (x = y))) (evalCol a rs) (evalCol b rs)
  | .and p q, rs => bin rs.length kand (evalPred p rs) (evalPred q rs)
  | .or p q, rs => bin rs.length kor (evalPred p rs) (evalPred q rs)
  | .not p, rs => umap knot (evalPred p rs)
  | .isNull a, rs => umap (fun v => some v.isNone) (evalCol a rs)
  | .lit b, _ => .unit (some b)

/-- Static shape: whether an expression evaluates to a unit. -/
def unitE : Expr → Bool
  | .col _ => false
  | .lit _ | .sum _ => true
  | .add a b | .mul a b | .fillNull a b => unitE a && unitE b
  | .neg a | .abs a | .cumSum a | .shift a => unitE a

/-- Row-wise evaluation, meaningful for elementwise expressions. -/
def evalRow : Expr → Row → Val
  | .col i, r => getCol r i
  | .lit v, _ => some v
  | .add a b, r => lift2 (· + ·) (evalRow a r) (evalRow b r)
  | .mul a b, r => lift2 (· * ·) (evalRow a r) (evalRow b r)
  | .neg a, r => (evalRow a r).map (fun x => -x)
  | .abs a, r => (evalRow a r).map (fun x => (Int.natAbs x : Int))
  | .fillNull a b, r => (evalRow a r).or (evalRow b r)
  | _, _ => none

def predRow : Pred → Row → KBool
  | .gt a b, r => cmp2 (fun x y => decide (x > y)) (evalRow a r) (evalRow b r)
  | .lt a b, r => cmp2 (fun x y => decide (x < y)) (evalRow a r) (evalRow b r)
  | .eq a b, r => cmp2 (fun x y => decide (x = y)) (evalRow a r) (evalRow b r)
  | .and p q, r => kand (predRow p r) (predRow q r)
  | .or p q, r => kor (predRow p r) (predRow q r)
  | .not p, r => knot (predRow p r)
  | .isNull a, r => some (evalRow a r).isNone
  | .lit b, _ => some b

/-- Model of `expr.IsElementwise`. -/
def isElem : Expr → Bool
  | .col _ | .lit _ => true
  | .add a b | .mul a b | .fillNull a b => isElem a && isElem b
  | .neg a | .abs a => isElem a
  | .cumSum _ | .shift _ | .sum _ => false

def isElemP : Pred → Bool
  | .gt a b | .lt a b | .eq a b => isElem a && isElem b
  | .and p q | .or p q => isElemP p && isElemP q
  | .not p => isElemP p
  | .isNull a => isElem a
  | .lit _ => true

/-- Columns read by an expression (`expr.Columns`). -/
def refs : Expr → List Nat
  | .col i => [i]
  | .lit _ => []
  | .add a b | .mul a b | .fillNull a b => refs a ++ refs b
  | .neg a | .abs a | .cumSum a | .shift a | .sum a => refs a

def refsP : Pred → List Nat
  | .gt a b | .lt a b | .eq a b => refs a ++ refs b
  | .and p q | .or p q => refsP p ++ refsP q
  | .not p => refsP p
  | .isNull a => refs a
  | .lit _ => []

/-! ## Plans -/

inductive Plan where
  | scan (rows : List Row)
  | filter (p : Pred) (input : Plan)
  | withCol (i : Nat) (e : Expr) (input : Plan)
  | select (es : List Expr) (input : Plan)
  | slice (off len : Nat) (input : Plan)
  | sort (i : Nat) (desc nullsLast : Bool) (input : Plan)
  deriving Repr, Inhabited

/-- Keep the rows whose mask entry is true (null counts as false). -/
def maskFilter (rs : List Row) (m : List KBool) : List Row :=
  ((rs.zip m).filter (fun x => keep x.2)).map (·.1)

/-- Replace column `i`, or append it when `i` is the row width. Plans
built by the vector generator only use `i ≤ width`. -/
def setCol (r : Row) (i : Nat) (v : Val) : Row :=
  if i < r.length then r.set i v else if i = r.length then r ++ [v] else r

def colsToRows : Nat → List (List Val) → List Row
  | 0, _ => []
  | n + 1, cs => cs.map (·.headD none) :: colsToRows n (cs.map (·.tail))

def sortLE (i : Nat) (desc nullsLast : Bool) (a b : Row) : Bool :=
  Golars.Sort.nullLE (fun x y : Int => decide (x ≤ y)) desc nullsLast (getCol a i) (getCol b i)

def eval : Plan → List Row
  | .scan rows => rows
  | .filter p P => maskFilter (eval P) (bcast (eval P).length (evalPred p (eval P)))
  | .withCol i e P =>
    List.zipWith (fun r v => setCol r i v) (eval P) (bcast (eval P).length (evalCol e (eval P)))
  | .select es P =>
    let cs := es.map (evalCol · (eval P))
    -- polars: a projection of only unit results has one row
    if es ≠ [] ∧ cs.all Colv.isUnit then [cs.map (Colv.head none)]
    else colsToRows (eval P).length (cs.map (bcast (eval P).length))
  | .slice off len P => ((eval P).drop off).take len
  | .sort i desc nl P => (eval P).mergeSort (sortLE i desc nl)

/-! ## Elementwise lemmas -/

theorem zipWith_map_map {α β γ δ : Type} (f : β → γ → δ) (g : α → β) (h : α → γ) :
    ∀ l : List α, List.zipWith f (l.map g) (l.map h) = l.map (fun x => f (g x) (h x))
  | [] => rfl
  | x :: l => by simp [zipWith_map_map f g h l]

theorem bcast_umap {γ δ : Type} (n : Nat) (f : γ → δ) (c : Colv γ) :
    bcast n (umap f c) = (bcast n c).map f := by
  cases c <;> simp [umap, bcast, List.map_replicate]

theorem zipWith_replicate' {γ δ ε : Type} (f : γ → δ → ε) (x : γ) (y : δ) :
    ∀ n, List.zipWith f (List.replicate n x) (List.replicate n y) = List.replicate n (f x y)
  | 0 => rfl
  | n + 1 => by simp [List.replicate_succ, zipWith_replicate' f x y n]

theorem bcast_bin {γ δ ε : Type} (n : Nat) (f : γ → δ → ε) (a : Colv γ) (b : Colv δ) :
    bcast n (bin n f a b) = List.zipWith f (bcast n a) (bcast n b) := by
  cases a <;> cases b <;> simp [bin, bcast, zipWith_replicate']

theorem map_const_eq_replicate {γ δ : Type} (l : List γ) (v : δ) :
    l.map (fun _ => v) = List.replicate l.length v := by
  induction l <;> simp_all [List.replicate_succ]

theorem evalCol_elem : ∀ (e : Expr) (rs : List Row), isElem e = true →
    bcast rs.length (evalCol e rs) = rs.map (evalRow e)
  | .col _, _, _ => rfl
  | .lit v, rs, _ => by simp [evalCol, bcast, evalRow, map_const_eq_replicate]
  | .add a b, rs, h => by
    simp only [isElem, Bool.and_eq_true] at h
    simp only [evalCol, bcast_bin, evalCol_elem a rs h.1, evalCol_elem b rs h.2, zipWith_map_map]; rfl
  | .mul a b, rs, h => by
    simp only [isElem, Bool.and_eq_true] at h
    simp only [evalCol, bcast_bin, evalCol_elem a rs h.1, evalCol_elem b rs h.2, zipWith_map_map]; rfl
  | .fillNull a b, rs, h => by
    simp only [isElem, Bool.and_eq_true] at h
    simp only [evalCol, bcast_bin, evalCol_elem a rs h.1, evalCol_elem b rs h.2, zipWith_map_map]; rfl
  | .neg a, rs, h => by
    simp only [isElem] at h
    simp only [evalCol, bcast_umap, evalCol_elem a rs h, List.map_map]; rfl
  | .abs a, rs, h => by
    simp only [isElem] at h
    simp only [evalCol, bcast_umap, evalCol_elem a rs h, List.map_map]; rfl
  | .cumSum _, _, h => by simp [isElem] at h
  | .shift _, _, h => by simp [isElem] at h
  | .sum _, _, h => by simp [isElem] at h

theorem evalPred_elem : ∀ (p : Pred) (rs : List Row), isElemP p = true →
    bcast rs.length (evalPred p rs) = rs.map (predRow p)
  | .gt a b, rs, h | .lt a b, rs, h | .eq a b, rs, h => by
    simp only [isElemP, Bool.and_eq_true] at h
    simp only [evalPred, bcast_bin, evalCol_elem a rs h.1, evalCol_elem b rs h.2, zipWith_map_map]; rfl
  | .and p q, rs, h => by
    simp only [isElemP, Bool.and_eq_true] at h
    simp only [evalPred, bcast_bin, evalPred_elem p rs h.1, evalPred_elem q rs h.2, zipWith_map_map]; rfl
  | .or p q, rs, h => by
    simp only [isElemP, Bool.and_eq_true] at h
    simp only [evalPred, bcast_bin, evalPred_elem p rs h.1, evalPred_elem q rs h.2, zipWith_map_map]; rfl
  | .not p, rs, h => by
    simp only [isElemP] at h
    simp only [evalPred, bcast_umap, evalPred_elem p rs h, List.map_map]; rfl
  | .isNull a, rs, h => by
    simp only [isElemP] at h
    simp only [evalPred, bcast_umap, evalCol_elem a rs h, List.map_map]; rfl
  | .lit b, rs, _ => by simp [evalPred, bcast, predRow, map_const_eq_replicate]

theorem evalCol_isUnit : ∀ (e : Expr) (rs : List Row), (evalCol e rs).isUnit = unitE e
  | .col _, _ => rfl
  | .lit _, _ => rfl
  | .add a b, rs | .mul a b, rs | .fillNull a b, rs => by
    simp only [evalCol, unitE, ← evalCol_isUnit a rs, ← evalCol_isUnit b rs]
    cases evalCol a rs <;> cases evalCol b rs <;> rfl
  | .neg a, rs | .abs a, rs => by
    simp only [evalCol, unitE, ← evalCol_isUnit a rs]; cases evalCol a rs <;> rfl
  | .cumSum a, rs | .shift a, rs => by
    simp only [evalCol, unitE, ← evalCol_isUnit a rs]; cases evalCol a rs <;> rfl
  | .sum a, rs => by simp only [evalCol, unitE]; cases evalCol a rs <;> rfl

theorem maskFilter_map (rs : List Row) (f : Row → KBool) :
    maskFilter rs (rs.map f) = rs.filter (fun r => keep (f r)) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    unfold maskFilter at *
    by_cases h : keep (f r) = true <;> simp_all [List.filter_cons]

theorem filter_elem (p : Pred) (P : Plan) (h : isElemP p = true) :
    eval (.filter p P) = (eval P).filter (fun r => keep (predRow p r)) := by
  simp only [eval, evalPred_elem p _ h, maskFilter_map]

theorem colsToRows_map (es : List Expr) :
    ∀ rs : List Row, colsToRows rs.length (es.map (fun e => rs.map (evalRow e))) =
      rs.map (fun r => es.map (evalRow · r))
  | [] => rfl
  | r :: rs => by
    simp only [List.length_cons, colsToRows, List.map_map, List.map_cons, List.cons.injEq]
    constructor
    · rfl
    · have := colsToRows_map es rs
      rw [← this]; congr 1

theorem select_elem (es : List Expr) (P : Plan) (h : ∀ e ∈ es, isElem e = true)
    (hn : es.any (fun e => !unitE e) = true) :
    eval (.select es P) = (eval P).map (fun r => es.map (evalRow · r)) := by
  simp only [eval]
  have hu : ¬ (es ≠ [] ∧ (es.map (evalCol · (eval P))).all Colv.isUnit = true) := by
    intro ⟨_, ha⟩
    simp only [List.all_map, List.all_eq_true, Function.comp, evalCol_isUnit] at ha
    simp only [List.any_eq_true, Bool.not_eq_true'] at hn
    obtain ⟨e, he, hu⟩ := hn
    have := ha e he; simp_all
  simp only [hu, ↓reduceIte, List.map_map]
  have : es.map (bcast (eval P).length ∘ (evalCol · (eval P))) =
      es.map (fun e => (eval P).map (evalRow e)) :=
    List.map_congr_left (fun e he => evalCol_elem e _ (h e he))
  rw [this, colsToRows_map]

theorem withCol_elem (i : Nat) (e : Expr) (P : Plan) (h : isElem e = true) :
    eval (.withCol i e P) = (eval P).map (fun r => setCol r i (evalRow e r)) := by
  simp only [eval, evalCol_elem e _ h]
  induction (eval P) with
  | nil => rfl
  | cons r rs ih => simp [ih]

/-! ## Reading unchanged columns -/

theorem getCol_setCol_ne (r : Row) (i j : Nat) (v : Val) (h : j ≠ i) :
    getCol (setCol r i v) j = getCol r j := by
  unfold getCol setCol
  split
  · simp [List.getD_eq_getElem?_getD, List.getElem?_set, Ne.symm h]
  · split
    · rename_i h1 h2
      subst h2
      simp only [List.getD_eq_getElem?_getD]
      by_cases hj : j < r.length
      · rw [List.getElem?_append_left hj]
      · rw [List.getElem?_eq_none (l := r) (by omega), List.getElem?_eq_none (by simp; omega)]
    · rfl

theorem evalRow_setCol (e : Expr) (r : Row) (i : Nat) (v : Val) (h : i ∉ refs e) :
    evalRow e (setCol r i v) = evalRow e r := by
  induction e with
  | col j => simp only [refs, List.mem_singleton] at h; exact getCol_setCol_ne r i j v (Ne.symm h)
  | lit _ => rfl
  | add a b iha ihb | mul a b iha ihb | fillNull a b iha ihb =>
    simp only [refs, List.mem_append, not_or] at h
    simp only [evalRow, iha h.1, ihb h.2]
  | neg a ih | abs a ih => simp only [refs] at h; simp only [evalRow, ih h]
  | cumSum _ _ | shift _ _ | sum _ _ => rfl

theorem predRow_setCol (p : Pred) (r : Row) (i : Nat) (v : Val) (h : i ∉ refsP p) :
    predRow p (setCol r i v) = predRow p r := by
  induction p with
  | gt a b | lt a b | eq a b =>
    simp only [refsP, List.mem_append, not_or] at h
    simp only [predRow, evalRow_setCol a r i v h.1, evalRow_setCol b r i v h.2]
  | and p q ihp ihq | or p q ihp ihq =>
    simp only [refsP, List.mem_append, not_or] at h
    simp only [predRow, ihp h.1, ihq h.2]
  | not p ih => simp only [refsP] at h; simp only [predRow, ih h]
  | isNull a => simp only [refsP] at h; simp only [predRow, evalRow_setCol a r i v h]
  | lit _ => rfl

/-! ## Rewrite theorems -/

/-- **Filter pushdown through `with_columns`.** When the new column is
elementwise and the predicate does not read it, the filter can run first.
This is the `WithColumns` case of `PredicatePushdownPass`. -/
theorem filter_withCol_pushdown (p : Pred) (i : Nat) (e : Expr) (P : Plan)
    (hp : isElemP p = true) (he : isElem e = true) (hi : i ∉ refsP p) :
    eval (.filter p (.withCol i e P)) = eval (.withCol i e (.filter p P)) := by
  rw [filter_elem p _ hp, withCol_elem i e _ he, withCol_elem i e _ he, filter_elem p _ hp,
    List.filter_map]
  congr 1
  apply List.filter_congr; intro r _
  simp only [Function.comp, predRow_setCol p r i _ hi]

/-- Substitute expression `e` for column `i` in a predicate. -/
def substE (i : Nat) (e : Expr) : Expr → Expr
  | .col j => if j = i then e else .col j
  | .lit v => .lit v
  | .add a b => .add (substE i e a) (substE i e b)
  | .mul a b => .mul (substE i e a) (substE i e b)
  | .fillNull a b => .fillNull (substE i e a) (substE i e b)
  | .neg a => .neg (substE i e a)
  | .abs a => .abs (substE i e a)
  | .cumSum a => .cumSum (substE i e a)
  | .shift a => .shift (substE i e a)
  | .sum a => .sum (substE i e a)

def substP (i : Nat) (e : Expr) : Pred → Pred
  | .gt a b => .gt (substE i e a) (substE i e b)
  | .lt a b => .lt (substE i e a) (substE i e b)
  | .eq a b => .eq (substE i e a) (substE i e b)
  | .and p q => .and (substP i e p) (substP i e q)
  | .or p q => .or (substP i e p) (substP i e q)
  | .not p => .not (substP i e p)
  | .isNull a => .isNull (substE i e a)
  | .lit b => .lit b

theorem getCol_setCol_eq (r : Row) (i : Nat) (v : Val) (h : i ≤ r.length) :
    getCol (setCol r i v) i = v := by
  unfold getCol setCol
  by_cases h1 : i < r.length
  · simp [h1, List.getD_eq_getElem?_getD, List.getElem?_set]
  · have : i = r.length := by omega
    subst this; simp [List.getD_eq_getElem?_getD]

theorem evalRow_subst (e x : Expr) (r : Row) (i : Nat) (h : i ≤ r.length) (hx : isElem x = true) :
    evalRow (substE i e x) r = evalRow x (setCol r i (evalRow e r)) := by
  induction x with
  | col j =>
    by_cases hj : j = i
    · subst hj; simp [substE, evalRow, getCol_setCol_eq r j _ h]
    · simp [substE, hj, evalRow, getCol_setCol_ne r i j _ hj]
  | lit _ => rfl
  | add a b iha ihb | mul a b iha ihb | fillNull a b iha ihb =>
    simp only [isElem, Bool.and_eq_true] at hx
    simp only [substE, evalRow, iha hx.1, ihb hx.2]
  | neg a ih | abs a ih => simp only [isElem] at hx; simp only [substE, evalRow, ih hx]
  | cumSum _ _ | shift _ _ | sum _ _ => simp [isElem] at hx

theorem predRow_subst (e : Expr) (p : Pred) (r : Row) (i : Nat) (h : i ≤ r.length)
    (hp : isElemP p = true) :
    predRow (substP i e p) r = predRow p (setCol r i (evalRow e r)) := by
  induction p with
  | gt a b | lt a b | eq a b =>
    simp only [isElemP, Bool.and_eq_true] at hp
    simp only [substP, predRow, evalRow_subst e a r i h hp.1, evalRow_subst e b r i h hp.2]
  | and p q ihp ihq | or p q ihp ihq =>
    simp only [isElemP, Bool.and_eq_true] at hp
    simp only [substP, predRow, ihp hp.1, ihq hp.2]
  | not p ih => simp only [isElemP] at hp; simp only [substP, predRow, ih hp]
  | isNull a =>
    simp only [isElemP] at hp; simp only [substP, predRow, evalRow_subst e a r i h hp]
  | lit _ => rfl

theorem isElem_subst (e x : Expr) (he : isElem e = true) (hx : isElem x = true) :
    isElem (substE i e x) = true := by
  induction x with
  | col j => by_cases hj : j = i <;> simp [substE, hj, he, isElem]
  | lit _ => rfl
  | add a b iha ihb | mul a b iha ihb | fillNull a b iha ihb =>
    simp only [isElem, Bool.and_eq_true] at hx
    simp [substE, isElem, iha hx.1, ihb hx.2]
  | neg a ih | abs a ih => simp only [isElem] at hx; simp [substE, isElem, ih hx]
  | cumSum _ _ | shift _ _ | sum _ _ => simp [isElem] at hx

theorem isElemP_subst (e : Expr) (p : Pred) (he : isElem e = true) (hp : isElemP p = true) :
    isElemP (substP i e p) = true := by
  induction p with
  | gt a b | lt a b | eq a b =>
    simp only [isElemP, Bool.and_eq_true] at hp
    simp [substP, isElemP, isElem_subst e a he hp.1, isElem_subst e b he hp.2]
  | and p q ihp ihq | or p q ihp ihq =>
    simp only [isElemP, Bool.and_eq_true] at hp
    simp [substP, isElemP, ihp hp.1, ihq hp.2]
  | not p ih => simp only [isElemP] at hp; simp [substP, isElemP, ih hp]
  | isNull a => simp only [isElemP] at hp; simp [substP, isElemP, isElem_subst e a he hp]
  | lit _ => rfl

/-- **Filter pushdown through a computed column.** A predicate that
reads a column computed elementwise can be pushed below it by
substituting the defining expression (what polars does; golars only
pushes when the column passes through, which is the special case above). -/
theorem filter_withCol_subst (p : Pred) (i : Nat) (e : Expr) (P : Plan)
    (hp : isElemP p = true) (he : isElem e = true) (hw : ∀ r ∈ eval P, i ≤ r.length) :
    eval (.filter p (.withCol i e P)) = eval (.withCol i e (.filter (substP i e p) P)) := by
  rw [filter_elem p _ hp, withCol_elem i e _ he, withCol_elem i e _ he,
    filter_elem _ _ (isElemP_subst e p he hp), List.filter_map]
  congr 1
  apply List.filter_congr; intro r hr
  simp only [Function.comp, predRow_subst e p r i (hw r hr) hp]

/-- The passthrough condition of `projPassesThrough`: every column the
predicate reads is output column `j` defined as the bare input column `j`. -/
def passesThrough (es : List Expr) (p : Pred) : Bool :=
  (refsP p).all (fun j => es[j]? == some (.col j))

theorem getCol_select (es : List Expr) (r : Row) (j : Nat) (h : es[j]? = some (.col j)) :
    getCol (es.map (evalRow · r)) j = getCol r j := by
  unfold getCol
  simp [List.getD_eq_getElem?_getD, List.getElem?_map, h, evalRow, getCol]

theorem evalRow_select (es : List Expr) (x : Expr) (r : Row)
    (h : ∀ j ∈ refs x, es[j]? = some (.col j)) :
    evalRow x (es.map (evalRow · r)) = evalRow x r := by
  induction x with
  | col j => exact getCol_select es r j (h j (by simp [refs]))
  | lit _ => rfl
  | add a b iha ihb | mul a b iha ihb | fillNull a b iha ihb =>
    simp only [evalRow, iha (fun j hj => h j (by simp [refs, hj])),
      ihb (fun j hj => h j (by simp [refs, hj]))]
  | neg a ih | abs a ih => simp only [evalRow, ih (fun j hj => h j (by simp [refs, hj]))]
  | cumSum _ _ | shift _ _ | sum _ _ => rfl

theorem predRow_select (es : List Expr) (p : Pred) (r : Row)
    (h : ∀ j ∈ refsP p, es[j]? = some (.col j)) :
    predRow p (es.map (evalRow · r)) = predRow p r := by
  induction p with
  | gt a b | lt a b | eq a b =>
    simp only [predRow, evalRow_select es a r (fun j hj => h j (by simp [refsP, hj])),
      evalRow_select es b r (fun j hj => h j (by simp [refsP, hj]))]
  | and p q ihp ihq | or p q ihp ihq =>
    simp only [predRow, ihp (fun j hj => h j (by simp [refsP, hj])),
      ihq (fun j hj => h j (by simp [refsP, hj]))]
  | not p ih => simp only [predRow, ih (fun j hj => h j (by simp [refsP, hj]))]
  | isNull a => simp only [predRow, evalRow_select es a r (fun j hj => h j (by simp [refsP, hj]))]
  | lit _ => rfl

/-- **Filter pushdown through a projection** (the `Projection` case of
`PredicatePushdownPass`): sound when every projection expression is
elementwise and the predicate only reads passthrough columns. -/
theorem filter_select_pushdown (p : Pred) (es : List Expr) (P : Plan)
    (hp : isElemP p = true) (he : ∀ e ∈ es, isElem e = true) (ht : passesThrough es p = true)
    (hn : es.any (fun e => !unitE e) = true) :
    eval (.filter p (.select es P)) = eval (.select es (.filter p P)) := by
  rw [filter_elem p _ hp, select_elem es _ he hn, select_elem es _ he hn, filter_elem p _ hp,
    List.filter_map]
  congr 1
  apply List.filter_congr; intro r _
  simp only [passesThrough, List.all_eq_true, beq_iff_eq] at ht
  simp only [Function.comp, predRow_select es p r ht]

/-- **Consecutive filters fuse** into one `and` predicate when the outer
predicate is elementwise (golars merges a filter into the scan predicate
only in that case). -/
theorem filter_fuse (p q : Pred) (P : Plan) (hp : isElemP p = true) (hq : isElemP q = true) :
    eval (.filter p (.filter q P)) = eval (.filter (.and q p) P) := by
  have hqp : isElemP (.and q p) = true := by simp [isElemP, hp, hq]
  rw [filter_elem p _ hp, filter_elem q _ hq, filter_elem _ _ hqp, List.filter_filter]
  apply List.filter_congr; intro r _
  simp only [predRow, keep_kand, Bool.and_comm]

theorem sortLE_preorder (i : Nat) (desc nl : Bool) : Golars.Sort.TotalPreorder (sortLE i desc nl) :=
  have h := Golars.Sort.nullLE_preorder (β := Int) (le := fun x y => decide (x ≤ y))
    ⟨fun a b c h1 h2 => by simp at *; omega, fun a b => by simp; omega⟩ desc nl
  ⟨fun a b c => h.trans _ _ _, fun a b => h.total _ _⟩

/-- **Filter commutes with a stable sort** for every direction and null
placement. -/
theorem filter_sort (p : Pred) (i : Nat) (desc nl : Bool) (P : Plan) (hp : isElemP p = true) :
    eval (.filter p (.sort i desc nl P)) = eval (.sort i desc nl (.filter p P)) := by
  rw [filter_elem p _ hp]
  simp only [eval]
  rw [evalPred_elem p _ hp, maskFilter_map]
  exact Golars.Sort.filter_mergeSort (sortLE_preorder i desc nl) _ _

/-- **Slice pushdown through elementwise `with_columns`**
(`SlicePushdownPass`). -/
theorem slice_withCol_pushdown (off len i : Nat) (e : Expr) (P : Plan) (he : isElem e = true) :
    eval (.slice off len (.withCol i e P)) = eval (.withCol i e (.slice off len P)) := by
  have h1 := withCol_elem i e P he
  have h2 := withCol_elem i e (.slice off len P) he
  simp only [eval] at h1 h2 ⊢
  rw [h1, h2, List.map_take, List.map_drop]

/-! ## Projection pruning -/

/-- Null out every column not in `S`: the effect of reading only `S`. -/
def prune (S : List Nat) (r : Row) : Row := r.mapIdx (fun j v => if j ∈ S then v else none)

theorem getCol_prune (S : List Nat) (r : Row) (j : Nat) (h : j ∈ S) :
    getCol (prune S r) j = getCol r j := by
  unfold getCol prune
  simp [List.getD_eq_getElem?_getD, List.getElem?_mapIdx]
  cases r[j]? <;> simp [h]

theorem evalCol_prune (S : List Nat) : ∀ (e : Expr) (rs : List Row), (∀ j ∈ refs e, j ∈ S) →
    evalCol e (rs.map (prune S)) = evalCol e rs
  | .col j, rs, h => by
    simp only [evalCol, List.map_map]
    congr 1
    apply List.map_congr_left; intro r _
    exact getCol_prune S r j (h j (by simp [refs]))
  | .lit _, rs, _ => by simp [evalCol]
  | .add a b, rs, h | .mul a b, rs, h | .fillNull a b, rs, h => by
    simp only [evalCol, List.length_map, evalCol_prune S a rs (fun j hj => h j (by simp [refs, hj])),
      evalCol_prune S b rs (fun j hj => h j (by simp [refs, hj]))]
  | .neg a, rs, h | .abs a, rs, h | .cumSum a, rs, h | .shift a, rs, h | .sum a, rs, h =>
    by simp only [evalCol, evalCol_prune S a rs (fun j hj => h j (by simp [refs, hj]))]

/-- **Projection pruning is sound**: a projection only sees the columns
it reads, so reading the input restricted to those columns gives the
same result. -/
theorem select_prune (S : List Nat) (es : List Expr) (P : Plan)
    (h : ∀ e ∈ es, ∀ j ∈ refs e, j ∈ S) :
    eval (.select es (.scan ((eval P).map (prune S)))) = eval (.select es P) := by
  have hc : es.map (evalCol · ((eval P).map (prune S))) = es.map (evalCol · (eval P)) :=
    List.map_congr_left (fun e he => evalCol_prune S e _ (h e he))
  simp only [eval, List.length_map, hc]

/-! ## Counterexamples: rewrites that are not sound -/

def ex1 : List Row := [[some 1], [some 2], [some 3], [some 4]]

/-- Filter does not commute with slice. -/
theorem filter_slice_not_commute :
    eval (.filter (.gt (.col 0) (.lit 2)) (.slice 0 2 (.scan ex1))) ≠
      eval (.slice 0 2 (.filter (.gt (.col 0) (.lit 2)) (.scan ex1))) := by decide

/-- Filter cannot move below a `cum_sum` column even if it does not read it. -/
theorem filter_cumSum_not_commute :
    eval (.filter (.gt (.col 0) (.lit 1)) (.withCol 1 (.cumSum (.col 0)) (.scan ex1))) ≠
      eval (.withCol 1 (.cumSum (.col 0)) (.filter (.gt (.col 0) (.lit 1)) (.scan ex1))) := by
  decide

theorem filter_shift_not_commute :
    eval (.filter (.gt (.col 0) (.lit 1)) (.withCol 1 (.shift (.col 0)) (.scan ex1))) ≠
      eval (.withCol 1 (.shift (.col 0)) (.filter (.gt (.col 0) (.lit 1)) (.scan ex1))) := by
  decide

theorem filter_sum_not_commute :
    eval (.filter (.gt (.col 0) (.lit 1)) (.withCol 1 (.sum (.col 0)) (.scan ex1))) ≠
      eval (.withCol 1 (.sum (.col 0)) (.filter (.gt (.col 0) (.lit 1)) (.scan ex1))) := by
  decide

/-- Slice cannot move below `cum_sum` either. -/
theorem slice_cumSum_not_commute :
    eval (.slice 1 2 (.withCol 1 (.cumSum (.col 0)) (.scan ex1))) ≠
      eval (.withCol 1 (.cumSum (.col 0)) (.slice 1 2 (.scan ex1))) := by decide

/-- Two filters do not fuse when the outer one reads other rows
(`a > sum(a)` style predicates). -/
theorem filter_fuse_nonelem :
    eval (.filter (.gt (.mul (.col 0) (.lit 2)) (.sum (.col 0)))
        (.filter (.gt (.col 0) (.lit 3)) (.scan ex1))) ≠
      eval (.filter (.and (.gt (.col 0) (.lit 3)) (.gt (.mul (.col 0) (.lit 2)) (.sum (.col 0))))
        (.scan ex1)) := by decide

/-! ## The optimizer model

`optimize` applies the golars rules bottom-up. Each rule fires only
under the conditions proved sound above, so `optimize_sound` holds. -/

def pushStep : Plan → Plan
  | .filter p (.filter q P) =>
    if isElemP p && isElemP q then .filter (.and q p) P else .filter p (.filter q P)
  | .filter p (.withCol i e P) =>
    if isElemP p && isElem e && !(refsP p).contains i then .withCol i e (.filter p P)
    else .filter p (.withCol i e P)
  | .filter p (.select es P) =>
    if isElemP p && es.all isElem && passesThrough es p && es.any (fun e => !unitE e) then
      .select es (.filter p P)
    else .filter p (.select es P)
  | .filter p (.sort i d n P) =>
    if isElemP p then .sort i d n (.filter p P) else .filter p (.sort i d n P)
  | .slice o l (.withCol i e P) =>
    if isElem e then .withCol i e (.slice o l P) else .slice o l (.withCol i e P)
  | P => P

theorem pushStep_sound (P : Plan) : eval (pushStep P) = eval P := by
  unfold pushStep
  split
  · rename_i p q P
    split
    · rename_i h; simp only [Bool.and_eq_true] at h; exact (filter_fuse p q P h.1 h.2).symm
    · rfl
  · rename_i p i e P
    split
    · rename_i h
      simp only [Bool.and_eq_true, Bool.not_eq_true', List.contains_eq_mem, decide_eq_false_iff_not] at h
      exact (filter_withCol_pushdown p i e P h.1.1 h.1.2 h.2).symm
    · rfl
  · rename_i p es P
    split
    · rename_i h
      simp only [Bool.and_eq_true, List.all_eq_true] at h
      exact (filter_select_pushdown p es P h.1.1.1 h.1.1.2 h.1.2 h.2).symm
    · rfl
  · rename_i p i d n P
    split
    · rename_i h; exact (filter_sort p i d n P h).symm
    · rfl
  · rename_i o l i e P
    split
    · rename_i h; exact (slice_withCol_pushdown o l i e P h).symm
    · rfl
  · rfl

theorem eval_congr_input (P Q : Plan) (h : eval P = eval Q) :
    (∀ p, eval (.filter p P) = eval (.filter p Q)) ∧
    (∀ i e, eval (.withCol i e P) = eval (.withCol i e Q)) ∧
    (∀ es, eval (.select es P) = eval (.select es Q)) ∧
    (∀ o l, eval (.slice o l P) = eval (.slice o l Q)) ∧
    (∀ i d n, eval (.sort i d n P) = eval (.sort i d n Q)) := by
  simp only [eval, h]; exact ⟨fun _ => trivial, fun _ _ => trivial, fun _ => trivial,
    fun _ _ => trivial, fun _ _ _ => trivial⟩

def optimize : Plan → Plan
  | .scan rs => .scan rs
  | .filter p P => pushStep (.filter p (optimize P))
  | .withCol i e P => pushStep (.withCol i e (optimize P))
  | .select es P => pushStep (.select es (optimize P))
  | .slice o l P => pushStep (.slice o l (optimize P))
  | .sort i d n P => pushStep (.sort i d n (optimize P))

/-- **Optimizer soundness**: the modelled rewrite pass never changes the
result of a plan. -/
theorem optimize_sound : ∀ P : Plan, eval (optimize P) = eval P
  | .scan _ => rfl
  | .filter p P => by
    rw [optimize, pushStep_sound]; exact (eval_congr_input _ _ (optimize_sound P)).1 p
  | .withCol i e P => by
    rw [optimize, pushStep_sound]; exact (eval_congr_input _ _ (optimize_sound P)).2.1 i e
  | .select es P => by
    rw [optimize, pushStep_sound]; exact (eval_congr_input _ _ (optimize_sound P)).2.2.1 es
  | .slice o l P => by
    rw [optimize, pushStep_sound]; exact (eval_congr_input _ _ (optimize_sound P)).2.2.2.1 o l
  | .sort i d n P => by
    rw [optimize, pushStep_sound]; exact (eval_congr_input _ _ (optimize_sound P)).2.2.2.2 i d n

end Golars.Plan
