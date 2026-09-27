import Golars

/-!
# Test vector generator

Runs the executable Lean model on edge cases and seeded random inputs
and writes JSON files that the Go tests read (`testdata/verified`).
Usage: `lake exe gen-vectors <output dir>`.

Sorting uses `List.mergeSort` with the comparators of the model. Float
comparisons use `floatRank`, which `Golars.Float.floatRank_le_iff`
proves equivalent to the polars order.
-/

open Golars

/-! ## Minimal JSON -/

inductive J where
  | null
  | bool (b : Bool)
  | num (i : Int)
  | str (s : String)
  | arr (xs : List J)
  | obj (kvs : List (String × J))

instance : Inhabited J := ⟨.null⟩

partial def J.render : J → String
  | .null => "null"
  | .bool b => if b then "true" else "false"
  | .num i => toString i
  | .str s => "\"" ++ s ++ "\""
  | .arr xs => "[" ++ ",".intercalate (xs.map J.render) ++ "]"
  | .obj kvs => "{" ++ ",".intercalate (kvs.map fun (k, v) => "\"" ++ k ++ "\":" ++ v.render) ++ "}"

def J.opt {α : Type} (f : α → J) : Option α → J
  | none => .null
  | some a => f a

def hexDigit (n : Nat) : Char :=
  if n < 10 then Char.ofNat (48 + n) else Char.ofNat (87 + n)

def hex64 (x : UInt64) : String := Id.run do
  let mut s := ""
  for i in [0:16] do
    s := s.push (hexDigit ((x >>> (UInt64.ofNat (60 - 4 * i))).toNat % 16))
  return s

def jbits (x : UInt64) : J := .str (hex64 x)

def natsJ (l : List Nat) : J := .arr (l.map fun (i : Nat) => J.num (Int.ofNat i))

/-! ## Seeded random numbers (64-bit LCG, top bits) -/

structure Rng where
  s : UInt64

def Rng.next (r : Rng) : UInt64 × Rng :=
  let s := r.s * 6364136223846793005 + 1442695040888963407
  (s ^^^ (s >>> 29), ⟨s⟩)

def Rng.below (r : Rng) (n : Nat) : Nat × Rng :=
  let (x, r) := r.next
  ((x >>> 11).toNat % (max n 1), r)

def Rng.int (r : Rng) (lo hi : Int) : Int × Rng :=
  let (x, r) := r.below (hi - lo + 1).toNat
  (lo + x, r)

def Rng.list {α : Type} (r : Rng) (n : Nat) (f : Rng → α × Rng) : List α × Rng := Id.run do
  let mut r := r
  let mut out := #[]
  for _ in [0:n] do
    let (a, r') := f r
    out := out.push a
    r := r'
  return (out.toList, r)

/-! ## 1. Float and int keys -/

def floatPool : List UInt64 :=
  [ 0x0000000000000000, 0x8000000000000000, 0x3FF0000000000000, 0xBFF0000000000000
  , 0x7FF0000000000000, 0xFFF0000000000000, 0x7FF8000000000000, 0x7FF8000000000001
  , 0xFFF8000000000000, 0x7FF0000000000001, 0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF
  , 0x0000000000000001, 0x8000000000000001, 0x000FFFFFFFFFFFFF, 0x800FFFFFFFFFFFFF
  , 0x0010000000000000, 0x8010000000000000, 0x7FEFFFFFFFFFFFFF, 0xFFEFFFFFFFFFFFFF
  , 0x4004000000000000, 0xC004000000000000, 0x3FE0000000000000, 0xBFE0000000000000
  , 0x4000000000000000, 0xC000000000000000, 0x4059000000000000, 0xC059000000000000 ]

def floatKeyVectors : J := Id.run do
  let (rand, _) := (Rng.mk 17).list 200 (fun r => r.next)
  let xs := floatPool ++ rand
  return .arr (xs.map fun b =>
    .obj [ ("bits", jbits b)
         , ("is_nan", .bool (Golars.Float.isNaN b))
         , ("ieee_key", jbits (Golars.Float.ieeeKey b))
         , ("rank", jbits (Golars.Float.floatRank b)) ])

def intPool : List Int :=
  [0, 1, -1, 2, -2, 255, 256, -256, 65535, -65536, 9223372036854775807, -9223372036854775808,
   -9223372036854775807, 4294967296, -4294967296]

def toU64 (i : Int) : UInt64 := UInt64.ofInt i

def intKeyVectors : J :=
  .arr (intPool.map fun i =>
    .obj [("value", .num i), ("key", jbits (Golars.Float.intKey (toU64 i)))])

/-! ## 2 and 3. Sorting with nulls, NaN and multi-key -/

def floatLE (a b : UInt64) : Bool := Golars.Float.floatRank a ≤ Golars.Float.floatRank b
def intLE (a b : Int) : Bool := decide (a ≤ b)

def argsort {α : Type} [Inhabited α] (vals : List α) (le : α → α → Bool) : List Nat :=
  let arr := vals.toArray
  (List.range vals.length).mergeSort (fun i j => le arr[i]! arr[j]!)

instance : Inhabited (Option UInt64) := ⟨none⟩

def genFloatVals (r : Rng) (n : Nat) (nullPct : Nat) (poolOnly : Bool) : List (Option UInt64) × Rng :=
  r.list n fun r =>
    let (u, r) := r.below 100
    if u < nullPct then (none, r)
    else
      let (c, r) := r.below 3
      if c == 0 && !poolOnly then
        let (x, r) := r.next
        (some x, r)
      else if c == 1 then
        -- small numbers with many ties, including both zeros
        let (k, r) := r.int (-4) 4
        (some (if k == 0 then (if u % 2 == 0 then 0 else 0x8000000000000000)
               else Golars.Float.ieeeKeyInv (Golars.Float.ieeeKey (UInt64.ofInt (k * 4503599627370496)) )), r)
      else
        let (i, r) := r.below floatPool.length
        (some floatPool[i]!, r)

def genIntVals (r : Rng) (n : Nat) (nullPct : Nat) (lo hi : Int) (pool := true) :
    List (Option Int) × Rng :=
  r.list n fun r =>
    let (u, r) := r.below 100
    if u < nullPct then (none, r)
    else
      let (c, r) := r.below 10
      if c == 0 && pool then
        let (i, r) := r.below intPool.length
        (some intPool[i]!, r)
      else
        let (x, r) := r.int lo hi
        (some x, r)

def sortCase (dtype : String) (vals : List J) (perm : List Nat) (desc nl : Bool) : J :=
  .obj [ ("dtype", .str dtype), ("values", .arr vals), ("desc", .bool desc)
       , ("nulls_last", .bool nl), ("perm", natsJ perm) ]

def sortVectors : J := Id.run do
  let mut r : Rng := ⟨42⟩
  let mut out : Array J := #[]
  let sizes := [0, 1, 2, 5, 63, 64, 65, 200, 255, 256, 257, 1023, 1024, 1025]
  for n in sizes do
    for nullPct in [0, 20] do
      let dirs := if n > 300 then [(false, false), (true, true)]
        else [(false, false), (true, false), (false, true), (true, true)]
      for (desc, nl) in dirs do
        let (fv, r1) := genFloatVals r n nullPct false
        let (iv, r2) := genIntVals r1 n nullPct (-1000) 1000
        r := r2
        let fperm := argsort fv (Sort.nullLE floatLE desc nl)
        out := out.push (sortCase "f64" (fv.map (J.opt jbits)) fperm desc nl)
        let iperm := argsort iv (Sort.nullLE intLE desc nl)
        out := out.push (sortCase "i64" (iv.map (J.opt J.num)) iperm desc nl)
  -- wide int64 values (all eight radix bytes live)
  for n in [100, 2000] do
    let (iv, r1) := genIntVals r n 0 (-9223372036854775808) 9223372036854775807
    r := r1
    for desc in [false, true] do
      out := out.push (sortCase "i64" (iv.map (J.opt J.num)) (argsort iv (Sort.nullLE intLE desc false)) desc false)
  return .arr out.toList

/-- One large float64 case above the parallel radix cutoff (64K). Values
are indices into `floatPool` plus small numbers to keep the file small. -/
def bigSortVector : J := Id.run do
  let n := 66000
  let (fv, _) := genFloatVals ⟨7⟩ n 0 true
  let vals := fv.map (fun v => v.getD 0)
  let perm := argsort fv (Sort.nullLE floatLE false false)
  let permD := argsort fv (Sort.nullLE floatLE true false)
  -- distinct patterns, then each value as an index into that table
  let table := vals.foldl (fun (t : Array UInt64) v => if t.contains v then t else t.push v) #[]
  let idx := vals.map (fun v => (table.toList.idxOf v))
  return .obj [ ("table", .arr (table.toList.map jbits)), ("idx", natsJ idx)
              , ("perm", natsJ perm)
              , ("perm_desc", natsJ permD) ]

def multiSortVectors : J := Id.run do
  let mut r : Rng := ⟨99⟩
  let mut out : Array J := #[]
  for n in [0, 10, 63, 64, 300, 1200] do
    for nullPct in [0, 15] do
      for (d1, n1, d2, n2) in [(false, false, false, false), (true, false, false, true),
          (false, true, true, false), (true, true, true, true)] do
        let (a, r1) := genIntVals r n nullPct (-5) 5
        let (b, r2) := genFloatVals r1 n nullPct false
        let (c, r3) := genIntVals r2 n (if nullPct == 0 then 0 else 5) (-3) 3
        r := r3
        let arrA := a.toArray; let arrB := b.toArray; let arrC := c.toArray
        let le1 := fun i j => Sort.nullLE intLE d1 n1 arrA[i]! arrA[j]!
        let le2 := fun i j => Sort.nullLE floatLE d2 n2 arrB[i]! arrB[j]!
        let le3 := fun i j => Sort.nullLE intLE d1 n2 arrC[i]! arrC[j]!
        let perm := (List.range n).mergeSort (Sort.lexLE le1 (Sort.lexLE le2 le3))
        out := out.push (.obj
          [ ("a", .arr (a.map (J.opt J.num))), ("b", .arr (b.map (J.opt jbits)))
          , ("c", .arr (c.map (J.opt J.num)))
          , ("desc", .arr [.bool d1, .bool d2, .bool d1])
          , ("nulls_last", .arr [.bool n1, .bool n2, .bool n2])
          , ("perm", natsJ perm) ])
      -- all int64, ascending, no nulls: the arg-radix fast path
      let (a, r1) := genIntVals r n 0 (-50) 50
      let (c, r2) := genIntVals r1 n 0 (-1000000) 1000000
      r := r2
      let arrA := a.toArray; let arrC := c.toArray
      let perm := (List.range n).mergeSort
        (Sort.lexLE (fun i j => Sort.nullLE intLE false false arrA[i]! arrA[j]!)
          (fun i j => Sort.nullLE intLE false false arrC[i]! arrC[j]!))
      out := out.push (.obj
        [ ("a", .arr (a.map (J.opt J.num))), ("c", .arr (c.map (J.opt J.num)))
        , ("perm", natsJ perm) ])
  return .arr out.toList

/-! ## 4. Kleene logic -/

def jk : Kleene.KBool → J := J.opt J.bool

def kleeneVectors : J :=
  .arr (Kleene.polarsTable.map fun (a, b, _, _) =>
    .obj [ ("a", jk a), ("b", jk b), ("and", jk (Kleene.kand a b)), ("or", jk (Kleene.kor a b))
         , ("not_a", jk (Kleene.knot a)) ])

/-! ## 5. Expressions and plans -/

open Plan in
partial def exprJ : Expr → J
  | .col i => .obj [("op", .str "col"), ("i", .num i)]
  | .lit v => .obj [("op", .str "lit"), ("v", .num v)]
  | .add a b => .obj [("op", .str "add"), ("a", exprJ a), ("b", exprJ b)]
  | .mul a b => .obj [("op", .str "mul"), ("a", exprJ a), ("b", exprJ b)]
  | .fillNull a b => .obj [("op", .str "fill_null"), ("a", exprJ a), ("b", exprJ b)]
  | .neg a => .obj [("op", .str "neg"), ("a", exprJ a)]
  | .abs a => .obj [("op", .str "abs"), ("a", exprJ a)]
  | .cumSum a => .obj [("op", .str "cum_sum"), ("a", exprJ a)]
  | .shift a => .obj [("op", .str "shift"), ("a", exprJ a)]
  | .sum a => .obj [("op", .str "sum"), ("a", exprJ a)]

open Plan in
partial def predJ : Pred → J
  | .gt a b => .obj [("op", .str "gt"), ("a", exprJ a), ("b", exprJ b)]
  | .lt a b => .obj [("op", .str "lt"), ("a", exprJ a), ("b", exprJ b)]
  | .eq a b => .obj [("op", .str "eq"), ("a", exprJ a), ("b", exprJ b)]
  | .and p q => .obj [("op", .str "and"), ("p", predJ p), ("q", predJ q)]
  | .or p q => .obj [("op", .str "or"), ("p", predJ p), ("q", predJ q)]
  | .not p => .obj [("op", .str "not"), ("p", predJ p)]
  | .isNull a => .obj [("op", .str "is_null"), ("a", exprJ a)]
  | .lit b => .obj [("op", .str "litb"), ("v", .bool b)]

open Plan in
partial def planJ : Plan → J
  | .scan _ => .obj [("op", .str "scan")]
  | .filter p P => .obj [("op", .str "filter"), ("p", predJ p), ("input", planJ P)]
  | .withCol i e P => .obj [("op", .str "with_col"), ("i", .num i), ("e", exprJ e), ("input", planJ P)]
  | .select es P => .obj [("op", .str "select"), ("es", .arr (es.map exprJ)), ("input", planJ P)]
  | .slice o l P => .obj [("op", .str "slice"), ("off", .num o), ("len", .num l), ("input", planJ P)]
  | .sort i d n P => .obj [("op", .str "sort"), ("i", .num i), ("desc", .bool d),
      ("nulls_last", .bool n), ("input", planJ P)]

def rowsJ (rs : List Plan.Row) : J := .arr (rs.map fun r => .arr (r.map (J.opt J.num)))

open Plan in
partial def genExpr (r : Rng) (w depth : Nat) (allowWindow : Bool) : Expr × Rng :=
  let (c, r) := r.below (if depth == 0 then 2 else (if allowWindow then 10 else 7))
  match c with
  | 0 => let (i, r) := r.below w; (.col i, r)
  | 1 => let (v, r) := r.int (-3) 3; (.lit v, r)
  | 2 => let (a, r) := genExpr r w (depth - 1) allowWindow
         let (b, r) := genExpr r w (depth - 1) allowWindow; (.add a b, r)
  | 3 => let (a, r) := genExpr r w (depth - 1) allowWindow
         let (b, r) := genExpr r w (depth - 1) allowWindow; (.mul a b, r)
  | 4 => let (a, r) := genExpr r w (depth - 1) allowWindow; (.neg a, r)
  | 5 => let (a, r) := genExpr r w (depth - 1) allowWindow; (.abs a, r)
  | 6 => let (a, r) := genExpr r w (depth - 1) allowWindow
         let (b, r) := genExpr r w (depth - 1) allowWindow; (.fillNull a b, r)
  -- Window and aggregate arguments always read a column: golars
  -- broadcasts a literal to the frame height before shift, cum_sum or sum
  -- (pl.lit(2).shift(1) is null in polars but not in golars), which the
  -- vectors do not cover. See docs/verification.md.
  | 7 => let (a, r) := genColArg r w (depth - 1) allowWindow; (.cumSum a, r)
  | 8 => let (a, r) := genColArg r w (depth - 1) allowWindow; (.shift a, r)
  | _ => let (a, r) := genColArg r w (depth - 1) allowWindow; (.sum a, r)

where
  genColArg (r : Rng) (w depth : Nat) (allowWindow : Bool) : Expr × Rng :=
    let (a, r) := genExpr r w depth allowWindow
    if (Plan.refs a).isEmpty then let (i, r) := r.below w; (.col i, r) else (a, r)

open Plan in
partial def genPred (r : Rng) (w depth : Nat) (allowWindow : Bool) : Pred × Rng :=
  let (c, r) := r.below (if depth == 0 then 5 else 8)
  match c with
  | 0 | 1 => let (a, r) := genExpr r w 1 allowWindow
             let (b, r) := genExpr r w 0 allowWindow; (.gt a b, r)
  | 2 => let (a, r) := genExpr r w 1 allowWindow
         let (b, r) := genExpr r w 0 allowWindow; (.lt a b, r)
  | 3 => let (a, r) := genExpr r w 0 allowWindow
         let (b, r) := genExpr r w 0 allowWindow; (.eq a b, r)
  | 4 => let (a, r) := genExpr r w 1 allowWindow; (.isNull a, r)
  | 5 => let (p, r) := genPred r w (depth - 1) allowWindow
         let (q, r) := genPred r w (depth - 1) allowWindow; (.and p q, r)
  | 6 => let (p, r) := genPred r w (depth - 1) allowWindow
         let (q, r) := genPred r w (depth - 1) allowWindow; (.or p q, r)
  | _ => let (p, r) := genPred r w (depth - 1) allowWindow; (.not p, r)

def elementwiseVectors : J := Id.run do
  let mut r : Rng := ⟨5⟩
  let mut out : Array J := #[]
  for _ in [0:300] do
    let (e, r1) := genExpr r 3 3 true
    let (p, r2) := genPred r1 3 2 true
    r := r2
    out := out.push (.obj [("kind", .str "expr"), ("e", exprJ e), ("elementwise", .bool (Plan.isElem e))])
    out := out.push (.obj [("kind", .str "pred"), ("e", predJ p), ("elementwise", .bool (Plan.isElemP p))])
  return .arr out.toList

open Plan in
/-- A random plan over a scan of width `w`; returns the plan and its output width. -/
partial def genPlan (r : Rng) (rows : List Row) (w depth : Nat) : Plan × Nat × Rng :=
  if depth == 0 then (.scan rows, w, r) else
  let (inp, w, r) := genPlan r rows w (depth - 1)
  let (c, r) := r.below 8
  match c with
  | 0 | 1 =>
    let (p, r) := genPred r w 1 true
    (.filter p inp, w, r)
  | 2 =>
    let (i, r) := r.below (w + 1)
    let (e, r) := genExpr r w 2 true
    (.withCol i e inp, if i == w then w + 1 else w, r)
  | 3 =>
    let (k, r) := r.below 3
    -- the first output is a bare column: golars keeps the frame height
    -- for a projection of only literals where polars returns one row
    let (i0, r) := r.below w
    let (es, r) := r.list k (fun r =>
      let (b, r) := r.below 2
      if b == 0 then let (i, r) := r.below w; (Expr.col i, r) else genExpr r w 1 false)
    (.select (.col i0 :: es) inp, k + 1, r)
  | 4 =>
    let (o, r) := r.below 4
    let (l, r) := r.below 6
    (.slice o l inp, w, r)
  | _ =>
    let (i, r) := r.below w
    let (d, r) := r.below 2
    let (n, r) := r.below 2
    (.sort i (d == 1) (n == 1) inp, w, r)

/-- Handwritten plans for the rewrites and counterexamples. -/
def fixedPlans (rows : List Plan.Row) : List Plan.Plan :=
  open Plan in
  let s := Plan.scan rows
  [ .filter (.gt (.col 0) (.lit 0)) (.withCol 3 (.cumSum (.col 1)) s)
  , .filter (.gt (.col 0) (.lit 0)) (.withCol 3 (.shift (.col 1)) s)
  , .filter (.gt (.col 0) (.lit 0)) (.withCol 3 (.sum (.col 1)) s)
  , .filter (.gt (.col 0) (.lit 0)) (.withCol 3 (.add (.col 1) (.lit 1)) s)
  , .filter (.gt (.col 3) (.lit 0)) (.withCol 3 (.add (.col 1) (.lit 1)) s)
  , .filter (.gt (.col 0) (.lit 0)) (.slice 1 3 s)
  , .slice 1 3 (.withCol 3 (.cumSum (.col 0)) s)
  , .slice 1 3 (.withCol 3 (.mul (.col 0) (.col 1)) s)
  , .filter (.gt (.mul (.col 0) (.lit 2)) (.sum (.col 0))) (.filter (.gt (.col 0) (.lit 0)) s)
  , .filter (.and (.gt (.col 0) (.lit 0)) (.not (.isNull (.col 1)))) (.filter (.lt (.col 2) (.lit 3)) s)
  , .filter (.gt (.col 0) (.lit 0)) (.sort 1 true false s)
  , .filter (.gt (.col 0) (.lit 0)) (.select [.col 0, .add (.col 1) (.col 2)] s)
  , .filter (.gt (.col 1) (.lit 0)) (.select [.col 0, .add (.col 1) (.col 2)] s)
  , .filter (.lit false) (.select [.col 0, .lit 1] s)
  , .withCol 3 (.cumSum (.sum (.col 0))) s
  , .withCol 3 (.add (.col 0) (.sum (.col 1))) s
  , .filter (.or (.isNull (.col 0)) (.eq (.col 1) (.col 2))) s ]

def planVectors : J := Id.run do
  let mut r : Rng := ⟨2024⟩
  let mut out : Array J := #[]
  for t in [0:160] do
    let (n, r1) := r.below 9
    let (rows, r2) := r1.list n (fun r => r.list 3 (fun r =>
      let (u, r) := r.below 5
      if u == 0 then (none, r) else let (v, r) := r.int (-4) 4; (some v, r)))
    let (d, r3) := r2.below 4
    let (p, _, r4) := genPlan r3 rows 3 (d + 1)
    r := r4
    let plans := if t < 8 then p :: fixedPlans rows else [p]
    for p in plans do
      let o := Plan.optimize p
      out := out.push (.obj [ ("rows", rowsJ rows), ("plan", planJ p), ("optimized", planJ o)
                            , ("result", rowsJ (Plan.eval p)) ])
  return .arr out.toList

/-! ## 6. Aggregation -/

def aggObj (l : List (Option Int)) : J :=
  .obj [ ("sum", .num (Agg.sumI l)), ("count", .num (Agg.countI l))
       , ("min", J.opt J.num (Agg.minI l)), ("max", J.opt J.num (Agg.maxI l)) ]

def chunkVectors : J := Id.run do
  let mut r : Rng := ⟨11⟩
  let mut out : Array J := #[]
  for _ in [0:40] do
    let (k, r1) := r.below 5
    let (cs, r2) := r1.list (k + 1) (fun r =>
      let (n, r) := r.below 12
      genIntVals r n 30 (-100) 100 (pool := false))
    r := r2
    -- values fed through `Hom.chunks`: the partials merged equal the whole
    let whole := cs.flatten
    out := out.push (.obj [ ("chunks", .arr (cs.map fun c => .arr (c.map (J.opt J.num))))
                          , ("agg", aggObj whole)
                          , ("partials", .arr (cs.map aggObj)) ])
  return .arr out.toList

def groupVectors : J := Id.run do
  let mut r : Rng := ⟨77⟩
  let mut out : Array J := #[]
  for n in [0, 1, 7, 30, 200, 1000] do
    for nk in [1, 4, 50] do
      let (keys, r1) := genIntVals r n 10 0 (nk - 1)
      let (vals, r2) := genIntVals r1 n 20 (-50) 50 (pool := false)
      r := r2
      let rows := keys.zip vals
      let g := Agg.hashGroupBy (fun (x : Option Int × Option Int) => x.1) rows
      out := out.push (.obj [ ("keys", .arr (keys.map (J.opt J.num)))
                            , ("values", .arr (vals.map (J.opt J.num)))
                            , ("groups", .arr (g.map fun (k, grp) =>
                                .obj [("key", J.opt J.num k), ("agg", aggObj (grp.map (·.2)))])) ])
  return .arr out.toList

def fvJ : Option Agg.FV → J
  | none => .null
  | some .nan => .str "nan"
  | some (.num x) => .num x

def floatGroupVectors : J := Id.run do
  let mut r : Rng := ⟨78⟩
  let mut out : Array J := #[]
  for n in [0, 1, 5, 40, 300] do
    let (keys, r1) := genIntVals r n 0 0 5
    let (vals, r2) := r1.list n (fun r =>
      let (u, r) := r.below 10
      if u == 0 then (none, r) else if u < 4 then (some Agg.FV.nan, r)
      else let (x, r) := r.int (-20) 20; (some (Agg.FV.num x), r))
    r := r2
    let rows := keys.zip vals
    let g := Agg.hashGroupBy (fun (x : Option Int × Option Agg.FV) => x.1) rows
    out := out.push (.obj [ ("keys", .arr (keys.map (J.opt J.num)))
                          , ("values", .arr (vals.map fvJ))
                          , ("groups", .arr (g.map fun (k, grp) =>
                              .obj [ ("key", J.opt J.num k)
                                   , ("min", fvJ (Agg.fAgg true (grp.map (·.2))))
                                   , ("max", fvJ (Agg.fAgg false (grp.map (·.2)))) ])) ])
  return .arr out.toList

/-! ## 7. Joins -/

def joinVectors : J := Id.run do
  let mut r : Rng := ⟨313⟩
  let mut out : Array J := #[]
  for (nl, nr) in [(0, 3), (3, 0), (5, 5), (20, 10), (50, 80), (300, 200)] do
    for fl in [false, true] do
      -- int keys, or float keys drawn from a small pool with both zeros and NaN
      let (lk, r1) := if fl then genFloatVals r nl 10 true
        else let (v, r) := genIntVals r nl 10 0 40; (v.map (·.map toU64), r)
      let (rk, r2) := if fl then genFloatVals r1 nr 10 true
        else let (v, r) := genIntVals r1 nr 10 0 40; (v.map (·.map toU64), r)
      r := r2
      -- float keys join through their canonical rank (0.0 = -0.0, NaN = NaN)
      let keyOf := fun (v : Option UInt64) => if fl then v.map Golars.Float.floatRank else v
      let left := (List.range nl).zip lk
      let right := (List.range nr).zip rk
      let inner := Join.innerHash (fun (x : Nat × Option UInt64) => keyOf x.2)
        (fun (x : Nat × Option UInt64) => keyOf x.2) left right
      let lj := Join.leftJoin (fun (x : Nat × Option UInt64) => keyOf x.2)
        (fun (x : Nat × Option UInt64) => keyOf x.2) left right
      let kJ := fun (v : Option UInt64) => if fl then J.opt jbits v
        else J.opt (fun u => J.num (Golars.Float.sval u)) v
      out := out.push (.obj
        [ ("float", .bool fl)
        , ("left", .arr (lk.map kJ)), ("right", .arr (rk.map kJ))
        , ("inner", .arr (inner.map fun (a, b) => .arr [.num a.1, .num b.1]))
        , ("left_join", .arr (lj.map fun (a, b) => .arr [.num a.1, J.opt (fun b => .num b.1) b])) ])
  return .arr out.toList

def main (args : List String) : IO Unit := do
  let dir := args.headD "../testdata/verified"
  IO.FS.createDirAll dir
  let write := fun (name : String) (j : J) => IO.FS.writeFile (dir ++ "/" ++ name) (j.render ++ "\n")
  write "float_keys.json" (.obj [("float", floatKeyVectors), ("int", intKeyVectors)])
  write "sort.json" sortVectors
  write "sort_big.json" bigSortVector
  write "sort_multi.json" multiSortVectors
  write "kleene.json" kleeneVectors
  write "elementwise.json" elementwiseVectors
  write "plans.json" planVectors
  write "agg_chunks.json" chunkVectors
  write "groupby.json" groupVectors
  write "groupby_float.json" floatGroupVectors
  write "joins.json" joinVectors
  IO.println s!"wrote vectors to {dir}"
