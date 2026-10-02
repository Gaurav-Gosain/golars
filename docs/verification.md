# Verification

golars has a small formal model of its core semantics, written in Lean 4
and kept in [`verify/`](../verify). The important properties are proved
about the model, and the Go implementation is checked against the model
with test vectors that the model generates. This follows the approach
AWS used for Cedar: a readable executable specification, proofs about the
specification, and differential testing of the production code against it.

## The trust boundary

Go code is not proved. What is proved is a statement about the Lean model.
The link between the model and golars is testing, not proof:

```
  Lean model ==(proofs)==> properties hold for the model
      |
      | lake exe gen-vectors (the model, executed)
      v
  testdata/verified/*.json ==(go test)==> golars gives the same answers
```

So a theorem such as "hash inner join equals the nested-loop join" means:
the modelled hash join equals the modelled nested-loop join for every
input. golars' hash join is then checked to produce the modelled result
on the committed inputs (edge cases plus seeded random cases). A bug in
golars on an input the vectors do not cover is not ruled out. A bug in the
model (the model not matching polars) is also possible; the model was
written from the golars source and checked against polars 1.39.3 where
the two should agree, and the choices are listed below.

What each side contributes:

- The proofs show that the algorithms golars uses are correct in
  principle: the key transforms are order embeddings, the pass structure
  of the radix sort (including skipped passes and the parallel chunked
  scatter) is a stable sort, the optimizer rewrites preserve results, and
  so on. They also pin down exactly where a rewrite is unsound.
- The vectors check that the Go code implements those algorithms, bit for
  bit where it matters (float sort output is compared as bit patterns).

## What is proved

Every theorem below is fully proved: there is no `sorry` and no `axiom`
in `verify/`, and `make verify` prints the axioms of each theorem
(`verify/Axioms.lean`) and fails on anything beyond Lean's standard
`propext`, `Classical.choice` and `Quot.sound`. Only Lean core is used
(no Mathlib, no Batteries).

### 1. Float total order and radix keys (`Golars/Float.lean`)

A float64 is modelled as its 64-bit pattern with the standard field
interpretation (sign, 11-bit exponent, 52-bit significand). The polars
sort order `cmp` is defined from the fields: NaN equals NaN and is above
every number, `-0.0` equals `0.0` (polars ties them and keeps input
order, checked against polars 1.39.3), and otherwise the order is by sign
and then by `(exponent, significand)`.

| Theorem | Statement |
| --- | --- |
| `floatRank_le_iff` | `series.floatRank` (the float64 `ArgSort` key) is an exact order embedding: `a ≤ b` in the polars order iff `floatRank a ≤ floatRank b`. |
| `floatRank_eq_iff` | Ranks are equal exactly for tied values (the two zeros, all NaN patterns). |
| `le_refl`, `le_total`, `le_trans` | The polars float order is a total preorder. |
| `ieeeKey_strictMono` | The IEEE transform in `compute.sortValuesFast` (`bits \| 1<<63` for non-negative, `^bits` for negative) maps strictly smaller non-NaN values to strictly smaller keys. |
| `ieeeKey_reflects` | A smaller or equal key means the value sorts before or ties, so key order is a valid polars order. |
| `ieeeKey_injective`, `zeros_equiv`, `ieeeKey_splits_zeros` | The transform is injective, so it separates `-0.0` from `0.0` although polars ties them. This is why the radix output needs its zero run restored (see "Bugs found"). |
| `ieeeKey_eq_of_equiv` | Apart from the two zeros, tied non-NaN values have equal keys. |
| `ieeeKeyInv_ieeeKey` | The inverse transform used to write values back returns the input pattern. |
| `raw_strictMono`, `raw_reflects` | When no sign bit is set (the fast path with no transform), raw bit order is the polars order. |
| `intKey_le_iff`, `intKey_injective` | The int64 key `uint64(v) ^ 1<<63` is an order embedding of signed order into unsigned order. |

### 2. Radix sort (`Golars/Sort.lean`)

The specification of a stable sort is `List.mergeSort` from Lean core.
`IsStableSortOf` characterises it (permutation, sorted, each class of
tied elements in input order) and `eq_mergeSort` shows the
characterisation determines the result.

| Theorem | Statement |
| --- | --- |
| `lsd_eq_mergeSort` | LSD radix sort in base `B` with `P` passes (each pass a stable counting scatter by one digit) equals the stable sort by key, for keys below `B^P`. |
| `lsdSkip_eq_mergeSort` | The same with golars' skipped passes (a pass whose digit is equal for every key is not run). |
| `lsd_sorted_perm` | The output is a permutation of the input and sorted. |
| `passChunks_eq` | The parallel pass (each worker scatters its chunk, workers in order within a digit) equals the serial pass. |
| `goDigit8`, `goDigit11` | The Go digit extraction (`v>>(8p)&0xFF`, and `v>>(11p)&2047` with a 9-bit last digit) computes digit `p` in base 256 and 2048. |
| `packed_le_iff`, `packed_or` | `series.packInt64Keys` packs `(value, row)` so that unsigned order is value order and then row order: a stable argsort. |

### 3. Null ordering and multi-key sorts (`Golars/Sort.lean`)

| Theorem | Statement |
| --- | --- |
| `nullLE_preorder` | The comparator of `compute.singleKeyCompare` (nulls first or last independent of direction, values reversed when descending) is a total preorder for all four combinations. |
| `nullsLast_compact` | Sorting the valid rows and appending the null rows in input order (`series.argSortInt64Asc`, `argSortFloat64Asc`) equals the stable sort with nulls last. |
| `lexLE_preorder` | The lexicographic comparator chain of `compute.SortIndicesMulti` is a total preorder. |
| `two_pass_eq_lex` | A stable sort by the second key followed by a stable sort by the first key is the lexicographic stable sort (the int64 multi-key arg-radix path). |
| `filter_mergeSort` | Filtering commutes with a stable sort. |

### 4. Three-valued logic (`Golars/Kleene.lean`)

| Theorem | Statement |
| --- | --- |
| `and_matches_polars`, `or_matches_polars`, `not_matches_polars`, `table_complete` | Kleene `and`, `or` and `not` match the truth tables observed from polars 1.39.3 on all nine input pairs. |
| `kand_comm`, `kor_comm`, `kand_assoc`, `kor_assoc`, `kand_distrib`, `kor_distrib`, `knot_knot` | The usual laws hold with null. |
| `demorgan_and`, `demorgan_or` | De Morgan's laws hold with null. |
| `keep_kand`, `keep_kor`, `keep_null` | The filter rule (keep only true rows, null counts as false) turns `and` into "both keep". |
| `keep_not_not_complement`, `excluded_middle_fails` | `filter(~p)` is not the complement of `filter(p)`: a null row is dropped by both. |

### 5. Optimizer rewrites (`Golars/Plan.lean`)

Plans are `scan`, `filter`, `with_columns` (one column), `select`,
`slice` and `sort`, over lists of rows of nullable integers, with a
denotational semantics. Expressions: columns, literals, `+`, `*`, `neg`,
`abs`, `fill_null`, `cum_sum`, `shift(1)` and `sum`; predicates: `>`,
`<`, `==`, `and`, `or`, `not`, `is_null`, boolean literals. As in polars,
literals and aggregations are unit (scalar) results that broadcast when
combined with a column. `isElem` models `expr.IsElementwise`.

| Theorem | Statement |
| --- | --- |
| `evalCol_elem` | An expression `isElem` accepts is computed row by row. This is the property the optimizer relies on. |
| `filter_withCol_pushdown` | Filter moves below an elementwise `with_columns` when the predicate does not read the new column (the golars rule). |
| `filter_withCol_subst` | More generally the filter can move below it by substituting the column's definition into the predicate (what polars does). |
| `filter_select_pushdown` | Filter moves below a projection whose expressions are elementwise when the predicate reads only passthrough columns and the projection has a non-scalar output. |
| `filter_fuse` | Two elementwise filters fuse into one `and`. |
| `filter_sort` | Filter commutes with a stable sort (any direction and null placement). |
| `slice_withCol_pushdown` | Slice moves below an elementwise `with_columns`. |
| `select_prune` | Projection pruning is sound: a projection gives the same result when its input only keeps the columns it reads. |
| `optimize_sound` | The modelled optimizer (the golars rules above, applied bottom-up) never changes the result. |
| `filter_slice_not_commute` | Counterexample: filter does not commute with slice. |
| `filter_cumSum_not_commute`, `filter_shift_not_commute`, `filter_sum_not_commute` | Counterexamples: filter cannot move below `cum_sum`, `shift` or an aggregation even when the predicate does not read that column. This is the class of bug fixed in `lazy/optimize.go`. |
| `slice_cumSum_not_commute` | Counterexample: slice cannot move below `cum_sum`. |
| `filter_fuse_nonelem` | Counterexample: filters do not fuse when the outer predicate reads other rows (`a*2 > sum(a)`). |

### 6. Aggregation (`Golars/Agg.lean`)

| Theorem | Statement |
| --- | --- |
| `Hom.chunks` | For a monoid homomorphism, merging per-chunk partial results equals aggregating the concatenation. |
| `sum_hom`, `count_hom`, `min_hom`, `max_hom`, `mean_hom` | Integer sum, non-null count, min, max and mean carried as `(sum, count)` are such homomorphisms (nulls skipped). |
| `fAgg_hom`, `fAgg_eq_spec` | Float min and max with NaN are mergeable and equal the polars rule: NaN is ignored unless every non-null value is NaN, null when there is no non-null value. |
| `hashGroupBy_eq` | Hash group-by gives groups in first-occurrence order, each holding its rows in input order. |
| `hash_perm_sort` | Hash group-by and sort group-by (stable sort by key, then runs) give the same `(key, aggregate)` pairs as a multiset, for any aggregate. |

Float sum is not associative, so a chunked or parallel float sum can
differ from a serial one in the last bits. Nothing is proved about it;
golars only guarantees a tolerance there.

### 7. Joins (`Golars/Join.lean`)

Keys are nullable and a null key matches nothing (polars'
`nulls_equal=False`).

| Theorem | Statement |
| --- | --- |
| `innerHash_eq` | The hash inner join (table built by the modelled hash group-by, probed with each left row) equals the nested-loop join row for row, so also as a multiset. |
| `leftJoin_keeps`, `leftJoin_length` | A left join keeps every left row. |
| `leftJoin_inner` | The matched rows of a left join are exactly the inner join. |
| `semi_anti_partition`, `semi_anti_disjoint`, `semi_mem` | Semi and anti joins partition the left side; the semi join keeps exactly the left rows the inner join uses. golars does not implement semi or anti joins yet, so these are model-only. |

## What is tested against the model

`lake exe gen-vectors` runs the model and writes `testdata/verified/`
(about 3 MB, committed so `go test` does not need Lean). Random cases use
a fixed seed, so regeneration is deterministic.

| File | Go test | What is compared |
| --- | --- | --- |
| `float_keys.json` | `series.TestVerifiedFloatRank` | `floatRank` on edge patterns (both zeros, infinities, NaN payloads of both signs, subnormals) and random patterns. |
| `sort.json` | `compute.TestVerifiedSortIndices`, `TestVerifiedSortValues`, `series.TestVerifiedArgSort` | Stable argsort for int64 and float64 with nulls, NaN, both zeros, every direction and null placement, sizes around the insertion and radix cutoffs; value sort output bit for bit. |
| `sort_big.json` | `compute.TestVerifiedSortBig` | A 66000-row float64 sort (above the parallel radix cutoff), both directions. |
| `sort_multi.json` | `compute.TestVerifiedSortMulti`, `dataframe.TestVerifiedSortBy` | Three-key sorts with mixed options and the all-int64 arg-radix path. |
| `kleene.json` | `compute.TestVerifiedKleene` | `And`, `Or`, `Not` truth tables and the null-drops filter rule. |
| `elementwise.json` | `lazy.TestVerifiedIsElementwise` | `expr.IsElementwise` against `isElem` on 600 generated expressions and predicates. |
| `plans.json` | `lazy.TestVerifiedPlans` | Each plan collected with and without the golars optimizer, and the model-optimized plan, all equal to the model's result. |
| `agg_chunks.json` | `compute.TestVerifiedAggChunks` | `SumInt64`, `Count`, `MinInt64`, `MaxInt64` on multi-chunk series and on each chunk. |
| `groupby.json`, `groupby_float.json` | `dataframe.TestVerifiedGroupBy`, `TestVerifiedGroupByFloatMinMax` | Group-by sum, count, min, max; float min/max with NaN. Compared as multisets. |
| `joins.json` | `dataframe.TestVerifiedJoins` | Inner and left joins on int keys and on float keys (0.0 matches -0.0, NaN matches NaN). Compared as multisets. |

## Bugs found

The vectors exposed three bugs, fixed in the same change:

1. **Float value sort tie order** (`compute/sort.go`). The radix path
   ordered `-0.0` before `0.0` (the transform is injective, see
   `ieeeKey_splits_zeros`), ordered NaN payloads by bits and wrote every
   NaN back as `math.NaN()`, and the descending sort reversed the
   ascending result, flipping every tie. polars keeps tied values in input
   order in both directions. The NaN run and the zero run are now
   restored from the input after the radix pass. The float32 path had the
   same problem through an unstable sort and a reverse.
2. **`fill_null(expr)` on empty and one-row frames** (`eval/eval_func.go`)
   failed with "wants int64 value, got <nil>".
3. **Scalar filter predicates** (`dataframe/filter.go`): a predicate that
   reduces to one value (`col("a").sum().is_null()`) failed with a length
   mismatch instead of broadcasting.

## Known differences not covered

The model follows polars. Two golars behaviours differ from polars and
are kept out of the vectors (the generator avoids them) rather than
changed here:

- A `select` of only literals keeps the frame height in golars; polars
  returns one row (`df.select(pl.lit(1))` has height 1).
- golars broadcasts a literal to the frame height before a window or
  aggregate function: `lit(2).shift(1)` is `[null, 2, 2, ...]` in golars
  and null on every row in polars; `lit(2).sum()` is `2 * height`.

## What is not covered

- Strings, temporal types, categoricals, nested types, decimals.
- Arithmetic semantics beyond the integer model: overflow, integer
  division, float arithmetic (float values appear only as bit patterns for
  ordering and as opaque numbers for min/max).
- Group-by keys other than nullable integers, multi-key joins, outer,
  cross and as-of joins, `over`, rolling and dynamic group-by.
- The streaming engine, IO, the SQL frontend and the scripting language.
- Optimizer passes other than predicate, slice and projection pushdown
  (simplification, CSE, type coercion) and pushdown through
  `Rename`, `Drop`, joins and group-by.
- Concurrency: the parallel radix pass is modelled as a chunked scatter,
  but data races and memory safety of the Go code are only covered by
  `go test -race`.

## How to run

The Go tests need nothing extra: `go test ./...` reads the committed
vectors.

To rebuild the proofs and regenerate the vectors, install
[elan](https://github.com/leanprover/elan) (the Lean toolchain manager):

```sh
curl https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh -sSf \
  | sh -s -- -y --default-toolchain none
make verify
```

`make verify` runs `verify/verify.sh`: `lake build` (checks every proof),
the axiom check, then `lake exe gen-vectors ../testdata/verified`, and
prints a `git diff --stat` of the vectors. The first run downloads the
pinned toolchain, about 2.7 GB under `~/.elan`.

The Lean toolchain is pinned in `verify/lean-toolchain`:
`leanprover/lean4:v4.34.1`. The project uses Lean core only.

## Layout

```
verify/
  lean-toolchain     pinned Lean version
  lakefile.toml      Lake project: library Golars, executable gen-vectors
  Golars/Float.lean  float order, IEEE and int64 keys
  Golars/Sort.lean   stable sort spec, LSD radix, nulls, multi-key
  Golars/Kleene.lean three-valued logic
  Golars/Plan.lean   plans, optimizer rewrites, counterexamples
  Golars/Agg.lean    partial aggregation, NaN min/max, group-by
  Golars/Join.lean   hash join, left/semi/anti joins
  GenVectors.lean    test vector generator
  Axioms.lean        #print axioms for every main theorem
  verify.sh          build, axiom check, regenerate vectors
testdata/verified/   generated vectors (committed)
```
