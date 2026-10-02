import Golars

/-! Prints the axioms each main theorem depends on. `make verify` fails
if any line mentions `sorryAx`; the expected set is Lean's standard
`propext`, `Quot.sound` and `Classical.choice` (or a subset). -/

-- 1. Float order and radix keys
#print axioms Golars.Float.floatRank_le_iff
#print axioms Golars.Float.floatRank_eq_iff
#print axioms Golars.Float.le_trans
#print axioms Golars.Float.le_total
#print axioms Golars.Float.ieeeKey_strictMono
#print axioms Golars.Float.ieeeKey_reflects
#print axioms Golars.Float.ieeeKey_injective
#print axioms Golars.Float.ieeeKey_eq_of_equiv
#print axioms Golars.Float.ieeeKeyInv_ieeeKey
#print axioms Golars.Float.zeros_equiv
#print axioms Golars.Float.ieeeKey_splits_zeros
#print axioms Golars.Float.raw_strictMono
#print axioms Golars.Float.raw_reflects
#print axioms Golars.Float.intKey_le_iff
#print axioms Golars.Float.intKey_injective
-- 2. Radix sort
#print axioms Golars.Sort.eq_mergeSort
#print axioms Golars.Sort.lsd_eq_mergeSort
#print axioms Golars.Sort.lsdSkip_eq_mergeSort
#print axioms Golars.Sort.lsd_sorted_perm
#print axioms Golars.Sort.passChunks_eq
#print axioms Golars.Sort.goDigit8
#print axioms Golars.Sort.goDigit11
#print axioms Golars.Sort.packed_le_iff
-- 3. Null ordering and multi-key sorts
#print axioms Golars.Sort.nullLE_preorder
#print axioms Golars.Sort.nullsLast_compact
#print axioms Golars.Sort.lexLE_preorder
#print axioms Golars.Sort.two_pass_eq_lex
#print axioms Golars.Sort.filter_mergeSort
-- 4. Three-valued logic
#print axioms Golars.Kleene.and_matches_polars
#print axioms Golars.Kleene.or_matches_polars
#print axioms Golars.Kleene.not_matches_polars
#print axioms Golars.Kleene.demorgan_and
#print axioms Golars.Kleene.demorgan_or
#print axioms Golars.Kleene.kand_assoc
#print axioms Golars.Kleene.kor_assoc
#print axioms Golars.Kleene.kand_comm
#print axioms Golars.Kleene.kor_comm
#print axioms Golars.Kleene.keep_kand
#print axioms Golars.Kleene.keep_not_not_complement
-- 5. Optimizer rewrites
#print axioms Golars.Plan.evalCol_elem
#print axioms Golars.Plan.filter_withCol_pushdown
#print axioms Golars.Plan.filter_withCol_subst
#print axioms Golars.Plan.filter_select_pushdown
#print axioms Golars.Plan.filter_fuse
#print axioms Golars.Plan.filter_sort
#print axioms Golars.Plan.slice_withCol_pushdown
#print axioms Golars.Plan.select_prune
#print axioms Golars.Plan.optimize_sound
#print axioms Golars.Plan.filter_slice_not_commute
#print axioms Golars.Plan.filter_cumSum_not_commute
#print axioms Golars.Plan.filter_shift_not_commute
#print axioms Golars.Plan.filter_sum_not_commute
#print axioms Golars.Plan.slice_cumSum_not_commute
#print axioms Golars.Plan.filter_fuse_nonelem
-- 6. Aggregation
#print axioms Golars.Agg.Hom.chunks
#print axioms Golars.Agg.sum_hom
#print axioms Golars.Agg.count_hom
#print axioms Golars.Agg.min_hom
#print axioms Golars.Agg.max_hom
#print axioms Golars.Agg.mean_hom
#print axioms Golars.Agg.fAgg_hom
#print axioms Golars.Agg.fAgg_eq_spec
#print axioms Golars.Agg.hashGroupBy_eq
#print axioms Golars.Agg.hash_perm_sort
-- 7. Joins
#print axioms Golars.Join.innerHash_eq
#print axioms Golars.Join.leftJoin_keeps
#print axioms Golars.Join.leftJoin_inner
#print axioms Golars.Join.leftJoin_length
#print axioms Golars.Join.semi_anti_partition
#print axioms Golars.Join.semi_anti_disjoint
#print axioms Golars.Join.semi_mem
