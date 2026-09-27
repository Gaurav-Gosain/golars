package dataframe

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// joinFrames runs a resolved join.
func joinFrames(ctx context.Context, left, right *DataFrame, spec JoinSpec, cfg joinConfig) (*DataFrame, error) {
	types, err := checkJoinKeys(left.Schema(), right.Schema(), spec)
	if err != nil {
		return nil, err
	}
	layout, err := joinLayout(left.Schema(), right.Schema(), spec)
	if err != nil {
		return nil, err
	}
	if spec.How == CrossJoin {
		return crossJoin(ctx, left, right, layout, cfg)
	}
	lkeys := make([]*series.Series, len(spec.LeftOn))
	rkeys := make([]*series.Series, len(spec.RightOn))
	for i := range spec.LeftOn {
		lkeys[i], _ = left.Column(spec.LeftOn[i])
		rkeys[i], _ = right.Column(spec.RightOn[i])
	}

	var lIdx, rIdx []int
	if fastJoinEligible(spec, lkeys, rkeys) {
		lk, rk := lkeys[0], rkeys[0]
		if spec.How == RightJoin {
			rIdx, lIdx, err = hashJoinIndices(rk, lk, LeftJoin)
		} else {
			lIdx, rIdx, err = hashJoinIndices(lk, rk, spec.How)
		}
		if err != nil {
			return nil, err
		}
	} else {
		// polars' full-join validation counts repeated null keys as
		// duplicates on the smaller side (the left one on a tie), even
		// when nulls do not match.
		keep := keepNullsNone
		if spec.How == FullJoin && spec.Validate != ValidateManyToMany {
			keep = keepNullsRight
			if left.Height() <= right.Height() {
				keep = keepNullsLeft
			}
		}
		codes, err := encodeJoinKeys(ctx, lkeys, rkeys, types, spec.NullsEqual, keep)
		if err != nil {
			return nil, err
		}
		if err := validateJoinCodes(&codes, spec); err != nil {
			codes.release()
			return nil, err
		}
		lIdx, rIdx = joinByCodes(&codes, spec)
		codes.release()
	}
	out, err := buildJoinFrame(ctx, left, right, lIdx, rIdx, layout, spec.RightOn, cfg)
	// The index arrays are only read by the gathers; recycle them.
	intScratch.put(lIdx)
	intScratch.put(rIdx)
	return out, err
}

// fastJoinEligible reports whether the tuned single-key kernels in
// join.go apply. They produce left-major output with nulls never
// matching, so they serve inner, left and (with the sides swapped)
// right joins when no order or validation is requested.
func fastJoinEligible(spec JoinSpec, lkeys, rkeys []*series.Series) bool {
	if len(lkeys) != 1 || spec.NullsEqual || spec.Validate != ValidateManyToMany || spec.Order != JoinOrderNone {
		return false
	}
	switch spec.How {
	case InnerJoin, LeftJoin, RightJoin:
	default:
		return false
	}
	lt, rt := lkeys[0].DType(), rkeys[0].DType()
	if !lt.Equal(rt) {
		return false
	}
	switch lt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME32, arrow.TIME64,
		arrow.STRING:
		return true
	}
	return false
}

// validateJoinCodes applies the validate option: the checked sides must
// not repeat a key. Rows that can never match (null keys while nulls
// are not equal) do not count.
func validateJoinCodes(c *joinCodes, spec JoinSpec) error {
	var checkLeft, checkRight bool
	switch spec.Validate {
	case ValidateOneToOne:
		checkLeft, checkRight = true, true
	case ValidateOneToMany:
		checkLeft = true
	case ValidateManyToOne:
		checkRight = true
	default:
		return nil
	}
	seen := make([]bool, c.card)
	unique := func(codes []int32) bool {
		clear(seen)
		for _, k := range codes {
			if k < 0 {
				continue
			}
			if seen[k] {
				return false
			}
			seen[k] = true
		}
		return true
	}
	leftCodes, rightCodes := c.left, c.right
	if c.rawNulls != nil {
		if c.rawNullsLeft {
			leftCodes = c.rawNulls
		} else {
			rightCodes = c.rawNulls
		}
	}
	if (checkLeft && !unique(leftCodes)) || (checkRight && !unique(rightCodes)) {
		return fmt.Errorf("dataframe.Join: join keys did not fulfill %s validation", spec.Validate)
	}
	return nil
}

// joinByCodes emits the (left, right) row pairs of the join in the
// order the spec asks for. -1 in either slice marks a missing side. For
// semi and anti joins only the left slice is returned.
func joinByCodes(c *joinCodes, spec JoinSpec) (lIdx, rIdx []int) {
	switch spec.How {
	case SemiJoin, AntiJoin:
		return semiAntiByCodes(c, spec.How == SemiJoin), nil
	}
	rightMajor := spec.Order == JoinOrderRight || spec.Order == JoinOrderRightLeft
	if spec.How == RightJoin {
		rightMajor = !(spec.Order == JoinOrderLeft || spec.Order == JoinOrderLeftRight)
	}
	// The probe side drives the output order; the build side is
	// grouped by code so each probe row finds its matches in build
	// order.
	probe, build := c.left, c.right
	if rightMajor {
		probe, build = c.right, c.left
	}
	var keepProbe, keepBuild bool
	switch spec.How {
	case LeftJoin:
		keepProbe, keepBuild = !rightMajor, rightMajor
	case RightJoin:
		keepProbe, keepBuild = rightMajor, !rightMajor
	case FullJoin:
		keepProbe, keepBuild = true, true
	}
	pIdx, bIdx := probeCodes(probe, build, c.card, keepProbe, keepBuild)
	if rightMajor {
		return bIdx, pIdx
	}
	return pIdx, bIdx
}

// probeCodes joins probe rows against build rows with equal codes.
// keepProbe emits unmatched probe rows (paired with -1) in place;
// keepBuild appends the unmatched build rows at the end in build order.
func probeCodes(probe, build []int32, card int, keepProbe, keepBuild bool) (pIdx, bIdx []int) {
	// Group build rows by code: start[k]..start[k+1] indexes rows,
	// which holds build row numbers in ascending order per code.
	start := int32Scratch.get(card + 1)
	defer int32Scratch.put(start)
	clear(start)
	nb := 0
	for _, k := range build {
		if k >= 0 {
			start[k+1]++
			nb++
		}
	}
	for k := 1; k <= card; k++ {
		start[k] += start[k-1]
	}
	rows := int32Scratch.get(nb)
	defer int32Scratch.put(rows)
	cursor := int32Scratch.get(card)
	defer int32Scratch.put(cursor)
	copy(cursor, start[:card])
	for j, k := range build {
		if k >= 0 {
			rows[cursor[k]] = int32(j)
			cursor[k]++
		}
	}

	np := len(probe)
	count := func(lo, hi int) int {
		total := 0
		for _, k := range probe[lo:hi] {
			m := 0
			if k >= 0 {
				m = int(start[k+1] - start[k])
			}
			if m == 0 && keepProbe {
				m = 1
			}
			total += m
		}
		return total
	}
	fill := func(lo, hi, at int, pOut, bOut []int) {
		for i := lo; i < hi; i++ {
			k := probe[i]
			if k >= 0 {
				s, e := start[k], start[k+1]
				if s < e {
					for _, j := range rows[s:e] {
						pOut[at] = i
						bOut[at] = int(j)
						at++
					}
					continue
				}
			}
			if keepProbe {
				pOut[at] = i
				bOut[at] = -1
				at++
			}
		}
	}

	var unmatched []int
	if keepBuild {
		// A build row is unmatched when no probe row shares its code.
		hit := make([]bool, card)
		for _, k := range probe {
			if k >= 0 {
				hit[k] = true
			}
		}
		for j, k := range build {
			if k < 0 || !hit[k] {
				unmatched = append(unmatched, j)
			}
		}
	}

	parts := 1
	if np >= 64*1024 {
		parts = min(runtime.GOMAXPROCS(0), 8)
	}
	bounds := make([]int, parts+1)
	for p := range bounds {
		bounds[p] = p * np / parts
	}
	offs := make([]int, parts+1)
	if parts == 1 {
		offs[1] = count(0, np)
	} else {
		var wg sync.WaitGroup
		for p := range parts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				offs[p+1] = count(bounds[p], bounds[p+1])
			}()
		}
		wg.Wait()
	}
	for p := 1; p <= parts; p++ {
		offs[p] += offs[p-1]
	}
	total := offs[parts] + len(unmatched)
	pIdx = intScratch.get(total)
	bIdx = intScratch.get(total)
	if parts == 1 {
		fill(0, np, 0, pIdx, bIdx)
	} else {
		var wg sync.WaitGroup
		for p := range parts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fill(bounds[p], bounds[p+1], offs[p], pIdx, bIdx)
			}()
		}
		wg.Wait()
	}
	at := offs[parts]
	for _, j := range unmatched {
		pIdx[at] = -1
		bIdx[at] = j
		at++
	}
	return pIdx, bIdx
}

// semiAntiByCodes returns the left rows whose key appears on the right
// (semi) or does not (anti), in left order. The right side is only
// scanned for its codes.
func semiAntiByCodes(c *joinCodes, semi bool) []int {
	present := make([]bool, c.card)
	for _, k := range c.right {
		if k >= 0 {
			present[k] = true
		}
	}
	n := 0
	for _, k := range c.left {
		if (k >= 0 && present[k]) == semi {
			n++
		}
	}
	out := intScratch.get(n)
	at := 0
	for i, k := range c.left {
		if (k >= 0 && present[k]) == semi {
			out[at] = i
			at++
		}
	}
	return out
}

// buildJoinFrame gathers the output columns listed in layout. lIdx and
// rIdx pair up output rows with source rows (-1 for none); rIdx is nil
// for semi and anti joins.
func buildJoinFrame(ctx context.Context, left, right *DataFrame, lIdx, rIdx []int, layout []joinOutCol, rightOn []string, cfg joinConfig) (*DataFrame, error) {
	// Repeated joins (a benchmark loop or a streaming pipeline) recycle
	// the output buffers through the pool.
	cfg.alloc = compute.PoolingMem(cfg.alloc)
	n := len(lIdx)
	leftIdentity := n == left.Height() && isIdentity(lIdx)
	rightIdentity := rIdx != nil && n == right.Height() && isIdentity(rIdx)
	outCols := make([]*series.Series, 0, len(layout))
	release := func() {
		for _, c := range outCols {
			c.Release()
		}
	}
	for _, c := range layout {
		var (
			out *series.Series
			err error
		)
		switch c.src {
		case joinFromLeft:
			src, _ := left.Column(c.col)
			if leftIdentity {
				out = src.Clone()
			} else {
				out, err = gatherWithNulls(ctx, src, lIdx, n, cfg.alloc)
			}
		case joinFromRight:
			src, _ := right.Column(c.col)
			if rightIdentity {
				out = src.Clone()
			} else {
				out, err = gatherWithNulls(ctx, src, rIdx, n, cfg.alloc)
			}
		case joinFromBoth:
			lk, _ := left.Column(c.col)
			rk, _ := right.Column(rightOn[c.key])
			out, err = coalesceJoinKey(ctx, lk, rk, lIdx, rIdx, c.dtype, cfg.alloc)
		}
		if err != nil {
			release()
			return nil, err
		}
		if out.Name() != c.name {
			renamed := out.Rename(c.name)
			out.Release()
			out = renamed
		}
		outCols = append(outCols, out)
	}
	return New(outCols...)
}

// coalesceJoinKey builds a coalesced full-join key column: the left key
// where the left row exists, otherwise the right key, in the key pair's
// comparison dtype.
func coalesceJoinKey(ctx context.Context, lk, rk *series.Series, lIdx, rIdx []int, t dtype.DType, alloc memory.Allocator) (*series.Series, error) {
	// Categoricals are merged as strings: the two sides may carry
	// different dictionaries.
	work := t
	if t.IsDictionary() {
		work = dtype.String()
	}
	prep := func(s *series.Series) (*series.Series, error) {
		if s.DType().Equal(work) {
			return s.Clone(), nil
		}
		return compute.Cast(ctx, s, work)
	}
	l, err := prep(lk)
	if err != nil {
		return nil, err
	}
	defer l.Release()
	r, err := prep(rk)
	if err != nil {
		return nil, err
	}
	defer r.Release()
	both, err := series.ConcatSeries(lk.Name(), []*series.Series{l, r})
	if err != nil {
		return nil, err
	}
	defer both.Release()
	nl := lk.Len()
	idx := intScratch.get(len(lIdx))
	defer intScratch.put(idx)
	for i, li := range lIdx {
		switch {
		case li >= 0:
			idx[i] = li
		case rIdx[i] >= 0:
			idx[i] = nl + rIdx[i]
		default:
			idx[i] = -1
		}
	}
	out, err := both.Gather(idx, series.WithAllocator(alloc))
	if err != nil || !t.IsDictionary() {
		return out, err
	}
	defer out.Release()
	return compute.Cast(ctx, out, t)
}
