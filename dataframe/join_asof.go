package dataframe

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/intmap"
	"github.com/Gaurav-Gosain/golars/series"
)

// AsofStrategy selects which right row an asof join picks for a left key.
type AsofStrategy uint8

const (
	// AsofBackward picks the last right row whose key is <= the left key.
	// It is polars' default strategy.
	AsofBackward AsofStrategy = iota
	// AsofForward picks the first right row whose key is >= the left key.
	AsofForward
	// AsofNearest picks the right row with the closest key. On a distance
	// tie the larger key wins, and among equal keys the last row wins.
	AsofNearest
)

func (s AsofStrategy) String() string {
	switch s {
	case AsofBackward:
		return "backward"
	case AsofForward:
		return "forward"
	case AsofNearest:
		return "nearest"
	}
	return "?"
}

// AsofOptions configures DataFrame.JoinAsof. Field names follow the
// polars join_asof arguments. Boolean options whose polars default is
// true are expressed negatively so the Go zero value matches polars.
type AsofOptions struct {
	// On names the key column when it has the same name on both sides.
	// Set either On, or both LeftOn and RightOn.
	On      string
	LeftOn  string
	RightOn string

	// By lists exact-match group columns present on both sides. Use
	// ByLeft and ByRight (same length) when the names differ. Rows only
	// match inside the same group; null group keys never match.
	By      []string
	ByLeft  []string
	ByRight []string

	// Strategy defaults to AsofBackward.
	Strategy AsofStrategy

	// Suffix is appended to right column names that collide with a left
	// column name. Default "_right".
	Suffix string

	// Tolerance limits the key distance of a match (inclusive). nil
	// means unlimited. Numeric keys take an int or float. Temporal keys
	// take an integer in the key's physical unit (days for dates), a
	// time.Duration, or a polars duration string such as "2h" or "1d3h".
	Tolerance any

	// DisallowExactMatches mirrors allow_exact_matches=False: right rows
	// with a key equal to the left key are not considered.
	DisallowExactMatches bool

	// SkipSortednessCheck mirrors check_sortedness=False. Without by
	// groups both key columns must otherwise be sorted ascending (nulls
	// first). With by groups polars never checks, and neither do we.
	SkipSortednessCheck bool

	// NoCoalesce mirrors coalesce=False: when the key has the same name
	// on both sides the right key column is kept (with Suffix). When the
	// names differ the right key column is always kept, as in polars.
	NoCoalesce bool

	// Allocator is used for the output columns. Default
	// memory.DefaultAllocator.
	Allocator memory.Allocator
}

// ErrAsofNotSorted is returned when an asof key column is not sorted
// ascending and the sortedness check is enabled.
var ErrAsofNotSorted = errors.New("argument in operation 'asof_join' is not sorted, please sort the 'expr/series/column' first")

// JoinAsof performs a polars-compatible asof join: every left row is
// matched with at most one right row whose key is nearest according to
// opts.Strategy, optionally within exact-match By groups. The output has
// every left column followed by the right columns minus the right By
// columns (and minus the right key when it is coalesced), with
// colliding right names suffixed. Unmatched left rows get nulls.
//
// Keys may be any integer, float, date, datetime or duration column.
// Integer keys of different widths are compared as int64; mixing an
// integer and a float key is an error, as in polars.
func (df *DataFrame) JoinAsof(ctx context.Context, right *DataFrame, opts AsofOptions) (*DataFrame, error) {
	leftOn, rightOn := opts.LeftOn, opts.RightOn
	if opts.On != "" {
		if leftOn != "" || rightOn != "" {
			return nil, fmt.Errorf("dataframe.JoinAsof: set either On or LeftOn/RightOn, not both")
		}
		leftOn, rightOn = opts.On, opts.On
	}
	if leftOn == "" || rightOn == "" {
		return nil, fmt.Errorf("dataframe.JoinAsof: missing join key (set On or LeftOn and RightOn)")
	}
	byLeft, byRight := opts.ByLeft, opts.ByRight
	if len(opts.By) > 0 {
		if len(byLeft) > 0 || len(byRight) > 0 {
			return nil, fmt.Errorf("dataframe.JoinAsof: set either By or ByLeft/ByRight, not both")
		}
		byLeft, byRight = opts.By, opts.By
	}
	if len(byLeft) != len(byRight) {
		return nil, fmt.Errorf("dataframe.JoinAsof: ByLeft and ByRight must have the same length")
	}
	suffix := opts.Suffix
	if suffix == "" {
		suffix = "_right"
	}
	mem := opts.Allocator
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	lkCol, err := df.Column(leftOn)
	if err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	rkCol, err := right.Column(rightOn)
	if err != nil {
		return nil, fmt.Errorf("right: %w", err)
	}
	if err := asofKeysCompatible(lkCol, rkCol); err != nil {
		return nil, err
	}
	lk, err := newCmpCol(lkCol)
	if err != nil {
		return nil, fmt.Errorf("dataframe.JoinAsof: %w", err)
	}
	rk, err := newCmpCol(rkCol)
	if err != nil {
		return nil, fmt.Errorf("dataframe.JoinAsof: %w", err)
	}
	tolI, tolF, hasTol, err := asofTolerance(opts.Tolerance, lkCol.DType().Arrow(), lk.kind)
	if err != nil {
		return nil, err
	}

	lByCols := make([]*series.Series, len(byLeft))
	rByCols := make([]*series.Series, len(byRight))
	for i := range byLeft {
		if lByCols[i], err = df.Column(byLeft[i]); err != nil {
			return nil, fmt.Errorf("left: %w", err)
		}
		if rByCols[i], err = right.Column(byRight[i]); err != nil {
			return nil, fmt.Errorf("right: %w", err)
		}
	}

	if len(byLeft) == 0 && !opts.SkipSortednessCheck {
		if !cmpSortedNullsFirst(&lk) || !cmpSortedNullsFirst(&rk) {
			return nil, ErrAsofNotSorted
		}
	}

	sc := asofScanner{
		strategy: opts.Strategy,
		exact:    !opts.DisallowExactMatches,
		hasTol:   hasTol,
		tolI:     tolI,
		tolF:     tolF,
	}
	rightIdx := make([]int, df.Height())
	if len(byLeft) == 0 {
		asofNoGroups(ctx, &sc, &lk, &rk, rightIdx)
	} else {
		if err := asofWithGroups(ctx, &sc, &lk, &rk, lByCols, rByCols, rightIdx); err != nil {
			return nil, err
		}
	}

	dropRight := make(map[string]struct{}, len(byRight)+1)
	for _, b := range byRight {
		dropRight[b] = struct{}{}
	}
	if !opts.NoCoalesce && leftOn == rightOn {
		dropRight[rightOn] = struct{}{}
	}
	return asofBuildOutput(ctx, df, right, rightIdx, dropRight, suffix, compute.PoolingMem(mem))
}

func asofKeysCompatible(l, r *series.Series) error {
	lt, rt := l.DType(), r.DType()
	if lt.Equal(rt) {
		return nil
	}
	if lt.IsInteger() && rt.IsInteger() {
		return nil
	}
	if lt.IsFloating() && rt.IsFloating() {
		return nil
	}
	return fmt.Errorf("dataframe.JoinAsof: datatypes of join keys don't match - `%s`: %s on left does not match `%s`: %s on right",
		l.Name(), lt, r.Name(), rt)
}

// cmpSortedNullsFirst reports whether c is sorted ascending with any
// nulls before the first valid value. Floats order NaN last.
func cmpSortedNullsFirst(c *cmpCol) bool {
	n := c.arr.Len()
	i := 0
	for i < n && c.isNull(i) {
		i++
	}
	if c.nulls {
		for j := i; j < n; j++ {
			if c.isNull(j) {
				return false
			}
		}
	}
	switch c.kind {
	case cmpInt:
		v := c.ints[i:n]
		for j := 1; j < len(v); j++ {
			if v[j] < v[j-1] {
				return false
			}
		}
		return true
	case cmpFloat:
		v := c.floats[i:n]
		for j := 1; j < len(v); j++ {
			// Fast accept for ordered non-NaN pairs; NaN sorts last.
			if v[j] >= v[j-1] {
				continue
			}
			if compareFloatTotal(v[j-1], v[j]) > 0 {
				return false
			}
		}
		return true
	}
	for j := i + 1; j < n; j++ {
		if compareTotal(c, j-1, c, j) > 0 {
			return false
		}
	}
	return true
}

// asofScanner carries the per-join matching rules.
type asofScanner struct {
	strategy AsofStrategy
	exact    bool
	hasTol   bool
	tolI     int64
	tolF     float64
}

// asofScan matches left rows (rows, or the range [lo, hi) when rows is
// nil) against the ascending right keys rk. rpos maps positions in rk
// back to right row numbers (nil means identity). Results are written to
// out indexed by left row number, -1 for no match. The right pointers
// only move forward, so the scan is linear for sorted input.
func asofScan[T int64 | float64](sc *asofScanner, lk []T, lnull func(int) bool, rows []int, lo, hi int, rk []T, rpos []int, tol T, out []int) {
	m := len(rk)
	le, lt, fe := -1, 0, 0
	count := hi - lo
	if rows != nil {
		count = len(rows)
	}
	for k := range count {
		i := lo + k
		if rows != nil {
			i = rows[k]
		}
		if lnull(i) {
			out[i] = -1
			continue
		}
		x := lk[i]
		if x != x {
			out[i] = -1
			continue
		}
		if le < 0 {
			le = searchFirst(rk, func(v T) bool { return v > x })
			lt = searchFirst(rk, func(v T) bool { return v >= x })
		}
		for le < m && rk[le] <= x {
			le++
		}
		for lt < m && rk[lt] < x {
			lt++
		}
		b, f := lt-1, le
		if sc.exact {
			b, f = le-1, lt
		}
		bOK := b >= 0
		fOK := f < m && rk[f] == rk[f]
		c := -1
		switch sc.strategy {
		case AsofBackward:
			if bOK && (!sc.hasTol || x-rk[b] <= tol) {
				c = b
			}
		case AsofForward:
			if fOK && (!sc.hasTol || rk[f]-x <= tol) {
				c = f
			}
		case AsofNearest:
			var d T
			switch {
			case bOK && fOK:
				if rk[f]-x <= x-rk[b] {
					if fe < f {
						fe = f
					}
					for fe+1 < m && rk[fe+1] == rk[f] {
						fe++
					}
					c, d = fe, rk[f]-x
				} else {
					c, d = b, x-rk[b]
				}
			case bOK:
				c, d = b, x-rk[b]
			case fOK:
				if fe < f {
					fe = f
				}
				for fe+1 < m && rk[fe+1] == rk[f] {
					fe++
				}
				c, d = fe, rk[f]-x
			}
			if c >= 0 && sc.hasTol && d > tol {
				c = -1
			}
		}
		if c >= 0 && rpos != nil {
			c = rpos[c]
		}
		out[i] = c
	}
}

func searchFirst[T int64 | float64](xs []T, pred func(T) bool) int {
	lo, hi := 0, len(xs)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if !pred(xs[mid]) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// asofParallelMin is the left height at which the ungrouped scan splits
// into per-goroutine partitions, each seeded with a binary search.
const asofParallelMin = 64 * 1024

func asofNoGroups(ctx context.Context, sc *asofScanner, lk, rk *cmpCol, out []int) {
	n := len(out)
	// Right keys without nulls: sorted input puts nulls first, but the
	// check can be skipped, so compact whenever nulls are present.
	var rpos []int
	var rInts []int64
	var rFloats []float64
	if rk.nulls {
		m := rk.arr.Len()
		rpos = make([]int, 0, m-rk.arr.NullN())
		for j := range m {
			if !rk.isNull(j) {
				rpos = append(rpos, j)
			}
		}
		if lk.kind == cmpFloat {
			rFloats = make([]float64, len(rpos))
			for k, j := range rpos {
				rFloats[k] = rk.floats[j]
			}
		} else {
			rInts = make([]int64, len(rpos))
			for k, j := range rpos {
				rInts[k] = rk.ints[j]
			}
		}
	} else {
		rInts, rFloats = rk.ints, rk.floats
	}
	run := func(lo, hi int) {
		if lk.kind == cmpFloat {
			asofScan(sc, lk.floats, lk.isNull, nil, lo, hi, rFloats, rpos, sc.tolF, out)
		} else {
			asofScan(sc, lk.ints, lk.isNull, nil, lo, hi, rInts, rpos, sc.tolI, out)
		}
	}
	if n < asofParallelMin {
		run(0, n)
		return
	}
	workers := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for lo := 0; lo < n; lo += chunk {
		hi := min(lo+chunk, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			run(lo, hi)
		}()
	}
	wg.Wait()
}

// asofWithGroups assigns group ids from the by columns, lays out the
// right keys of each group contiguously, then scans each group
// independently. Groups are spread across goroutines for large inputs.
func asofWithGroups(ctx context.Context, sc *asofScanner, lk, rk *cmpCol, lBy, rBy []*series.Series, out []int) error {
	lArrs := make([]arrow.Array, len(lBy))
	rArrs := make([]arrow.Array, len(rBy))
	for i := range lBy {
		lArrs[i] = chunk0(lBy[i])
		rArrs[i] = chunk0(rBy[i])
		if err := groupKeyCompatible(lBy[i], rBy[i]); err != nil {
			return err
		}
	}
	nL, nR := len(out), rk.arr.Len()
	var lGid, rGid []int32
	var numGroups int
	if len(lBy) == 1 {
		var err error
		lGid, rGid, numGroups, err = singleByGroupIDs(lBy[0], rBy[0], nL, nR)
		if err != nil {
			return err
		}
	} else {
		lGid, rGid, numGroups = tupleGroupIDs(lArrs, rArrs, nL, nR)
	}
	// Right rows with a null asof key can never match.
	if rk.nulls {
		for j := range nR {
			if rk.isNull(j) {
				rGid[j] = -1
			}
		}
	}

	rOff := csrOffsets(rGid, numGroups)
	lOff := csrOffsets(lGid, numGroups)
	rPos := csrFill(rGid, rOff)
	lRows := csrFill(lGid, lOff)
	for i, g := range lGid {
		if g < 0 {
			out[i] = -1
		}
	}
	var rInts []int64
	var rFloats []float64
	if lk.kind == cmpFloat {
		rFloats = make([]float64, len(rPos))
		for k, j := range rPos {
			rFloats[k] = rk.floats[j]
		}
	} else {
		rInts = make([]int64, len(rPos))
		for k, j := range rPos {
			rInts[k] = rk.ints[j]
		}
	}
	runGroups := func(g0, g1 int) {
		for g := g0; g < g1; g++ {
			rows := lRows[lOff[g]:lOff[g+1]]
			if len(rows) == 0 {
				continue
			}
			r0, r1 := rOff[g], rOff[g+1]
			if lk.kind == cmpFloat {
				asofScan(sc, lk.floats, lk.isNull, rows, 0, 0, rFloats[r0:r1], rPos[r0:r1], sc.tolF, out)
			} else {
				asofScan(sc, lk.ints, lk.isNull, rows, 0, 0, rInts[r0:r1], rPos[r0:r1], sc.tolI, out)
			}
		}
	}
	if nL+nR < asofParallelMin || numGroups < 2 {
		runGroups(0, numGroups)
		return nil
	}
	// Split groups into contiguous ranges of roughly equal row counts.
	workers := min(runtime.GOMAXPROCS(0), 8)
	target := (nL + nR + workers - 1) / workers
	var wg sync.WaitGroup
	start, acc := 0, 0
	for g := range numGroups {
		acc += (lOff[g+1] - lOff[g]) + (rOff[g+1] - rOff[g])
		if acc >= target || g == numGroups-1 {
			g0, g1 := start, g+1
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ctx.Err() != nil {
					return
				}
				runGroups(g0, g1)
			}()
			start, acc = g+1, 0
		}
	}
	wg.Wait()
	return ctx.Err()
}

// singleByGroupIDs assigns group ids for a single by column. Numeric,
// temporal and boolean keys go through the open-addressing int map;
// strings use the string itself as the map key. Right rows define the
// groups; left rows only look them up. Null keys get id -1.
func singleByGroupIDs(l, r *series.Series, nL, nR int) ([]int32, []int32, int, error) {
	lc, err := newCmpCol(l)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("dataframe.JoinAsof: %w", err)
	}
	rc, err := newCmpCol(r)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("dataframe.JoinAsof: %w", err)
	}
	lGid := make([]int32, nL)
	rGid := make([]int32, nR)
	if lc.kind == cmpStr {
		ids := make(map[string]int32)
		for j := range nR {
			if rc.isNull(j) {
				rGid[j] = -1
				continue
			}
			v := rc.strs.Value(j)
			id, seen := ids[v]
			if !seen {
				id = int32(len(ids))
				ids[v] = id
			}
			rGid[j] = id
		}
		for i := range nL {
			if lc.isNull(i) {
				lGid[i] = -1
				continue
			}
			id, seen := ids[lc.strs.Value(i)]
			if !seen {
				id = -1
			}
			lGid[i] = id
		}
		return lGid, rGid, len(ids), nil
	}
	key := func(c *cmpCol, i int) int64 {
		if c.kind == cmpFloat {
			return int64(canonicalFloatBits(c.floats[i]))
		}
		return c.ints[i]
	}
	table := intmap.New(64)
	defer table.Release()
	next := int32(0)
	for j := range nR {
		if rc.isNull(j) {
			rGid[j] = -1
			continue
		}
		id, inserted := table.InsertOrGet(key(&rc, j), next)
		if inserted {
			next++
		}
		rGid[j] = id
	}
	for i := range nL {
		if lc.isNull(i) {
			lGid[i] = -1
			continue
		}
		id, ok := table.Get(key(&lc, i))
		if !ok {
			id = -1
		}
		lGid[i] = id
	}
	return lGid, rGid, int(next), nil
}

// tupleGroupIDs assigns group ids for multi-column by keys via the
// encoded key tuple.
func tupleGroupIDs(lArrs, rArrs []arrow.Array, nL, nR int) ([]int32, []int32, int) {
	ids := make(map[string]int32)
	var buf []byte
	rGid := make([]int32, nR)
	for j := range nR {
		var ok bool
		buf, ok = appendGroupKey(buf[:0], rArrs, j)
		if !ok {
			rGid[j] = -1
			continue
		}
		id, seen := ids[string(buf)]
		if !seen {
			id = int32(len(ids))
			ids[string(buf)] = id
		}
		rGid[j] = id
	}
	lGid := make([]int32, nL)
	for i := range nL {
		var ok bool
		buf, ok = appendGroupKey(buf[:0], lArrs, i)
		if !ok {
			lGid[i] = -1
			continue
		}
		id, seen := ids[string(buf)]
		if !seen {
			id = -1
		}
		lGid[i] = id
	}
	return lGid, rGid, len(ids)
}

// csrOffsets returns group start offsets (length numGroups+1) for the
// rows whose id is non-negative.
func csrOffsets(gid []int32, numGroups int) []int {
	off := make([]int, numGroups+1)
	for _, g := range gid {
		if g >= 0 {
			off[g+1]++
		}
	}
	for g := range numGroups {
		off[g+1] += off[g]
	}
	return off
}

// csrFill lists row numbers grouped by id, keeping row order inside each
// group.
func csrFill(gid []int32, off []int) []int {
	pos := make([]int, off[len(off)-1])
	next := make([]int, len(off)-1)
	copy(next, off[:len(off)-1])
	for i, g := range gid {
		if g >= 0 {
			pos[next[g]] = i
			next[g]++
		}
	}
	return pos
}

func groupKeyCompatible(l, r *series.Series) error {
	lt, rt := l.DType(), r.DType()
	if lt.Equal(rt) || (lt.IsInteger() && rt.IsInteger()) || (lt.IsFloating() && rt.IsFloating()) {
		return nil
	}
	return fmt.Errorf("dataframe.JoinAsof: by column dtypes differ: `%s` %s vs `%s` %s", l.Name(), lt, r.Name(), rt)
}

// appendGroupKey encodes row i of every column into buf so equal tuples
// produce equal bytes. Integers are widened to 64 bits and floats to
// float64 so keys of different widths still match. ok is false when any
// value is null.
func appendGroupKey(buf []byte, cols []arrow.Array, i int) ([]byte, bool) {
	for _, a := range cols {
		if a.IsNull(i) {
			return buf, false
		}
		switch v := a.(type) {
		case *array.Int64:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Int32:
			buf = appendU64(buf, uint64(int64(v.Value(i))))
		case *array.Int16:
			buf = appendU64(buf, uint64(int64(v.Value(i))))
		case *array.Int8:
			buf = appendU64(buf, uint64(int64(v.Value(i))))
		case *array.Uint64:
			buf = appendU64(buf, v.Value(i))
		case *array.Uint32:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Uint16:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Uint8:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Float64:
			buf = appendU64(buf, canonicalFloatBits(v.Value(i)))
		case *array.Float32:
			buf = appendU64(buf, canonicalFloatBits(float64(v.Value(i))))
		case *array.Boolean:
			if v.Value(i) {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case *array.String:
			s := v.Value(i)
			buf = appendU64(buf, uint64(len(s)))
			buf = append(buf, s...)
		case *array.LargeString:
			s := v.Value(i)
			buf = appendU64(buf, uint64(len(s)))
			buf = append(buf, s...)
		case *array.Binary:
			b := v.Value(i)
			buf = appendU64(buf, uint64(len(b)))
			buf = append(buf, b...)
		case *array.Timestamp:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Date32:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Date64:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Duration:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Time32:
			buf = appendU64(buf, uint64(v.Value(i)))
		case *array.Time64:
			buf = appendU64(buf, uint64(v.Value(i)))
		default:
			// Fall back to the string form for dtypes without a direct
			// encoding; length-prefixed so tuples cannot collide.
			s := a.ValueStr(i)
			buf = appendU64(buf, uint64(len(s)))
			buf = append(buf, s...)
		}
	}
	return buf, true
}

func appendU64(buf []byte, v uint64) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

// canonicalFloatBits maps -0 to +0 and every NaN to one payload so equal
// floats encode identically.
func canonicalFloatBits(f float64) uint64 {
	if f == 0 {
		return 0
	}
	if f != f {
		return 0x7ff8000000000001
	}
	return math.Float64bits(f)
}

// asofTolerance converts a user tolerance into the key's comparison
// domain. Integer-domain keys get an int64 bound (a float bound is
// floored since key distances are whole numbers).
func asofTolerance(tol any, keyType arrow.DataType, kind cmpKind) (int64, float64, bool, error) {
	if tol == nil {
		return 0, 0, false, nil
	}
	bad := func() (int64, float64, bool, error) {
		return 0, 0, false, fmt.Errorf("dataframe.JoinAsof: tolerance %v (%T) is not valid for key dtype %s", tol, tol, keyType)
	}
	temporalNanos := func() (int64, bool, error) {
		switch v := tol.(type) {
		case time.Duration:
			return int64(v), true, nil
		case string:
			ns, err := parsePolarsDuration(v)
			return ns, true, err
		}
		return 0, false, nil
	}
	if kind == cmpFloat {
		if f, ok := numericTolerance(tol); ok {
			return 0, f, true, nil
		}
		return bad()
	}
	var unitNanos int64
	switch t := keyType.(type) {
	case *arrow.TimestampType:
		unitNanos = timeUnitNanos(t.Unit)
	case *arrow.DurationType:
		unitNanos = timeUnitNanos(t.Unit)
	case *arrow.Date32Type:
		unitNanos = int64(24 * time.Hour)
	case *arrow.Date64Type:
		unitNanos = int64(time.Millisecond)
	}
	if ns, ok, err := temporalNanos(); ok {
		if err != nil {
			return 0, 0, false, err
		}
		if unitNanos == 0 {
			return bad()
		}
		return floorDiv(ns, unitNanos), 0, true, nil
	}
	if f, ok := numericTolerance(tol); ok {
		if math.IsNaN(f) {
			return bad()
		}
		if f >= math.MaxInt64 {
			return math.MaxInt64, 0, true, nil
		}
		return int64(math.Floor(f)), 0, true, nil
	}
	return bad()
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

func timeUnitNanos(u arrow.TimeUnit) int64 {
	switch u {
	case arrow.Second:
		return int64(time.Second)
	case arrow.Millisecond:
		return int64(time.Millisecond)
	case arrow.Microsecond:
		return int64(time.Microsecond)
	}
	return 1
}

func numericTolerance(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// parsePolarsDuration parses a fixed-length polars duration string such
// as "1d", "2h30m" or "500ms" into nanoseconds. Calendar units (mo, q,
// y) and index counts (i) have no fixed length and are rejected.
func parsePolarsDuration(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("dataframe: empty duration string")
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	var total int64
	for len(s) > 0 {
		k := 0
		for k < len(s) && s[k] >= '0' && s[k] <= '9' {
			k++
		}
		if k == 0 {
			return 0, fmt.Errorf("dataframe: invalid duration %q", s)
		}
		n, err := strconv.ParseInt(s[:k], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("dataframe: invalid duration %q: %w", s, err)
		}
		s = s[k:]
		u := 0
		for u < len(s) && (s[u] < '0' || s[u] > '9') {
			u++
		}
		var unit int64
		switch s[:u] {
		case "ns":
			unit = 1
		case "us":
			unit = int64(time.Microsecond)
		case "ms":
			unit = int64(time.Millisecond)
		case "s":
			unit = int64(time.Second)
		case "m":
			unit = int64(time.Minute)
		case "h":
			unit = int64(time.Hour)
		case "d":
			unit = int64(24 * time.Hour)
		case "w":
			unit = int64(7 * 24 * time.Hour)
		default:
			return 0, fmt.Errorf("dataframe: duration unit %q is not supported here (only fixed-length units ns, us, ms, s, m, h, d, w)", s[:u])
		}
		total += n * unit
		s = s[u:]
	}
	if neg {
		total = -total
	}
	return total, nil
}

// asofBuildOutput assembles left columns plus the gathered right columns.
func asofBuildOutput(ctx context.Context, left, right *DataFrame, rightIdx []int, dropRight map[string]struct{}, suffix string, mem memory.Allocator) (*DataFrame, error) {
	out := make([]*series.Series, 0, left.Width()+right.Width())
	release := func() {
		for _, c := range out {
			c.Release()
		}
	}
	for _, c := range left.cols {
		out = append(out, c.Clone())
	}
	for _, c := range right.cols {
		if _, drop := dropRight[c.Name()]; drop {
			continue
		}
		g, err := takeOptional(ctx, c, rightIdx, mem)
		if err != nil {
			release()
			return nil, err
		}
		if left.sch.Contains(c.Name()) {
			r := g.Rename(c.Name() + suffix)
			g.Release()
			g = r
		}
		out = append(out, g)
	}
	res, err := New(out...)
	if err != nil {
		release()
		return nil, err
	}
	return res, nil
}
