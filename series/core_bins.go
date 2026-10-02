package series

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// matchIndex returns, for every row of s, the index into old of the
// first old value equal to it (null matches null, NaN matches NaN), or
// -1 when unmatched. s and old must share a dtype.
func (s *Series) matchIndex(old *Series) ([]int, error) {
	if !arrow.TypeEqual(s.data.DataType(), old.data.DataType()) {
		return nil, fmt.Errorf("series: replace old values have dtype %s, want %s", old.DType(), s.DType())
	}
	both, err := ConcatSeries("", []*Series{old, s})
	if err != nil {
		return nil, err
	}
	defer both.Release()
	ids, err := both.denseIDs()
	if err != nil {
		return nil, err
	}
	k := old.Len()
	pos := make([]int, ids.count())
	for i := range pos {
		pos[i] = -1
	}
	for j := range k {
		id := ids.ids[j]
		if pos[id] < 0 {
			pos[id] = j
		}
	}
	out := make([]int, s.Len())
	for i := range out {
		out[i] = pos[ids.ids[k+i]]
	}
	return out, nil
}

// ReplaceSeries replaces every value found in old by the value at the
// same position in new, keeping other values. old and new must already
// be cast to s's dtype (the eval layer does that). Mirrors polars'
// Expr.replace.
func (s *Series) ReplaceSeries(old, new *Series, opts ...Option) (*Series, error) {
	if old.Len() != new.Len() {
		return nil, fmt.Errorf("series: replace needs as many new values as old (%d vs %d)", new.Len(), old.Len())
	}
	match, err := s.matchIndex(old)
	if err != nil {
		return nil, err
	}
	pool, err := ConcatSeries(s.name, []*Series{s, new}, opts...)
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	n := s.Len()
	idx := make([]int, n)
	for i, m := range match {
		if m >= 0 {
			idx[i] = n + m
		} else {
			idx[i] = i
		}
	}
	return pool.Gather(idx, opts...)
}

// ReplaceStrictSeries maps old[k] to new[k]. Unmatched rows take the
// value of dflt at the same row (dflt has s's length, or length 1 to
// broadcast, and new's dtype); with dflt nil an unmatched non-null value is an error and
// an unmatched null stays null. Mirrors polars' Expr.replace_strict.
func (s *Series) ReplaceStrictSeries(old, new, dflt *Series, opts ...Option) (*Series, error) {
	if old.Len() != new.Len() {
		return nil, fmt.Errorf("series: replace_strict needs as many new values as old (%d vs %d)", new.Len(), old.Len())
	}
	match, err := s.matchIndex(old)
	if err != nil {
		return nil, err
	}
	k := new.Len()
	idx := make([]int, len(match))
	var pool *Series
	if dflt != nil {
		scalar := dflt.Len() == 1 && s.Len() != 1
		if dflt.Len() != s.Len() && !scalar {
			return nil, fmt.Errorf("series: replace_strict default has length %d, want %d", dflt.Len(), s.Len())
		}
		pool, err = ConcatSeries(s.name, []*Series{new, dflt}, opts...)
		if err != nil {
			return nil, err
		}
		for i, m := range match {
			if m >= 0 {
				idx[i] = m
			} else if scalar {
				idx[i] = k
			} else {
				idx[i] = k + i
			}
		}
	} else {
		pool = new.Rename(s.name)
		arr, err := s.single(nil)
		if err != nil {
			pool.Release()
			return nil, err
		}
		for i, m := range match {
			switch {
			case m >= 0:
				idx[i] = m
			case arr.IsNull(i):
				idx[i] = -1
			default:
				v := ValueAt(arr, i)
				arr.Release()
				pool.Release()
				return nil, fmt.Errorf("series: incomplete mapping specified for replace_strict: value %v has no replacement (pass a default)", v)
			}
		}
		arr.Release()
	}
	defer pool.Release()
	return pool.Gather(idx, opts...)
}

// CutOptions tunes Cut, QCut and QCutN.
type CutOptions struct {
	Labels          []string
	LeftClosed      bool
	IncludeBreaks   bool
	AllowDuplicates bool
}

// formatBreak renders a bin edge the way polars labels cut categories:
// shortest round-trip decimal, "inf" and "-inf" for the open ends.
func formatBreak(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// Cut assigns each value to an interval of breaks. Categories come back
// as strings (polars returns Categorical with the same labels).
// Nulls and NaN give null. With IncludeBreaks the output is a struct
// {breakpoint f64, category str}. Mirrors polars' Series.cut.
func (s *Series) Cut(breaks []float64, opts CutOptions, callerOpts ...Option) (*Series, error) {
	cfg := resolve(callerOpts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) && arr.DataType().ID() != arrow.NULL {
		return nil, fmt.Errorf("series: cut needs numeric input, got %s", s.DType())
	}
	br := append([]float64(nil), breaks...)
	if !slices.IsSorted(br) {
		return nil, fmt.Errorf("series: cut breaks must be sorted")
	}
	for i := 1; i < len(br); i++ {
		if br[i] == br[i-1] {
			return nil, fmt.Errorf("series: cut breaks are not unique")
		}
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	return cutValues(s.name, arr, vals, br, opts, cfg)
}

func cutValues(name string, arr arrow.Array, vals, br []float64, opts CutOptions, cfg config) (*Series, error) {
	nb := len(br) + 1
	labels := opts.Labels
	if labels != nil && len(labels) != nb {
		return nil, fmt.Errorf("series: cut needs %d labels, got %d", nb, len(labels))
	}
	edges := make([]float64, 0, nb+1)
	edges = append(edges, math.Inf(-1))
	edges = append(edges, br...)
	edges = append(edges, math.Inf(1))
	if labels == nil {
		labels = make([]string, nb)
		for b := range nb {
			if opts.LeftClosed {
				labels[b] = "[" + formatBreak(edges[b]) + ", " + formatBreak(edges[b+1]) + ")"
			} else {
				labels[b] = "(" + formatBreak(edges[b]) + ", " + formatBreak(edges[b+1]) + "]"
			}
		}
	}
	n := len(vals)
	cats := make([]string, n)
	bps := make([]float64, n)
	valid := make([]bool, n)
	for i, v := range vals {
		if arr.IsNull(i) || math.IsNaN(v) {
			continue
		}
		var b int
		if opts.LeftClosed {
			// Bin b is [edges[b], edges[b+1]).
			b = sortSearchFloat(br, v, true)
		} else {
			// Bin b is (edges[b], edges[b+1]].
			b = sortSearchFloat(br, v, false)
		}
		cats[i] = labels[b]
		bps[i] = edges[b+1]
		valid[i] = true
	}
	vo := validOrNil(valid)
	catS, err := FromString(name, cats, vo, WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	if !opts.IncludeBreaks {
		return catS, nil
	}
	defer catS.Release()
	cat := catS.Rename("category")
	defer cat.Release()
	bp, err := floatSeries("breakpoint", bps, vo, false, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bp.Release()
	return StructFromSeries(name, []*Series{bp, cat}, vo, WithAllocator(cfg.alloc))
}

// sortSearchFloat returns the bin index of v among sorted breaks. With
// leftClosed a value equal to a break goes to the bin on its right.
func sortSearchFloat(br []float64, v float64, leftClosed bool) int {
	lo, hi := 0, len(br)
	for lo < hi {
		mid := (lo + hi) / 2
		var goRight bool
		if leftClosed {
			goRight = v >= br[mid]
		} else {
			goRight = v > br[mid]
		}
		if goRight {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// QCut bins values by the quantiles (linear interpolation) at the given
// probabilities. Duplicate breaks are an error unless AllowDuplicates.
// Mirrors polars' Series.qcut.
func (s *Series) QCut(quantiles []float64, opts CutOptions, callerOpts ...Option) (*Series, error) {
	cfg := resolve(callerOpts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) && arr.DataType().ID() != arrow.NULL {
		return nil, fmt.Errorf("series: qcut needs numeric input, got %s", s.DType())
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	sorted, err := sortedFloats(arr)
	if err != nil {
		return nil, err
	}
	if len(sorted) == 0 {
		return FullNull(s.name, arrow.BinaryTypes.String, arr.Len(), callerOpts...)
	}
	br := make([]float64, 0, len(quantiles))
	for _, q := range quantiles {
		v, err := quantileSorted(sorted, q, QuantileLinear)
		if err != nil {
			return nil, err
		}
		br = append(br, v)
	}
	slices.Sort(br)
	dedup := br[:0:0]
	for i, b := range br {
		if i > 0 && b == br[i-1] {
			if !opts.AllowDuplicates {
				return nil, fmt.Errorf("series: qcut quantile breaks are not unique (set AllowDuplicates)")
			}
			continue
		}
		dedup = append(dedup, b)
	}
	return cutValues(s.name, arr, vals, dedup, opts, cfg)
}

// QCutN bins values into n equal-probability intervals.
func (s *Series) QCutN(n int, opts CutOptions, callerOpts ...Option) (*Series, error) {
	if n < 1 {
		return nil, fmt.Errorf("series: qcut needs at least one bin")
	}
	qs := make([]float64, n-1)
	for i := range qs {
		qs[i] = float64(i+1) / float64(n)
	}
	return s.QCut(qs, opts, callerOpts...)
}

// HistOptions tunes Hist.
type HistOptions struct {
	Bins              []float64
	BinCount          int
	IncludeBreakpoint bool
	IncludeCategory   bool
}

// formatHistEdge renders a hist category edge: up to six decimals with
// trailing zeros trimmed but at least one decimal, as polars prints.
func formatHistEdge(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	out := strconv.FormatFloat(v, 'f', 6, 64)
	out = strings.TrimRight(out, "0")
	if strings.HasSuffix(out, ".") {
		out += "0"
	}
	return out
}

// Hist counts values per bin. With explicit bins the first bin is
// closed on both ends and the rest are (a, b]; values outside the edges
// are ignored. Without bins, BinCount (default 10) equal-width bins
// span [min, max]. Nulls and NaN are skipped. Mirrors polars' hist.
func (s *Series) Hist(opts HistOptions, callerOpts ...Option) (*Series, error) {
	cfg := resolve(callerOpts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) && arr.DataType().ID() != arrow.NULL {
		return nil, fmt.Errorf("series: hist needs numeric input, got %s", s.DType())
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	data := vals[:0:0]
	for i, v := range vals {
		if arr.IsValid(i) && !math.IsNaN(v) {
			data = append(data, v)
		}
	}
	edges := append([]float64(nil), opts.Bins...)
	if len(edges) == 0 {
		bc := opts.BinCount
		if bc <= 0 {
			bc = 10
		}
		lo, hi := 0.0, 1.0
		if len(data) > 0 {
			lo, hi = slices.Min(data), slices.Max(data)
		}
		if lo == hi {
			lo -= 0.5
			hi += 0.5
		}
		width := (hi - lo) / float64(bc)
		edges = make([]float64, bc+1)
		for i := range edges {
			edges[i] = lo + float64(float64(i)*width)
		}
		edges[bc] = hi
	}
	nb := len(edges) - 1
	if nb < 1 {
		return nil, fmt.Errorf("series: hist needs at least two bin edges")
	}
	counts := make([]int, nb)
	for _, v := range data {
		if v < edges[0] || v > edges[nb] {
			continue
		}
		b := sortSearchFloat(edges[1:], v, false)
		if b >= nb {
			b = nb - 1
		}
		counts[b]++
	}
	countS, err := u32Series("count", counts, nil, cfg.alloc)
	if err != nil {
		return nil, err
	}
	if !opts.IncludeBreakpoint && !opts.IncludeCategory {
		defer countS.Release()
		return countS.Rename(s.name), nil
	}
	fields := []*Series{}
	defer func() { releaseSeries(fields) }()
	if opts.IncludeBreakpoint {
		bp, err := floatSeries("breakpoint", edges[1:], nil, false, cfg.alloc)
		if err != nil {
			countS.Release()
			return nil, err
		}
		fields = append(fields, bp)
	}
	if opts.IncludeCategory {
		cats := make([]string, nb)
		for b := range nb {
			open := "("
			if b == 0 {
				open = "["
			}
			cats[b] = open + formatHistEdge(edges[b]) + ", " + formatHistEdge(edges[b+1]) + "]"
		}
		c, err := FromString("category", cats, nil, WithAllocator(cfg.alloc))
		if err != nil {
			countS.Release()
			return nil, err
		}
		fields = append(fields, c)
	}
	fields = append(fields, countS)
	return StructFromSeries(s.name, fields, nil, WithAllocator(cfg.alloc))
}

func releaseSeries(ss []*Series) {
	for _, s := range ss {
		if s != nil {
			s.Release()
		}
	}
}

// SearchSortedSeries returns, for every value of needles, the insertion
// index (u32) into the ascending column s. side is "left", "right" or
// "any" (treated as left). A block of nulls at either end of s is
// respected; a null needle maps to the start of the null block.
func (s *Series) SearchSortedSeries(needles *Series, side string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	hay, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer hay.Release()
	nd, err := needles.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer nd.Release()
	c, err := pairCmp(hay, nd)
	if err != nil {
		return nil, err
	}
	n := hay.Len()
	// Locate the contiguous non-null range [lo, hi).
	lo, hi := 0, n
	if hay.NullN() > 0 {
		for lo < n && hay.IsNull(lo) {
			lo++
		}
		for hi > lo && hay.IsNull(hi-1) {
			hi--
		}
	}
	right := side == "right"
	out := make([]int, nd.Len())
	for j := range out {
		if nd.IsNull(j) {
			// Nulls sort as a block at one end; left finds its start and
			// right its end.
			switch {
			case lo > 0 && right:
				out[j] = lo
			case lo > 0:
				out[j] = 0
			case right:
				out[j] = n
			default:
				out[j] = hi
			}
			continue
		}
		a, b := lo, hi
		for a < b {
			mid := (a + b) / 2
			r := c(mid, j)
			if r < 0 || (right && r == 0) {
				a = mid + 1
			} else {
				b = mid
			}
		}
		out[j] = a
	}
	return u32Series(s.name, out, nil, cfg.alloc)
}

// Explode flattens a list or fixed-size list column into its elements.
// An empty or null list contributes one null row, as in polars. Other
// dtypes are returned as a clone.
func (s *Series) Explode(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	var child arrow.Array
	var bounds func(i int) (int, int)
	switch a := arr.(type) {
	case *array.List:
		child = a.ListValues()
		bounds = func(i int) (int, int) {
			x, y := a.ValueOffsets(i)
			return int(x), int(y)
		}
	case *array.LargeList:
		child = a.ListValues()
		bounds = func(i int) (int, int) {
			x, y := a.ValueOffsets(i)
			return int(x), int(y)
		}
	case *array.FixedSizeList:
		child = a.ListValues()
		w := int(a.DataType().(*arrow.FixedSizeListType).Len())
		off := a.Data().Offset()
		bounds = func(i int) (int, int) { return (off + i) * w, (off + i + 1) * w }
	default:
		return s.Clone(), nil
	}
	var idx []int
	for i := range arr.Len() {
		if arr.IsNull(i) {
			idx = append(idx, -1)
			continue
		}
		x, y := bounds(i)
		if x == y {
			idx = append(idx, -1)
			continue
		}
		for k := x; k < y; k++ {
			idx = append(idx, k)
		}
	}
	out, err := gatherArray(child, idx, cfg.alloc)
	if err != nil {
		return nil, err
	}
	return New(s.name, out)
}
