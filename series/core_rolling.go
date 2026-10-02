package series

import (
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
)

// RollingWindow configures the rolling methods in this file.
type RollingWindow struct {
	WindowSize int
	// MinSamples is the minimum number of non-null values per window;
	// 0 means WindowSize.
	MinSamples int
	// Center aligns the window on the row: rows [i-w/2, i-w/2+w-1].
	Center bool
}

func (o RollingWindow) bounds(i, n int) (int, int) {
	w := o.WindowSize
	start := i - w + 1
	if o.Center {
		start = i - w/2
	}
	end := start + w // exclusive
	return max(start, 0), min(end, n)
}

func (o RollingWindow) minSamples() int {
	if o.MinSamples <= 0 {
		return o.WindowSize
	}
	return o.MinSamples
}

func (o RollingWindow) check() error {
	if o.WindowSize < 1 {
		return fmt.Errorf("series: rolling window size must be at least 1")
	}
	return nil
}

// rollingFloat runs fn over the non-null float values of every window.
// fn receives the window values (in row order) and the row index and
// returns (value, ok). Windows with fewer than MinSamples values are
// null.
func (s *Series) rollingFloat(o RollingWindow, fn func(win []float64, i int) (float64, bool), opts []Option) (*Series, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) {
		return nil, fmt.Errorf("series: rolling needs numeric input, got %s", s.DType())
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	n := len(vals)
	out := make([]float64, n)
	valid := make([]bool, n)
	ms := o.minSamples()
	win := make([]float64, 0, o.WindowSize)
	for i := range n {
		a, b := o.bounds(i, n)
		win = win[:0]
		for j := a; j < b; j++ {
			if arr.IsValid(j) {
				win = append(win, vals[j])
			}
		}
		if len(win) < ms || len(win) == 0 {
			continue
		}
		v, ok := fn(win, i)
		if ok {
			out[i], valid[i] = v, true
		}
	}
	return floatSeries(s.name, out, validOrNil(valid), arr.DataType().ID() == arrow.FLOAT32, cfg.alloc)
}

// RollingQuantile computes the q-th quantile of each window with the
// given interpolation ("nearest" is polars' default). NaN sorts last.
func (s *Series) RollingQuantile(q float64, method string, o RollingWindow, opts ...Option) (*Series, error) {
	if q < 0 || q > 1 {
		return nil, fmt.Errorf("series: quantile must be in [0, 1], got %v", q)
	}
	scratch := make([]float64, 0, o.WindowSize)
	return s.rollingFloat(o, func(win []float64, _ int) (float64, bool) {
		scratch = append(scratch[:0], win...)
		slices.SortFunc(scratch, cmpTotalFloat)
		v, err := quantileSorted(scratch, q, method)
		return v, err == nil
	}, opts)
}

// RollingMedianWindow is the rolling median (linear interpolation).
func (s *Series) RollingMedianWindow(o RollingWindow, opts ...Option) (*Series, error) {
	return s.RollingQuantile(0.5, QuantileLinear, o, opts...)
}

// centralMoments returns n, m2, m3, m4 (population central moments).
func centralMoments(win []float64) (n, m2, m3, m4 float64) {
	n = float64(len(win))
	var mean float64
	for _, v := range win {
		mean += v
	}
	mean /= n
	for _, v := range win {
		d := v - mean
		d2 := d * d
		m2 += d2
		m3 += d2 * d
		m4 += d2 * d2
	}
	return n, m2 / n, m3 / n, m4 / n
}

// RollingSkew is the rolling sample skewness. bias=false applies the
// adjusted Fisher-Pearson correction.
func (s *Series) RollingSkew(bias bool, o RollingWindow, opts ...Option) (*Series, error) {
	return s.rollingFloat(o, func(win []float64, _ int) (float64, bool) {
		n, m2, m3, _ := centralMoments(win)
		if m2 == 0 {
			if n < 3 && !bias {
				return 0, false
			}
			if m3 == 0 {
				return 0, true
			}
		}
		g := m3 / math.Pow(m2, 1.5)
		if !bias {
			if n < 3 {
				return 0, false
			}
			g *= math.Sqrt(n*(n-1)) / (n - 2)
		}
		return g, true
	}, opts)
}

// RollingKurtosis is the rolling kurtosis. fisher subtracts 3 (excess
// kurtosis); bias=false applies the small-sample correction.
func (s *Series) RollingKurtosis(fisher, bias bool, o RollingWindow, opts ...Option) (*Series, error) {
	return s.rollingFloat(o, func(win []float64, _ int) (float64, bool) {
		n, m2, _, m4 := centralMoments(win)
		k := m4 / (m2 * m2)
		if !bias {
			if n < 4 {
				return 0, false
			}
			k = 1/(n-2)/(n-3)*((n*n-1)*k-3*(n-1)*(n-1)) + 3
		}
		if fisher {
			k -= 3
		}
		return k, true
	}, opts)
}

// RollingRank ranks the last value of each window among the window's
// non-null values. method is "average" (f64 output) or "min", "max",
// "dense" (u32 output). A null last value gives null.
func (s *Series) RollingRank(method string, o RollingWindow, opts ...Option) (*Series, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	c, err := orderCmp(arr)
	if err != nil {
		return nil, err
	}
	n := arr.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	ms := o.minSamples()
	distinct := make([]int, 0, o.WindowSize)
	for i := range n {
		if arr.IsNull(i) {
			continue
		}
		a, b := o.bounds(i, n)
		var less, equal, count int
		distinct = distinct[:0]
		for j := a; j < b; j++ {
			if arr.IsNull(j) {
				continue
			}
			count++
			r := c(j, i)
			switch {
			case r < 0:
				less++
				if method == "dense" {
					seen := false
					for _, d := range distinct {
						if c(d, j) == 0 {
							seen = true
							break
						}
					}
					if !seen {
						distinct = append(distinct, j)
					}
				}
			case r == 0:
				equal++
			}
		}
		if count < ms {
			continue
		}
		valid[i] = true
		switch method {
		case "min":
			out[i] = float64(less + 1)
		case "max":
			out[i] = float64(less + equal)
		case "dense":
			out[i] = float64(len(distinct) + 1)
		default:
			out[i] = float64(less) + float64(equal+1)/2
		}
	}
	vo := validOrNil(valid)
	if method == "min" || method == "max" || method == "dense" {
		ints := make([]int, n)
		for i, v := range out {
			ints[i] = int(v)
		}
		return u32Series(s.name, ints, vo, cfg.alloc)
	}
	return floatSeries(s.name, out, vo, false, cfg.alloc)
}

// RollingMap applies fn to every window (a zero-copy slice of s,
// nulls included) whose non-null count reaches MinSamples. fn returns a
// length-1 Series; the results are concatenated, with null for short
// windows. Mirrors polars' rolling_map.
func (s *Series) RollingMap(fn func(window *Series) (*Series, error), o RollingWindow, opts ...Option) (*Series, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	n := s.Len()
	ms := o.minSamples()
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	results := make([]*Series, 0, n)
	defer func() { releaseSeries(results) }()
	idx := make([]int, n)
	var dt arrow.DataType
	for i := range n {
		a, b := o.bounds(i, n)
		cnt := 0
		for j := a; j < b; j++ {
			if arr.IsValid(j) {
				cnt++
			}
		}
		if cnt < ms || cnt == 0 {
			idx[i] = -1
			continue
		}
		win, err := s.Slice(a, b-a)
		if err != nil {
			return nil, err
		}
		r, err := fn(win)
		win.Release()
		if err != nil {
			return nil, err
		}
		if r.Len() != 1 {
			n := r.Len()
			r.Release()
			return nil, fmt.Errorf("series: rolling_map function must return one value, got %d", n)
		}
		if dt == nil {
			dt = r.data.DataType()
		} else if !arrow.TypeEqual(dt, r.data.DataType()) {
			r.Release()
			return nil, fmt.Errorf("series: rolling_map returned mixed dtypes %s and %s", dt, r.DType())
		}
		idx[i] = len(results)
		results = append(results, r)
	}
	if len(results) == 0 {
		return FullNull(s.name, s.data.DataType(), n, opts...)
	}
	pool, err := ConcatSeries(s.name, results, opts...)
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	return pool.Gather(idx, opts...)
}

// RollingCov is the rolling covariance of a and b over rows where both
// are non-null. Windows with fewer than MinSamples pairs, or at most
// ddof pairs, are null.
func RollingCov(a, b *Series, o RollingWindow, ddof int, opts ...Option) (*Series, error) {
	return rollingPair(a, b, o, ddof, false, opts)
}

// RollingCorr is the rolling Pearson correlation of a and b.
func RollingCorr(a, b *Series, o RollingWindow, ddof int, opts ...Option) (*Series, error) {
	return rollingPair(a, b, o, ddof, true, opts)
}

func rollingPair(sa, sb *Series, o RollingWindow, ddof int, corr bool, opts []Option) (*Series, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	a, b, err := twoArrays(sa, sb, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	x, err := floatVals(a)
	if err != nil {
		return nil, err
	}
	y, err := floatVals(b)
	if err != nil {
		return nil, err
	}
	n := len(x)
	out := make([]float64, n)
	valid := make([]bool, n)
	ms := o.minSamples()
	for i := range n {
		lo, hi := o.bounds(i, n)
		var cnt, mx, my float64
		for j := lo; j < hi; j++ {
			if a.IsValid(j) && b.IsValid(j) {
				cnt++
				mx += x[j]
				my += y[j]
			}
		}
		if int(cnt) < ms || cnt <= float64(ddof) || cnt == 0 {
			continue
		}
		mx /= cnt
		my /= cnt
		var sxy, sxx, syy float64
		for j := lo; j < hi; j++ {
			if a.IsValid(j) && b.IsValid(j) {
				dx, dy := x[j]-mx, y[j]-my
				sxy += dx * dy
				sxx += dx * dx
				syy += dy * dy
			}
		}
		if corr {
			if cnt < 2 {
				continue
			}
			out[i] = sxy / math.Sqrt(sxx*syy)
		} else {
			out[i] = sxy / (cnt - float64(ddof))
		}
		valid[i] = true
	}
	return floatSeries(sa.name, out, validOrNil(valid), false, cfg.alloc)
}

// Cov is the covariance of a and b over rows where both are non-null.
func Cov(a, b *Series, ddof int, opts ...Option) (*Series, error) {
	return pairStat(a, b, ddof, false, opts)
}

// PearsonCorrSeries is the Pearson correlation of a and b over rows
// where both are non-null, as a length-1 f64 Series.
func PearsonCorrSeries(a, b *Series, opts ...Option) (*Series, error) {
	return pairStat(a, b, 1, true, opts)
}

func pairStat(sa, sb *Series, ddof int, corr bool, opts []Option) (*Series, error) {
	n := sa.Len()
	o := RollingWindow{WindowSize: max(n, 1), MinSamples: 1}
	r, err := rollingPair(sa, sb, o, ddof, corr, opts)
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if n == 0 {
		return scalarFloat(sa.name, 0, false, false, resolve(opts).alloc)
	}
	return r.Slice(n-1, 1)
}

// SpearmanCorrSeries is the Spearman rank correlation of a and b: the
// Pearson correlation of their average ranks over rows where both are
// non-null.
func SpearmanCorrSeries(a, b *Series, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	x, y, err := twoArrays(a, b, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer x.Release()
	defer y.Release()
	var keep []int
	for i := range x.Len() {
		if x.IsValid(i) && y.IsValid(i) {
			keep = append(keep, i)
		}
	}
	xs, err := a.Gather(keep)
	if err != nil {
		return nil, err
	}
	defer xs.Release()
	ys, err := b.Gather(keep)
	if err != nil {
		return nil, err
	}
	defer ys.Release()
	rx, err := averageRanks(xs)
	if err != nil {
		return nil, err
	}
	ry, err := averageRanks(ys)
	if err != nil {
		return nil, err
	}
	sx, err := floatSeries(a.name, rx, nil, false, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer sx.Release()
	sy, err := floatSeries(b.name, ry, nil, false, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer sy.Release()
	return PearsonCorrSeries(sx, sy, opts...)
}

// averageRanks returns 1-based average ranks of a null-free Series.
func averageRanks(s *Series) ([]float64, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	c, err := orderCmp(arr)
	if err != nil {
		return nil, err
	}
	n := arr.Len()
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool { return c(idx[i], idx[j]) < 0 })
	ranks := make([]float64, n)
	for i := 0; i < n; {
		j := i + 1
		for j < n && c(idx[j], idx[i]) == 0 {
			j++
		}
		r := float64(i+j+1) / 2
		for k := i; k < j; k++ {
			ranks[idx[k]] = r
		}
		i = j
	}
	return ranks, nil
}
