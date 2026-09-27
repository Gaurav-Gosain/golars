package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Skew returns the skewness of the non-null values. Matches polars'
// default (bias=True): the simple moment ratio g1 = m3 / m2^(3/2),
// where m_k = (1/n)*sum((x - mean)^k). Call SkewUnbiased for the
// Fisher-Pearson adjusted form.
func (s *Series) Skew() (float64, error) { return s.skew(false) }

// SkewUnbiased returns the Fisher-Pearson (bias=False) skewness.
func (s *Series) SkewUnbiased() (float64, error) { return s.skew(true) }

func (s *Series) skew(unbiased bool) (float64, error) {
	v, ok, err := s.SkewNullable(!unbiased)
	if err != nil || !ok {
		return math.NaN(), err
	}
	return v, nil
}

// momentState holds polars' central moment sums of the non-null values.
type momentState struct{ n, mean, m2, m3, m4 float64 }

func momentsOf(vals []float64) momentState {
	n := float64(len(vals))
	if n == 0 {
		return momentState{}
	}
	st := momentState{n: n, mean: sumFloats(vals) / n}
	for _, v := range vals {
		d := v - st.mean
		d2 := d * d
		st.m2 += d2
		st.m3 += d * d2
		st.m4 += d2 * d2
	}
	return st
}

// isZeroVar is polars' test for a numerically zero variance.
func (st momentState) isZeroVar(m2 float64) bool {
	lim := 2.220446049250313e-16 * st.mean
	return m2 <= lim*lim
}

// SkewNullable returns polars' skew: null for no values (or at most
// two with bias=false), NaN for a zero variance.
func (s *Series) SkewNullable(bias bool) (float64, bool, error) {
	vals, err := floatValuesForStats(s)
	if err != nil {
		return 0, false, err
	}
	st := momentsOf(vals)
	m2, m3 := st.m2/st.n, st.m3/st.n
	g1 := math.NaN()
	if !st.isZeroVar(m2) {
		g1 = m3 / math.Pow(m2, 1.5)
	}
	if bias {
		return g1, st.n > 0, nil
	}
	if st.n <= 2 {
		return 0, false, nil
	}
	return math.Sqrt(st.n*(st.n-1)) / (st.n - 2) * g1, true, nil
}

// KurtosisNullable returns polars' kurtosis: null for no values (or at
// most three with bias=false), NaN for a zero variance. fisher
// subtracts 3.
func (s *Series) KurtosisNullable(fisher, bias bool) (float64, bool, error) {
	vals, err := floatValuesForStats(s)
	if err != nil {
		return 0, false, err
	}
	st := momentsOf(vals)
	m4, m2 := st.m4/st.n, st.m2/st.n
	est := math.NaN()
	if !st.isZeroVar(m2) {
		est = m4 / (m2 * m2)
	}
	var out float64
	if bias {
		if st.n == 0 {
			return 0, false, nil
		}
		out = est
	} else {
		n := st.n
		if n <= 3 {
			return 0, false, nil
		}
		out = (n-1)/(n-2)*((n+1)/(n-3)*est-3*(n-1)/(n-3)) + 3
	}
	if fisher {
		out -= 3
	}
	return out, true, nil
}

func (s *Series) kurtosis(unbiased bool) (float64, error) {
	v, ok, err := s.KurtosisNullable(true, !unbiased)
	if err != nil || !ok {
		return math.NaN(), err
	}
	return v, nil
}

// Entropy returns the Shannon entropy of s' value distribution: the
// sum of -p_i * log(p_i) across distinct non-null values, where p_i
// is the empirical probability. `base` selects the log base
// (e for natural entropy, 2 for bits).
func (s *Series) Entropy(base float64) (float64, error) {
	if base <= 0 || base == 1 {
		return 0, fmt.Errorf("series: Entropy base must be > 0 and != 1")
	}
	counts := map[float64]int64{}
	total := 0.0
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			counts[v]++
			total++
		}
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			counts[float64(v)]++
			total++
		}
	case *array.Int32:
		for i, v := range a.Int32Values() {
			if a.NullN() > 0 && a.IsNull(i) {
				continue
			}
			counts[float64(v)]++
			total++
		}
	default:
		return 0, fmt.Errorf("series: Entropy unsupported for dtype %s", s.DType())
	}
	if total == 0 {
		return math.NaN(), nil
	}
	ln := math.Log
	baseLn := math.Log(base)
	var h float64
	for _, c := range counts {
		p := float64(c) / total
		h -= p * ln(p) / baseLn
	}
	return h, nil
}

// PearsonCorr returns the Pearson correlation coefficient between s
// and other. Length mismatch or an all-null pair returns NaN.
func (s *Series) PearsonCorr(other *Series) (float64, error) {
	if s.Len() != other.Len() {
		return 0, fmt.Errorf("series: PearsonCorr length mismatch")
	}
	return alignedPearson(s, other)
}

// Covariance returns the sample covariance (ddof=1) between s and
// other. Requires numeric inputs and equal length. Mirrors polars'
// pl.cov(..., ddof=1).
func (s *Series) Covariance(other *Series, ddof int) (float64, error) {
	if s.Len() != other.Len() {
		return 0, fmt.Errorf("series: Covariance length mismatch")
	}
	xs, ys, ok, err := alignedFloats(s, other)
	if err != nil {
		return 0, err
	}
	if !ok || len(xs) < ddof+1 {
		return math.NaN(), nil
	}
	nf := float64(len(xs))
	mx := sumFloats(xs) / nf
	my := sumFloats(ys) / nf
	var cov float64
	for i := range xs {
		cov += (xs[i] - mx) * (ys[i] - my)
	}
	denom := nf - float64(ddof)
	if denom == 0 {
		return math.NaN(), nil
	}
	return cov / denom, nil
}

// alignedPearson computes Pearson correlation over row-wise non-null
// pairs of s and other. Used by PearsonCorr.
func alignedPearson(s, other *Series) (float64, error) {
	xs, ys, ok, err := alignedFloats(s, other)
	if err != nil {
		return 0, err
	}
	if !ok || len(xs) < 2 {
		return math.NaN(), nil
	}
	nf := float64(len(xs))
	mx := sumFloats(xs) / nf
	my := sumFloats(ys) / nf
	var num, dx, dy float64
	for i := range xs {
		a := xs[i] - mx
		b := ys[i] - my
		num += a * b
		dx += a * a
		dy += b * b
	}
	if dx == 0 || dy == 0 {
		return math.NaN(), nil
	}
	return num / math.Sqrt(dx*dy), nil
}

// alignedFloats converts s and other into []float64 slices, dropping
// any row where either is null. The second return indicates whether
// any usable pairs remained.
func alignedFloats(s, other *Series) ([]float64, []float64, bool, error) {
	a := s.Chunk(0)
	b := other.Chunk(0)
	n := a.Len()
	if b.Len() != n {
		return nil, nil, false, fmt.Errorf("series: alignedFloats length mismatch")
	}
	xs := make([]float64, 0, n)
	ys := make([]float64, 0, n)
	for i := range n {
		if !a.IsValid(i) || !b.IsValid(i) {
			continue
		}
		xv, ok := floatFromChunk(a, i)
		if !ok {
			return nil, nil, false, fmt.Errorf("series: alignedFloats unsupported dtype %s", s.DType())
		}
		yv, ok := floatFromChunk(b, i)
		if !ok {
			return nil, nil, false, fmt.Errorf("series: alignedFloats unsupported dtype %s", other.DType())
		}
		xs = append(xs, xv)
		ys = append(ys, yv)
	}
	return xs, ys, len(xs) > 0, nil
}

func floatFromChunk(chunk any, i int) (float64, bool) {
	switch a := chunk.(type) {
	case *array.Float64:
		return a.Value(i), true
	case *array.Float32:
		return float64(a.Value(i)), true
	case *array.Int64:
		return float64(a.Value(i)), true
	case *array.Int32:
		return float64(a.Value(i)), true
	}
	return 0, false
}

// floatValuesForStats returns a []float64 of non-null values from s.
// Used by reductions that don't need cross-series alignment.
func floatValuesForStats(s *Series) ([]float64, error) {
	out := make([]float64, 0, s.Len())
	for _, chunk := range s.Chunks() {
		var err error
		out, err = appendStatValues(out, chunk)
		if err != nil {
			return nil, fmt.Errorf("series: floatValuesForStats unsupported for dtype %s", s.DType())
		}
	}
	return out, nil
}

func appendStatNums[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64](out []float64, a arrow.Array, vals []T) []float64 {
	for i, v := range vals {
		if a.NullN() == 0 || a.IsValid(i) {
			out = append(out, float64(v))
		}
	}
	return out
}

func appendStatValues(out []float64, chunk arrow.Array) ([]float64, error) {
	switch a := chunk.(type) {
	case *array.Float64:
		return appendStatNums(out, a, a.Float64Values()), nil
	case *array.Float32:
		return appendStatNums(out, a, a.Float32Values()), nil
	case *array.Int64:
		return appendStatNums(out, a, a.Int64Values()), nil
	case *array.Int32:
		return appendStatNums(out, a, a.Int32Values()), nil
	case *array.Int16:
		return appendStatNums(out, a, a.Int16Values()), nil
	case *array.Int8:
		return appendStatNums(out, a, a.Int8Values()), nil
	case *array.Uint64:
		return appendStatNums(out, a, a.Uint64Values()), nil
	case *array.Uint32:
		return appendStatNums(out, a, a.Uint32Values()), nil
	case *array.Uint16:
		return appendStatNums(out, a, a.Uint16Values()), nil
	case *array.Uint8:
		return appendStatNums(out, a, a.Uint8Values()), nil
	case *array.Boolean:
		for i := range a.Len() {
			if a.IsValid(i) {
				v := 0.0
				if a.Value(i) {
					v = 1
				}
				out = append(out, v)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported")
}

func sumFloats(xs []float64) float64 {
	var s float64
	for _, v := range xs {
		s += v
	}
	return s
}

// Kurtosis returns the excess kurtosis of the non-null values.
// Matches polars' default (bias=True, fisher=True): m4/m2^2 - 3,
// where m_k = (1/n)*sum((x-mean)^k). It is NaN where polars returns
// null; see KurtosisNullable.
func (s *Series) Kurtosis() (float64, error) { return s.kurtosis(false) }

// KurtosisUnbiased returns the bias-corrected (scipy bias=False)
// excess kurtosis.
func (s *Series) KurtosisUnbiased() (float64, error) { return s.kurtosis(true) }
