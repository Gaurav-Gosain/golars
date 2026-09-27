package dataframe

import (
	"fmt"
	"math"
	"slices"
)

// QuantileMethod selects how a quantile between two data points is
// resolved. The names and semantics match polars' QuantileMethod.
type QuantileMethod string

// Quantile interpolation methods. QuantileNearest is polars' default for
// DataFrame.quantile and GroupBy.quantile.
const (
	QuantileNearest      QuantileMethod = "nearest"
	QuantileHigher       QuantileMethod = "higher"
	QuantileLower        QuantileMethod = "lower"
	QuantileMidpoint     QuantileMethod = "midpoint"
	QuantileLinear       QuantileMethod = "linear"
	QuantileEquiprobable QuantileMethod = "equiprobable"
)

func (m QuantileMethod) validate() error {
	switch m {
	case QuantileNearest, QuantileHigher, QuantileLower, QuantileMidpoint,
		QuantileLinear, QuantileEquiprobable:
		return nil
	}
	return fmt.Errorf("dataframe: unknown quantile method %q", string(m))
}

func (m QuantileMethod) orDefault() QuantileMethod {
	if m == "" {
		return QuantileNearest
	}
	return m
}

// sortFloatsNaNLast sorts vals ascending with NaN ordered after every
// other value, which is how polars orders floats for quantiles.
func sortFloatsNaNLast(vals []float64) {
	slices.SortFunc(vals, func(a, b float64) int {
		an, bn := math.IsNaN(a), math.IsNaN(b)
		switch {
		case an && bn:
			return 0
		case an:
			return 1
		case bn:
			return -1
		case a < b:
			return -1
		case a > b:
			return 1
		}
		return 0
	})
}

// quantileSorted returns the q-quantile of sorted (ascending, NaN last)
// under method. ok is false for empty input.
func quantileSorted(sorted []float64, q float64, method QuantileMethod) (float64, bool) {
	n := len(sorted)
	if n == 0 {
		return 0, false
	}
	if n == 1 {
		return sorted[0], true
	}
	pos := q * float64(n-1)
	switch method.orDefault() {
	case QuantileNearest:
		return sorted[clampIdx(int(math.Round(pos)), n)], true
	case QuantileLower:
		return sorted[clampIdx(int(math.Floor(pos)), n)], true
	case QuantileHigher:
		return sorted[clampIdx(int(math.Ceil(pos)), n)], true
	case QuantileEquiprobable:
		return sorted[clampIdx(int(math.Ceil(q*float64(n)))-1, n)], true
	case QuantileMidpoint:
		lo := clampIdx(int(math.Floor(pos)), n)
		hi := clampIdx(int(math.Ceil(pos)), n)
		if lo == hi {
			return sorted[lo], true
		}
		return (sorted[lo] + sorted[hi]) / 2, true
	}
	lo := clampIdx(int(math.Floor(pos)), n)
	hi := clampIdx(int(math.Ceil(pos)), n)
	if lo == hi {
		return sorted[lo], true
	}
	frac := pos - float64(lo)
	return sorted[lo] + (sorted[hi]-sorted[lo])*frac, true
}

func errQuantileRange(q float64) error {
	return fmt.Errorf("dataframe: quantile %v must be within [0, 1]", q)
}

func clampIdx(i, n int) int {
	if i < 0 {
		return 0
	}
	if i >= n {
		return n - 1
	}
	return i
}
