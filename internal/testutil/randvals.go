package testutil

import (
	"cmp"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Property tests use the helpers below to build random columns as plain
// Go values ([]any, nil for null) in the same representation that
// series.ValueAt returns: signed ints as int64, unsigned as uint64,
// floats as float64, strings as string, bools as bool. The caller turns
// them into a Series with series.FromValues. Keeping this package free
// of a series import avoids an import cycle with internal series tests.

// PropSizes are the lengths property tests sweep: the small-vector
// boundaries around 16 and 64 plus the empty and single-row cases.
var PropSizes = []int{0, 1, 2, 3, 15, 16, 17, 63, 64, 65, 100}

// RandValues returns n random values of arrow type dt with roughly
// nullFrac of them null. Values are drawn so that ties, extremes and
// special floats (NaN, -0.0, +-Inf) show up often.
func RandValues(r *rand.Rand, dt arrow.DataType, n int, nullFrac float64) []any {
	out := make([]any, n)
	// A narrow domain makes duplicates (and so stability and grouping
	// issues) likely; a wide one exercises radix digits.
	narrow := r.IntN(2) == 0
	for i := range out {
		if nullFrac > 0 && r.Float64() < nullFrac {
			continue
		}
		out[i] = RandValue(r, dt, narrow)
	}
	return out
}

// RandValue returns one non-null random value of type dt.
func RandValue(r *rand.Rand, dt arrow.DataType, narrow bool) any {
	switch dt.ID() {
	case arrow.BOOL:
		return r.IntN(2) == 0
	case arrow.INT8:
		return randSigned(r, 8, narrow)
	case arrow.INT16:
		return randSigned(r, 16, narrow)
	case arrow.INT32:
		return randSigned(r, 32, narrow)
	case arrow.INT64:
		return randSigned(r, 64, narrow)
	case arrow.UINT8:
		return randUnsigned(r, 8, narrow)
	case arrow.UINT16:
		return randUnsigned(r, 16, narrow)
	case arrow.UINT32:
		return randUnsigned(r, 32, narrow)
	case arrow.UINT64:
		return randUnsigned(r, 64, narrow)
	case arrow.FLOAT32:
		return float64(float32(randFloat(r, narrow, true)))
	case arrow.FLOAT64:
		return randFloat(r, narrow, false)
	case arrow.STRING, arrow.LARGE_STRING:
		return randString(r, narrow)
	case arrow.DATE32:
		return int64(r.IntN(40000) - 20000)
	case arrow.TIMESTAMP, arrow.DURATION:
		if narrow {
			return int64(r.IntN(20) - 10)
		}
		// Kept within +-2^42 so ms values still fit time.Duration in ns.
		return r.Int64N(1<<43) - 1<<42
	}
	panic("testutil.RandValue: unsupported type " + dt.String())
}

func randSigned(r *rand.Rand, bits uint, narrow bool) int64 {
	lo := int64(-1) << (bits - 1)
	hi := -(lo + 1)
	if narrow {
		return int64(r.IntN(9) - 4)
	}
	switch r.IntN(8) {
	case 0:
		return lo
	case 1:
		return hi
	case 2:
		return 0
	case 3:
		return -1
	}
	if bits == 64 {
		return int64(r.Uint64())
	}
	return lo + r.Int64N(hi-lo+1)
}

func randUnsigned(r *rand.Rand, bits uint, narrow bool) uint64 {
	if narrow {
		return uint64(r.IntN(6))
	}
	hi := uint64(math.MaxUint64)
	if bits < 64 {
		hi = uint64(1)<<bits - 1
	}
	switch r.IntN(6) {
	case 0:
		return 0
	case 1:
		return hi
	}
	if bits == 64 {
		return r.Uint64()
	}
	return r.Uint64N(hi + 1)
}

var specialFloats = []float64{
	math.NaN(), math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1),
	math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, 1, -1, 0.5,
}

var specialFloats32 = []float64{
	math.NaN(), math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1),
	math.MaxFloat32, -math.MaxFloat32, math.SmallestNonzeroFloat32, 1, -1, 0.5,
}

func randFloat(r *rand.Rand, narrow, f32 bool) float64 {
	sp := specialFloats
	if f32 {
		sp = specialFloats32
	}
	if narrow || r.IntN(4) == 0 {
		if r.IntN(2) == 0 {
			return sp[r.IntN(len(sp))]
		}
		return float64(r.IntN(7) - 3)
	}
	return (r.Float64() - 0.5) * math.Pow(10, float64(r.IntN(20)-10))
}

var stringAtoms = []string{"", "a", "b", "ab", "abc", "A", "Z", "é", "ß", "日本", "\U0001F600", " ", "\t", "zzzzzzzz", "prefix_", "prefix_long_shared_"}

func randString(r *rand.Rand, narrow bool) string {
	if narrow {
		return stringAtoms[r.IntN(len(stringAtoms))]
	}
	var b strings.Builder
	for range r.IntN(4) {
		b.WriteString(stringAtoms[r.IntN(len(stringAtoms))])
	}
	if r.IntN(3) == 0 {
		b.WriteByte(byte('a' + r.IntN(26)))
	}
	return b.String()
}

// CompareValues orders two non-null values of the same kind with
// polars' total order: NaN equals NaN and sorts above every number,
// -0.0 equals 0.0, false sorts before true, strings compare bytewise.
func CompareValues(a, b any) int {
	switch x := a.(type) {
	case int64:
		return cmp.Compare(x, b.(int64))
	case uint64:
		return cmp.Compare(x, b.(uint64))
	case float64:
		y := b.(float64)
		xn, yn := math.IsNaN(x), math.IsNaN(y)
		switch {
		case xn && yn:
			return 0
		case xn:
			return 1
		case yn:
			return -1
		}
		return cmp.Compare(x, y)
	case string:
		return strings.Compare(x, b.(string))
	case time.Time:
		return x.Compare(b.(time.Time))
	case time.Duration:
		return cmp.Compare(x, b.(time.Duration))
	case bool:
		y := b.(bool)
		switch {
		case x == y:
			return 0
		case !x:
			return -1
		}
		return 1
	}
	panic("testutil.CompareValues: unsupported value")
}

// ValuesIdentical is ValuesEqual except that floats must also agree in
// sign, so -0.0 differs from 0.0. Use it for lossless round trips.
func ValuesIdentical(a, b any) bool {
	if fa, ok := a.(float64); ok {
		if fb, ok := b.(float64); ok && !math.IsNaN(fa) {
			return math.Float64bits(fa) == math.Float64bits(fb)
		}
	}
	return ValuesEqual(a, b)
}

// ValuesEqual reports whether two values (either may be nil) are equal
// under CompareValues. nil equals only nil.
func ValuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return CompareValues(a, b) == 0
}
