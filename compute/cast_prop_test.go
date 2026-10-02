package compute_test

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestCastRoundTripProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(11, 12))
	type path struct {
		from arrow.DataType
		via  []dtype.DType
	}
	paths := []path{
		{arrow.PrimitiveTypes.Int8, []dtype.DType{dtype.Int16(), dtype.Int32(), dtype.Int64(), dtype.Float64(), dtype.String()}},
		{arrow.PrimitiveTypes.Int16, []dtype.DType{dtype.Int32(), dtype.Float32(), dtype.String()}},
		{arrow.PrimitiveTypes.Int32, []dtype.DType{dtype.Int64(), dtype.Float64(), dtype.String()}},
		{arrow.PrimitiveTypes.Int64, []dtype.DType{dtype.String()}},
		{arrow.PrimitiveTypes.Uint8, []dtype.DType{dtype.Uint16(), dtype.Int16(), dtype.Uint32(), dtype.Int64(), dtype.Float32(), dtype.String()}},
		{arrow.PrimitiveTypes.Uint32, []dtype.DType{dtype.Uint64(), dtype.Int64(), dtype.Float64(), dtype.String()}},
		{arrow.PrimitiveTypes.Uint64, []dtype.DType{dtype.String()}},
		{arrow.PrimitiveTypes.Float32, []dtype.DType{dtype.Float64()}},
		{arrow.FixedWidthTypes.Boolean, []dtype.DType{dtype.Int8(), dtype.Uint8(), dtype.Float64()}},
		{arrow.FixedWidthTypes.Date32, []dtype.DType{dtype.Datetime(dtype.Microsecond, ""), dtype.Datetime(dtype.Millisecond, ""), dtype.Datetime(dtype.Nanosecond, ""), dtype.Int32(), dtype.Int64()}},
		{arrow.FixedWidthTypes.Timestamp_us, []dtype.DType{dtype.Datetime(dtype.Nanosecond, ""), dtype.Int64()}},
		{arrow.FixedWidthTypes.Duration_ms, []dtype.DType{dtype.Duration(dtype.Microsecond), dtype.Duration(dtype.Nanosecond), dtype.Int64()}},
	}
	for _, p := range paths {
		for _, via := range p.via {
			for _, n := range testutil.PropSizes {
				vals := testutil.RandValues(r, p.from, n, 0.2)
				if via.IsFloating() {
					// Keep ints exactly representable in the float type.
					for i, v := range vals {
						switch x := v.(type) {
						case int64:
							vals[i] = x % (1 << 20)
						case uint64:
							vals[i] = x % (1 << 20)
						}
					}
				}
				if p.from.ID() == arrow.TIMESTAMP || p.from.ID() == arrow.DURATION {
					// Keep finer unit casts in range; overflow is tested separately.
					for i, v := range vals {
						if x, ok := v.(int64); ok {
							vals[i] = x % (1 << 40)
						}
					}
				}
				s := propSeries(t, mem, r, "x", p.from, vals, r.IntN(numLayouts))
				mid, err := compute.Cast(ctx, s, via, compute.WithAllocator(mem))
				if err != nil {
					t.Fatalf("cast %s -> %s: %v", p.from, via, err)
				}
				back, err := compute.Cast(ctx, mid, dtype.FromArrow(p.from), compute.WithAllocator(mem))
				if err != nil {
					t.Fatalf("cast %s -> %s -> back: %v", p.from, via, err)
				}
				want := s.ToList()
				got := back.ToList()
				for i := range want {
					if !testutil.ValuesEqual(got[i], want[i]) {
						t.Fatalf("round trip %s -> %s -> %s row %d: got %v want %v (mid %v)",
							p.from, via, p.from, i, got[i], want[i], mid.ToList()[i])
					}
				}
				back.Release()
				mid.Release()
				s.Release()
			}
		}
	}
}

// polarsCasts reports whether polars 1.39 accepts the cast (with
// strict=False). It rejects string to bool, date or datetime to
// duration or bool, and duration to anything but numbers and durations.
func polarsCasts(from, to dtype.DType) bool {
	switch {
	case from.IsString() && to.IsBool():
		return false
	case (from.IsDate() || from.IsDatetime()) && (to.IsDuration() || to.IsBool()):
		return false
	case from.IsDuration() && !(to.IsNumeric() || to.IsDuration()):
		return false
	}
	return true
}

// TestCastMatrixProp casts between every pair of the property test
// dtypes (numeric, bool, string, date, datetime, duration) that polars
// supports. Each cast must succeed, keep length and dtype, and may only
// add nulls (values that do not fit become null).
func TestCastMatrixProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(13, 14))
	for _, from := range sortPropTypes {
		for _, toArrow := range sortPropTypes {
			to := dtype.FromArrow(toArrow)
			if !polarsCasts(dtype.FromArrow(from), to) {
				continue
			}
			vals := testutil.RandValues(r, from, 40, 0.2)
			s := propSeries(t, mem, r, "x", from, vals, r.IntN(numLayouts))
			out, err := compute.Cast(ctx, s, to, compute.WithAllocator(mem))
			if err != nil {
				t.Errorf("cast %s -> %s: %v", from, to, err)
				s.Release()
				continue
			}
			if out.Len() != s.Len() || out.NullCount() < s.NullCount() || !out.DType().Equal(to) {
				t.Errorf("cast %s -> %s: len %d nulls %d dtype %s (input len %d nulls %d)",
					from, to, out.Len(), out.NullCount(), out.DType(), s.Len(), s.NullCount())
			}
			out.Release()
			s.Release()
		}
	}
}

// TestCastUnitOverflowIsNull is a regression test: casting to a finer
// time unit used to wrap int64 on overflow. polars 1.39 reports
// "conversion from `duration[ms]` to `duration[ns]` failed" in strict
// mode and gives null with strict=False, which is the golars default.
func TestCastUnitOverflowIsNull(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	cases := []struct {
		from arrow.DataType
		to   dtype.DType
		vals []any
	}{
		{arrow.FixedWidthTypes.Duration_ms, dtype.Duration(dtype.Nanosecond), []any{int64(1) << 51, int64(5), nil}},
		{arrow.FixedWidthTypes.Timestamp_ms, dtype.Datetime(dtype.Nanosecond, ""), []any{int64(1) << 51, int64(5), nil}},
		{arrow.FixedWidthTypes.Date32, dtype.Datetime(dtype.Nanosecond, ""), []any{int64(200000), int64(5), nil}},
	}
	for _, c := range cases {
		s, err := series.FromValues("x", c.from, c.vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		out, err := compute.Cast(ctx, s, c.to, compute.WithAllocator(mem))
		if err != nil {
			t.Fatalf("%s -> %s: %v", c.from, c.to, err)
		}
		got, err := out.Get(0)
		if err != nil || got != nil {
			t.Errorf("%s -> %s: overflowing value = %v, want null", c.from, c.to, got)
		}
		if out.NullCount() != 2 || !out.DType().Equal(c.to) {
			t.Errorf("%s -> %s: nulls=%d dtype=%s", c.from, c.to, out.NullCount(), out.DType())
		}
		out.Release()
		s.Release()
	}
}

// TestCastGenericPairs pins values for cast pairs that used to fail
// with "unsupported dtype", checked against polars 1.39 (strict=False).
func TestCastGenericPairs(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	cases := []struct {
		from arrow.DataType
		vals []any
		to   dtype.DType
		want []any
	}{
		// pl.Series([3.7, -1.5, 5e9, None]).cast(pl.UInt32, strict=False)
		{arrow.PrimitiveTypes.Float64, []any{3.7, -1.5, 5e9, nil}, dtype.Uint32(), []any{uint64(3), nil, nil, nil}},
		// pl.Series([1, 4000000000], dtype=pl.UInt32).cast(pl.Int32, strict=False)
		{arrow.PrimitiveTypes.Uint32, []any{uint64(1), uint64(4000000000)}, dtype.Int32(), []any{int64(1), nil}},
		// pl.Series([0, 7], dtype=pl.UInt64).cast(pl.Boolean)
		{arrow.PrimitiveTypes.Uint64, []any{uint64(0), uint64(7)}, dtype.Bool(), []any{false, true}},
		// pl.Series([True, False]).cast(pl.Float32)
		{arrow.FixedWidthTypes.Boolean, []any{true, false}, dtype.Float32(), []any{1.0, 0.0}},
		// pl.Series([0.1, 2.5], dtype=pl.Float32).cast(pl.String)
		{arrow.PrimitiveTypes.Float32, []any{float32(0.1), 2.5}, dtype.String(), []any{"0.1", "2.5"}},
		// pl.Series([12, None], dtype=pl.UInt32).cast(pl.String)
		{arrow.PrimitiveTypes.Uint32, []any{uint64(12), nil}, dtype.String(), []any{"12", nil}},
		// pl.Series(["12", "300", "x"]).cast(pl.Int8, strict=False)
		{arrow.BinaryTypes.String, []any{"12", "300", "x"}, dtype.Int8(), []any{int64(12), nil, nil}},
		// pl.Series([nan, 2.9, -1.5]).cast(pl.Date, strict=False)
		{arrow.PrimitiveTypes.Float64, []any{math.NaN(), 2.9, -1.5}, dtype.Date(), []any{nil, time.Unix(2*86400, 0).UTC(), time.Unix(-86400, 0).UTC()}},
		// pl.Series([1.5], dtype=pl.Float32).cast(pl.Datetime("us"))
		{arrow.PrimitiveTypes.Float32, []any{1.5}, dtype.Datetime(dtype.Microsecond, ""), []any{time.UnixMicro(1).UTC()}},
		// pl.Series([True]).cast(pl.Duration("ms"))
		{arrow.FixedWidthTypes.Boolean, []any{true}, dtype.Duration(dtype.Millisecond), []any{time.Millisecond}},
		// pl.Series(["7", "x"]).cast(pl.Duration("ms"), strict=False)
		{arrow.BinaryTypes.String, []any{"7", "x"}, dtype.Duration(dtype.Millisecond), []any{7 * time.Millisecond, nil}},
	}
	for _, c := range cases {
		s, err := series.FromValues("x", c.from, c.vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		out, err := compute.Cast(ctx, s, c.to, compute.WithAllocator(mem))
		if err != nil {
			t.Fatalf("%s -> %s: %v", c.from, c.to, err)
		}
		got := out.ToList()
		for i := range c.want {
			if !testutil.ValuesEqual(got[i], c.want[i]) {
				t.Errorf("%s -> %s row %d: got %v want %v", c.from, c.to, i, got[i], c.want[i])
			}
		}
		out.Release()
		s.Release()
	}
}
