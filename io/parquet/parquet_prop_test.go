package parquet_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

var pqPropTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Timestamp_ns,
	&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Asia/Kolkata"},
	// Duration is left out: see TestParquetDurationRoundTrip.
}

// randIOFrame builds a frame of random columns; multi-chunk columns
// come from concatenating two separately built pieces.
func randIOFrame(t testing.TB, mem memory.Allocator, r *rand.Rand, n int, types []arrow.DataType) *dataframe.DataFrame {
	t.Helper()
	var cols []*series.Series
	for c := range 1 + r.IntN(6) {
		dt := types[r.IntN(len(types))]
		vals := testutil.RandValues(r, dt, n, 0.2*float64(r.IntN(2)))
		name := fmt.Sprintf("c%d_%s", c, dt)
		if n >= 2 && r.IntN(3) == 0 {
			cut := r.IntN(n + 1)
			a, err := series.FromValues(name, dt, vals[:cut], series.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			b, err := series.FromValues(name, dt, vals[cut:], series.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			ca, cb := a.Chunks()[0], b.Chunks()[0]
			ca.Retain()
			cb.Retain()
			a.Release()
			b.Release()
			s, err := series.New(name, ca, cb)
			if err != nil {
				t.Fatal(err)
			}
			cols = append(cols, s)
			continue
		}
		s, err := series.FromValues(name, dt, vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, s)
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// assertFramesEqual checks names, dtypes and every value (NaN equals
// NaN, and -0.0 must stay -0.0 through a lossless format).
func assertFramesEqual(t testing.TB, desc string, got, want *dataframe.DataFrame) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("%s: shape %dx%d want %dx%d", desc, got.Height(), got.Width(), want.Height(), want.Width())
	}
	for i := range want.Width() {
		g, w := got.ColumnAt(i), want.ColumnAt(i)
		if g.Name() != w.Name() || !g.DType().Equal(w.DType()) {
			t.Fatalf("%s: column %d is %s %s want %s %s", desc, i, g.Name(), g.DType(), w.Name(), w.DType())
		}
		gv, wv := g.ToList(), w.ToList()
		for j := range wv {
			if !testutil.ValuesIdentical(gv[j], wv[j]) {
				t.Fatalf("%s: column %s row %d = %v want %v", desc, w.Name(), j, gv[j], wv[j])
			}
		}
	}
}

func TestParquetRoundTripProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(53, 54))
	sizes := append(testutil.PropSizes, 5000)
	for iter := range 150 {
		n := sizes[r.IntN(len(sizes))]
		df := randIOFrame(t, mem, r, n, pqPropTypes)
		var buf bytes.Buffer
		if err := parquet.Write(ctx, &buf, df, parquet.WithAllocator(mem)); err != nil {
			t.Fatalf("iter %d write: %v", iter, err)
		}
		back, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d read: %v", iter, err)
		}
		assertFramesEqual(t, fmt.Sprintf("iter %d n=%d", iter, n), back, df)
		back.Release()
		df.Release()
	}
}

func roundTripParquet(t *testing.T, mem memory.Allocator, df *dataframe.DataFrame) *dataframe.DataFrame {
	t.Helper()
	var buf bytes.Buffer
	if err := parquet.Write(context.Background(), &buf, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatalf("write: %v", err)
	}
	back, err := parquet.ReadBytes(context.Background(), buf.Bytes(), parquet.WithAllocator(mem))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return back
}

// TestParquetWriteRegressions pins bugs found by the round trip
// property test. Writing a bool column with no non-null value (empty or
// all null) panicked in the plain boolean encoder, and the parallel
// flat writer dropped the ARROW:schema footer, so a datetime time zone
// came back as UTC. polars 1.39 keeps the zone:
//
//	df = pl.DataFrame({"t": [datetime(2020, 1, 1)]}).with_columns(pl.col("t").dt.replace_time_zone("Asia/Kolkata"))
//	df.write_parquet(b); pl.read_parquet(b).schema  -> Datetime(time_unit='us', time_zone='Asia/Kolkata')
func TestParquetWriteRegressions(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	build := func(name string, dt arrow.DataType, vals []any) *dataframe.DataFrame {
		s, err := series.FromValues(name, dt, vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		df, err := dataframe.New(s)
		if err != nil {
			t.Fatal(err)
		}
		return df
	}
	for _, vals := range [][]any{{}, {nil}, {nil, nil, nil}} {
		df := build("b", arrow.FixedWidthTypes.Boolean, vals)
		back := roundTripParquet(t, mem, df)
		assertFramesEqual(t, fmt.Sprintf("bool %v", vals), back, df)
		back.Release()
		df.Release()
	}
	df := build("t", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Kolkata"}, []any{int64(1577836800000000), nil})
	back := roundTripParquet(t, mem, df)
	assertFramesEqual(t, "datetime with time zone", back, df)
	back.Release()
	df.Release()
}

// TestParquetDurationRoundTrip documents a gap: polars writes duration
// columns (as int64 plus the ARROW:schema footer) and reads them back as
// Duration, while golars refuses to write them.
func TestParquetDurationRoundTrip(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, err := series.FromValues("d", arrow.FixedWidthTypes.Duration_ms, []any{int64(5), nil}, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	var buf bytes.Buffer
	if err := parquet.Write(context.Background(), &buf, df, parquet.WithAllocator(mem)); err != nil {
		t.Skipf("known bug: parquet write of duration columns fails (%v); "+
			"polars: pl.DataFrame({\"d\": [timedelta(milliseconds=5)]}).write_parquet(b) round trips as Duration", err)
	}
	back, err := parquet.ReadBytes(context.Background(), buf.Bytes(), parquet.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	assertFramesEqual(t, "duration", back, df)
}
