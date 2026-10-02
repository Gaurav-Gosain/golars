package ipc_test

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
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/series"
)

var ipcPropTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Timestamp_ns,
	&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Asia/Kolkata"},
	arrow.FixedWidthTypes.Duration_ms,
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

func TestIPCRoundTripProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(51, 52))
	for iter := range 200 {
		n := testutil.PropSizes[r.IntN(len(testutil.PropSizes))]
		df := randIOFrame(t, mem, r, n, ipcPropTypes)
		var buf bytes.Buffer
		if err := ipc.Write(ctx, &buf, df, ipc.WithAllocator(mem)); err != nil {
			t.Fatalf("iter %d write: %v", iter, err)
		}
		back, err := ipc.Read(ctx, &buf, ipc.WithAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d read: %v", iter, err)
		}
		assertFramesEqual(t, fmt.Sprintf("iter %d", iter), back, df)
		back.Release()
		df.Release()
	}
}

// FuzzIPCRead feeds arbitrary bytes to the IPC reader. It must return
// an error or a frame, never panic, and never leak.
func FuzzIPCRead(f *testing.F) {
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	for range 4 {
		df := randIOFrame(f, memory.DefaultAllocator, r, 5, ipcPropTypes)
		var buf bytes.Buffer
		if err := ipc.Write(ctx, &buf, df); err != nil {
			f.Fatal(err)
		}
		df.Release()
		f.Add(buf.Bytes())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		df, err := ipc.Read(ctx, bytes.NewReader(data))
		if err != nil {
			return
		}
		// Touch every value so lazily detected corruption surfaces.
		for _, c := range df.Columns() {
			_ = c.ToList()
		}
		df.Release()
	})
}
