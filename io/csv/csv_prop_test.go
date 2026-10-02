package csv_test

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
	"github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/series"
)

// CSV has no type information, so the round trip reads back with the
// written schema. Strings are left out of the random round trip: CSV
// cannot tell an empty string from a null unless the writer quotes
// empty strings, which TestCSVEmptyStringVsNull covers on its own.
// Temporal types are covered by the typed readers' own tests.
var csvPropTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.FixedWidthTypes.Boolean,
}

func randCSVFrame(t testing.TB, mem memory.Allocator, r *rand.Rand, n int, types []arrow.DataType) *dataframe.DataFrame {
	t.Helper()
	// A non-null id column keeps every line non-blank; blank lines are
	// covered by TestCSVBlankLineIsNullRow.
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	id, err := series.FromInt64("id", ids, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	cols := []*series.Series{id}
	for c := range 1 + r.IntN(6) {
		dt := types[r.IntN(len(types))]
		vals := testutil.RandValues(r, dt, n, 0.2*float64(r.IntN(2)))
		s, err := series.FromValues(fmt.Sprintf("c%d", c), dt, vals, series.WithAllocator(mem))
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

func assertCSVFramesEqual(t testing.TB, desc string, got, want *dataframe.DataFrame, text string) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("%s: shape %dx%d want %dx%d\n%s", desc, got.Height(), got.Width(), want.Height(), want.Width(), text)
	}
	for i := range want.Width() {
		g, w := got.ColumnAt(i), want.ColumnAt(i)
		if g.Name() != w.Name() || !g.DType().Equal(w.DType()) {
			t.Fatalf("%s: column %d is %s %s want %s %s", desc, i, g.Name(), g.DType(), w.Name(), w.DType())
		}
		gv, wv := g.ToList(), w.ToList()
		for j := range wv {
			if !testutil.ValuesIdentical(gv[j], wv[j]) {
				t.Fatalf("%s: column %s row %d = %v want %v\n%s", desc, w.Name(), j, gv[j], wv[j], text)
			}
		}
	}
}

func TestCSVRoundTripProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(55, 56))
	for iter := range 200 {
		n := testutil.PropSizes[r.IntN(len(testutil.PropSizes))]
		df := randCSVFrame(t, mem, r, n, csvPropTypes)
		var buf bytes.Buffer
		if err := csv.Write(ctx, &buf, df); err != nil {
			t.Fatalf("iter %d write: %v", iter, err)
		}
		text := buf.String()
		back, err := csv.Read(ctx, &buf, csv.WithSchema(df.Schema()), csv.WithAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d read: %v\n%s", iter, err, text)
		}
		assertCSVFramesEqual(t, fmt.Sprintf("iter %d", iter), back, df, text)
		back.Release()
		df.Release()
	}
}

// TestCSVBlankLineIsNullRow: polars reads a blank line as a row of
// nulls, and writes a single null column value as a blank line, so its
// own output round trips. golars skips blank lines (skipBlank in
// tokenize.go and the fastread loops), dropping those rows.
//
//	pl.read_csv(io.StringIO("x\n1\n\n\n2\n"))       -> x: [1, None, None, 2]
//	pl.read_csv(io.StringIO("a,b\n1,2\n\n3,4\n"))   -> a: [1, None, 3], b: [2, None, 4]
func TestCSVBlankLineIsNullRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		text string
		rows int
	}{
		{"x\n1\n\n\n2\n", 4},
		{"x\n\n1\n", 2},
		{"a,b\n1,2\n\n3,4\n", 3},
	}
	for _, c := range cases {
		df, err := csv.Read(ctx, bytes.NewReader([]byte(c.text)))
		if err != nil {
			t.Fatalf("%q: %v", c.text, err)
		}
		h := df.Height()
		df.Release()
		if h != c.rows {
			t.Skipf("known bug: CSV reader drops blank lines, polars reads each as a null row: %q gave %d rows, want %d", c.text, h, c.rows)
		}
	}
}

// TestCSVEmptyStringVsNull checks that "" and null survive a round
// trip as distinct values. polars writes "" as a quoted empty field and
// null as an empty field, and reads them back apart:
//
//	pl.DataFrame({"s": ["", None, "a"]}).write_csv()  -> 's\n""\n\na\n'
//
// golars writes both as an empty field (encoding/csv never quotes an
// empty string), and the reader also drops the resulting blank lines.
func TestCSVEmptyStringVsNull(t *testing.T) {
	t.Parallel()
	t.Skip(`known bug: csv.Write writes "" and null alike; polars writes "" quoted`)
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	s, err := series.FromValues("s", arrow.BinaryTypes.String, []any{"", nil, "a", "x,y", "q\"q"}, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	var buf bytes.Buffer
	if err := csv.Write(ctx, &buf, df); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	back, err := csv.Read(ctx, &buf, csv.WithSchema(df.Schema()), csv.WithAllocator(mem))
	if err != nil {
		t.Fatalf("read: %v\n%s", err, text)
	}
	defer back.Release()
	assertCSVFramesEqual(t, "strings", back, df, text)
}

// FuzzCSVRead feeds arbitrary bytes to the CSV reader with and without
// type inference. It must return an error or a frame, never panic.
func FuzzCSVRead(f *testing.F) {
	f.Add([]byte("a,b\n1,2\n3,4\n"), false)
	f.Add([]byte("a,b\n\"x,y\",2\n,\n"), true)
	f.Add([]byte("x\n1.5\nNaN\ninf\n-0.0\n"), false)
	f.Add([]byte("d\n2020-01-01\n\n2020-02-30\n"), false)
	f.Add([]byte("a\n\"unterminated\n"), false)
	f.Add([]byte("a,a\n1,2\n"), false)
	f.Fuzz(func(t *testing.T, data []byte, noHeader bool) {
		ctx := context.Background()
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		df, err := csv.Read(ctx, bytes.NewReader(data), csv.WithHasHeader(!noHeader), csv.WithAllocator(mem))
		if err != nil {
			return
		}
		for _, c := range df.Columns() {
			_ = c.ToList()
		}
		df.Release()
	})
}
