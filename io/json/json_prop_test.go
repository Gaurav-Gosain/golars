package json_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	gjson "github.com/Gaurav-Gosain/golars/io/json"
	"github.com/Gaurav-Gosain/golars/series"
)

// JSON readers infer types, so the round trip uses only the dtypes the
// reader infers (i64, f64, str, bool). Each column has at least one
// non-null value, since an all-null column has no type to infer, and
// floats exclude NaN and infinities, which JSON cannot represent
// (polars writes them as null). Frames have at least one row: an empty
// array of row objects carries no columns.
var jsonPropTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
}

func randJSONFrame(t testing.TB, mem memory.Allocator, r *rand.Rand, n int) *dataframe.DataFrame {
	t.Helper()
	var cols []*series.Series
	for c := range 1 + r.IntN(5) {
		dt := jsonPropTypes[r.IntN(len(jsonPropTypes))]
		vals := testutil.RandValues(r, dt, n, 0.2)
		vals[r.IntN(n)] = testutil.RandValue(r, dt, true)
		for i, v := range vals {
			if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				vals[i] = 0.25
			}
		}
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

func assertJSONFramesEqual(t *testing.T, desc string, got, want *dataframe.DataFrame, text string) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("%s: shape %dx%d want %dx%d\n%s", desc, got.Height(), got.Width(), want.Height(), want.Width(), text)
	}
	for i := range want.Width() {
		g, w := got.ColumnAt(i), want.ColumnAt(i)
		if g.Name() != w.Name() || !g.DType().Equal(w.DType()) {
			t.Fatalf("%s: column %d is %s %s want %s %s\n%s", desc, i, g.Name(), g.DType(), w.Name(), w.DType(), text)
		}
		gv, wv := g.ToList(), w.ToList()
		for j := range wv {
			if !testutil.ValuesEqual(gv[j], wv[j]) {
				t.Fatalf("%s: column %s row %d = %v want %v\n%s", desc, w.Name(), j, gv[j], wv[j], text)
			}
		}
	}
}

func TestJSONRoundTripProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(57, 58))
	for iter := range 200 {
		n := 1 + r.IntN(70)
		df := randJSONFrame(t, mem, r, n)
		for _, nd := range []bool{false, true} {
			var buf bytes.Buffer
			var err error
			if nd {
				err = gjson.WriteNDJSON(ctx, &buf, df)
			} else {
				err = gjson.Write(ctx, &buf, df)
			}
			if err != nil {
				t.Fatalf("iter %d write: %v", iter, err)
			}
			text := buf.String()
			var back *dataframe.DataFrame
			if nd {
				back, err = gjson.ReadNDJSON(ctx, &buf, gjson.WithAllocator(mem))
			} else {
				back, err = gjson.Read(ctx, &buf, gjson.WithAllocator(mem))
			}
			if err != nil {
				t.Fatalf("iter %d read: %v\n%s", iter, err, text)
			}
			assertJSONFramesEqual(t, fmt.Sprintf("iter %d ndjson=%v", iter, nd), back, df, text)
			back.Release()
		}
		df.Release()
	}
}

// TestJSONNumberRegressions pins two reader bugs the round trip found:
// integers were decoded through float64 (losing digits past 2^53) and
// 1.0 was inferred as an integer. The writer also wrote 1.0 as 1.
// polars 1.39:
//
//	pl.read_ndjson(io.StringIO('{"i":9007199254740993,"f":1.0}\n{"i":-1146835261686228365,"f":2}\n'))
//	  -> i: [9007199254740993, -1146835261686228365] (i64), f: [1.0, 2.0] (f64)
func TestJSONNumberRegressions(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	text := "{\"i\":9007199254740993,\"f\":1.0}\n{\"i\":-1146835261686228365,\"f\":2}\n"
	df, err := gjson.ReadNDJSONString(ctx, text, gjson.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	i, _ := df.Column("i")
	f, _ := df.Column("f")
	if got := i.ToList(); len(got) != 2 || got[0] != int64(9007199254740993) || got[1] != int64(-1146835261686228365) {
		t.Errorf("i = %v (%s)", got, i.DType())
	}
	if got := f.ToList(); !f.DType().IsFloating() || got[0] != 1.0 || got[1] != 2.0 {
		t.Errorf("f = %v (%s), want [1 2] f64", got, f.DType())
	}
}

// FuzzNDJSONRead feeds arbitrary bytes to the NDJSON and JSON readers.
// They must return an error or a frame, never panic.
func FuzzNDJSONRead(f *testing.F) {
	f.Add([]byte(`{"a":1,"b":"x"}` + "\n" + `{"a":2.5,"b":null}`))
	f.Add([]byte(`[{"a":true},{"a":false},{}]`))
	f.Add([]byte(`{"a":[1,2]}`))
	f.Add([]byte(`{"a":{"b":1}}`))
	f.Add([]byte(`{"a":1e400}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		ctx := context.Background()
		for _, nd := range []bool{false, true} {
			var df *dataframe.DataFrame
			var err error
			if nd {
				df, err = gjson.ReadNDJSON(ctx, bytes.NewReader(data))
			} else {
				df, err = gjson.Read(ctx, bytes.NewReader(data))
			}
			if err != nil {
				continue
			}
			for _, c := range df.Columns() {
				_ = c.ToList()
			}
			df.Release()
		}
	})
}
