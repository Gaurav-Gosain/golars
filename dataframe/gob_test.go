package dataframe_test

import (
	"bytes"
	"encoding/gob"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// gobFrame builds a frame with one column per interesting arrow type. The
// int column has two chunks, so the frame is multi-chunk.
func gobFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	var cols []*series.Series
	add := func(name string, arrs ...arrow.Array) {
		s, err := series.New(name, arrs...)
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, s)
	}

	ib := array.NewInt64Builder(mem)
	ib.AppendValues([]int64{1, 2}, []bool{true, false})
	i1 := ib.NewArray()
	ib.AppendValues([]int64{3, 4}, nil)
	i2 := ib.NewArray()
	ib.Release()
	add("i", i1, i2)

	fb := array.NewFloat64Builder(mem)
	fb.AppendValues([]float64{1.5, math.NaN(), math.Inf(-1), 0}, []bool{true, true, true, false})
	add("f", fb.NewArray())
	fb.Release()

	sb := array.NewStringBuilder(mem)
	sb.AppendValues([]string{"a", "", "c", "d"}, []bool{true, true, false, true})
	add("s", sb.NewArray())
	sb.Release()

	bb := array.NewBooleanBuilder(mem)
	bb.AppendValues([]bool{true, false, true, false}, []bool{true, true, true, false})
	add("b", bb.NewArray())
	bb.Release()

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String}
	db := array.NewDictionaryBuilder(mem, dt).(*array.BinaryDictionaryBuilder)
	for _, v := range []string{"x", "y", "x"} {
		if err := db.AppendString(v); err != nil {
			t.Fatal(err)
		}
	}
	db.AppendNull()
	add("cat", db.NewArray())
	db.Release()

	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Berlin"}
	tb := array.NewTimestampBuilder(mem, ts)
	tb.AppendValues([]arrow.Timestamp{0, 1_700_000_000_000_000, -5, 7}, []bool{true, true, true, false})
	add("ts", tb.NewArray())
	tb.Release()

	d32 := array.NewDate32Builder(mem)
	d32.AppendValues([]arrow.Date32{0, 19000, -1, 3}, nil)
	add("d", d32.NewArray())
	d32.Release()

	lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	lv := lb.ValueBuilder().(*array.Int64Builder)
	lb.Append(true)
	lv.AppendValues([]int64{1, 2}, nil)
	lb.AppendNull()
	lb.Append(true)
	lb.Append(true)
	lv.Append(9)
	add("l", lb.NewArray())
	lb.Release()

	st := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	stb := array.NewStructBuilder(mem, st)
	ab := stb.FieldBuilder(0).(*array.Int64Builder)
	bsb := stb.FieldBuilder(1).(*array.StringBuilder)
	for i := range 4 {
		if i == 2 {
			stb.AppendNull()
			ab.AppendNull()
			bsb.AppendNull()
			continue
		}
		stb.Append(true)
		ab.Append(int64(i))
		bsb.Append(strings.Repeat("z", i))
	}
	add("st", stb.NewArray())
	stb.Release()

	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// assertSameFrame checks names, exact arrow types (including time zones
// and dictionary types) and values.
func assertSameFrame(t *testing.T, got, want *dataframe.DataFrame) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("shape: got %dx%d, want %dx%d", got.Height(), got.Width(), want.Height(), want.Width())
	}
	for i := range want.Width() {
		g, w := got.ColumnAt(i), want.ColumnAt(i)
		if g.Name() != w.Name() {
			t.Fatalf("column %d: name %q, want %q", i, g.Name(), w.Name())
		}
		assertSameSeries(t, g, w)
	}
}

func assertSameSeries(t *testing.T, g, w *series.Series) {
	t.Helper()
	if g.Name() != w.Name() {
		t.Fatalf("name %q, want %q", g.Name(), w.Name())
	}
	if !arrow.TypeEqual(g.Chunked().DataType(), w.Chunked().DataType()) {
		t.Fatalf("%s: type %s, want %s", w.Name(), g.Chunked().DataType(), w.Chunked().DataType())
	}
	if !array.ChunkedApproxEqual(g.Chunked(), w.Chunked(), array.WithNaNsEqual(true)) {
		t.Fatalf("%s: values differ", w.Name())
	}
}

func gobRoundTrip[T any](t *testing.T, v T) T {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	var out T
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGobDataFrameRoundTrip(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := gobFrame(t, mem)
	defer df.Release()

	got := gobRoundTrip(t, df)
	defer got.Release()
	assertSameFrame(t, got, df)
	if n := len(got.ColumnAt(0).Chunks()); n != 2 {
		t.Errorf("multi-chunk column decoded as %d chunks, want 2", n)
	}

	// Nested in other values.
	type holder struct {
		DF     *dataframe.DataFrame
		S      *series.Series
		Frames map[string]*dataframe.DataFrame
		Nil    *dataframe.DataFrame
	}
	col := df.ColumnAt(4)
	h := gobRoundTrip(t, holder{DF: df, S: col, Frames: map[string]*dataframe.DataFrame{"a": df}})
	assertSameFrame(t, h.DF, df)
	assertSameFrame(t, h.Frames["a"], df)
	assertSameSeries(t, h.S, col)
	if h.Nil != nil {
		t.Error("nil frame decoded as non-nil")
	}
}

func TestGobDataFrameEdges(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := gobFrame(t, mem)
	defer df.Release()

	sliced, err := df.Slice(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer sliced.Release()
	got := gobRoundTrip(t, sliced)
	assertSameFrame(t, got, sliced)
	got.Release()

	empty, err := df.Slice(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Release()
	got = gobRoundTrip(t, empty)
	assertSameFrame(t, got, empty)
	got.Release()

	none, err := dataframe.New()
	if err != nil {
		t.Fatal(err)
	}
	got = gobRoundTrip(t, none)
	if got.Width() != 0 || got.Height() != 0 {
		t.Errorf("empty frame decoded as %dx%d", got.Height(), got.Width())
	}

	nb := array.NewInt64Builder(mem)
	nb.AppendNulls(3)
	s, err := series.New("n", nb.NewArray())
	nb.Release()
	if err != nil {
		t.Fatal(err)
	}
	nulls, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer nulls.Release()
	got = gobRoundTrip(t, nulls)
	assertSameFrame(t, got, nulls)
	got.Release()

	var nilDF *dataframe.DataFrame
	if _, err := nilDF.GobEncode(); err == nil {
		t.Error("GobEncode of a nil frame: no error")
	}
	var bad dataframe.DataFrame
	if err := bad.GobDecode([]byte("not arrow")); err == nil {
		t.Error("GobDecode of garbage: no error")
	}
}

func TestGobSeriesRoundTrip(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := gobFrame(t, mem)
	defer df.Release()
	for i := range df.Width() {
		s := df.ColumnAt(i)
		got := gobRoundTrip(t, s)
		assertSameSeries(t, got, s)
		got.Release()
	}
	e := series.Empty("e", df.ColumnAt(5).DType())
	got := gobRoundTrip(t, e)
	if got.Len() != 0 || got.Name() != "e" {
		t.Errorf("empty series decoded as %q len %d", got.Name(), got.Len())
	}
}

func TestGobLazyFrameError(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := gobFrame(t, mem)
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	err := gob.NewEncoder(&bytes.Buffer{}).Encode(struct{ V lazy.LazyFrame }{lf})
	if err == nil || !strings.Contains(err.Error(), "call Collect") {
		t.Fatalf("got %v, want an error that says to collect", err)
	}
}

func BenchmarkGobDataFrame(b *testing.B) {
	const n = 1 << 20
	vals := make([]int64, n)
	fv := make([]float64, n)
	strs := make([]string, n)
	for i := range vals {
		vals[i] = int64(i)
		fv[i] = float64(i) / 3
		strs[i] = "key" + string(rune('a'+i%26))
	}
	a, _ := series.FromInt64("a", vals, nil)
	f, _ := series.FromFloat64("f", fv, nil)
	s, _ := series.FromString("s", strs, nil)
	df, err := dataframe.New(a, f, s)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()
	data, err := df.GobEncode()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			if _, err := df.GobEncode(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			var out dataframe.DataFrame
			if err := out.GobDecode(data); err != nil {
				b.Fatal(err)
			}
			out.Release()
		}
	})
}
