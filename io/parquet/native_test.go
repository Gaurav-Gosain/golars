package parquet_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

// nativeTypes lists every arrow type the native reader and writer
// handle, plus a few that go through pqarrow.
var nativeTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8,
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8,
	arrow.PrimitiveTypes.Uint16,
	arrow.PrimitiveTypes.Uint32,
	arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32,
	arrow.PrimitiveTypes.Float64,
	arrow.FixedWidthTypes.Boolean,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.LargeString,
	arrow.BinaryTypes.Binary,
	arrow.FixedWidthTypes.Date32,
	arrow.FixedWidthTypes.Timestamp_ms,
	arrow.FixedWidthTypes.Timestamp_us,
	arrow.FixedWidthTypes.Timestamp_ns,
	&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Dubai"},
	arrow.FixedWidthTypes.Time32ms,
	arrow.FixedWidthTypes.Time64us,
	arrow.FixedWidthTypes.Time64ns,
}

// fallbackTypes are written and read through pqarrow.
var fallbackTypes = []arrow.DataType{
	arrow.FixedWidthTypes.Date64,
	arrow.FixedWidthTypes.Timestamp_s,
}

// genArray returns n random values of dt. nullFrac is the share of
// nulls (1 means all null). Low cardinality values exercise dictionary
// encoding.
func genArray(mem memory.Allocator, dt arrow.DataType, n int, nullFrac float64, lowCard bool, r *rand.Rand) arrow.Array {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	card := int64(1 << 40)
	if lowCard {
		card = 7
	}
	for range n {
		if nullFrac > 0 && r.Float64() < nullFrac {
			b.AppendNull()
			continue
		}
		v := r.Int64N(card)
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(int8(v))
		case *array.Int16Builder:
			bb.Append(int16(v))
		case *array.Int32Builder:
			bb.Append(int32(v))
		case *array.Int64Builder:
			bb.Append(v - card/2)
		case *array.Uint8Builder:
			bb.Append(uint8(v))
		case *array.Uint16Builder:
			bb.Append(uint16(v))
		case *array.Uint32Builder:
			bb.Append(uint32(v))
		case *array.Uint64Builder:
			bb.Append(uint64(v) << 20)
		case *array.Float32Builder:
			bb.Append(float32(v) / 3)
		case *array.Float64Builder:
			if v%11 == 3 {
				bb.Append(math.NaN())
			} else {
				bb.Append(float64(v) / 7)
			}
		case *array.BooleanBuilder:
			bb.Append(v%2 == 0)
		case *array.StringBuilder:
			bb.Append(fmt.Sprintf("s%d", v%100000))
		case *array.LargeStringBuilder:
			bb.Append(fmt.Sprintf("large-%d", v))
		case *array.BinaryBuilder:
			if v%5 == 0 {
				bb.Append([]byte{})
			} else {
				bb.Append([]byte(fmt.Sprintf("b%x", v)))
			}
		case *array.Date32Builder:
			bb.Append(arrow.Date32(v % 40000))
		case *array.Date64Builder:
			bb.Append(arrow.Date64((v % 40000) * 86400000))
		case *array.TimestampBuilder:
			bb.Append(arrow.Timestamp(v % (1 << 50)))
		case *array.Time32Builder:
			bb.Append(arrow.Time32(v % 86400000))
		case *array.Time64Builder:
			bb.Append(arrow.Time64(v % 86400000000))
		case *array.DurationBuilder:
			bb.Append(arrow.Duration(v))
		default:
			panic(fmt.Sprintf("genArray: %T", b))
		}
	}
	return b.NewArray()
}

func concatSeries(t *testing.T, mem memory.Allocator, s *series.Series) arrow.Array {
	t.Helper()
	chunks := s.Chunks()
	if len(chunks) == 1 {
		chunks[0].Retain()
		return chunks[0]
	}
	if len(chunks) == 0 {
		return array.MakeArrayOfNull(mem, s.DType().Arrow(), 0)
	}
	a, err := array.Concatenate(chunks, mem)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// assertFramesEqual checks that two frames have the same names, arrow
// types and values.
func assertFramesEqual(t *testing.T, mem memory.Allocator, want, got *dataframe.DataFrame) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("shape %dx%d, want %dx%d", got.Height(), got.Width(), want.Height(), want.Width())
	}
	for i := range want.Width() {
		ws, gs := want.ColumnAt(i), got.ColumnAt(i)
		if ws.Name() != gs.Name() {
			t.Fatalf("column %d name %q, want %q", i, gs.Name(), ws.Name())
		}
		wa, ga := concatSeries(t, mem, ws), concatSeries(t, mem, gs)
		if !arrow.TypeEqual(wa.DataType(), ga.DataType()) {
			t.Errorf("column %q type %s, want %s", ws.Name(), ga.DataType(), wa.DataType())
		} else if !array.ApproxEqual(wa, ga, array.WithNaNsEqual(true)) {
			t.Errorf("column %q values differ\nwant %v\ngot  %v", ws.Name(), trunc(wa), trunc(ga))
		}
		if wa.NullN() != ga.NullN() {
			t.Errorf("column %q nulls %d, want %d", ws.Name(), ga.NullN(), wa.NullN())
		}
		wa.Release()
		ga.Release()
	}
}

func trunc(a arrow.Array) string {
	if a.Len() > 20 {
		s := array.NewSlice(a, 0, 20)
		defer s.Release()
		return s.String() + "..."
	}
	return a.String()
}

func buildFrame(t *testing.T, mem memory.Allocator, n int, nullFrac float64, lowCard bool, seed uint64, types []arrow.DataType) *dataframe.DataFrame {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, 99))
	cols := make([]*series.Series, 0, len(types))
	for i, dt := range types {
		a := genArray(mem, dt, n, nullFrac, lowCard, r)
		s, err := series.New(fmt.Sprintf("c%d_%s", i, dt), a)
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

// TestNativeMatchesPqarrow writes frames of every supported type
// natively and with pqarrow and compares the native reader and pqarrow.
func TestNativeMatchesPqarrow(t *testing.T) {
	codecs := []compress.Compression{
		compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd,
		compress.Codecs.Gzip, compress.Codecs.Brotli, compress.Codecs.Lz4Raw,
	}
	type shape struct {
		rows      int
		chunk     int64
		nullFrac  float64
		lowCard   bool
		codecMask int
	}
	shapes := []shape{
		{0, 100, 0, false, 0b11},
		{1, 100, 0, false, 0b11},
		{1, 100, 1, false, 0b11},
		{7, 3, 0.3, false, 0b111111},
		{1000, 333, 0, false, 0b111111},
		{1000, 333, 0.2, true, 0b111111},
		{5000, 1000, 1, false, 0b11},
		{20000, 7001, 0.05, false, 0b110},
		{20000, 64000, 0.5, true, 0b10},
	}
	for si, sh := range shapes {
		for ci, codec := range codecs {
			if sh.codecMask&(1<<ci) == 0 {
				continue
			}
			for _, fb := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d_%s_rows%d_null%.2f_fallback%v", si, codec, sh.rows, sh.nullFrac, fb), func(t *testing.T) {
					mem := testutil.NewCheckedAllocator(t)
					types := nativeTypes
					if fb {
						types = append(append([]arrow.DataType{}, nativeTypes...), fallbackTypes...)
					}
					df := buildFrame(t, mem, sh.rows, sh.nullFrac, sh.lowCard, uint64(si*10+ci), types)
					defer df.Release()
					checkWriteRead(t, mem, df, codec, sh.chunk, !fb)
				})
			}
		}
	}
}

// checkWriteRead writes df with the native writer and with pqarrow and
// checks that pqarrow and the native reader decode both files to the
// same frame.
func checkWriteRead(t *testing.T, mem memory.Allocator, df *dataframe.DataFrame, codec compress.Compression, chunk int64, exact bool) {
	t.Helper()
	ctx := context.Background()
	write := func(opts ...parquet.Option) []byte {
		var buf bytes.Buffer
		opts = append(opts, parquet.WithAllocator(mem), parquet.WithCompression(codec), parquet.WithChunkSize(chunk))
		if err := parquet.Write(ctx, &buf, df, opts...); err != nil {
			t.Fatalf("Write: %v", err)
		}
		return buf.Bytes()
	}
	read := func(b []byte, opts ...parquet.Option) *dataframe.DataFrame {
		got, err := parquet.ReadBytes(ctx, b, append(opts, parquet.WithAllocator(mem))...)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		return got
	}
	nativeFile := write()
	refFile := write(parquet.WithoutNative())
	if exact {
		compareStats(t, nativeFile, refFile)
	}
	for _, f := range [][]byte{nativeFile, refFile} {
		want := read(f, parquet.WithoutNative())
		got := read(f)
		assertFramesEqual(t, mem, want, got)
		want.Release()
		got.Release()
	}
	want := read(nativeFile)
	defer want.Release()
	if exact {
		// The native writer stores the arrow schema, so every type
		// round-trips exactly.
		assertFramesEqual(t, mem, df, want)
	}
	if df.Width() < 12 {
		return
	}
	// Projection keeps the requested order.
	names := []string{df.ColumnAt(11).Name(), df.ColumnAt(3).Name(), df.ColumnAt(10).Name()}
	proj := read(nativeFile, parquet.WithColumns(names...))
	defer proj.Release()
	wantProj, err := want.Select(names...)
	if err != nil {
		t.Fatal(err)
	}
	defer wantProj.Release()
	assertFramesEqual(t, mem, wantProj, proj)
}

// TestNativeMultiChunkSliced writes multi-chunk and sliced frames whose
// row groups cut across arrow chunk boundaries.
func TestNativeMultiChunkSliced(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(5, 6))
	lens := []int{100, 1, 0, 257, 64}
	cols := make([]*series.Series, 0, len(nativeTypes))
	for i, dt := range nativeTypes {
		var chunks []arrow.Array
		for j, n := range lens {
			chunks = append(chunks, genArray(mem, dt, n, 0.15*float64(j%2), j == 3, r))
		}
		s, err := series.New(fmt.Sprintf("c%d", i), chunks...)
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, s)
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	for _, chunk := range []int64{7, 64, 1000} {
		checkWriteRead(t, mem, df, compress.Codecs.Snappy, chunk, true)
	}
	sl, err := df.Slice(5, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Release()
	for _, chunk := range []int64{9, 100} {
		checkWriteRead(t, mem, sl, compress.Codecs.Zstd, chunk, true)
	}
}

// compareStats checks that two files with the same row groups carry the
// same column chunk statistics.
func compareStats(t *testing.T, a, b []byte) {
	t.Helper()
	fa, err := file.NewParquetReader(bytes.NewReader(a))
	if err != nil {
		t.Fatal(err)
	}
	fb, err := file.NewParquetReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if fa.NumRowGroups() != fb.NumRowGroups() {
		t.Fatalf("row groups %d, want %d", fa.NumRowGroups(), fb.NumRowGroups())
	}
	for r := range fa.NumRowGroups() {
		ra, rb := fa.MetaData().RowGroup(r), fb.MetaData().RowGroup(r)
		if ra.NumRows() != rb.NumRows() {
			t.Fatalf("row group %d rows %d, want %d", r, ra.NumRows(), rb.NumRows())
		}
		for c := range ra.NumColumns() {
			ca, _ := ra.ColumnChunk(c)
			cb, _ := rb.ColumnChunk(c)
			sa, err := ca.Statistics()
			if err != nil {
				t.Fatal(err)
			}
			sb, err := cb.Statistics()
			if err != nil {
				t.Fatal(err)
			}
			name := fa.MetaData().Schema.Column(c).Name()
			if (sa == nil) != (sb == nil) {
				t.Errorf("rg %d column %s: stats present %v, want %v", r, name, sa != nil, sb != nil)
				continue
			}
			if sa == nil {
				continue
			}
			if sa.NullCount() != sb.NullCount() || sa.HasMinMax() != sb.HasMinMax() {
				t.Errorf("rg %d column %s: nulls %d minmax %v, want %d %v", r, name, sa.NullCount(), sa.HasMinMax(), sb.NullCount(), sb.HasMinMax())
				continue
			}
			if sa.HasMinMax() && (!bytes.Equal(sa.EncodeMin(), sb.EncodeMin()) || !bytes.Equal(sa.EncodeMax(), sb.EncodeMax())) {
				t.Errorf("rg %d column %s: min/max %x/%x, want %x/%x", r, name, sa.EncodeMin(), sa.EncodeMax(), sb.EncodeMin(), sb.EncodeMax())
			}
		}
	}
}
