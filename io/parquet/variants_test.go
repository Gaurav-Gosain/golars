package parquet_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqlib "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
)

// TestNativeReadWriterVariants reads files written by pqarrow with
// options the golars writer never uses: v2 data pages, the parquet 1.0
// PLAIN_DICTIONARY encoding, dictionaries that overflow into plain
// pages, delta and byte-stream-split encodings (read through pqarrow)
// and small pages.
func TestNativeReadWriterVariants(t *testing.T) {
	ctx := context.Background()
	variants := map[string][]pqlib.WriterProperty{
		"v2pages":    {pqlib.WithDataPageVersion(pqlib.DataPageV2)},
		"v2pages_uc": {pqlib.WithDataPageVersion(pqlib.DataPageV2), pqlib.WithCompression(compress.Codecs.Uncompressed)},
		"v2pages_zs": {pqlib.WithDataPageVersion(pqlib.DataPageV2), pqlib.WithCompression(compress.Codecs.Zstd)},
		"v1format":   {pqlib.WithVersion(pqlib.V1_0)},
		"dictspill":  {pqlib.WithDictionaryPageSizeLimit(64), pqlib.WithDataPageSize(128)},
		"delta":      {pqlib.WithDictionaryDefault(false), pqlib.WithEncoding(pqlib.Encodings.DeltaBinaryPacked)},
		"bss":        {pqlib.WithDictionaryDefault(false), pqlib.WithEncoding(pqlib.Encodings.ByteStreamSplit)},
		"smallpages": {pqlib.WithDataPageSize(64), pqlib.WithCompression(compress.Codecs.Snappy)},
	}
	for name, props := range variants {
		t.Run(name, func(t *testing.T) {
			var mem memory.Allocator = testutil.NewCheckedAllocator(t)
			if name == "delta" {
				// arrow-go's delta decoder leaks a small buffer per page.
				mem = memory.DefaultAllocator
			}
			types := nativeTypes
			switch name {
			case "delta":
				types = []arrow.DataType{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64}
			case "bss":
				types = []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64}
			}
			df := buildFrame(t, mem, 3000, 0.2, true, 11, types)
			defer df.Release()
			sc := df.Schema().ToArrow()
			cols := make([]arrow.Column, df.Width())
			for i, s := range df.Columns() {
				cols[i] = *arrow.NewColumn(sc.Field(i), s.Chunked())
			}
			tbl := array.NewTable(sc, cols, int64(df.Height()))
			for i := range cols {
				cols[i].Release()
			}
			defer tbl.Release()
			var buf bytes.Buffer
			wp := pqlib.NewWriterProperties(append([]pqlib.WriterProperty{pqlib.WithAllocator(mem)}, props...)...)
			if err := pqarrow.WriteTable(tbl, &buf, 700, wp, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())); err != nil {
				t.Fatal(err)
			}
			want, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem), parquet.WithoutNative())
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()
			before := parquet.PqarrowReads()
			got, err := parquet.ReadBytes(ctx, buf.Bytes(), parquet.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			native := parquet.PqarrowReads() == before
			if want := name != "delta" && name != "bss"; native != want {
				t.Errorf("native read %v, want %v", native, want)
			}
			defer got.Release()
			assertFramesEqual(t, mem, want, got)
			if name != "v1format" { // parquet 1.0 has no nanosecond timestamps
				assertFramesEqual(t, mem, df, got)
			}
		})
	}
}
