package parquet_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

// A dictionary-encoded column whose raw size reaches the 1 MiB data page
// limit exactly at the last write batch of a row group has its page cut
// by the writer itself. The final flush must then see an empty encoder
// and skip, rather than emit an empty page (which used to panic in the
// pqarrow writer). The native writer is checked on the same shape.
func TestParquetWriteDictPageCutAtChunkEnd(t *testing.T) {
	t.Parallel()
	for _, native := range []bool{true, false} {
		t.Run(fmt.Sprintf("native=%v", native), func(t *testing.T) {
			testDictPageCutAtChunkEnd(t, native)
		})
	}
}

func testDictPageCutAtChunkEnd(t *testing.T, native bool) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	opts := []parquet.Option{parquet.WithAllocator(mem)}
	if !native {
		opts = append(opts, parquet.WithoutNative())
	}

	// 60-byte strings account 64 raw bytes each (length prefix), so
	// 16384 rows are exactly 1 MiB, hit after the 16th 1024-value batch.
	const rows = 16384
	vals := make([]string, rows)
	for i := range vals {
		v := fmt.Sprintf("value-%03d-", i%100)
		vals[i] = v + strings.Repeat("x", 60-len(v))
	}
	s, err := series.FromString("s", vals, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	var buf bytes.Buffer
	if err := parquet.Write(ctx, &buf, df, opts...); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := parquet.ReadBytes(ctx, buf.Bytes(), opts...)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer got.Release()
	col, _ := got.Column("s")
	if got.Height() != rows {
		t.Fatalf("Height = %d, want %d", got.Height(), rows)
	}
	for _, i := range []int{0, 1023, 1024, rows - 1} {
		if v, _ := col.Get(i); v != vals[i] {
			t.Fatalf("row %d = %q, want %q", i, v, vals[i])
		}
	}
}
