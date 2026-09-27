package parquet_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/io/parquet"
)

// TestPolarsFixtures reads parquet files written by polars (see
// testdata/polars/gen.py) with the native reader and with pqarrow and
// checks both against the Arrow IPC copy polars wrote of the same frame.
func TestPolarsFixtures(t *testing.T) {
	fixtures := map[string]string{
		"all_uncompressed": "base",
		"all_snappy":       "base",
		"all_zstd":         "base",
		"all_gzip":         "base",
		"all_lz4":          "base",
		"all_brotli":       "base",
		"multi_rg":         "base",
		"no_stats":         "base",
		"one_row":          "one_row",
		"empty":            "empty",
		"nested":           "nested",
	}
	ctx := context.Background()
	for name, expect := range fixtures {
		t.Run(name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			dir := filepath.Join("testdata", "polars")
			want, err := ipc.ReadFile(ctx, filepath.Join(dir, expect+".arrow"))
			if err != nil {
				t.Fatalf("ipc: %v", err)
			}
			defer want.Release()
			path := filepath.Join(dir, name+".parquet")
			ref, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithoutNative())
			if err != nil {
				t.Fatalf("pqarrow read: %v", err)
			}
			defer ref.Release()
			got, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem))
			if err != nil {
				t.Fatalf("native read: %v", err)
			}
			defer got.Release()
			assertFramesEqual(t, mem, ref, got)
			assertSameValues(t, mem, want, got)

			// A projection that mixes native and pqarrow columns.
			names := got.ColumnNames()
			if len(names) > 3 {
				names = []string{names[3], names[0], names[len(names)-1]}
			}
			proj, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithColumns(names...))
			if err != nil {
				t.Fatalf("projected read: %v", err)
			}
			defer proj.Release()
			wantProj, err := ref.Select(names...)
			if err != nil {
				t.Fatal(err)
			}
			defer wantProj.Release()
			assertFramesEqual(t, mem, wantProj, proj)
		})
	}
}

// assertSameValues compares golars dtypes and the string form of every
// value, which tolerates arrow layout differences (string and
// large_string) between the IPC and parquet readers.
func assertSameValues(t *testing.T, mem memory.Allocator, want, got *dataframe.DataFrame) {
	t.Helper()
	if want.Height() != got.Height() || want.Width() != got.Width() {
		t.Fatalf("shape %dx%d, want %dx%d", got.Height(), got.Width(), want.Height(), want.Width())
	}
	for i := range want.Width() {
		ws, gs := want.ColumnAt(i), got.ColumnAt(i)
		if ws.Name() != gs.Name() {
			t.Fatalf("column %d is %q, want %q", i, gs.Name(), ws.Name())
		}
		// Parquet names list items "element" where IPC uses "item".
		if wd, gd := ws.DType().String(), strings.ReplaceAll(gs.DType().String(), "element:", "item:"); wd != gd {
			// Categorical columns come back as strings from parquet.
			if !strings.HasPrefix(wd, "cat") {
				t.Errorf("column %q dtype %s, want %s", ws.Name(), gs.DType(), ws.DType())
			}
			continue
		}
		wa, ga := concatSeries(t, mem, ws), concatSeries(t, mem, gs)
		for r := range wa.Len() {
			if wa.IsNull(r) != ga.IsNull(r) || valueStr(wa, r) != valueStr(ga, r) {
				t.Errorf("column %q row %d: got %s, want %s", ws.Name(), r, valueStr(ga, r), valueStr(wa, r))
				break
			}
		}
		wa.Release()
		ga.Release()
	}
}

func valueStr(a arrow.Array, i int) string {
	if a.IsNull(i) {
		return "null"
	}
	return a.ValueStr(i)
}
