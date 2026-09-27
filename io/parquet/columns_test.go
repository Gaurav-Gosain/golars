package parquet_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestParquetWithColumns(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()
	path := filepath.Join(t.TempDir(), "cols.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatal(err)
	}

	got, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithColumns("c", "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	if names := got.Schema().Names(); !slices.Equal(names, []string{"c", "a"}) {
		t.Fatalf("names = %v, want [c a]", names)
	}
	c, _ := got.Column("c")
	if v := c.Chunk(0).(*array.String).Value(4); v != "z" {
		t.Errorf("c[4] = %q, want z", v)
	}

	if _, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithColumns("nope")); err == nil {
		t.Error("unknown column should error")
	}
	if _, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithColumns("a", "a")); err == nil {
		t.Error("repeated column should error")
	}

	empty, err := parquet.ReadFile(ctx, path, parquet.WithAllocator(mem), parquet.WithColumns())
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Release()
	if empty.Width() != 0 {
		t.Errorf("empty projection width = %d, want 0", empty.Width())
	}
}

func TestParquetReadSchema(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := sampleDF(t, mem)
	defer df.Release()
	path := filepath.Join(t.TempDir(), "schema.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithAllocator(mem)); err != nil {
		t.Fatal(err)
	}
	sc, err := parquet.ReadSchema(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sc.Names(), []string{"a", "b", "c"}) {
		t.Fatalf("names = %v", sc.Names())
	}
	if !sc.Field(0).DType.Equal(dtype.Int64()) || !sc.Field(1).DType.Equal(dtype.Float64()) || !sc.Field(2).DType.Equal(dtype.String()) {
		t.Errorf("dtypes = %v", sc.DTypes())
	}
}

// TestParquetManyRowGroups checks values survive a file with many row
// groups and nulls, read back with and without projection.
func TestParquetManyRowGroups(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	const n = 10_000
	vals := make([]int64, n)
	valid := make([]bool, n)
	strs := make([]string, n)
	for i := range n {
		vals[i] = int64(i * 3)
		valid[i] = i%7 != 0
		strs[i] = "s" + string(rune('a'+i%26))
	}
	a, _ := series.FromInt64("a", vals, valid, series.WithAllocator(mem))
	s, _ := series.FromString("s", strs, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, s)
	defer df.Release()

	path := filepath.Join(t.TempDir(), "rg.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithAllocator(mem), parquet.WithChunkSize(999)); err != nil {
		t.Fatal(err)
	}
	for _, cols := range [][]string{nil, {"s", "a"}} {
		var opts []parquet.Option
		opts = append(opts, parquet.WithAllocator(mem))
		if cols != nil {
			opts = append(opts, parquet.WithColumns(cols...))
		}
		got, err := parquet.ReadFile(ctx, path, opts...)
		if err != nil {
			t.Fatal(err)
		}
		col, _ := got.Column("a")
		if col.Len() != n || col.NullCount() != (n+6)/7 {
			t.Fatalf("len=%d nulls=%d", col.Len(), col.NullCount())
		}
		row := 0
		for _, ch := range col.Chunks() {
			arr := ch.(*array.Int64)
			for j := range arr.Len() {
				if arr.IsValid(j) != valid[row] || (valid[row] && arr.Value(j) != vals[row]) {
					t.Fatalf("row %d mismatch", row)
				}
				row++
			}
		}
		sc, _ := got.Column("s")
		row = 0
		for _, ch := range sc.Chunks() {
			arr := ch.(*array.String)
			for j := range arr.Len() {
				if arr.Value(j) != strs[row] {
					t.Fatalf("string row %d mismatch", row)
				}
				row++
			}
		}
		got.Release()
	}
}
