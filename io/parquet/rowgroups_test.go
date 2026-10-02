package parquet_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

// Row-group reads decode flat columns with the native reader (pqarrow
// is not used) and return the file's rows in order.
func TestRowGroupReaderNative(t *testing.T) {
	ctx := context.Background()
	const n = 5000
	k := make([]int64, n)
	s := make([]string, n)
	valid := make([]bool, n)
	for i := range n {
		k[i] = int64(i)
		s[i] = []string{"AIR", "MAIL", "SHIP"}[i%3]
		valid[i] = i%11 != 0
	}
	ks, _ := series.FromInt64("k", k, nil)
	ss, _ := series.FromString("s", s, valid)
	df, _ := dataframe.New(ks, ss)
	defer df.Release()
	path := filepath.Join(t.TempDir(), "rg.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithChunkSize(700)); err != nil {
		t.Fatal(err)
	}

	before := parquet.PqarrowReads()
	r, err := parquet.OpenRowGroups(path, []string{"s", "k"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	row := 0
	for i := range r.NumRowGroups() {
		part, err := r.Read(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if names := part.ColumnNames(); names[0] != "s" || names[1] != "k" {
			t.Fatalf("columns = %v", names)
		}
		scol, _ := part.Column("s")
		kcol, _ := part.Column("k")
		for j := range part.Height() {
			sv, _ := scol.Get(j)
			kv, _ := kcol.Get(j)
			var want any = s[row]
			if !valid[row] {
				want = nil
			}
			if sv != want || kv != k[row] {
				t.Fatalf("row %d: got (%v, %v), want (%v, %v)", row, sv, kv, want, k[row])
			}
			row++
		}
		part.Release()
	}
	if row != n {
		t.Fatalf("read %d rows, want %d", row, n)
	}
	if got := parquet.PqarrowReads() - before; got != 0 {
		t.Fatalf("pqarrow reads = %d, want 0", got)
	}
}
