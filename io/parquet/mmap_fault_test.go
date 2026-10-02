package parquet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestTruncatedMappingIsError maps a parquet file, truncates it and
// reads the stale mapping: the reader must return an error instead of
// crashing on the memory fault.
func TestTruncatedMappingIsError(t *testing.T) {
	n := 200_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i) * 7919
	}
	s, err := series.FromInt64("a", vals, nil)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	ctx := context.Background()
	for _, keep := range []int64{0, 64 << 10} {
		path := filepath.Join(t.TempDir(), "in.parquet")
		if err := WriteFile(ctx, path, df); err != nil {
			t.Fatal(err)
		}
		m, err := mmapfile.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if !m.Mapped() {
			m.Close()
			t.Skip("file not mapped on this platform")
		}
		if err := os.Truncate(path, keep); err != nil {
			t.Fatal(err)
		}
		got, err := readGuarded(ctx, m.Data, nil)
		m.Close()
		if err == nil {
			got.Release()
			t.Fatalf("keep=%d: read of a truncated mapping succeeded", keep)
		}
		if !errors.Is(err, mmapfile.ErrFault) {
			t.Fatalf("keep=%d: err = %v, want a fault error", keep, err)
		}
	}
}
