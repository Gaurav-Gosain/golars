package parquet_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/parquet"
)

// TestScanProjectionPushdown: a scan whose consumers read a subset of
// columns decodes only that subset and still answers correctly.
func TestScanProjectionPushdown(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := sampleDF(t, mem)
	defer df.Release()
	path := filepath.Join(t.TempDir(), "s.parquet")
	if err := parquet.WriteFile(ctx, path, df); err != nil {
		t.Fatal(err)
	}

	q := parquet.Scan(path).
		Filter(expr.Col("a").GtLit(int64(2))).
		Select(expr.Col("b").Sum().Alias("s"))
	plan, err := q.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "projection=[a b]") {
		t.Errorf("scan projection not pushed down:\n%s", plan)
	}
	out, err := q.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := out.ColumnAt(0).Chunks()[0].(*array.Float64).Value(0); got != 3.5+4.5+5.5 {
		t.Errorf("sum = %v, want 13.5", out)
	}

	// Without a narrowing consumer every column is read.
	all, err := parquet.Scan(path).Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer all.Release()
	if all.Width() != 3 || all.Height() != 5 {
		t.Errorf("full scan shape = (%d,%d)", all.Height(), all.Width())
	}
}
