package lazy_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestStreamingMatchesEagerWithNulls runs the same
// filter/project/with-columns pipeline both ways over randomized data
// with nulls and asserts identical output. Morsel boundaries (small
// morsel size) stress partitioning, null handling and concat order.
func TestStreamingMatchesEagerWithNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	r := rand.New(rand.NewPCG(11, 12))
	const n = 5000
	a := make([]int64, n)
	b := make([]float64, n)
	va := make([]bool, n)
	vb := make([]bool, n)
	for i := range a {
		a[i] = r.Int64N(100)
		b[i] = r.Float64() * 100
		va[i] = r.IntN(8) != 0
		vb[i] = r.IntN(8) != 0
	}
	sa, _ := series.FromInt64("a", a, va, series.WithAllocator(mem))
	sb, _ := series.FromFloat64("b", b, vb, series.WithAllocator(mem))
	df, _ := dataframe.New(sa, sb)
	defer df.Release()

	build := func() lazy.LazyFrame {
		return lazy.FromDataFrame(df).
			Filter(expr.Col("a").GtLit(int64(10))).
			WithColumns(expr.Col("a").Add(expr.Col("a")).Alias("aa"))
	}

	eager, err := build().Select(expr.Col("a"), expr.Col("aa")).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer eager.Release()

	for _, morsels := range []int{64, 1024, 1 << 20} {
		streamed, err := build().Select(expr.Col("a"), expr.Col("aa")).Collect(ctx,
			lazy.WithExecAllocator(mem), lazy.WithStreaming(), lazy.WithStreamingMorselRows(morsels))
		if err != nil {
			t.Fatalf("morsels=%d: %v", morsels, err)
		}
		if streamed.Height() != eager.Height() || streamed.Width() != eager.Width() {
			streamed.Release()
			t.Fatalf("morsels=%d: shape %dx%d want %dx%d", morsels,
				streamed.Height(), streamed.Width(), eager.Height(), eager.Width())
		}
		for _, name := range []string{"a", "aa"} {
			ec, _ := eager.Column(name)
			sc, _ := streamed.Column(name)
			ea := ec.Chunk(0).(*array.Int64)
			sa := sc.Chunk(0).(*array.Int64)
			if ea.Len() != sa.Len() {
				streamed.Release()
				t.Fatalf("morsels=%d col %s len mismatch", morsels, name)
			}
			for i := range ea.Len() {
				if ea.IsValid(i) != sa.IsValid(i) || (ea.IsValid(i) && ea.Value(i) != sa.Value(i)) {
					streamed.Release()
					t.Fatalf("morsels=%d col %s row %d mismatch", morsels, name, i)
				}
			}
		}
		streamed.Release()
	}
}
