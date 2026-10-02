package dataframe_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

var joinKeyTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean, arrow.FixedWidthTypes.Date32,
}

type joinPair struct{ l, r int64 } // r == -1 marks an unmatched left row

func sortPairs(p []joinPair) {
	slices.SortFunc(p, func(a, b joinPair) int {
		if c := cmp.Compare(a.l, b.l); c != 0 {
			return c
		}
		return cmp.Compare(a.r, b.r)
	})
}

// keysMatch is polars' default join equality: null never matches,
// NaN matches NaN and -0.0 matches 0.0.
func keysMatch(a, b []any, i, j int) bool {
	if a[i] == nil || b[j] == nil {
		return false
	}
	return testutil.CompareValues(a[i], b[j]) == 0
}

func joinFrame(t *testing.T, mem memory.Allocator, r *rand.Rand, keyName, idName string, dt arrow.DataType, keys []any) (*dataframe.DataFrame, []any) {
	t.Helper()
	ids := make([]any, len(keys))
	for i := range ids {
		ids[i] = int64(i)
	}
	k := propSeries(t, mem, r, keyName, dt, keys)
	id := propSeries(t, mem, r, idName, arrow.PrimitiveTypes.Int64, ids)
	df, err := dataframe.New(k, id)
	if err != nil {
		t.Fatal(err)
	}
	return df, k.ToList()
}

func TestJoinProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(31, 32))
	sizes := []int{0, 1, 2, 3, 15, 16, 17, 64, 65, 300}
	for iter := range 400 {
		dt := joinKeyTypes[r.IntN(len(joinKeyTypes))]
		nl, nr := sizes[r.IntN(len(sizes))], sizes[r.IntN(len(sizes))]
		// A shared narrow domain makes matches likely.
		lk := testutil.RandValues(r, dt, nl, 0.15)
		rk := testutil.RandValues(r, dt, nr, 0.15)
		left, lkeys := joinFrame(t, mem, r, "k", "lid", dt, lk)
		right, rkeys := joinFrame(t, mem, r, "k", "rid", dt, rk)

		for _, how := range []dataframe.JoinType{dataframe.InnerJoin, dataframe.LeftJoin, dataframe.CrossJoin} {
			desc := fmt.Sprintf("iter %d %s join key=%s nl=%d nr=%d", iter, how, dt, nl, nr)
			var want []joinPair
			for i := range nl {
				matched := false
				for j := range nr {
					if how == dataframe.CrossJoin || keysMatch(lkeys, rkeys, i, j) {
						want = append(want, joinPair{int64(i), int64(j)})
						matched = true
					}
				}
				if how == dataframe.LeftJoin && !matched {
					want = append(want, joinPair{int64(i), -1})
				}
			}
			on := []string{"k"}
			if how == dataframe.CrossJoin {
				on = nil
			}
			out, err := left.Join(ctx, right, on, how, dataframe.WithJoinAllocator(mem))
			if err != nil {
				t.Fatalf("%s: %v", desc, err)
			}
			if out.Height() != len(want) {
				t.Fatalf("%s: %d rows, want %d", desc, out.Height(), len(want))
			}
			lidCol, err := out.Column("lid")
			if err != nil {
				t.Fatalf("%s: %v", desc, err)
			}
			ridCol, err := out.Column("rid")
			if err != nil {
				t.Fatalf("%s: %v", desc, err)
			}
			lids, rids := lidCol.ToList(), ridCol.ToList()
			got := make([]joinPair, len(lids))
			for i := range lids {
				got[i] = joinPair{lids[i].(int64), -1}
				if rids[i] != nil {
					got[i].r = rids[i].(int64)
				}
			}
			if how == dataframe.LeftJoin {
				// polars keeps the left row order for left joins.
				for i := 1; i < len(got); i++ {
					if got[i].l < got[i-1].l {
						t.Fatalf("%s: left order broken at row %d", desc, i)
					}
				}
			}
			sortPairs(got)
			sortPairs(want)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: row pairs differ\n got %v\nwant %v", desc, got, want)
			}
			if how != dataframe.CrossJoin {
				// The key column carries the left key for every row.
				kc, err := out.Column("k")
				if err != nil {
					t.Fatalf("%s: %v", desc, err)
				}
				kv := kc.ToList()
				for i := range kv {
					if !testutil.ValuesEqual(kv[i], lkeys[lids[i].(int64)]) {
						t.Fatalf("%s: row %d key %v want %v", desc, i, kv[i], lkeys[lids[i].(int64)])
					}
				}
			}
			out.Release()
		}
		left.Release()
		right.Release()
	}
}

// TestJoinRegressions pins two bugs the property test found. A left
// join against an empty right frame panicked with index out of range
// in gatherNullable, and temporal keys were rejected with "unsupported
// key dtype". polars 1.39:
//
//	l = pl.DataFrame({"k": [1, 2], "lid": [0, 1]})
//	l.join(l.clear().rename({"lid": "rid"}), on="k", how="left")  -> rid [None, None]
//	d = pl.DataFrame({"k": [date(2020,1,1), None]}).with_row_index("lid")
//	d.join(d.rename({"lid": "rid"}), on="k")  -> 1 row
func TestJoinRegressions(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 1))

	left, _ := joinFrame(t, mem, r, "k", "lid", arrow.PrimitiveTypes.Int64, []any{int64(1), int64(2)})
	right, _ := joinFrame(t, mem, r, "k", "rid", arrow.PrimitiveTypes.Int64, nil)
	out, err := left.Join(ctx, right, []string{"k"}, dataframe.LeftJoin, dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	rid, _ := out.Column("rid")
	if out.Height() != 2 || rid.NullCount() != 2 {
		t.Errorf("left join with empty right: height %d rid nulls %d, want 2 and 2", out.Height(), rid.NullCount())
	}
	out.Release()
	left.Release()
	right.Release()

	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Duration_ns} {
		left, _ := joinFrame(t, mem, r, "k", "lid", dt, []any{int64(18262), nil})
		right, _ := joinFrame(t, mem, r, "k", "rid", dt, []any{int64(18262), nil})
		out, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin, dataframe.WithJoinAllocator(mem))
		if err != nil {
			t.Fatalf("%s key: %v", dt, err)
		}
		if out.Height() != 1 {
			t.Errorf("%s key: %d rows, want 1", dt, out.Height())
		}
		out.Release()
		left.Release()
		right.Release()
	}
}

// TestJoinMultiKeyProp checks a multi-key self join: polars'
// df.join(df, on=["a", "b"]) returns 3 rows.
func TestJoinMultiKeyProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	a, _ := series.FromInt64("a", []int64{1, 1, 2}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"x", "y", "x"}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := df.Join(context.Background(), df, []string{"a", "b"}, dataframe.InnerJoin, dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Fatalf("multi-key self join: %d rows, want 3", out.Height())
	}
}
