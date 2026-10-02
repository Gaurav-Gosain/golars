package dataframe_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

func expected(t *testing.T, key string) string {
	t.Helper()
	w, ok := frameExpected[key]
	if !ok {
		t.Fatalf("no expected output for %q", key)
	}
	return w
}

// aggFixture mirrors the polars frame used for the aggregate parity cases.
func aggFixture(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	return fxFrame(t,
		fxCol(t, mem, "i8", fxI8, 1, nil, 3, 7),
		fxCol(t, mem, "i32", fxI32, 1, 2, 3, 4),
		fxCol(t, mem, "i64", fxI64, 1, nil, 3, 3),
		fxCol(t, mem, "u16", fxU16, 1, 2, 3, 9),
		fxCol(t, mem, "u32", fxU32, 1, 2, nil, 3),
		fxCol(t, mem, "u64", fxU64, 1, 2, 3, 5),
		fxCol(t, mem, "f32", fxF32, 1.5, nil, 2.25, -1.0),
		fxCol(t, mem, "f64", fxF64, 1.5, math.NaN(), nil, 2.0),
		fxCol(t, mem, "s", fxStr, "b", nil, "a", "a"),
		fxCol(t, mem, "b", fxBool, true, false, nil, true),
		fxCol(t, mem, "d", fxDate, fxDay(2020, 1, 1), nil, fxDay(2020, 1, 4), fxDay(2020, 1, 2)),
		fxCol(t, mem, "dt", fxDT, fxDay(2020, 1, 1), fxDay(2020, 1, 2).Add(12*time.Hour), nil, fxDay(2020, 1, 3)),
		fxCol(t, mem, "du", fxDur, time.Hour, nil, 3*time.Hour, 30*time.Minute),
		fxCol(t, mem, "n", fxNull, nil, nil, nil, nil),
		fxCol(t, mem, "l", listType, []any{1}, []any{2, 3}, nil, []any{}),
	)
}

func TestParityFrameAggregates(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := aggFixture(t, mem)
	defer df.Release()

	cases := []struct {
		key string
		fn  func(*dataframe.DataFrame) (*dataframe.DataFrame, error)
	}{
		{"sum", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.SumAll(ctx) }},
		{"mean", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MeanAll(ctx) }},
		{"median", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MedianAll(ctx) }},
		{"min", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MinAll(ctx) }},
		{"max", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MaxAll(ctx) }},
		{"std", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.StdAll(ctx) }},
		{"var", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.VarAll(ctx) }},
		{"product", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.ProductAll(ctx) }},
		{"null_count", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.NullCountAll(ctx) }},
		{"count", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.CountAll(ctx) }},
		{"std ddof0", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.StdAll(ctx, 0) }},
		{"var ddof3", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.VarAll(ctx, 3) }},
		{"quantile 0.5 nearest", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.5, dataframe.QuantileNearest)
		}},
		{"quantile 0.25 linear", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.25, dataframe.QuantileLinear)
		}},
		{"quantile 0.75 higher", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.75, dataframe.QuantileHigher)
		}},
		{"quantile 0.3 lower", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.3, dataframe.QuantileLower)
		}},
		{"quantile 0.4 midpoint", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.4, dataframe.QuantileMidpoint)
		}},
		{"quantile 0.6 equiprobable", func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return d.QuantileAll(ctx, 0.6, dataframe.QuantileEquiprobable)
		}},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			out, err := c.fn(df)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, c.key))
		})
	}

	empty := df.Clear()
	defer empty.Release()
	emptyCases := map[string]func(*dataframe.DataFrame) (*dataframe.DataFrame, error){
		"empty sum":     func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.SumAll(ctx) },
		"empty mean":    func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MeanAll(ctx) },
		"empty min":     func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.MinAll(ctx) },
		"empty std":     func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.StdAll(ctx) },
		"empty product": func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.ProductAll(ctx) },
		"empty count":   func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.CountAll(ctx) },
	}
	for key, fn := range emptyCases {
		t.Run(key, func(t *testing.T) {
			out, err := fn(empty)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, key))
		})
	}

	t.Run("approx_n_unique", func(t *testing.T) {
		noList := df.Drop("l")
		defer noList.Release()
		out, err := noList.ApproxNUnique(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		framerepr.AssertFrame(t, out, expected(t, "approx_n_unique"))
	})

	t.Run("n_unique", func(t *testing.T) {
		noList := df.Drop("l")
		defer noList.Release()
		if n, err := noList.NUnique(ctx); err != nil || n != 4 {
			t.Fatalf("n_unique = %d, %v; want 4", n, err)
		}
		if n, err := df.NUnique(ctx, "i64", "s"); err != nil || n != 3 {
			t.Fatalf("n_unique(i64, s) = %d, %v; want 3", n, err)
		}
	})
}

// miscFixture mirrors polars' {"a": [1, 2, 1, None, 1], "b": [...], "f": [...]}.
func miscFixture(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	return fxFrame(t,
		fxCol(t, mem, "a", fxI64, 1, 2, 1, nil, 1),
		fxCol(t, mem, "b", fxStr, "x", "y", "x", nil, "z"),
		fxCol(t, mem, "f", fxF64, 1.0, math.NaN(), 1.0, nil, 3.0),
	)
}

func TestParityFrameRowOps(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := miscFixture(t, mem)
	defer df.Release()
	sel := func(names ...string) *dataframe.DataFrame {
		out, err := df.Select(names...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	frameCases := []struct {
		key string
		fn  func() (*dataframe.DataFrame, error)
	}{
		{"shift 2", func() (*dataframe.DataFrame, error) { return df.Shift(ctx, 2, nil) }},
		{"shift -1 fill 0", func() (*dataframe.DataFrame, error) {
			s := sel("a", "f")
			defer s.Release()
			return s.Shift(ctx, -1, 0)
		}},
		{"shift 1 fill 2.5", func() (*dataframe.DataFrame, error) {
			s := sel("a")
			defer s.Release()
			return s.Shift(ctx, 1, 2.5)
		}},
		{"shift 1 fill q", func() (*dataframe.DataFrame, error) {
			s := sel("b")
			defer s.Release()
			return s.Shift(ctx, 1, "q")
		}},
		{"shift 9 fill 9", func() (*dataframe.DataFrame, error) {
			s := sel("a")
			defer s.Release()
			return s.Shift(ctx, 9, 9)
		}},
		{"gather_every 2", func() (*dataframe.DataFrame, error) { return df.GatherEvery(ctx, 2, 0) }},
		{"gather_every 2 1", func() (*dataframe.DataFrame, error) { return df.GatherEvery(ctx, 2, 1) }},
		{"fill_nan 0.5", func() (*dataframe.DataFrame, error) { return df.FillNan(0.5) }},
		{"fill_nan null", func() (*dataframe.DataFrame, error) { return df.FillNan(nil) }},
		{"drop_nans", func() (*dataframe.DataFrame, error) { return df.DropNans(ctx) }},
		{"drop_nans a", func() (*dataframe.DataFrame, error) { return df.DropNans(ctx, "a") }},
		{"fill_null forward", func() (*dataframe.DataFrame, error) {
			return df.FillNullWithStrategy(ctx, dataframe.FillForward, 0)
		}},
		{"fill_null backward", func() (*dataframe.DataFrame, error) {
			return df.FillNullWithStrategy(ctx, dataframe.FillBackward, 0)
		}},
		{"fill_null min", func() (*dataframe.DataFrame, error) { return df.FillNullWithStrategy(ctx, dataframe.FillMin, 0) }},
		{"fill_null max", func() (*dataframe.DataFrame, error) { return df.FillNullWithStrategy(ctx, dataframe.FillMax, 0) }},
		{"fill_null zero", func() (*dataframe.DataFrame, error) { return df.FillNullWithStrategy(ctx, dataframe.FillZero, 0) }},
		{"with_row_index", func() (*dataframe.DataFrame, error) { return df.WithRowIndex("index", 0) }},
		{"with_row_index rn 10", func() (*dataframe.DataFrame, error) { return df.WithRowIndex("rn", 10) }},
		{"to_dummies", func() (*dataframe.DataFrame, error) {
			s := sel("a", "b")
			defer s.Release()
			return s.ToDummies(ctx, dataframe.ToDummiesOptions{})
		}},
		{"to_dummies drop_first", func() (*dataframe.DataFrame, error) {
			s := sel("a", "b")
			defer s.Release()
			return s.ToDummies(ctx, dataframe.ToDummiesOptions{DropFirst: true})
		}},
		{"to_dummies b sep", func() (*dataframe.DataFrame, error) {
			return df.ToDummies(ctx, dataframe.ToDummiesOptions{Columns: []string{"b"}, Separator: ":"})
		}},
		{"to_dummies drop_nulls", func() (*dataframe.DataFrame, error) {
			s := sel("a", "b")
			defer s.Release()
			return s.ToDummies(ctx, dataframe.ToDummiesOptions{DropNulls: true})
		}},
		{"insert_column -1", func() (*dataframe.DataFrame, error) {
			return df.InsertColumn(-1, fxCol(t, mem, "z", fxI64, 9, 8, 7, 6, 5))
		}},
		{"insert_column 3", func() (*dataframe.DataFrame, error) {
			return df.InsertColumn(3, fxCol(t, mem, "z", fxI64, 9, 8, 7, 6, 5))
		}},
		{"replace_column 1", func() (*dataframe.DataFrame, error) {
			return df.ReplaceColumn(1, fxCol(t, mem, "q", fxI64, 0, 0, 0, 0, 0))
		}},
		{"map_rows tuple", func() (*dataframe.DataFrame, error) {
			s := sel("a")
			defer s.Release()
			return s.MapRows(ctx, func(row []any) (any, error) { return []any{row[0], "q"}, nil })
		}},
		{"map_rows scalar", func() (*dataframe.DataFrame, error) {
			s := sel("a")
			defer s.Release()
			return s.MapRows(ctx, func(row []any) (any, error) {
				if row[0] == nil {
					return 2, nil
				}
				return row[0].(int64) * 2, nil
			})
		}},
	}
	for _, c := range frameCases {
		t.Run(c.key, func(t *testing.T) {
			out, err := c.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, c.key))
		})
	}

	seriesCases := []struct {
		key string
		fn  func() (*series.Series, error)
	}{
		{"is_duplicated", func() (*series.Series, error) { return df.IsDuplicated(ctx) }},
		{"is_unique", func() (*series.Series, error) { return df.IsUnique(ctx) }},
		{"to_struct", func() (*series.Series, error) { return df.ToStruct("s") }},
		{"fold add", func() (*series.Series, error) {
			s := sel("a", "f")
			defer s.Release()
			return s.Fold(func(acc, next *series.Series) (*series.Series, error) {
				return addSeries(t, acc, next)
			})
		}},
	}
	for _, c := range seriesCases {
		t.Run(c.key, func(t *testing.T) {
			out, err := c.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertSeries(t, out, expected(t, c.key))
		})
	}

	if n, err := df.NUnique(ctx); err != nil || n != 4 {
		t.Fatalf("n_unique = %d, %v; want 4", n, err)
	}
}

// addSeries adds two numeric columns as f64 for the fold test; nulls
// propagate like polars' + operator.
func addSeries(t *testing.T, a, b *series.Series) (*series.Series, error) {
	t.Helper()
	fa := floatsOf(a)
	fb := floatsOf(b)
	vals := make([]float64, len(fa))
	valid := make([]bool, len(fa))
	for i := range fa {
		if fa[i] != nil && fb[i] != nil {
			vals[i] = *fa[i] + *fb[i]
			valid[i] = true
		}
	}
	return series.FromFloat64(a.Name(), vals, valid)
}

func floatsOf(s *series.Series) []*float64 {
	out := make([]*float64, s.Len())
	df, _ := dataframe.New(s.Clone())
	defer df.Release()
	i := 0
	for row := range df.IterRows() {
		switch v := row[0].(type) {
		case int64:
			f := float64(v)
			out[i] = &f
		case float64:
			f := v
			out[i] = &f
		}
		i++
	}
	return out
}

func TestParityFrameFillStrategies(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := fxFrame(t,
		fxCol(t, mem, "a", fxI64, 1, 2, nil, 4, nil, nil, 7),
		fxCol(t, mem, "f", fxF32, 1.0, nil, 2.5, 4.0, nil, nil, nil),
		fxCol(t, mem, "u", fxU8, 1, nil, 2, 3, nil, 1, 1),
	)
	defer df.Release()
	cases := []struct {
		key      string
		strategy dataframe.FillNullStrategy
		limit    int
	}{
		{"fill_null mean", dataframe.FillMean, 0},
		{"fill_null one", dataframe.FillOne, 0},
		{"fill_null forward 1", dataframe.FillForward, 1},
		{"fill_null backward 1", dataframe.FillBackward, 1},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			out, err := df.FillNullWithStrategy(ctx, c.strategy, c.limit)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, c.key))
		})
	}

	strs := fxFrame(t, fxCol(t, mem, "s", fxStr, "a", nil))
	defer strs.Release()
	if _, err := strs.FillNullWithStrategy(ctx, dataframe.FillMean, 0); err == nil {
		t.Fatal("mean fill on str should fail like polars")
	}
	if _, err := strs.FillNullWithStrategy(ctx, dataframe.FillOne, 0); err == nil {
		t.Fatal("one fill on str should fail like polars")
	}
}

func TestParityFrameToDummiesMixed(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := fxFrame(t,
		fxCol(t, mem, "a", fxI64, 10, 9, nil, -1),
		fxCol(t, mem, "f", fxF64, 1.5, nil, -2.0, 1.5),
		fxCol(t, mem, "b", fxBool, true, nil, false, true),
	)
	defer df.Release()
	out, err := df.ToDummies(context.Background(), dataframe.ToDummiesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	framerepr.AssertFrame(t, out, expected(t, "to_dummies mixed"))
}

func TestParityFrameUnstack(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := fxFrame(t,
		fxCol(t, mem, "x", fxStr, "A", "B", "C", "D", "E", "F", "G", "H", "I"),
		fxCol(t, mem, "y", fxI64, 0, 1, 2, 3, 4, 5, 6, 7, 8),
	)
	defer df.Release()
	cases := []struct {
		key  string
		opts dataframe.UnstackOptions
	}{
		{"unstack v3", dataframe.UnstackOptions{Step: 3}},
		{"unstack h3", dataframe.UnstackOptions{Step: 3, How: dataframe.UnstackHorizontal}},
		{"unstack v4", dataframe.UnstackOptions{Step: 4, How: dataframe.UnstackVertical}},
		{"unstack h4 fill -1", dataframe.UnstackOptions{Step: 4, How: dataframe.UnstackHorizontal, FillValues: []any{-1}}},
		{"unstack y 2", dataframe.UnstackOptions{Step: 2, Columns: []string{"y"}}},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			out, err := df.Unstack(ctx, c.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, c.key))
		})
	}
}

func TestParityFrameUpdate(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	a := fxFrame(t, fxCol(t, mem, "A", fxI64, 1, 2, 3, 4), fxCol(t, mem, "B", fxI64, 400, 500, 600, 700))
	defer a.Release()
	b := fxFrame(t, fxCol(t, mem, "B", fxI64, -66, nil, -99), fxCol(t, mem, "C", fxI64, 5, 3, 1))
	defer b.Release()
	long := fxFrame(t, fxCol(t, mem, "B", fxI64, 1, 2, 3, 4, 5, 6))
	defer long.Release()
	c := fxFrame(t, fxCol(t, mem, "A", fxI64, 3, 1, 9, 1), fxCol(t, mem, "B", fxF64, 1.5, nil, 3.0, 7.0))
	defer c.Release()
	on := func(how dataframe.UpdateHow, nulls bool) dataframe.UpdateOptions {
		return dataframe.UpdateOptions{LeftOn: []string{"A"}, RightOn: []string{"C"}, How: how, IncludeNulls: nulls}
	}
	cases := []struct {
		key   string
		other *dataframe.DataFrame
		opts  dataframe.UpdateOptions
	}{
		{"update rows", b, dataframe.UpdateOptions{}},
		{"update rows nulls", b, dataframe.UpdateOptions{IncludeNulls: true}},
		{"update rows full", long, dataframe.UpdateOptions{}},
		{"update on", b, on(dataframe.UpdateLeft, false)},
		{"update on inner", b, on(dataframe.UpdateInner, false)},
		{"update on full", b, on(dataframe.UpdateFull, false)},
		{"update on full nulls", b, on(dataframe.UpdateFull, true)},
		{"update dup", c, dataframe.UpdateOptions{On: []string{"A"}}},
		{"update dup full", c, dataframe.UpdateOptions{On: []string{"A"}, How: dataframe.UpdateFull}},
	}
	for _, cs := range cases {
		t.Run(cs.key, func(t *testing.T) {
			out, err := a.Update(ctx, cs.other, cs.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, expected(t, cs.key))
		})
	}
}

func TestParityFrameMergeSorted(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	m1 := fxFrame(t, fxCol(t, mem, "k", fxI64, nil, 1, 3, 5), fxCol(t, mem, "v", fxStr, "n1", "a", "b", "c"))
	defer m1.Release()
	m2 := fxFrame(t, fxCol(t, mem, "k", fxI64, nil, 2, 3, 4), fxCol(t, mem, "v", fxStr, "n2", "x", "y", "z"))
	defer m2.Release()
	out, err := m1.MergeSorted(ctx, m2, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	framerepr.AssertFrame(t, out, expected(t, "merge_sorted"))

	s1 := fxFrame(t, fxCol(t, mem, "k", fxStr, "a", "c"))
	defer s1.Release()
	s2 := fxFrame(t, fxCol(t, mem, "k", fxStr, "b", "c", "d"))
	defer s2.Release()
	out2, err := s1.MergeSorted(ctx, s2, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	framerepr.AssertFrame(t, out2, expected(t, "merge_sorted str"))
}

func TestParityFrameCastAndSchema(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	src := fxFrame(t,
		fxCol(t, mem, "a", fxI32, 1, 2),
		fxCol(t, mem, "b", fxStr, "x", "y"),
		fxCol(t, mem, "e", fxI64, 1, 2),
	)
	defer src.Release()
	target, _ := schema.New(
		schema.Field{Name: "b", DType: dtype.String()},
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "z", DType: dtype.Float64()},
	)
	out, err := src.MatchToSchema(ctx, target, dataframe.MatchToSchemaOptions{
		InsertMissing: true, IgnoreExtra: true, UpcastIntegers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	framerepr.AssertFrame(t, out, expected(t, "match_to_schema"))
	if _, err := src.MatchToSchema(ctx, target, dataframe.MatchToSchemaOptions{}); err == nil {
		t.Fatal("default options must reject extra columns and dtype changes")
	}

	c := fxFrame(t,
		fxCol(t, mem, "a", fxI64, 1, 2, 3),
		fxCol(t, mem, "b", fxStr, "x", "y", "z"),
		fxCol(t, mem, "c", fxF64, 1.5, 2.5, nil),
	)
	defer c.Release()
	m, err := c.CastColumns(ctx, map[string]dtype.DType{"a": dtype.Float64(), "c": dtype.Int64()}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	framerepr.AssertFrame(t, m, expected(t, "cast map"))

	d, err := c.CastDTypes(ctx, []dataframe.DTypeCast{
		{From: dtype.Int64(), To: dtype.String()},
		{From: dtype.Float64(), To: dtype.Float32()},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Release()
	framerepr.AssertFrame(t, d, expected(t, "cast dtypes"))

	ints := fxFrame(t, fxCol(t, mem, "a", fxI64, 1, 2), fxCol(t, mem, "b", fxI64, 3, 4))
	defer ints.Release()
	all, err := ints.Cast(ctx, dtype.Float32(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer all.Release()
	framerepr.AssertFrame(t, all, expected(t, "cast all"))

	strs := fxFrame(t, fxCol(t, mem, "a", fxStr, "1", "x"))
	defer strs.Release()
	lax, err := strs.Cast(ctx, dtype.Int64(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer lax.Release()
	framerepr.AssertFrame(t, lax, expected(t, "cast nonstrict"))
	if _, err := strs.Cast(ctx, dtype.Int64(), true); err == nil {
		t.Fatal("strict cast of an unparseable string must fail")
	}
}

func TestParityFrameAccessors(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := miscFixture(t, mem)
	defer df.Release()

	if i, err := df.GetColumnIndex("b"); err != nil || i != 1 {
		t.Fatalf("GetColumnIndex = %d, %v", i, err)
	}
	if v, err := df.ItemAt(1, "b"); err != nil || v != "y" {
		t.Fatalf("ItemAt = %v, %v", v, err)
	}
	head := df.Head(1)
	defer head.Release()
	one, _ := head.Select("a")
	defer one.Release()
	if v, err := one.Item(); err != nil || v != int64(1) {
		t.Fatalf("Item = %v, %v", v, err)
	}
	if _, err := df.Item(); err == nil {
		t.Fatal("Item on a non 1x1 frame must fail")
	}
	s, err := df.ToSeries(-1)
	if err != nil || s.Name() != "f" {
		t.Fatalf("ToSeries(-1) = %v, %v", s, err)
	}
	s.Release()

	dicts := df.ToDicts()
	if len(dicts) != 5 || dicts[1]["b"] != "y" || dicts[3]["a"] != nil {
		t.Fatalf("ToDicts = %v", dicts)
	}
	var names []string
	for c := range df.IterColumns() {
		names = append(names, c.Name())
	}
	testutil.AssertStrings(t, names, []string{"a", "b", "f"})
	var heights []int
	for sl := range df.IterSlices(2) {
		heights = append(heights, sl.Height())
	}
	testutil.AssertEqualAny(t, heights, []int{2, 2, 1})
	rows := 0
	for row := range df.IterRows() {
		if len(row) != 3 {
			t.Fatalf("row width %d", len(row))
		}
		rows++
	}
	if rows != 5 {
		t.Fatalf("IterRows yielded %d rows", rows)
	}
	if df.NChunks() != 1 {
		t.Fatalf("NChunks = %d", df.NChunks())
	}

	byKey, err := df.RowsByKey([]string{"a"}, dataframe.RowsByKeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(byKey[int64(1)]) != 3 || len(byKey[nil]) != 1 || len(byKey[int64(2)]) != 1 {
		t.Fatalf("RowsByKey = %v", byKey)
	}
	if first := byKey[int64(1)][2].([]any); first[0] != "z" {
		t.Fatalf("RowsByKey row order = %v", byKey[int64(1)])
	}
	named, err := df.RowsByKey([]string{"a", "b"}, dataframe.RowsByKeyOptions{Named: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := named[[2]any{int64(1), "x"}]; len(got) != 2 || got[0].(map[string]any)["f"] != 1.0 {
		t.Fatalf("RowsByKey named = %v", named)
	}
	uniq, err := df.RowsByKey([]string{"a"}, dataframe.RowsByKeyOptions{Unique: true, IncludeKey: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := uniq[int64(1)]; len(got) != 1 || got[0].([]any)[1] != "z" {
		t.Fatalf("RowsByKey unique keeps the last row: %v", uniq[int64(1)])
	}

	h1, err := df.HashRows(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Release()
	if h1.DType().String() != "u64" || h1.Len() != 5 {
		t.Fatalf("HashRows dtype %s len %d", h1.DType(), h1.Len())
	}
	hv, _ := dataframe.New(h1.Clone())
	defer hv.Release()
	var hashes []uint64
	for row := range hv.IterRows() {
		hashes = append(hashes, row[0].(uint64))
	}
	if hashes[0] != hashes[2] || hashes[0] == hashes[1] {
		t.Fatalf("equal rows must hash equal and different rows differ: %v", hashes)
	}
}
