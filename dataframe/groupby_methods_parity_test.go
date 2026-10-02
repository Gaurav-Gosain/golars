// Parity tests for the GroupBy convenience methods against polars
// 1.39.3. Expected outputs live in groupby_methods_expected_test.go and
// were produced by running the same frames through polars and rendering
// them with the framerepr format.

package dataframe_test

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/series"
)

// gbCol builds a Series of dtype dt from vals. nil entries are nulls.
// Integers are given as int, floats as float64, dates as "YYYY-MM-DD",
// datetimes as "YYYY-MM-DDTHH:MM:SS".
func gbCol(t *testing.T, mem memory.Allocator, name string, dt arrow.DataType, vals ...any) *series.Series {
	t.Helper()
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for _, v := range vals {
		if v == nil {
			b.AppendNull()
			continue
		}
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(int8(v.(int)))
		case *array.Int16Builder:
			bb.Append(int16(v.(int)))
		case *array.Int32Builder:
			bb.Append(int32(v.(int)))
		case *array.Int64Builder:
			bb.Append(int64(v.(int)))
		case *array.Uint8Builder:
			bb.Append(uint8(v.(int)))
		case *array.Uint16Builder:
			bb.Append(uint16(v.(int)))
		case *array.Uint32Builder:
			bb.Append(uint32(v.(int)))
		case *array.Uint64Builder:
			bb.Append(uint64(v.(int)))
		case *array.Float32Builder:
			bb.Append(float32(v.(float64)))
		case *array.Float64Builder:
			bb.Append(v.(float64))
		case *array.BooleanBuilder:
			bb.Append(v.(bool))
		case *array.StringBuilder:
			bb.Append(v.(string))
		case *array.Date32Builder:
			tm, err := time.Parse("2006-01-02", v.(string))
			if err != nil {
				t.Fatal(err)
			}
			bb.Append(arrow.Date32FromTime(tm))
		case *array.TimestampBuilder:
			tm, err := time.Parse("2006-01-02T15:04:05", v.(string))
			if err != nil {
				t.Fatal(err)
			}
			bb.Append(arrow.Timestamp(tm.UnixMicro()))
		default:
			t.Fatalf("gbCol: unsupported builder %T", b)
		}
	}
	s, err := series.New(name, b.NewArray())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func gbFrame(t *testing.T, cols ...*series.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

var nan = math.NaN()

// gbMainFrame mirrors the polars fixture "main" (withStr adds "s").
func gbMainFrame(t *testing.T, mem memory.Allocator, withStr bool) *dataframe.DataFrame {
	us := &arrow.TimestampType{Unit: arrow.Microsecond}
	cols := []*series.Series{
		gbCol(t, mem, "k", arrow.BinaryTypes.String, "a", "b", "a", nil, "b", "a"),
		gbCol(t, mem, "i8", arrow.PrimitiveTypes.Int8, 1, 2, nil, 4, 5, 6),
		gbCol(t, mem, "i32", arrow.PrimitiveTypes.Int32, 1, 2, 3, 4, 5, nil),
		gbCol(t, mem, "i64", arrow.PrimitiveTypes.Int64, 10, nil, 30, 40, 50, 60),
		gbCol(t, mem, "u8", arrow.PrimitiveTypes.Uint8, 1, 2, 3, 4, 5, 6),
		gbCol(t, mem, "u64", arrow.PrimitiveTypes.Uint64, 1, 2, 3, 4, 5, 6),
		gbCol(t, mem, "f32", arrow.PrimitiveTypes.Float32, 1.5, 2.5, nil, 4.0, 5.0, 6.0),
		gbCol(t, mem, "f64", arrow.PrimitiveTypes.Float64, 1.0, nan, 3.0, nil, 2.0, -1.0),
		gbCol(t, mem, "b", arrow.FixedWidthTypes.Boolean, true, false, nil, true, true, false),
		gbCol(t, mem, "d", arrow.FixedWidthTypes.Date32, "2020-01-01", "2020-01-02", nil, "2020-01-04", "2020-01-05", "2020-01-06"),
		gbCol(t, mem, "dt", us, "2020-01-01T00:00:00", nil, "2020-01-03T00:00:00", "2020-01-04T00:00:00", "2020-01-05T00:00:00", "2020-01-06T12:00:00"),
	}
	nulls, err := series.New("n", array.MakeArrayOfNull(mem, arrow.Null, 6))
	if err != nil {
		t.Fatal(err)
	}
	cols = append(cols, nulls)
	if withStr {
		cols = append(cols, gbCol(t, mem, "s", arrow.BinaryTypes.String, "x", "y", nil, "z", "y", "w"))
	}
	return gbFrame(t, cols...)
}

func gbMultiFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	return gbFrame(t,
		gbCol(t, mem, "a", arrow.PrimitiveTypes.Int64, 1, 1, nil, nil, 1, 2),
		gbCol(t, mem, "b", arrow.BinaryTypes.String, "x", "y", "x", "x", "x", nil),
		gbCol(t, mem, "v", arrow.PrimitiveTypes.Float64, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
		gbCol(t, mem, "w", arrow.PrimitiveTypes.Float64, nan, nan, 1.0, nan, nil, 3.0),
	)
}

func checkGB(t *testing.T, name string, got *dataframe.DataFrame, err error, sorted bool) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	defer got.Release()
	want, ok := gbExpected[name]
	if !ok {
		t.Fatalf("no expected output for %s", name)
	}
	g := framerepr.Frame(got)
	if sorted {
		g, want = sortRows(g), sortRows(want)
	}
	if g != want {
		t.Errorf("%s mismatch\ngot:\n%s\nwant:\n%s", name, g, want)
	}
}

func sortRows(s string) string {
	lines := strings.Split(s, "\n")
	slices.Sort(lines[1:])
	return strings.Join(lines, "\n")
}

type gbMethod func(g *dataframe.GroupBy, ctx context.Context, mem memory.Allocator) (*dataframe.DataFrame, error)

func TestParityGroupByMethods(t *testing.T) {
	ctx := context.Background()
	opt := dataframe.WithGroupByAllocator
	cases := []struct {
		name    string
		withStr bool
		run     gbMethod
	}{
		{"Len", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Len(ctx, "", opt(m))
		}},
		{"LenNamed", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Len(ctx, "cnt", opt(m))
		}},
		{"Count", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Count(ctx, opt(m))
		}},
		{"First", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.First(ctx, opt(m))
		}},
		{"Last", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Last(ctx, opt(m))
		}},
		{"Sum", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Sum(ctx, opt(m))
		}},
		{"Mean", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Mean(ctx, opt(m))
		}},
		{"Median", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Median(ctx, opt(m))
		}},
		{"Min", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Min(ctx, opt(m))
		}},
		{"Max", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Max(ctx, opt(m))
		}},
		{"NUnique", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.NUnique(ctx, opt(m))
		}},
		{"QuantileNearest", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.5, "", opt(m))
		}},
		{"QuantileLinear", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.3, dataframe.QuantileLinear, opt(m))
		}},
		{"QuantileHigher", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.3, dataframe.QuantileHigher, opt(m))
		}},
		{"QuantileLower", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.7, dataframe.QuantileLower, opt(m))
		}},
		{"QuantileMidpoint", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.3, dataframe.QuantileMidpoint, opt(m))
		}},
		{"QuantileEquiprobable", false, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Quantile(ctx, 0.3, dataframe.QuantileEquiprobable, opt(m))
		}},
		{"All", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.All(ctx, opt(m))
		}},
		{"Head", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Head(ctx, 2, opt(m))
		}},
		{"Tail", true, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Tail(ctx, 2, opt(m))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			df := gbMainFrame(t, mem, c.withStr)
			defer df.Release()
			out, err := c.run(df.GroupBy("k").MaintainOrder(), ctx, mem)
			checkGB(t, c.name, out, err, false)
		})
	}
}

func TestParityGroupByMethodsMultiKey(t *testing.T) {
	ctx := context.Background()
	opt := dataframe.WithGroupByAllocator
	cases := []struct {
		name string
		keys []string
		run  gbMethod
	}{
		{"MultiLen", []string{"a", "b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Len(ctx, "", opt(m))
		}},
		{"MultiSum", []string{"a", "b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Sum(ctx, opt(m))
		}},
		{"MultiMin", []string{"a", "b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Min(ctx, opt(m))
		}},
		{"MultiMax", []string{"a", "b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Max(ctx, opt(m))
		}},
		{"MultiMedian", []string{"a", "b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Median(ctx, opt(m))
		}},
		{"MultiNUnique", []string{"b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.NUnique(ctx, opt(m))
		}},
		{"MultiTail", []string{"a"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Tail(ctx, 5, opt(m))
		}},
		{"Head0", []string{"a"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Head(ctx, 0, opt(m))
		}},
		{"MapGroups", []string{"a"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.MapGroups(ctx, func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
				v, _ := d.Column("v")
				s, err := v.Sum()
				if err != nil {
					return nil, err
				}
				out, err := series.FromFloat64("v", []float64{s}, nil, series.WithAllocator(m))
				if err != nil {
					return nil, err
				}
				return dataframe.New(out)
			}, opt(m))
		}},
		{"AggMaintainSingle", []string{"b"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Agg(ctx, []expr.Expr{expr.Col("v").Sum(), expr.Col("v").Max().Alias("wm")}, opt(m))
		}},
		{"AggMaintainMulti", []string{"b", "a"}, func(g *dataframe.GroupBy, ctx context.Context, m memory.Allocator) (*dataframe.DataFrame, error) {
			return g.Agg(ctx, []expr.Expr{expr.Col("v").Sum()}, opt(m))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			df := gbMultiFrame(t, mem)
			defer df.Release()
			out, err := c.run(df.GroupBy(c.keys...).MaintainOrder(), ctx, mem)
			checkGB(t, c.name, out, err, false)
		})
	}
}

func TestParityGroupByKeyDTypes(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	opt := dataframe.WithGroupByAllocator(mem)

	fk := gbFrame(t,
		gbCol(t, mem, "k", arrow.PrimitiveTypes.Float64, nan, 0.0, math.Copysign(0, -1), nan, nil, 2.5),
		gbCol(t, mem, "v", arrow.PrimitiveTypes.Int64, 1, 2, 3, 4, 5, 6),
	)
	defer fk.Release()
	out, err := fk.GroupBy("k").MaintainOrder().Sum(ctx, opt)
	checkGB(t, "FloatKey", out, err, false)

	dk := gbFrame(t,
		gbCol(t, mem, "k", arrow.FixedWidthTypes.Date32, "2020-01-02", "2020-01-01", "2020-01-02", nil),
		gbCol(t, mem, "u", arrow.PrimitiveTypes.Uint16, 1, 2, 3, 4),
	)
	defer dk.Release()
	out, err = dk.GroupBy("k").MaintainOrder().Sum(ctx, opt)
	checkGB(t, "DateKey", out, err, false)

	bk := gbFrame(t,
		gbCol(t, mem, "k", arrow.FixedWidthTypes.Boolean, true, nil, false, true),
		gbCol(t, mem, "v", arrow.PrimitiveTypes.Int64, 1, 2, 3, 4),
	)
	defer bk.Release()
	out, err = bk.GroupBy("k").MaintainOrder().Mean(ctx, opt)
	checkGB(t, "BoolKey", out, err, false)
}

func TestParityGroupByEmptyFrame(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	opt := dataframe.WithGroupByAllocator(mem)
	e := gbFrame(t,
		gbCol(t, mem, "k", arrow.PrimitiveTypes.Int64),
		gbCol(t, mem, "v", arrow.PrimitiveTypes.Float64),
		gbCol(t, mem, "s", arrow.BinaryTypes.String),
	)
	defer e.Release()
	g := e.GroupBy("k")
	out, err := g.Len(ctx, "", opt)
	checkGB(t, "EmptyLen", out, err, false)
	out, err = g.Mean(ctx, opt)
	checkGB(t, "EmptyMean", out, err, false)
	out, err = g.NUnique(ctx, opt)
	checkGB(t, "EmptyNUnique", out, err, false)
	out, err = g.All(ctx, opt)
	checkGB(t, "EmptyAll", out, err, false)
	out, err = g.Head(ctx, 2, opt)
	checkGB(t, "EmptyHead", out, err, false)
	if _, err := g.Sum(ctx, opt); err == nil {
		t.Fatal("sum over a str column should fail like polars")
	}
}

func TestParityGroupByUnordered(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	opt := dataframe.WithGroupByAllocator(mem)
	multi := gbMultiFrame(t, mem)
	defer multi.Release()
	out, err := multi.GroupBy("a", "b").Sum(ctx, opt)
	checkGB(t, "UnorderedSum", out, err, true)
	main := gbMainFrame(t, mem, false)
	defer main.Release()
	out, err = main.GroupBy("k").Len(ctx, "", opt)
	checkGB(t, "UnorderedLen", out, err, true)
}

func TestGroupByMethodErrors(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	opt := dataframe.WithGroupByAllocator(mem)
	df := gbMainFrame(t, mem, true)
	defer df.Release()
	g := df.GroupBy("k")
	if _, err := g.Sum(ctx, opt); err == nil || !strings.Contains(err.Error(), "str") {
		t.Fatalf("sum over str: err=%v", err)
	}
	if _, err := g.Len(ctx, "k", opt); err == nil {
		t.Fatal("len named like a key should fail")
	}
	if _, err := g.Head(ctx, -1, opt); err == nil {
		t.Fatal("negative head should fail")
	}
	if _, err := g.Quantile(ctx, 1.5, "", opt); err == nil {
		t.Fatal("quantile out of range should fail")
	}
	if _, err := df.GroupBy("missing").Len(ctx, "", opt); err == nil {
		t.Fatal("missing key should fail")
	}
}
