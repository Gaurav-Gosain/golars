package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/selector"
	"github.com/Gaurav-Gosain/golars/series"
)

func nsFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	bb, _ := series.FromInt64("bb", []int64{3, 4, 5}, nil, series.WithAllocator(mem))
	st := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	arr, _, err := array.FromJSON(mem, st, strings.NewReader(`[{"x": 1, "y": "a"}, {"x": null, "y": "b"}, null]`))
	if err != nil {
		t.Fatal(err)
	}
	s, _ := series.New("s", arr)
	df, err := dataframe.New(a, bb, s)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func frameSchema(df *dataframe.DataFrame) string {
	parts := make([]string, 0, df.Width())
	for _, c := range df.Columns() {
		parts = append(parts, c.Name()+": "+c.DType().String())
	}
	return strings.Join(parts, ", ")
}

func frameRows(t *testing.T, df *dataframe.DataFrame) string {
	t.Helper()
	parts := make([]string, 0, df.Width())
	for _, c := range df.Columns() {
		a, err := c.Consolidated()
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, c.Name()+"="+testutil.PyRepr(a))
		a.Release()
	}
	return strings.Join(parts, " ")
}

// Output names checked against polars 1.39.3.
func TestNameNamespaceParity(t *testing.T) {
	upper := func(s string) string { return strings.ToUpper(s) }
	cases := []struct {
		name   string
		exprs  []expr.Expr
		schema string
	}{
		{"keep", []expr.Expr{expr.Col("a").Alias("z").Name().Keep()}, "a: i64"},
		{"prefix_binary", []expr.Expr{expr.Col("a").Add(expr.Col("bb")).Name().Prefix("p_")}, "p_a: i64"},
		{"prefix_alias", []expr.Expr{expr.Col("a").Alias("z").Name().Prefix("p_")}, "p_z: i64"},
		{"suffix_alias", []expr.Expr{expr.Col("a").Alias("z").Name().Suffix("_s")}, "z_s: i64"},
		{"map", []expr.Expr{expr.Col("a").Alias("z").Name().Map(upper)}, "Z: i64"},
		{"alias_after", []expr.Expr{expr.Col("a").Name().Suffix("_s").Alias("w")}, "w: i64"},
		{"all_upper", []expr.Expr{expr.AllColumns().Name().ToUppercase()}, "A: i64, BB: i64, S: struct{x: i64, y: str}"},
		{"regex_prefix", []expr.Expr{expr.Col("^b.*$").Name().Prefix("r_")}, "r_bb: i64"},
		{"lower", []expr.Expr{expr.Col("a").Alias("Z").Name().ToLowercase()}, "z: i64"},
		{"replace_regex", []expr.Expr{expr.Col("bb").Name().Replace("b", "c", false)}, "cc: i64"},
		{"replace_literal", []expr.Expr{expr.Col("bb").Name().Replace("^b", "c", true)}, "bb: i64"},
		{"prefix_fields", []expr.Expr{expr.Col("s").Name().PrefixFields("f_")}, "s: struct{f_x: i64, f_y: str}"},
		{"suffix_fields", []expr.Expr{expr.Col("s").Name().SuffixFields("_f")}, "s: struct{x_f: i64, y_f: str}"},
		{"map_fields", []expr.Expr{expr.Col("s").Name().MapFields(func(s string) string { return s + s })}, "s: struct{xx: i64, yy: str}"},
		{"unnest", []expr.Expr{expr.Col("s").Struct().Unnest()}, "x: i64, y: str"},
		{"unnest_prefix", []expr.Expr{expr.Col("s").Struct().Unnest().Name().Prefix("u_")}, "u_x: i64, u_y: str"},
		{"field_keep", []expr.Expr{expr.Col("s").Struct().Field("x").Name().Keep()}, "s: i64"},
		{"field", []expr.Expr{expr.Col("s").Struct().Field("x")}, "x: i64"},
		{"agg_suffix", []expr.Expr{expr.Col("a").Sum().Name().Suffix("_sum")}, "a_sum: i64"},
		{"chain", []expr.Expr{expr.Col("a").Name().Prefix("x").Name().Suffix("y")}, "xay: i64"},
		{"chain_keep", []expr.Expr{expr.Col("a").Name().Prefix("x").Name().Keep()}, "a: i64"},
		{"chain_map", []expr.Expr{expr.Col("a").Name().Prefix("x").Name().Map(func(s string) string { return s + "!" })}, "xa!: i64"},
		{"fields_then_suffix", []expr.Expr{expr.Col("s").Name().PrefixFields("f_").Name().Suffix("_z")}, "s_z: struct{f_x: i64, f_y: str}"},
		{"fields_unnest", []expr.Expr{expr.Col("s").Name().PrefixFields("f_").Struct().Unnest()}, "f_x: i64, f_y: str"},
		{"regex_multi", []expr.Expr{expr.Col("^(a|bb)$").Name().Suffix("_2")}, "a_2: i64, bb_2: i64"},
		{"selector", []expr.Expr{expr.ColSelector(selector.Numeric()).Name().Suffix("_n")}, "a_n: i64, bb_n: i64"},
		{"field_multi", []expr.Expr{expr.Col("s").Struct().Fields("x", "y")}, "x: i64, y: str"},
		{"field_plus_col", []expr.Expr{expr.Col("s").Struct().Field("x").Add(expr.Col("a"))}, "x: i64"},
		{"unnest_with_col", []expr.Expr{expr.Col("s").Struct().Unnest(), expr.Col("bb")}, "x: i64, y: str, bb: i64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			df := nsFrame(t, mem)
			defer df.Release()
			out, err := lazy.FromDataFrame(df).Select(c.exprs...).Collect(context.Background(), lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if got := frameSchema(out); got != c.schema {
				t.Errorf("schema = %s, want %s", got, c.schema)
			}
		})
	}
}

func TestNameAndStructInContexts(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := nsFrame(t, mem)
	defer df.Release()

	wc, err := lazy.FromDataFrame(df).WithColumns(expr.Col("^(a|bb)$").Name().Suffix("_2")).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	if got := frameSchema(wc); got != "a: i64, bb: i64, s: struct{x: i64, y: str}, a_2: i64, bb_2: i64" {
		t.Errorf("with_columns schema = %s", got)
	}
	wc.Release()

	gb, err := lazy.FromDataFrame(df).GroupBy("a").Agg(expr.Col("bb").Sum().Name().Suffix("_s")).
		Sort("a", false).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	if got := frameSchema(gb); got != "a: i64, bb_s: i64" {
		t.Errorf("group_by schema = %s", got)
	}
	gb.Release()

	// Struct namespace values (polars 1.39.3).
	st := expr.Col("s").Struct()
	out, err := lazy.FromDataFrame(df).Select(
		st.RenameFields("p", "q").Alias("r"),
		st.RenameFields("p").Alias("r1"),
		st.WithFields(expr.Col("a").Alias("z"), expr.Field("x").MulLit(int64(2)).Alias("x2")).Alias("w"),
		st.WithFields(expr.Field("x").AddLit(int64(1))).Alias("rep"),
		st.WithFields(expr.Field("y").Str().ToUpper()).Alias("up"),
	).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	want := "r: struct{p: i64, q: str}, r1: struct{p: i64}, w: struct{x: i64, y: str, z: i64, x2: i64}, rep: struct{x: i64, y: str}, up: struct{x: i64, y: str}"
	if got := frameSchema(out); got != want {
		t.Errorf("schema:\n got %s\nwant %s", got, want)
	}
	wantRows := "r=[{'p': 1, 'q': 'a'}, {'p': None, 'q': 'b'}, None] " +
		"r1=[{'p': 1}, {'p': None}, None] " +
		"w=[{'x': 1, 'y': 'a', 'z': 1, 'x2': 2}, {'x': None, 'y': 'b', 'z': 2, 'x2': None}, None] " +
		"rep=[{'x': 2, 'y': 'a'}, {'x': None, 'y': 'b'}, None] " +
		"up=[{'x': 1, 'y': 'A'}, {'x': None, 'y': 'B'}, None]"
	if got := frameRows(t, out); got != wantRows {
		t.Errorf("rows:\n got %s\nwant %s", got, wantRows)
	}

	un, err := lazy.FromDataFrame(df).WithColumns(expr.Col("s").Struct().Unnest()).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer un.Release()
	if got := frameRows(t, un); !strings.HasSuffix(got, "x=[1, None, None] y=['a', 'b', None]") {
		t.Errorf("unnest rows: %s", got)
	}

	// Unnest of a computed struct resolves its fields from the kernel
	// parameters.
	s2, _ := series.FromString("t", []string{"a-b", "c"}, nil, series.WithAllocator(mem))
	df2, _ := dataframe.New(s2)
	defer df2.Release()
	sp, err := lazy.FromDataFrame(df2).Select(expr.Col("t").Str().SplitN("-", 2).Struct().Unnest()).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Release()
	if got := frameRows(t, sp); got != "field_0=['a', 'c'] field_1=['b', None]" {
		t.Errorf("splitn unnest: %s", got)
	}
}
