package exprparse

import (
	"context"
	goparser "go/parser"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func probeFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	x, _ := series.FromInt64("x", []int64{7, -3, 15, 42}, nil)
	y, _ := series.FromFloat64("y", []float64{1.5, 0, 2.25, 4}, []bool{true, false, true, true})
	t0 := time.Date(2024, 1, 15, 9, 30, 0, 0, time.UTC)
	ts, _ := series.FromTimes("ts", []time.Time{t0, t0.Add(26 * time.Hour), t0.Add(960 * time.Hour), t0.Add(-5 * time.Hour)}, nil, dtype.Microsecond, "")
	d, _ := series.FromString("d", []string{"2024-01-15", "2024-02-29", "2023-12-31", "2024-07-04"}, nil)
	tags, _ := series.FromString("tags", []string{"a,b,c", "b", "c,a", "a,b"}, nil)
	name, _ := series.FromString("name", []string{"Alice", "bob", "Carol", "dave"}, nil)
	df, err := dataframe.New(x, y, ts, d, tags, name)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Every case evaluates to what polars 1.39 returns for the matching
// Python expression (checked with bench/polars-compare's uv env).
func TestFunctionsEvaluate(t *testing.T) {
	df := probeFrame(t)
	defer df.Release()
	cases := []struct{ src, want string }{
		{`x // 4`, "[1 -1 3 10]"},
		{`x % 4`, "[3 1 3 2]"},
		{`x ** 2`, "[49 9 225 1764]"},
		{`-x // 4`, "[-2 0 -4 -11]"},
		{`dt.year(ts)`, "[2024 2024 2024 2024]"},
		{`ts.dt.month()`, "[1 1 2 1]"},
		{`dt.hour(ts)`, "[9 11 9 4]"},
		{`dt.weekday(ts)`, "[1 2 6 1]"},
		{`dt.strftime(dt.truncate(ts, "1d"), "%Y-%m-%d %H:%M")`, "[2024-01-15 00:00 2024-01-16 00:00 2024-02-24 00:00 2024-01-15 00:00]"},
		{`dt.strftime(ts, "%Y/%m")`, "[2024/01 2024/01 2024/02 2024/01]"},
		{`dt.day(dt.offset_by(ts, "1mo"))`, "[15 16 24 15]"},
		{`dt.year(str.to_date(d, "%Y-%m-%d"))`, "[2024 2024 2023 2024]"},
		{`dt.month(d.str.to_date())`, "[1 2 12 7]"},
		{`list.len(str.split(tags, ","))`, "[3 1 2 2]"},
		{`list.first(str.split(tags, ","))`, "[a b c a]"},
		{`list.join(list.sort(str.split(tags, ","), descending=true), "|")`, "[c|b|a b c|a b|a]"},
		{`list.contains(str.split(tags, ","), "a")`, "[true false true true]"},
		{`str.split(tags, ",").list.eval(element().str.to_uppercase()).list.join("")`, "[ABC B CA AB]"},
		{`cut(x, [0, 10])`, "[(0, 10] (-inf, 0] (10, inf] (10, inf]]"},
		{`cut(x, [0, 10], labels=["neg", "small", "big"])`, "[small neg big big]"},
		{`qcut(x, 2)`, "[(-inf, 11] (-inf, 11] (11, inf] (11, inf]]"},
		{`replace_strict(x, [7, -3], ["seven", "minus"], default="other")`, "[seven minus other other]"},
		{`coalesce(y, x)`, "[1.5 -3 2.25 4]"},
		{`y.fill_null(x)`, "[1.5 -3 2.25 4]"},
		{`when x > 10 then "big" when x > 0 then "small" otherwise "neg"`, "[small neg big big]"},
		{`when x > 10 then 1`, "[<nil> <nil> 1 1]"},
		{`x in [7, 42]`, "[true false false true]"},
		{`x not in [7, 42]`, "[false true true false]"},
		{`str.to_lowercase(name)`, "[alice bob carol dave]"},
		{`str.pad_start(name, 6, "*")`, "[*Alice ***bob *Carol **dave]"},
		{`concat_str(name, tags, separator="-")`, "[Alice-a,b,c bob-b Carol-c,a dave-a,b]"},
		{`format("{}:{}", name, x)`, "[Alice:7 bob:-3 Carol:15 dave:42]"},
		{`is_between(x, 0, 20, closed="left")`, "[true false true false]"},
		{`rank(x, "dense", descending=true)`, "[3 4 2 1]"},
		{`shift(x, -1)`, "[-3 15 42 <nil>]"},
		{`sort_by(name, x)`, "[bob Alice Carol dave]"},
		{`top_k_by(name, x, 2)`, "[dave Carol]"},
		{`x.rolling_sum_by(ts, "2d")`, "[49 46 15 42]"},
		{`rolling_mean_by(x, ts, "2d", min_periods=1)`, "[24.5 15.333333333333334 15 42]"},
		{`struct.field(struct(x, name), "name")`, "[Alice bob Carol dave]"},
		{`dt.year(date(2024, 1, 15))`, "[2024]"},
		{`dt.hour(datetime(2024, 1, 15, hour=9))`, "[9]"},
		{`sum_horizontal(x, y)`, "[8.5 -3 17.25 46]"},
		{`x.sort(descending=true)`, "[42 15 7 -3]"},
		{`log(10 * x.abs(), 10).round(3)`, "[1.845 1.477 2.176 2.623]"},
	}
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			if got := evalString(t, df, tc.src); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func evalString(t *testing.T, df *dataframe.DataFrame, src string) string {
	t.Helper()
	e, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	out, err := lazy.FromDataFrame(df).Select(e.Alias("out")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	rows, err := out.Rows()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]any, len(rows))
	for i, r := range rows {
		got[i] = r[0]
	}
	return sprintAny(got)
}

// The two call forms and the operators build the same tree as the
// fluent Go API.
func TestFunctionForms(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want expr.Expr
	}{
		{`dt.year(ts)`, expr.Col("ts").Dt().Year()},
		{`ts.dt.year()`, expr.Col("ts").Dt().Year()},
		{`ts.dt.year`, expr.Col("ts").Dt().Year()},
		{`round(x, 2)`, expr.Col("x").Round(2)},
		{`x.round()`, expr.Col("x").Round(0)},
		{`a // b`, expr.Col("a").FloorDiv(expr.Col("b"))},
		{`a % 2`, expr.Col("a").Mod(expr.LitInt64(2))},
		{`a ** 2`, expr.Col("a").PowExpr(expr.LitInt64(2))},
		{`-a ** 2`, expr.Col("a").PowExpr(expr.LitInt64(2)).Neg()},
		{`a + b * c`, expr.Col("a").Add(expr.Col("b").Mul(expr.Col("c")))},
		{`str.to_date(s, "%Y")`, expr.Col("s").Str().ToDate("%Y")},
		{`str.to_date(s, "%Y", strict=false)`, expr.Col("s").Str().ToDateWith("%Y",
			expr.StrptimeOptions{Strict: false, Exact: true, Ambiguous: "raise"})},
		{`cut(x, [1, 2], left_closed=true)`, expr.Col("x").Cut([]float64{1, 2}, expr.CutOptions{LeftClosed: true})},
		{`list.get(tags, 0)`, expr.Col("tags").List().Get(0)},
		{`name.str.upper()`, expr.Col("name").Str().ToUpper()},
		// `name` is both a namespace and a common column name. The
		// namespace reading needs a receiver argument; when it does
		// not compile the head is a column.
		{`name.replace("a", "b")`, expr.Col("name").Replace([]any{"a"}, []any{"b"})},
		{`name.suffix(x, "_v")`, expr.Col("x").Name().Suffix("_v")},
		{`x.over("k", "j")`, expr.Col("x").Over("k", "j")},
		{`x.over(["k", "j"])`, expr.Col("x").Over("k", "j")},
		{`coalesce(a, b)`, expr.Coalesce(expr.Col("a"), expr.Col("b"))},
		{`date_range(a, b, "1h")`, expr.DateRange(expr.Col("a"), expr.Col("b"), "1h", "both")},
		{`date_range(a, b, closed="left")`, expr.DateRange(expr.Col("a"), expr.Col("b"), "1d", "left")},
		{`x.cast("datetime[ms]")`, expr.Col("x").Cast(dtype.Datetime(dtype.Millisecond, ""))},
		{`WHEN a THEN 1 OTHERWISE 2`, expr.When(expr.Col("a")).Then(expr.LitInt64(1)).Otherwise(expr.LitInt64(2))},
		{`name contains "x" AND NOT b is_null`, expr.Col("name").Str().Contains("x").And(expr.Col("b").IsNull().Not())},
		{`col("my col").sum()`, expr.Col("my col").Sum()},
	} {
		t.Run(tc.src, func(t *testing.T) {
			got, err := Parse(tc.src)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Node(), tc.want.Node()) {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestFunctionErrors(t *testing.T) {
	for src, want := range map[string]string{
		`dt.year()`:                    "dt.year needs the value to work on",
		`dt.frobnicate(ts)`:            `unknown dt function "frobnicate"`,
		`ts.dt.frobnicate()`:           `unknown dt method "frobnicate"`,
		`cut(x, [0], colour="red")`:    `unknown keyword argument "colour"`,
		`round(x, 1, 2)`:               "round takes 0 to 1 arguments, got 2",
		`x.cast("decimal")`:            `unknown dtype "decimal"`,
		`[1, 2]`:                       "only valid as a function argument",
		`x in 3`:                       "must be followed by a [list]",
		`when a then`:                  "unexpected end of input",
		`when a 1`:                     "expected `then`",
		`a = 1`:                        "did you mean '=='",
		`cut(x, labels=["a"], [1])`:    "positional argument after keyword argument",
		`x.map_batches(1)`:             `unknown method "map_batches"`,
		`str.pad_start(name, 6, "ab")`: "single character",
	} {
		_, err := Parse(src)
		if err == nil {
			t.Errorf("%s: want error", src)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error %q does not mention %q", src, err, want)
		}
	}
}

// GoSource output is a valid Go expression that names the same API
// calls, so transpiled programs compile.
func TestGoSource(t *testing.T) {
	for src, want := range map[string]string{
		`dt.year(ts)`: `expr.Col("ts").Dt().Year()`,
		`x // 2`:      `expr.Col("x").FloorDiv(expr.LitInt64(2))`,
		`cut(x, [0, 10], labels=["a", "b", "c"])`: `expr.Col("x").Cut([]float64{0, 10}, expr.CutOptions{Labels: []string{"a", "b", "c"}})`,
		`x in [1, 2]`:                  `expr.Col("x").IsIn(int64(1), int64(2))`,
		`when a then 1`:                `expr.When(expr.Col("a")).Then(expr.LitInt64(1)).Otherwise(expr.LitNull(dtype.Null()))`,
		`x.cast("date")`:               `expr.Col("x").Cast(dtype.Date())`,
		`str.to_date(s, strict=false)`: `expr.Col("s").Str().ToDateWith("", expr.StrptimeOptions{Strict: false, Exact: true, Ambiguous: "raise"})`,
		`rolling_mean_by(x, ts, "2d", min_periods=1)`: `expr.Col("x").RollingMeanBy(expr.Col("ts"), "2d", expr.WithMinPeriods(1))`,
		`replace_strict(x, [1], ["a"], default="z")`:  `expr.Col("x").ReplaceStrict([]any{int64(1)}, []any{"a"}, expr.ReplaceStrictOptions{Default: new(expr.LitString("z"))})`,
		`is_between(x, 1, 2)`:                         `expr.Col("x").IsBetween(expr.LitInt64(1), expr.LitInt64(2), expr.IntervalClosed("both"))`,
		`sort_by(a, b, descending=true)`:              `expr.Col("a").SortBy([]expr.Expr{expr.Col("b")}, expr.SortByOptions{Descending: []bool{true}})`,
	} {
		got, err := GoSource(src)
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if got != want {
			t.Errorf("%s:\ngot  %s\nwant %s", src, got, want)
		}
		if _, err := goparser.ParseExpr(got); err != nil {
			t.Errorf("%s: generated Go does not parse: %v", src, err)
		}
	}
}

// Functions reports the namespaces and a few well-known names, for
// editor completion.
func TestFunctionsListing(t *testing.T) {
	fns := Functions()
	for ns, name := range map[string]string{
		"": "rolling_mean_by", "str": "to_date", "dt": "truncate", "list": "len",
		"arr": "sum", "struct": "field", "name": "suffix", "bin": "encode", "cat": "get_categories",
		"free": "date_range",
	} {
		found := false
		for _, n := range fns[ns] {
			if n == name {
				found = true
			}
		}
		if !found {
			t.Errorf("Functions()[%q] lacks %q", ns, name)
		}
	}
}
