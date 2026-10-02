package eval_test

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	_ "time/tzdata" // make the zone database available on every CI image

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// nullStr marks a null cell in the generated parity tables.
const nullStr = "\x00null"

var (
	col      = expr.Col
	lenient  = expr.StrptimeOptions{Exact: true}
	notExact = expr.StrptimeOptions{}
)

type temporalParityColumn struct {
	Name   string
	DType  string
	Values []string
}

type temporalParityCase struct {
	Name  string
	Frame string
	Expr  expr.Expr
	DType string
	Want  []string
	Err   bool
}

func parseParityDType(t *testing.T, s string) dtype.DType {
	t.Helper()
	switch s {
	case "i64":
		return dtype.Int64()
	case "f64":
		return dtype.Float64()
	case "str":
		return dtype.String()
	case "bool":
		return dtype.Bool()
	case "date":
		return dtype.Date()
	case "time64[ns]":
		return dtype.Time(dtype.Nanosecond)
	}
	inner := strings.TrimSuffix(s[strings.Index(s, "[")+1:], "]")
	parts := strings.SplitN(inner, ", ", 2)
	u, ok := dtype.ParseTimeUnit(parts[0])
	if !ok {
		t.Fatalf("bad unit in %q", s)
	}
	if strings.HasPrefix(s, "duration") {
		return dtype.Duration(u)
	}
	tz := ""
	if len(parts) == 2 {
		tz = parts[1]
	}
	return dtype.Datetime(u, tz)
}

func buildParityFrame(t *testing.T, cols []temporalParityColumn, opts ...series.Option) *dataframe.DataFrame {
	t.Helper()
	var out []*series.Series
	for _, c := range cols {
		n := len(c.Values)
		valid := make([]bool, n)
		for i, v := range c.Values {
			valid[i] = v != nullStr
		}
		dt := parseParityDType(t, c.DType)
		ints := func() []int64 {
			vs := make([]int64, n)
			for i, v := range c.Values {
				if valid[i] {
					x, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						t.Fatalf("%s: %v", c.Name, err)
					}
					vs[i] = x
				}
			}
			return vs
		}
		var s *series.Series
		var err error
		switch {
		case dt.IsString():
			vs := make([]string, n)
			for i, v := range c.Values {
				if valid[i] {
					vs[i] = v
				}
			}
			s, err = series.FromString(c.Name, vs, valid, opts...)
		case dt.ID() == arrow.FLOAT64:
			vs := make([]float64, n)
			for i, v := range c.Values {
				if valid[i] {
					vs[i], _ = strconv.ParseFloat(v, 64)
				}
			}
			s, err = series.FromFloat64(c.Name, vs, valid, opts...)
		case dt.ID() == arrow.INT64:
			s, err = series.FromInt64(c.Name, ints(), valid, opts...)
		case dt.IsDate():
			iv := ints()
			days := make([]int32, n)
			for i, v := range iv {
				days[i] = int32(v)
			}
			s, err = series.FromDate(c.Name, days, valid, opts...)
		case dt.IsDatetime():
			u, _ := dt.TimeUnit()
			s, err = series.FromDatetime(c.Name, ints(), valid, u, dt.TimeZone(), opts...)
		case dt.IsDuration():
			u, _ := dt.TimeUnit()
			s, err = series.FromDuration(c.Name, ints(), valid, u, opts...)
		case dt.IsTime():
			s, err = series.FromTimeOfDay(c.Name, ints(), valid, opts...)
		default:
			t.Fatalf("unsupported parity dtype %s", c.DType)
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	df, err := dataframe.New(out...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// renderParity renders every row canonically: temporal values as their
// physical integer, floats with 12 significant digits.
func renderParity(s *series.Series) []string {
	arr := s.Chunk(0)
	out := make([]string, arr.Len())
	for i := range out {
		if arr.IsNull(i) {
			out[i] = nullStr
			continue
		}
		switch a := arr.(type) {
		case *array.Boolean:
			out[i] = strconv.FormatBool(a.Value(i))
		case *array.Float64:
			out[i] = formatParityFloat(a.Value(i))
		case *array.Float32:
			out[i] = formatParityFloat(float64(a.Value(i)))
		case *array.String:
			out[i] = a.Value(i)
		case *array.Int8:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Int16:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Int32:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Int64:
			out[i] = strconv.FormatInt(a.Value(i), 10)
		case *array.Uint32:
			out[i] = strconv.FormatUint(uint64(a.Value(i)), 10)
		case *array.Uint64:
			out[i] = strconv.FormatUint(a.Value(i), 10)
		case *array.Date32:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Timestamp:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Duration:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Time64:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		case *array.Time32:
			out[i] = strconv.FormatInt(int64(a.Value(i)), 10)
		default:
			out[i] = "?unsupported " + arr.DataType().String()
		}
	}
	return out
}

func formatParityFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "nan"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(v, 'g', 12, 64)
	s = strings.Replace(s, "e+", "e", 1)
	if i := strings.Index(s, "e"); i >= 0 {
		// Python pads exponents to two digits (1e06); Go does not.
		mant, exp := s[:i], s[i+1:]
		neg := strings.HasPrefix(exp, "-")
		exp = strings.TrimPrefix(exp, "-")
		if len(exp) < 2 {
			exp = "0" + exp
		}
		if neg {
			exp = "-" + exp
		}
		s = mant + "e" + exp
	}
	return s
}

type parityFrameCase struct {
	Name  string
	Frame string
	Plan  func(lf lazy.LazyFrame) lazy.LazyFrame
	Want  []temporalParityColumn
	Err   bool
}

func TestTemporalFrameParity(t *testing.T) {
	ctx := context.Background()
	for _, tc := range temporalFrameCases {
		t.Run(tc.Name, func(t *testing.T) {
			alloc := testutil.NewCheckedAllocator(t)
			df := buildParityFrame(t, temporalParityFrames[tc.Frame], series.WithAllocator(alloc))
			defer df.Release()
			got, err := tc.Plan(lazy.FromDataFrame(df)).Collect(ctx, lazy.WithExecAllocator(alloc))
			if tc.Err {
				if err == nil {
					got.Release()
					t.Fatalf("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("collect: %v", err)
			}
			defer got.Release()
			if got.Width() != len(tc.Want) {
				t.Fatalf("columns = %v, want %d columns", got.ColumnNames(), len(tc.Want))
			}
			for i, w := range tc.Want {
				c := got.ColumnAt(i)
				if c.Name() != w.Name {
					t.Errorf("column %d name = %q, want %q", i, c.Name(), w.Name)
				}
				if c.DType().String() != w.DType {
					t.Errorf("column %q dtype = %s, want %s", w.Name, c.DType(), w.DType)
				}
				vals := renderParity(c)
				if len(vals) != len(w.Values) {
					t.Errorf("column %q len = %d, want %d\n got: %q\nwant: %q", w.Name, len(vals), len(w.Values), vals, w.Values)
					continue
				}
				for r := range vals {
					if vals[r] != w.Values[r] {
						t.Errorf("column %q row %d = %q, want %q", w.Name, r, vals[r], w.Values[r])
					}
				}
			}
		})
	}
}

func TestTemporalParity(t *testing.T) {
	ctx := context.Background()
	for _, tc := range temporalParityCases {
		t.Run(tc.Name, func(t *testing.T) {
			alloc := testutil.NewCheckedAllocator(t)
			df := buildParityFrame(t, temporalParityFrames[tc.Frame], series.WithAllocator(alloc))
			defer df.Release()
			got, err := eval.Eval(ctx, eval.EvalContext{Alloc: alloc}, tc.Expr, df)
			if tc.Err {
				if err == nil {
					got.Release()
					t.Fatalf("expected an error, got %s", got.DType())
				}
				return
			}
			if err != nil {
				t.Fatalf("eval %s: %v", tc.Expr, err)
			}
			defer got.Release()
			if got.DType().String() != tc.DType {
				t.Errorf("dtype = %s, want %s", got.DType(), tc.DType)
			}
			vals := renderParity(got)
			if len(vals) != len(tc.Want) {
				t.Fatalf("len = %d, want %d\n got: %q\nwant: %q", len(vals), len(tc.Want), vals, tc.Want)
			}
			for i := range vals {
				if vals[i] != tc.Want[i] {
					t.Errorf("row %d = %q, want %q", i, vals[i], tc.Want[i])
				}
			}
		})
	}
}
