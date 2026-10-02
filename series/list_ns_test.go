package series_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// seriesFromJSON builds a single-chunk Series of dtype dt from a JSON
// array literal (arrow's JSON reader). "NaN" strings become NaN.
func seriesFromJSON(t *testing.T, mem memory.Allocator, name string, dt arrow.DataType, js string) *series.Series {
	t.Helper()
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(js))
	if err != nil {
		t.Fatalf("FromJSON(%s): %v", js, err)
	}
	s, err := series.New(name, arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var polarsDTypeNames = strings.NewReplacer(
	"Int8", "i8", "Int16", "i16", "Int32", "i32", "Int64", "i64",
	"UInt8", "u8", "UInt16", "u16", "UInt32", "u32", "UInt64", "u64",
	"Float32", "f32", "Float64", "f64", "Boolean", "bool", "String", "str",
)

var (
	polarsListRe   = regexp.MustCompile(`List\(([^()]*)\)`)
	polarsArrayRe  = regexp.MustCompile(`Array\(([^,()]*), shape=\((\d+),\)\)`)
	polarsStructRe = regexp.MustCompile(`Struct\(\{(.*)\}\)`)
	polarsFieldRe  = regexp.MustCompile(`'([^']*)': `)
)

// golarsDTypeName translates a polars dtype repr (List(Int64),
// Struct({'a': String})) into golars' dtype String().
func golarsDTypeName(p string) string {
	s := polarsDTypeNames.Replace(p)
	for polarsListRe.MatchString(s) {
		s = polarsListRe.ReplaceAllString(s, "list[$1]")
	}
	s = polarsArrayRe.ReplaceAllString(s, "list[$1; $2]")
	if m := polarsStructRe.FindStringSubmatch(s); m != nil {
		s = "struct{" + polarsFieldRe.ReplaceAllString(m[1], "$1: ") + "}"
	}
	return s
}

type listParityCase struct {
	Input string               `json:"input"`
	Ops   map[string][2]string `json:"ops"`
}

var listDTypes = map[string]arrow.DataType{
	"i64":  arrow.ListOf(arrow.PrimitiveTypes.Int64),
	"f64":  arrow.ListOf(arrow.PrimitiveTypes.Float64),
	"u8":   arrow.ListOf(arrow.PrimitiveTypes.Uint8),
	"i32":  arrow.ListOf(arrow.PrimitiveTypes.Int32),
	"f32":  arrow.ListOf(arrow.PrimitiveTypes.Float32),
	"bool": arrow.ListOf(arrow.FixedWidthTypes.Boolean),
	"str":  arrow.ListOf(arrow.BinaryTypes.String),
}

var listOpRunners = map[string]func(o series.ListOps, opt series.Option) (*series.Series, error){
	"len":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Len(opt) },
	"sum":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Sum(opt) },
	"mean":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Mean(opt) },
	"median":       func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Median(opt) },
	"std":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Std(1, opt) },
	"var0":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Var(0, opt) },
	"min":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Min(opt) },
	"max":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Max(opt) },
	"arg_min":      func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.ArgMin(opt) },
	"arg_max":      func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.ArgMax(opt) },
	"n_unique":     func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.NUnique(opt) },
	"any":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Any(opt) },
	"all":          func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.All(opt) },
	"first":        func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.First(opt) },
	"last":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Last(opt) },
	"get1":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Get(1, opt) },
	"drop_nulls":   func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.DropNulls(opt) },
	"reverse":      func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Reverse(opt) },
	"sort":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Sort(false, false, opt) },
	"sort_desc_nl": func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Sort(true, true, opt) },
	"unique":       func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Unique(false, opt) },
	"unique_stable": func(o series.ListOps, opt series.Option) (*series.Series, error) {
		return o.Unique(true, opt)
	},
	"head2":     func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Head(2, opt) },
	"tail2":     func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Tail(2, opt) },
	"slice":     func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Slice(1, 2, opt) },
	"slice_neg": func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Slice(-3, 2, opt) },
	"shift1":    func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Shift(1, opt) },
	"shiftm1":   func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Shift(-1, opt) },
	"gather": func(o series.ListOps, opt series.Option) (*series.Series, error) {
		return o.Gather([]int{0, -1}, true, opt)
	},
	"gather_every": func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.GatherEvery(2, 1, opt) },
	"diff":         func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Diff(1, opt) },
	"diffm1":       func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Diff(-1, opt) },
	"explode":      func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.Explode(opt) },
	"to_struct":    func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.ToStruct(nil, true, opt) },
	"count_null":   func(o series.ListOps, opt series.Option) (*series.Series, error) { return o.CountMatches(nil, opt) },
	"contains_null": func(o series.ListOps, opt series.Option) (*series.Series, error) {
		return o.Contains(nil, opt)
	},
}

// TestListNamespaceParity replays testdata/list_parity.json, which holds
// polars 1.39.3 results for every list kernel over several inner dtypes
// (with nulls, empty lists, null lists, NaN and unicode).
func TestListNamespaceParity(t *testing.T) {
	raw, err := os.ReadFile("testdata/list_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases map[string]listParityCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for dn, c := range cases {
		for op, want := range c.Ops {
			run, ok := listOpRunners[op]
			if !ok {
				t.Fatalf("no runner for %s", op)
			}
			if dn == "str" && op == "unique" {
				// Polars hashes strings, so its order changes from run
				// to run; golars keeps first-occurrence order.
				want = c.Ops["unique_stable"]
			}
			t.Run(dn+"/"+op, func(t *testing.T) {
				mem := testutil.NewCheckedAllocator(t)
				s := seriesFromJSON(t, mem, "l", listDTypes[dn], c.Input)
				defer s.Release()
				out, err := run(s.List(), series.WithAllocator(mem))
				if want[0] == "ERR" {
					if err == nil {
						out.Release()
						t.Fatalf("want error %q, got none", want[1])
					}
					if !strings.Contains(err.Error(), want[1]) {
						t.Errorf("error %q, want %q", err, want[1])
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer out.Release()
				if out.Name() != "l" {
					t.Errorf("name = %q", out.Name())
				}
				if got, w := out.DType().String(), golarsDTypeName(want[0]); got != w {
					t.Errorf("dtype = %s, want %s", got, w)
				}
				if got := seriesRepr(t, out); got != want[1] {
					t.Errorf("\n got: %s\nwant: %s", got, want[1])
				}
			})
		}
	}
}
