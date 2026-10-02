package series_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

var arrDTypes = map[string]arrow.DataType{
	"i64":  arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int64),
	"f64":  arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64),
	"bool": arrow.FixedSizeListOf(2, arrow.FixedWidthTypes.Boolean),
	"str":  arrow.FixedSizeListOf(2, arrow.BinaryTypes.String),
}

type arrRunner func(o series.ArrOps, opt series.Option) (*series.Series, error)

var arrOpRunners = map[string]arrRunner{
	"len":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Len(opt) },
	"sum":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Sum(opt) },
	"mean":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Mean(opt) },
	"median":       func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Median(opt) },
	"std":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Std(1, opt) },
	"var0":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Var(0, opt) },
	"min":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Min(opt) },
	"max":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Max(opt) },
	"arg_min":      func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.ArgMin(opt) },
	"arg_max":      func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.ArgMax(opt) },
	"n_unique":     func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.NUnique(opt) },
	"any":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Any(opt) },
	"all":          func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.All(opt) },
	"first":        func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.First(opt) },
	"last":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Last(opt) },
	"get1":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Get(1, false, opt) },
	"get5":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Get(5, true, opt) },
	"get5_err":     func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Get(5, false, opt) },
	"reverse":      func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Reverse(opt) },
	"sort":         func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Sort(false, false, opt) },
	"sort_desc_nl": func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Sort(true, true, opt) },
	"unique":       func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Unique(true, opt) },
	"head2":        func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Head(2, opt) },
	"tail1":        func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Tail(1, opt) },
	"slice":        func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Slice(1, 1, opt) },
	"shift1":       func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Shift(1, opt) },
	"explode":      func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Explode(opt) },
	"to_list":      func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.ToList(opt) },
	"to_struct":    func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.ToStruct(nil, opt) },
	"count_null":   func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.CountMatches(nil, opt) },
	"contains_null": func(o series.ArrOps, opt series.Option) (*series.Series, error) {
		return o.Contains(nil, opt)
	},
	"join": func(o series.ArrOps, opt series.Option) (*series.Series, error) { return o.Join("-", true, opt) },
}

// TestArrNamespaceParity replays testdata/arr_parity.json (polars
// 1.39.3 results for the Array namespace).
func TestArrNamespaceParity(t *testing.T) {
	raw, err := os.ReadFile("testdata/arr_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases map[string]listParityCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for dn, c := range cases {
		for op, want := range c.Ops {
			run, ok := arrOpRunners[op]
			if !ok {
				t.Fatalf("no runner for %s", op)
			}
			t.Run(dn+"/"+op, func(t *testing.T) {
				mem := testutil.NewCheckedAllocator(t)
				s := seriesFromJSON(t, mem, "a", arrDTypes[dn], c.Input)
				defer s.Release()
				out, err := run(s.Arr(), series.WithAllocator(mem))
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
