package difftest

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// Float tolerances. Summation order legitimately differs between the
// engines (pairwise, Kahan, chunked, parallel), so aggregates of
// floats are compared with a relative tolerance. Everything else is
// exact.
const (
	relTol64 = 1e-9
	absTol64 = 1e-10
	relTol32 = 1e-5
	absTol32 = 1e-6
)

// MismatchKind classifies a disagreement.
type MismatchKind string

const (
	MPanic         MismatchKind = "golars_panic"
	MTimeout       MismatchKind = "golars_timeout"
	MGolarsError   MismatchKind = "golars_error"  // polars ok, golars error
	MMissingError  MismatchKind = "missing_error" // polars error, golars ok
	MUnsupported   MismatchKind = "unsupported"   // golars cannot express the plan
	MSchema        MismatchKind = "schema"        // names or dtypes differ
	MHeight        MismatchKind = "height"        // row count differs
	MValue         MismatchKind = "value"         // a value differs
	MOraclePanic   MismatchKind = "polars_panic"  // polars itself panicked
	MHarness       MismatchKind = "harness"       // the harness failed
	MBothErrorOkay MismatchKind = "both_error"    // not a mismatch
	MNone          MismatchKind = ""              // results agree
)

// Verdict is the comparison result of one case.
type Verdict struct {
	Kind   MismatchKind
	Detail string
	// Col is the first differing column for MValue and MSchema.
	Col string
}

// OK reports whether the engines agree.
func (v Verdict) OK() bool { return v.Kind == MNone || v.Kind == MBothErrorOkay }

// Canonical converts a golars frame into canonical columns.
func Canonical(df *dataframe.DataFrame) []ResultCol {
	out := make([]ResultCol, df.Width())
	for i, s := range df.Columns() {
		c := ResultCol{Name: s.Name(), DType: ArrowName(s.DType().Arrow()), Values: []any{}}
		for _, ch := range s.Chunks() {
			c.Values = append(c.Values, ArrayValues(ch)...)
		}
		out[i] = c
	}
	return out
}

// Compare checks golars' result against polars' under the order
// contract.
func Compare(got, want []ResultCol, order Order) Verdict {
	gs, ws := schemaString(got), schemaString(want)
	if gs != ws {
		return Verdict{Kind: MSchema, Detail: fmt.Sprintf("golars %s\npolars %s", gs, ws)}
	}
	gh, wh := height(got), height(want)
	if gh != wh {
		return Verdict{Kind: MHeight, Detail: fmt.Sprintf("golars %d rows, polars %d rows", gh, wh)}
	}
	switch order.Mode {
	case OrderCountOnly:
		return Verdict{}
	case OrderKeysOnly:
		return compareRows(pickCols(got, order.Keys), pickCols(want, order.Keys))
	case OrderSorted:
		if v := compareRows(pickCols(got, order.Keys), pickCols(want, order.Keys)); !v.OK() {
			return v
		}
		return compareRows(sortRows(got), sortRows(want))
	case OrderUnordered:
		return compareRows(sortRows(got), sortRows(want))
	}
	return compareRows(got, want)
}

func schemaString(cols []ResultCol) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.Name + ": " + c.DType
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func height(cols []ResultCol) int {
	if len(cols) == 0 {
		return 0
	}
	return len(cols[0].Values)
}

func pickCols(cols []ResultCol, names []string) []ResultCol {
	var out []ResultCol
	for _, n := range names {
		for _, c := range cols {
			if c.Name == n {
				out = append(out, c)
			}
		}
	}
	return out
}

// sortRows reorders rows by a canonical row key so two multisets can be
// compared positionally.
func sortRows(cols []ResultCol) []ResultCol {
	h := height(cols)
	idx := make([]int, h)
	keys := make([]string, h)
	for r := range h {
		idx[r] = r
		var b strings.Builder
		for _, c := range cols {
			rowKey(&b, c.Values[r])
			b.WriteByte(0)
		}
		keys[r] = b.String()
	}
	slices.SortStableFunc(idx, func(a, b int) int { return strings.Compare(keys[a], keys[b]) })
	out := make([]ResultCol, len(cols))
	for i, c := range cols {
		vals := make([]any, h)
		for r, src := range idx {
			vals[r] = c.Values[src]
		}
		out[i] = ResultCol{Name: c.Name, DType: c.DType, Values: vals}
	}
	return out
}

func rowKey(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("~null")
	case float64:
		switch {
		case math.IsNaN(x):
			b.WriteString("nan")
		case x == 0:
			b.WriteString("0")
		default:
			// Round so tolerance-level differences sort the same way.
			b.WriteString(strconv.FormatFloat(x, 'e', 7, 64))
		}
	case []any:
		b.WriteByte('[')
		for _, e := range x {
			rowKey(b, e)
			b.WriteByte(',')
		}
		b.WriteByte(']')
	default:
		fmt.Fprintf(b, "%T:%v", x, x)
	}
}

func compareRows(got, want []ResultCol) Verdict {
	for i := range got {
		f32 := strings.Contains(got[i].DType, "f32")
		for r := range got[i].Values {
			if !ValueEqual(got[i].Values[r], want[i].Values[r], f32) {
				return Verdict{Kind: MValue, Col: got[i].Name, Detail: fmt.Sprintf(
					"column %q row %d: golars %s, polars %s", got[i].Name, r,
					FormatValue(got[i].Values[r]), FormatValue(want[i].Values[r]))}
			}
		}
	}
	return Verdict{}
}

// ValueEqual compares canonical values; floats use the documented
// tolerance, NaN equals NaN.
func ValueEqual(a, b any, f32 bool) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case float64:
		y, ok := b.(float64)
		if !ok {
			return false
		}
		return FloatEqual(x, y, f32)
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !ValueEqual(x[i], y[i], f32) {
				return false
			}
		}
		return true
	}
	return a == b
}

// FloatEqual is the float comparison rule: exact for NaN, infinities
// and signs of infinity; relative tolerance otherwise.
func FloatEqual(x, y float64, f32 bool) bool {
	if math.IsNaN(x) || math.IsNaN(y) {
		return math.IsNaN(x) && math.IsNaN(y)
	}
	if x == y {
		return true
	}
	if math.IsInf(x, 0) || math.IsInf(y, 0) {
		return false
	}
	rel, abs := relTol64, absTol64
	if f32 {
		rel, abs = relTol32, absTol32
	}
	d := math.Abs(x - y)
	return d <= abs || d <= rel*math.Max(math.Abs(x), math.Abs(y))
}

// FormatValue renders a canonical value for reports.
func FormatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(x)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = FormatValue(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(v)
}
