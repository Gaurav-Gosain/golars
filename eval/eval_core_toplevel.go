package eval

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func init() {
	for _, name := range []string{"sum_horizontal", "mean_horizontal", "min_horizontal", "max_horizontal", "all_horizontal", "any_horizontal", "cum_sum_horizontal"} {
		registerCore(name, func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
			return evalHorizontal(ctx, ec, n, df)
		})
	}
	registerCore("fold", evalFold)
	registerCore("reduce", evalFold)
	registerCore("cum_fold", evalFold)
	registerCore("cum_reduce", evalFold)
	registerCore("repeat", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		v, err := evalScalar(ctx, ec, n.Args[0], df)
		if err != nil {
			return nil, err
		}
		defer v.Release()
		out, err := v.Broadcast(paramInt(n, 0, 1), seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		return renamed(out, "repeat"), nil
	})
	registerCore("linear_space", evalLinearSpace)
	registerCore("format", evalFormat)
	registerCore("struct", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		args, err := evalBroadcast(ctx, ec, n.Args, df)
		if err != nil {
			return nil, err
		}
		defer releaseAll(args)
		fields := make([]*series.Series, len(args))
		for i, a := range args {
			fields[i] = a.Rename(expr.OutputName(n.Args[i]))
		}
		defer releaseAll(fields)
		return series.StructFromSeries(fields[0].Name(), fields, nil, seriesAlloc(ec))
	})
	registerCore("concat_list", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		return evalConcatList(ctx, ec, n, df, false)
	})
	registerCore("concat_arr", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		return evalConcatList(ctx, ec, n, df, true)
	})
	registerCore("int_ranges", evalIntRanges)
	registerCore("lit_values", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		vals, _ := paramAny(n, 1).([]any)
		var target arrow.DataType
		if dt, ok := paramDType(n, 0); ok {
			target = dt.Arrow()
		}
		return valuesSeries(ctx, ec, "literal", vals, target)
	})
	for _, name := range []string{"cols_all", "cols_nth", "cols_exclude", "cols_exclude_from"} {
		registerCore(name, func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
			return nil, fmt.Errorf("eval: %s selects several columns; use it in a lazy select, with_columns or agg so it can be expanded", n.Name)
		})
	}
}

// commonDType folds superType over every input dtype.
func commonDType(ss []*series.Series) arrow.DataType {
	var dt arrow.DataType
	for _, s := range ss {
		dt = superType(dt, s.Chunked().DataType())
	}
	return dt
}

func evalHorizontal(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: %s needs at least one expression", n.Name)
	}
	args, err := evalBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(args)
	name := expr.OutputName(n.Args[0])
	rows := args[0].Len()
	switch n.Name {
	case "all_horizontal", "any_horizontal":
		op := "and"
		if n.Name == "any_horizontal" {
			op = "or"
		}
		acc := args[0].Clone()
		for _, a := range args[1:] {
			next, err := acc.LogicOp(op, a, seriesAlloc(ec))
			acc.Release()
			if err != nil {
				return nil, err
			}
			acc = next
		}
		if !acc.DType().IsBool() {
			acc.Release()
			return nil, fmt.Errorf("eval: %s needs boolean inputs", n.Name)
		}
		return renamed(acc, name), nil
	}
	target := commonDType(args)
	boolSum := target.ID() == arrow.BOOL && (n.Name == "sum_horizontal" || n.Name == "cum_sum_horizontal")
	cast := make([]*series.Series, len(args))
	defer releaseAll(cast)
	for i, a := range args {
		c, err := castSeriesTo(ctx, ec, a, target)
		if err != nil {
			return nil, err
		}
		cast[i] = c
	}
	if boolSum {
		// Booleans sum as counts of true values (u32), as in polars.
		target = arrow.PrimitiveTypes.Uint32
	}
	switch n.Name {
	case "min_horizontal", "max_horizontal":
		return horizontalExtreme(cast, n.Name == "max_horizontal", name, rows, ec)
	case "cum_sum_horizontal":
		fields := make([]*series.Series, 0, len(cast))
		defer func() { releaseAll(fields) }()
		acc := cast[0].Rename(expr.OutputName(n.Args[0]))
		fields = append(fields, acc)
		for i, c := range cast[1:] {
			next, err := addPropagate(acc, c, target, ec)
			if err != nil {
				return nil, err
			}
			next = renamed(next, expr.OutputName(n.Args[i+1]))
			fields = append(fields, next)
			acc = next
		}
		return series.StructFromSeries("cum_sum", fields, nil, seriesAlloc(ec))
	}
	if target.ID() == arrow.STRING && n.Name == "sum_horizontal" {
		return horizontalConcatStrings(cast, name, rows, ec)
	}
	isFloat := arrow.IsFloating(target.ID())
	sum := make([]float64, rows)
	isum := make([]int64, rows)
	cnt := make([]int, rows)
	for _, c := range cast {
		vals, err := numericView(c)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			if vals.valid != nil && !vals.valid[i] {
				continue
			}
			cnt[i]++
			if isFloat || n.Name == "mean_horizontal" {
				sum[i] += vals.f[i]
			} else {
				isum[i] += vals.i[i]
			}
		}
	}
	switch n.Name {
	case "mean_horizontal":
		valid := make([]bool, rows)
		for i := range sum {
			if cnt[i] > 0 {
				sum[i] /= float64(cnt[i])
				valid[i] = true
			}
		}
		return series.FromFloat64(name, sum, validOrNil(valid), seriesAlloc(ec))
	case "sum_horizontal":
		if isFloat {
			s, err := series.FromFloat64(name, sum, nil, seriesAlloc(ec))
			if err != nil || target.ID() == arrow.FLOAT64 {
				return s, err
			}
			defer s.Release()
			return castSeriesTo(ctx, ec, s, target)
		}
		s, err := series.FromInt64(name, isum, nil, seriesAlloc(ec))
		if err != nil || target.ID() == arrow.INT64 {
			return s, err
		}
		defer s.Release()
		return castSeriesTo(ctx, ec, s, target)
	}
	return nil, fmt.Errorf("eval: unknown horizontal op %s", n.Name)
}

type numView struct {
	f     []float64
	i     []int64
	valid []bool
}

// numericView copies a numeric column as floats and, for integer and
// boolean columns, as exact int64s too.
func numericView(s *series.Series) (numView, error) {
	var v numView
	var err error
	v.f, v.valid, err = series.Float64Values(s)
	if err != nil {
		return v, fmt.Errorf("eval: horizontal op needs numeric inputs: %w", err)
	}
	dt := s.Chunked().DataType()
	if arrow.IsInteger(dt.ID()) || dt.ID() == arrow.BOOL {
		v.i, _, err = series.Int64Values(s)
	} else {
		v.i = make([]int64, len(v.f))
	}
	return v, err
}

func validOrNil(valid []bool) []bool {
	for _, v := range valid {
		if !v {
			return valid
		}
	}
	return nil
}

// horizontalExtreme picks the row-wise min or max across equal-dtype
// columns, skipping nulls (NaN counts as the largest value).
func horizontalExtreme(cols []*series.Series, isMax bool, name string, rows int, ec EvalContext) (*series.Series, error) {
	pool, err := series.ConcatSeries(name, cols, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	idx := make([]int, rows)
	arr, err := pool.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	for i := range rows {
		best := -1
		for k := range cols {
			j := k*rows + i
			if arr.IsNull(j) {
				continue
			}
			if best < 0 {
				best = j
				continue
			}
			c := compareRows(arr, j, best)
			if (isMax && c > 0) || (!isMax && c < 0) {
				best = j
			}
		}
		idx[i] = best
	}
	return pool.Gather(idx, seriesAlloc(ec))
}

// compareRows orders two rows of one array with polars' total order.
func compareRows(arr arrow.Array, i, j int) int {
	a, b := series.ValueAt(arr, i), series.ValueAt(arr, j)
	switch x := a.(type) {
	case int64:
		y := b.(int64)
		return cmpInts(x, y)
	case uint64:
		y := b.(uint64)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	case float64:
		y := b.(float64)
		xn, yn := math.IsNaN(x), math.IsNaN(y)
		switch {
		case xn && yn:
			return 0
		case xn:
			return 1
		case yn:
			return -1
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	case string:
		return strings.Compare(x, b.(string))
	case bool:
		y := b.(bool)
		switch {
		case x == y:
			return 0
		case !x:
			return -1
		}
		return 1
	case time.Time:
		return x.Compare(b.(time.Time))
	case time.Duration:
		return cmpInts(int64(x), int64(b.(time.Duration)))
	}
	return 0
}

func cmpInts(x, y int64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// horizontalConcatStrings sums string columns row-wise by
// concatenation, treating nulls as empty strings.
func horizontalConcatStrings(cols []*series.Series, name string, rows int, ec EvalContext) (*series.Series, error) {
	out := make([]string, rows)
	for _, c := range cols {
		arr, err := c.Consolidated()
		if err != nil {
			return nil, err
		}
		sa := arr.(*array.String)
		for i := range rows {
			if sa.IsValid(i) {
				out[i] += sa.Value(i)
			}
		}
		arr.Release()
	}
	return series.FromString(name, out, nil, seriesAlloc(ec))
}

// addPropagate adds two equal-dtype numeric columns with null
// propagation, keeping the dtype.
func addPropagate(a, b *series.Series, target arrow.DataType, ec EvalContext) (*series.Series, error) {
	rows := a.Len()
	isFloat := arrow.IsFloating(target.ID())
	xv, err := numericView(a)
	if err != nil {
		return nil, err
	}
	yv, err := numericView(b)
	if err != nil {
		return nil, err
	}
	valid := make([]bool, rows)
	for i := range rows {
		valid[i] = (xv.valid == nil || xv.valid[i]) && (yv.valid == nil || yv.valid[i])
	}
	var out *series.Series
	if isFloat {
		f := make([]float64, rows)
		for i := range f {
			f[i] = xv.f[i] + yv.f[i]
		}
		out, err = series.FromFloat64(a.Name(), f, validOrNil(valid), seriesAlloc(ec))
	} else {
		v := make([]int64, rows)
		for i := range v {
			v[i] = xv.i[i] + yv.i[i]
		}
		out, err = series.FromInt64(a.Name(), v, validOrNil(valid), seriesAlloc(ec))
	}
	if err != nil || arrow.TypeEqual(target, out.Chunked().DataType()) {
		return out, err
	}
	defer out.Release()
	return castSeriesTo(context.Background(), ec, out, target)
}

// evalFold implements fold, reduce, cum_fold and cum_reduce.
func evalFold(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	fn, ok := paramAny(n, 0).(expr.FoldFunc)
	if !ok || fn == nil {
		return nil, fmt.Errorf("eval: %s needs a fold function", n.Name)
	}
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: %s needs at least one expression", n.Name)
	}
	args, err := evalBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(args)
	cumulative := n.Name == "cum_fold" || n.Name == "cum_reduce"
	includeInit := n.Name == "cum_fold" && paramBool(n, 1, false)
	accName := expr.OutputName(n.Args[0])
	acc := args[0].Clone()
	var fields []*series.Series
	defer func() { releaseAll(fields) }()
	if n.Name == "cum_reduce" || includeInit {
		fields = append(fields, acc.Rename(accName))
	}
	for i, x := range args[1:] {
		next, err := fn(acc, x)
		acc.Release()
		if err != nil {
			return nil, err
		}
		acc = next
		if cumulative {
			fields = append(fields, acc.Rename(expr.OutputName(n.Args[i+1])))
		}
	}
	if !cumulative {
		return renamed(acc, accName), nil
	}
	acc.Release()
	if len(fields) == 0 {
		return nil, fmt.Errorf("eval: %s needs at least one input to fold", n.Name)
	}
	return series.StructFromSeries(n.Name, fields, nil, seriesAlloc(ec))
}

func evalLinearSpace(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	start, end := paramFloat(n, 0, 0), paramFloat(n, 1, 1)
	num := paramInt(n, 2, 0)
	closed := paramString(n, 3, "both")
	if num < 0 {
		return nil, fmt.Errorf("eval: linear_space num must be non-negative")
	}
	out := make([]float64, num)
	span := end - start
	switch closed {
	case "both":
		if num == 1 {
			out[0] = start
			break
		}
		step := span / float64(num-1)
		for i := range out {
			out[i] = start + float64(float64(i)*step)
		}
		if num > 1 {
			out[num-1] = end
		}
	case "left":
		step := span / float64(num)
		for i := range out {
			out[i] = start + float64(float64(i)*step)
		}
	case "right":
		step := span / float64(num)
		for i := range out {
			out[i] = start + float64(float64(i+1)*step)
		}
	case "none":
		step := span / float64(num+1)
		for i := range out {
			out[i] = start + float64(float64(i+1)*step)
		}
	default:
		return nil, fmt.Errorf("eval: linear_space closed must be both, left, right or none")
	}
	return series.FromFloat64("literal", out, nil, seriesAlloc(ec))
}

// formatValue renders a value the way polars casts it to a string.
func formatValue(v any) string {
	switch x := v.(type) {
	case float64:
		switch {
		case math.IsNaN(x):
			return "NaN"
		case math.IsInf(x, 1):
			return "inf"
		case math.IsInf(x, -1):
			return "-inf"
		}
		s := strconv.FormatFloat(x, 'f', -1, 64)
		if !strings.ContainsAny(s, ".e") {
			s += ".0"
		}
		return s
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case bool:
		return strconv.FormatBool(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}

func evalFormat(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	tmpl := paramString(n, 0, "")
	pieces := strings.Split(tmpl, "{}")
	if len(pieces)-1 != len(n.Args) {
		return nil, fmt.Errorf("eval: format has %d placeholders but %d arguments", len(pieces)-1, len(n.Args))
	}
	if len(n.Args) == 0 {
		return series.FromString("literal", []string{tmpl}, nil, seriesAlloc(ec))
	}
	args, err := evalBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(args)
	rows := args[0].Len()
	arrs := make([]arrow.Array, len(args))
	for i, a := range args {
		arr, err := a.Consolidated()
		if err != nil {
			for _, x := range arrs[:i] {
				x.Release()
			}
			return nil, err
		}
		arrs[i] = arr
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	out := make([]string, rows)
	valid := make([]bool, rows)
	var b strings.Builder
	for i := range rows {
		b.Reset()
		ok := true
		for k, p := range pieces {
			b.WriteString(p)
			if k < len(arrs) {
				if arrs[k].IsNull(i) {
					ok = false
					break
				}
				b.WriteString(formatValue(series.ValueAt(arrs[k], i)))
			}
		}
		if ok {
			out[i] = b.String()
			valid[i] = true
		}
	}
	return series.FromString(expr.OutputName(n.Args[0]), out, validOrNil(valid), seriesAlloc(ec))
}

// listParts views a column as per-row element ranges: list columns
// expose their child values, scalars one element per row.
type listParts struct {
	values *series.Series
	start  []int
	end    []int
	null   []bool
}

func toListParts(s *series.Series, ec EvalContext) (listParts, error) {
	arr, err := s.Consolidated()
	if err != nil {
		return listParts{}, err
	}
	defer arr.Release()
	rows := arr.Len()
	p := listParts{start: make([]int, rows), end: make([]int, rows), null: make([]bool, rows)}
	switch a := arr.(type) {
	case *array.List:
		child := a.ListValues()
		child.Retain()
		vals, err := series.New(s.Name(), child)
		if err != nil {
			child.Release()
			return listParts{}, err
		}
		p.values = vals
		for i := range rows {
			x, y := a.ValueOffsets(i)
			p.start[i], p.end[i] = int(x), int(y)
			p.null[i] = a.IsNull(i)
		}
		return p, nil
	case *array.FixedSizeList:
		child := a.ListValues()
		child.Retain()
		vals, err := series.New(s.Name(), child)
		if err != nil {
			child.Release()
			return listParts{}, err
		}
		p.values = vals
		w := int(a.DataType().(*arrow.FixedSizeListType).Len())
		off := a.Data().Offset()
		for i := range rows {
			p.start[i], p.end[i] = (off+i)*w, (off+i+1)*w
			p.null[i] = a.IsNull(i)
		}
		return p, nil
	}
	p.values = s.Clone()
	for i := range rows {
		p.start[i], p.end[i] = i, i+1
	}
	return p, nil
}

func evalConcatList(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame, fixed bool) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: %s needs at least one expression", n.Name)
	}
	args, err := evalBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(args)
	rows := args[0].Len()
	parts := make([]listParts, len(args))
	defer func() {
		for _, p := range parts {
			if p.values != nil {
				p.values.Release()
			}
		}
	}()
	var target arrow.DataType
	for i, a := range args {
		p, err := toListParts(a, ec)
		if err != nil {
			return nil, err
		}
		parts[i] = p
		target = superType(target, p.values.Chunked().DataType())
	}
	pool := make([]*series.Series, len(parts))
	defer releaseAll(pool)
	bases := make([]int, len(parts))
	base := 0
	for i, p := range parts {
		c, err := castSeriesTo(ctx, ec, p.values, target)
		if err != nil {
			return nil, err
		}
		pool[i] = c
		bases[i] = base
		base += c.Len()
	}
	all, err := series.ConcatSeries(expr.OutputName(n.Args[0]), pool, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer all.Release()
	offsets := make([]int32, rows+1)
	valid := make([]bool, rows)
	var idx []int
	width := -1
	for r := range rows {
		isNull := false
		for _, p := range parts {
			if p.null[r] {
				isNull = true
				break
			}
		}
		if !isNull {
			valid[r] = true
			cnt := 0
			for k, p := range parts {
				for j := p.start[r]; j < p.end[r]; j++ {
					idx = append(idx, bases[k]+j)
					cnt++
				}
			}
			if fixed {
				if width >= 0 && cnt != width {
					return nil, fmt.Errorf("eval: concat_arr rows have different widths")
				}
				width = cnt
			}
		}
		offsets[r+1] = int32(len(idx))
	}
	values, err := all.Gather(idx, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer values.Release()
	name := expr.OutputName(n.Args[0])
	if fixed {
		if width <= 0 {
			width = 1
		}
		return series.FixedListFromValues(name, values, width, seriesAlloc(ec))
	}
	return series.ListFromInt32Offsets(name, values, offsets, validOrNil(valid), seriesAlloc(ec))
}

func evalIntRanges(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	args, err := evalBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(args)
	arrs := make([]arrow.Array, 3)
	for i := range 3 {
		c, err := castSeriesTo(ctx, ec, args[i], arrow.PrimitiveTypes.Int64)
		if err != nil {
			return nil, err
		}
		arr, err := c.Consolidated()
		c.Release()
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		arrs[i] = arr
	}
	st, en, sp := arrs[0].(*array.Int64), arrs[1].(*array.Int64), arrs[2].(*array.Int64)
	rows := st.Len()
	offsets := make([]int32, rows+1)
	valid := make([]bool, rows)
	var vals []int64
	for r := range rows {
		if st.IsValid(r) && en.IsValid(r) && sp.IsValid(r) {
			step := sp.Value(r)
			if step == 0 {
				return nil, fmt.Errorf("eval: int_ranges step must not be zero")
			}
			valid[r] = true
			for v := st.Value(r); (step > 0 && v < en.Value(r)) || (step < 0 && v > en.Value(r)); v += step {
				vals = append(vals, v)
			}
		}
		offsets[r+1] = int32(len(vals))
	}
	values, err := series.FromInt64("", vals, nil, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer values.Release()
	return series.ListFromInt32Offsets(expr.OutputName(n.Args[0]), values, offsets, validOrNil(valid), seriesAlloc(ec))
}
