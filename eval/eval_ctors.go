package eval

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalIntRange materialises an int64 Series of [start, end) with step.
// Step=0 is an error. Mirrors polars' pl.int_range.
func evalIntRange(n expr.FunctionNode) (*series.Series, error) {
	if len(n.Params) < 3 {
		return nil, fmt.Errorf("eval: int_range needs start,end,step")
	}
	start, _ := n.Params[0].(int64)
	end, _ := n.Params[1].(int64)
	step, _ := n.Params[2].(int64)
	if step == 0 {
		return nil, fmt.Errorf("eval: int_range step must be non-zero")
	}
	var out []int64
	if step > 0 {
		for v := start; v < end; v += step {
			out = append(out, v)
		}
	} else {
		for v := start; v > end; v += step {
			out = append(out, v)
		}
	}
	return series.FromInt64("int_range", out, nil)
}

// evalOnesZeros builds a float64 Series of length n filled with value.
func evalOnesZeros(n expr.FunctionNode, value float64) (*series.Series, error) {
	if len(n.Params) < 1 {
		return nil, fmt.Errorf("eval: ones/zeros requires length")
	}
	length, _ := n.Params[0].(int)
	if length < 0 {
		return nil, fmt.Errorf("eval: ones/zeros length must be >= 0")
	}
	name := "zeros"
	if value != 0 {
		name = "ones"
	}
	out := make([]float64, length)
	if value != 0 {
		for i := range out {
			out[i] = value
		}
	}
	return series.FromFloat64(name, out, nil)
}

// evalCoalesce iterates through n.Args; for each row returns the first
// non-null value, preserving the dtype of the first argument.
func evalCoalesce(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: coalesce requires at least one argument")
	}
	// Evaluate all args upfront; they share the frame's height.
	// Broadcast unit-length arguments (literals, aggregates) and use
	// one chunk each: the row loops below index every part by row
	// through its first chunk (found by internal/difftest).
	parts, err := evalSingleChunkBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	return coalesceSeries(parts)
}

func coalesceSeries(parts []*series.Series) (*series.Series, error) {
	n := parts[0].Len()
	// Work in float64 when dtype is numeric; otherwise preserve first
	// dtype via a row-wise walk.
	first := parts[0]
	switch first.Chunk(0).(type) {
	case *array.Float64, *array.Int64, *array.Float32, *array.Int32:
		out := make([]float64, n)
		valid := make([]bool, n)
		for i := range n {
			for _, p := range parts {
				c := p.Chunk(0)
				if c.IsValid(i) {
					v, ok := floatFromCell(c, i)
					if ok {
						out[i] = v
						valid[i] = true
						break
					}
				}
			}
		}
		return series.FromFloat64(first.Name(), out, valid)
	case *array.String:
		out := make([]string, n)
		valid := make([]bool, n)
		for i := range n {
			for _, p := range parts {
				c, ok := p.Chunk(0).(*array.String)
				if !ok {
					continue
				}
				if c.IsValid(i) {
					out[i] = c.Value(i)
					valid[i] = true
					break
				}
			}
		}
		return series.FromString(first.Name(), out, valid)
	case *array.Boolean:
		out := make([]bool, n)
		valid := make([]bool, n)
		for i := range n {
			for _, p := range parts {
				c, ok := p.Chunk(0).(*array.Boolean)
				if !ok {
					continue
				}
				if c.IsValid(i) {
					out[i] = c.Value(i)
					valid[i] = true
					break
				}
			}
		}
		return series.FromBool(first.Name(), out, valid)
	}
	return nil, fmt.Errorf("eval: coalesce unsupported for dtype %s", first.DType())
}

func floatFromCell(c any, i int) (float64, bool) {
	switch a := c.(type) {
	case *array.Float64:
		return a.Value(i), true
	case *array.Float32:
		return float64(a.Value(i)), true
	case *array.Int64:
		return float64(a.Value(i)), true
	case *array.Int32:
		return float64(a.Value(i)), true
	}
	return 0, false
}

// evalConcatStr joins the string forms of each argument row-wise.
// Nulls in any argument propagate (output row is null). Sep is in Params[0].
func evalConcatStr(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: concat_str requires at least one arg")
	}
	sep := ""
	if len(n.Params) >= 1 {
		if s, ok := n.Params[0].(string); ok {
			sep = s
		}
	}
	// Broadcast unit-length arguments (literals, aggregates) and use
	// one chunk each: the row loops below index every part by row
	// through its first chunk (found by internal/difftest).
	parts, err := evalSingleChunkBroadcast(ctx, ec, n.Args, df)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	rows := parts[0].Len()
	out := make([]string, rows)
	valid := make([]bool, rows)
	for i := range rows {
		var sb strings.Builder
		ok := true
		for j, p := range parts {
			c := p.Chunk(0)
			if !c.IsValid(i) {
				ok = false
				break
			}
			if j > 0 {
				sb.WriteString(sep)
			}
			sb.WriteString(cellString(c, i))
		}
		if ok {
			out[i] = sb.String()
			valid[i] = true
		}
	}
	return series.FromString("concat_str", out, valid)
}

func cellString(c any, i int) string {
	switch a := c.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Float64:
		return strconv.FormatFloat(a.Value(i), 'g', -1, 64)
	case *array.Float32:
		return strconv.FormatFloat(float64(a.Value(i)), 'g', -1, 32)
	case *array.Boolean:
		if a.Value(i) {
			return "true"
		}
		return "false"
	}
	return ""
}

// evalSingleChunkBroadcast is evalBroadcast with every result
// consolidated into a single chunk.
func evalSingleChunkBroadcast(ctx context.Context, ec EvalContext, args []expr.Expr, df *dataframe.DataFrame) ([]*series.Series, error) {
	parts, err := evalBroadcast(ctx, ec, args, df)
	if err != nil {
		return nil, err
	}
	for i, p := range parts {
		if p.NumChunks() != 1 {
			parts[i] = p.Rechunk()
			p.Release()
		}
	}
	return parts, nil
}
