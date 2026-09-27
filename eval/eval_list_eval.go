package eval

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// list.eval and list.filter run an inner expression over the elements
// of every list, where expr.Element() (the column named "") is the
// current element. Like polars, an element-wise inner expression runs
// once over the flat child values and reuses the list offsets; any
// other expression (aggregations, sort, rank, cumulative ops) runs per
// list on a zero-copy slice of the child.

func evalListEvalFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	inner, ok := params{n}.expr(0)
	if !ok {
		return nil, fmt.Errorf("eval: %s requires an inner expression", n.Name)
	}
	return evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
		if n.Name == "list.filter" {
			return listFilter(ctx, ec, in, inner)
		}
		return listEval(ctx, ec, in, inner)
	})
}

// elementFrame wraps child as a one-column frame named "".
func elementFrame(child *series.Series) (*dataframe.DataFrame, error) {
	return dataframe.New(child.Rename(""))
}

func listEval(ctx context.Context, ec EvalContext, in *series.Series, inner expr.Expr) (*series.Series, error) {
	child, offsets, err := in.Flatten()
	if err != nil {
		return nil, err
	}
	defer child.Release()
	opt := seriesAlloc(ec)
	if isElementwise(inner) {
		frame, err := elementFrame(child)
		if err != nil {
			return nil, err
		}
		defer frame.Release()
		out, err := evalNode(ctx, ec, inner, frame)
		if err != nil {
			return nil, err
		}
		defer out.Release()
		if out.Len() == child.Len() {
			return series.ListFromOffsets(in.Name(), in, offsets, out, opt)
		}
		// A literal-only expression may not follow the frame height;
		// fall through to the per-list path.
	}
	parts, newOffsets, err := evalPerList(ctx, ec, in, child, offsets, inner)
	if err != nil {
		return nil, err
	}
	defer parts.Release()
	return series.ListFromOffsets(in.Name(), in, newOffsets, parts, opt)
}

// evalPerList runs inner on each list separately and concatenates the
// results, returning the combined values and their row offsets.
func evalPerList(ctx context.Context, ec EvalContext, in, child *series.Series, offsets []int, inner expr.Expr) (*series.Series, []int, error) {
	n := len(offsets) - 1
	nulls, err := in.Consolidated()
	if err != nil {
		return nil, nil, err
	}
	defer nulls.Release()
	var chunks []arrow.Array
	release := func() {
		for _, c := range chunks {
			c.Release()
		}
	}
	newOffsets := make([]int, n+1)
	total := 0
	var probeDT arrow.DataType
	for i := range n {
		newOffsets[i] = total
		if nulls.IsNull(i) {
			continue
		}
		out, err := evalOnSlice(ctx, ec, child, offsets[i], offsets[i+1], inner)
		if err != nil {
			release()
			return nil, nil, err
		}
		a, err := out.Consolidated()
		out.Release()
		if err != nil {
			release()
			return nil, nil, err
		}
		if probeDT == nil {
			probeDT = a.DataType()
		} else if !arrow.TypeEqual(probeDT, a.DataType()) {
			a.Release()
			release()
			return nil, nil, fmt.Errorf("list.eval: inner expression produced %s and %s", probeDT, a.DataType())
		}
		chunks = append(chunks, a)
		total += a.Len()
	}
	newOffsets[n] = total
	if len(chunks) == 0 {
		// No list to evaluate: infer the output dtype from an empty run.
		out, err := evalOnSlice(ctx, ec, child, 0, 0, inner)
		if err != nil {
			return nil, nil, err
		}
		empty, err := out.Slice(0, 0)
		out.Release()
		return empty, newOffsets, err
	}
	combined, err := array.Concatenate(chunks, ec.Alloc)
	release()
	if err != nil {
		return nil, nil, err
	}
	s, err := series.New("", combined)
	return s, newOffsets, err
}

func evalOnSlice(ctx context.Context, ec EvalContext, child *series.Series, start, end int, inner expr.Expr) (*series.Series, error) {
	// Slice the backing array directly: Series.Slice of an empty range
	// yields zero chunks, which many kernels do not expect.
	whole, err := child.Consolidated()
	if err != nil {
		return nil, err
	}
	part, err := series.New(child.Name(), array.NewSlice(whole, int64(start), int64(end)))
	whole.Release()
	if err != nil {
		return nil, err
	}
	frame, err := elementFrame(part)
	part.Release()
	if err != nil {
		return nil, err
	}
	defer frame.Release()
	return evalNode(ctx, ec, inner, frame)
}

func listFilter(ctx context.Context, ec EvalContext, in *series.Series, pred expr.Expr) (*series.Series, error) {
	child, offsets, err := in.Flatten()
	if err != nil {
		return nil, err
	}
	defer child.Release()
	opt := seriesAlloc(ec)
	var mask *series.Series
	if isElementwise(pred) {
		frame, err := elementFrame(child)
		if err != nil {
			return nil, err
		}
		mask, err = evalNode(ctx, ec, pred, frame)
		frame.Release()
		if err != nil {
			return nil, err
		}
	} else {
		parts, _, err := evalPerList(ctx, ec, in, child, offsets, pred)
		if err != nil {
			return nil, err
		}
		mask = parts
	}
	defer mask.Release()
	if mask.Len() != child.Len() {
		return nil, fmt.Errorf("list.filter: predicate produced %d values for %d elements", mask.Len(), child.Len())
	}
	// Rebuild the input list over exactly the flattened child so the
	// mask lines up with it.
	aligned, err := series.ListFromOffsets(in.Name(), in, offsets, child, opt)
	if err != nil {
		return nil, err
	}
	defer aligned.Release()
	return aligned.List().Filter(mask, opt)
}

// isElementwise reports whether e maps each input row to exactly one
// output row independently of the others, so it can run over all list
// elements at once.
func isElementwise(e expr.Expr) bool {
	ok := true
	expr.Walk(e, func(x expr.Expr) bool {
		if !ok {
			return false
		}
		switch n := x.Node().(type) {
		case expr.ColNode, expr.LitNode, expr.BinaryNode, expr.UnaryNode,
			expr.AliasNode, expr.CastNode, expr.IsNullNode, expr.WhenThenNode:
		case expr.FunctionNode:
			if !elementwiseFunction(n.Name) {
				ok = false
			}
		default:
			ok = false
		}
		return ok
	})
	return ok
}

func elementwiseFunction(name string) bool {
	switch name {
	case "abs", "sqrt", "exp", "log", "log2", "log10", "sin", "cos", "tan", "sign",
		"round", "floor", "ceil", "clip", "pow", "fill_nan", "cbrt", "log1p", "expm1",
		"radians", "degrees", "arccos", "arcsin", "arctan", "cot", "sinh", "cosh", "tanh",
		"arcsinh", "arccosh", "arctanh", "arctan2", "hash", "coalesce", "concat_str":
		return true
	case "str.join", "str.explode", "list.explode", "cat.get_categories":
		return false
	}
	for _, ns := range []string{"str.", "list.", "arr.", "bin.", "struct.", "name.", "cat."} {
		if strings.HasPrefix(name, ns) {
			return true
		}
	}
	return false
}
