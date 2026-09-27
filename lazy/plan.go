// Package lazy provides the lazy query planner, optimizer, and executor.
//
// A LazyFrame wraps a logical plan tree. Fluent methods (Select, Filter,
// Sort, Slice, WithColumns) append nodes. Collect runs the optimizer and
// executes the resulting plan. Explain prints the plan at every stage.
package lazy

import (
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// Aggregate is a group-by-aggregate node. Keys are the grouping columns; each
// expr in Aggs must match the shape accepted by dataframe.GroupBy.Agg.
type Aggregate struct {
	Input Node
	Keys  []string
	Aggs  []expr.Expr
}

func (Aggregate) isLogicalNode() {}

func (a Aggregate) Children() []Node { return []Node{a.Input} }

func (a Aggregate) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Aggregate takes one child")
	}
	return Aggregate{Input: children[0], Keys: a.Keys, Aggs: a.Aggs}
}

func (a Aggregate) Schema() (*schema.Schema, error) {
	in, err := a.Input.Schema()
	if err != nil {
		return nil, err
	}
	fields := make([]schema.Field, 0, len(a.Keys)+len(a.Aggs))
	for _, k := range a.Keys {
		f, ok := in.FieldByName(k)
		if !ok {
			return nil, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, k)
		}
		fields = append(fields, f)
	}
	for _, e := range a.Aggs {
		dt, err := inferExprDType(e, in)
		if err != nil {
			return nil, err
		}
		fields = append(fields, schema.Field{Name: expr.OutputName(e), DType: dt})
	}
	return schema.New(fields...)
}

func (a Aggregate) String() string {
	aggs := make([]string, len(a.Aggs))
	for i, e := range a.Aggs {
		aggs[i] = e.String()
	}
	return fmt.Sprintf("AGG keys=%v [%s]", a.Keys, strings.Join(aggs, ", "))
}

// Ensure dtype import is referenced in this file even when callers do not use
// nested inference paths.
var _ = dtype.Null

// Node is the interface for logical plan nodes. Nodes are immutable values;
// rewrites return new nodes.
type Node interface {
	isLogicalNode()
	// Children returns the plan inputs.
	Children() []Node
	// WithChildren returns a node of the same kind with the given inputs.
	WithChildren([]Node) Node
	// Schema returns the output schema after this node. It may derive the
	// schema from the input. Errors indicate a malformed plan.
	Schema() (*schema.Schema, error)
	// String returns a single-line header for pretty-print. Details like
	// expressions and sort keys are included; children are printed by the
	// caller.
	String() string
}

// DataFrameScan reads from an in-memory DataFrame.
//
// An optional Projection restricts the emitted columns (used by projection
// pushdown). When Projection is nil, all columns are emitted.
//
// An optional Predicate applies a boolean expression during scan (used by
// predicate pushdown for sources that support it). The in-memory scan simply
// applies the filter after loading.
//
// An optional Slice limits rows produced.
type DataFrameScan struct {
	Source     *dataframe.DataFrame
	Projection []string // empty = all columns
	Predicate  *expr.Expr
	Offset     int
	Length     int // -1 for no limit
}

func (DataFrameScan) isLogicalNode() {}

func (s DataFrameScan) Children() []Node { return nil }

func (s DataFrameScan) WithChildren(children []Node) Node {
	if len(children) != 0 {
		panic("lazy: DataFrameScan takes no children")
	}
	return s
}

func (s DataFrameScan) Schema() (*schema.Schema, error) {
	src := s.Source.Schema()
	if len(s.Projection) == 0 {
		return src, nil
	}
	return src.Select(s.Projection...)
}

func (s DataFrameScan) String() string {
	var b strings.Builder
	b.WriteString("SCAN df")
	if len(s.Projection) > 0 {
		fmt.Fprintf(&b, " projection=%v", s.Projection)
	}
	if s.Predicate != nil {
		fmt.Fprintf(&b, " predicate=%s", s.Predicate)
	}
	if s.Length >= 0 {
		fmt.Fprintf(&b, " slice=(%d,%d)", s.Offset, s.Length)
	}
	return b.String()
}

// Projection applies a set of expressions to produce output columns.
// The output schema has one column per expression, named by OutputName.
type Projection struct {
	Input Node
	Exprs []expr.Expr
}

func (Projection) isLogicalNode() {}

func (p Projection) Children() []Node { return []Node{p.Input} }

func (p Projection) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Projection takes one child")
	}
	return Projection{Input: children[0], Exprs: p.Exprs}
}

func (p Projection) Schema() (*schema.Schema, error) {
	in, err := p.Input.Schema()
	if err != nil {
		return nil, err
	}
	fields := make([]schema.Field, len(p.Exprs))
	for i, e := range p.Exprs {
		name := expr.OutputName(e)
		dt, err := inferExprDType(e, in)
		if err != nil {
			return nil, fmt.Errorf("projection: %s: %w", e, err)
		}
		fields[i] = schema.Field{Name: name, DType: dt}
	}
	return schema.New(fields...)
}

func (p Projection) String() string {
	parts := make([]string, len(p.Exprs))
	for i, e := range p.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("PROJECT [%s]", strings.Join(parts, ", "))
}

// WithColumns extends the input with new or replacement columns.
type WithColumns struct {
	Input Node
	Exprs []expr.Expr
}

func (WithColumns) isLogicalNode() {}

func (w WithColumns) Children() []Node { return []Node{w.Input} }

func (w WithColumns) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: WithColumns takes one child")
	}
	return WithColumns{Input: children[0], Exprs: w.Exprs}
}

func (w WithColumns) Schema() (*schema.Schema, error) {
	in, err := w.Input.Schema()
	if err != nil {
		return nil, err
	}
	out := in
	for _, e := range w.Exprs {
		name := expr.OutputName(e)
		dt, err := inferExprDType(e, in)
		if err != nil {
			return nil, err
		}
		out = out.WithField(schema.Field{Name: name, DType: dt})
	}
	return out, nil
}

func (w WithColumns) String() string {
	parts := make([]string, len(w.Exprs))
	for i, e := range w.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("WITH_COLUMNS [%s]", strings.Join(parts, ", "))
}

// Filter applies a boolean predicate.
type Filter struct {
	Input     Node
	Predicate expr.Expr
}

func (Filter) isLogicalNode() {}

func (f Filter) Children() []Node { return []Node{f.Input} }

func (f Filter) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Filter takes one child")
	}
	return Filter{Input: children[0], Predicate: f.Predicate}
}

func (f Filter) Schema() (*schema.Schema, error) { return f.Input.Schema() }

func (f Filter) String() string {
	return fmt.Sprintf("FILTER %s", f.Predicate)
}

// Sort sorts by one or more key columns.
type Sort struct {
	Input   Node
	Keys    []string
	Options []compute.SortOptions
}

func (Sort) isLogicalNode() {}

func (s Sort) Children() []Node { return []Node{s.Input} }

func (s Sort) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Sort takes one child")
	}
	return Sort{Input: children[0], Keys: s.Keys, Options: s.Options}
}

func (s Sort) Schema() (*schema.Schema, error) { return s.Input.Schema() }

func (s Sort) String() string {
	descr := make([]string, len(s.Keys))
	for i, k := range s.Keys {
		dir := "asc"
		if i < len(s.Options) && s.Options[i].Descending {
			dir = "desc"
		}
		descr[i] = fmt.Sprintf("%s %s", k, dir)
	}
	return fmt.Sprintf("SORT [%s]", strings.Join(descr, ", "))
}

// SliceNode restricts to rows [Offset, Offset+Length).
type SliceNode struct {
	Input  Node
	Offset int
	Length int
}

func (SliceNode) isLogicalNode() {}

func (s SliceNode) Children() []Node { return []Node{s.Input} }

func (s SliceNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Slice takes one child")
	}
	return SliceNode{Input: children[0], Offset: s.Offset, Length: s.Length}
}

func (s SliceNode) Schema() (*schema.Schema, error) { return s.Input.Schema() }

func (s SliceNode) String() string {
	return fmt.Sprintf("SLICE offset=%d length=%d", s.Offset, s.Length)
}

// Rename renames a column.
type Rename struct {
	Input Node
	Old   string
	New   string
}

func (Rename) isLogicalNode() {}

func (r Rename) Children() []Node { return []Node{r.Input} }

func (r Rename) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Rename takes one child")
	}
	return Rename{Input: children[0], Old: r.Old, New: r.New}
}

func (r Rename) Schema() (*schema.Schema, error) {
	in, err := r.Input.Schema()
	if err != nil {
		return nil, err
	}
	return in.Rename(r.Old, r.New)
}

func (r Rename) String() string {
	return fmt.Sprintf("RENAME %s -> %s", r.Old, r.New)
}

// Drop removes columns.
type Drop struct {
	Input   Node
	Columns []string
}

func (Drop) isLogicalNode() {}

func (d Drop) Children() []Node { return []Node{d.Input} }

func (d Drop) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: Drop takes one child")
	}
	return Drop{Input: children[0], Columns: d.Columns}
}

func (d Drop) Schema() (*schema.Schema, error) {
	in, err := d.Input.Schema()
	if err != nil {
		return nil, err
	}
	return in.Drop(d.Columns...), nil
}

func (d Drop) String() string {
	return fmt.Sprintf("DROP %v", d.Columns)
}

// inferExprDType returns a best-effort dtype for e given the input schema.
// For arithmetic with mixed operands we return the promoted dtype (e.g. f64
// when combining i64 and f64). The executor still produces the authoritative
// types at runtime.
func inferExprDType(e expr.Expr, in *schema.Schema) (dtype.DType, error) {
	switch n := e.Node().(type) {
	case expr.ColNode:
		f, ok := in.FieldByName(n.Name)
		if !ok {
			return dtype.DType{}, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, n.Name)
		}
		return f.DType, nil
	case expr.LitNode:
		return n.DType, nil
	case expr.AliasNode:
		return inferExprDType(n.Inner, in)
	case expr.CastNode:
		return n.To, nil
	case expr.BinaryNode:
		if isCompareOp(n.Op) || isLogicalOp(n.Op) {
			return dtype.Bool(), nil
		}
		lt, err := inferExprDType(n.Left, in)
		if err != nil {
			return dtype.DType{}, err
		}
		rt, err := inferExprDType(n.Right, in)
		if err != nil {
			return dtype.DType{}, err
		}
		out := promote(lt, rt)
		if n.Op == expr.OpDiv && out.IsInteger() {
			return dtype.Float64(), nil // `/` is true division
		}
		return out, nil
	case expr.UnaryNode:
		if n.Op == expr.OpNot {
			return dtype.Bool(), nil
		}
		return inferExprDType(n.Arg, in)
	case expr.IsNullNode:
		return dtype.Bool(), nil
	case expr.AggNode:
		inner, err := inferExprDType(n.Inner, in)
		if err != nil {
			return dtype.DType{}, err
		}
		switch n.Op {
		case expr.AggMean:
			return dtype.Float64(), nil
		case expr.AggCount, expr.AggNullCount:
			return dtype.Uint32(), nil
		}
		return inner, nil
	case expr.WhenThenNode:
		return inferExprDType(n.Then, in)
	}
	return inferByEmptyEval(e, in), nil
}

// promote returns the dtype both operands would need to share for a binary
// arithmetic operation. For Phase 2 this covers the common int/float cases;
// the optimizer's type-coercion pass may refine this further.
func promote(a, b dtype.DType) dtype.DType {
	if a.Equal(b) {
		return a
	}
	if a.IsFloating() || b.IsFloating() {
		return dtype.Float64()
	}
	if a.IsInteger() && b.IsInteger() {
		return dtype.Int64()
	}
	return a
}

func isCompareOp(op expr.BinaryOp) bool {
	return op == expr.OpEq || op == expr.OpNe ||
		op == expr.OpLt || op == expr.OpLe ||
		op == expr.OpGt || op == expr.OpGe
}

func isLogicalOp(op expr.BinaryOp) bool {
	return op == expr.OpAnd || op == expr.OpOr
}
