package lazy

import (
	"context"
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// DynamicGroupNode is a time-window group-by (group_by_dynamic).
type DynamicGroupNode struct {
	Input   Node
	Index   string
	Options dataframe.DynamicGroupOptions
	Aggs    []expr.Expr
}

// RollingGroupNode is a per-row window group-by (rolling).
type RollingGroupNode struct {
	Input   Node
	Index   string
	Options dataframe.RollingGroupOptions
	Aggs    []expr.Expr
}

func (DynamicGroupNode) isLogicalNode() {}
func (RollingGroupNode) isLogicalNode() {}

func (n DynamicGroupNode) Children() []Node { return []Node{n.Input} }
func (n RollingGroupNode) Children() []Node { return []Node{n.Input} }

func (n DynamicGroupNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: DynamicGroupNode takes one child")
	}
	n.Input = children[0]
	return n
}

func (n RollingGroupNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: RollingGroupNode takes one child")
	}
	n.Input = children[0]
	return n
}

func windowSchema(in *schema.Schema, keys []string, bounds bool, index string, aggs []expr.Expr) (*schema.Schema, error) {
	var fields []schema.Field
	for _, k := range keys {
		f, ok := in.FieldByName(k)
		if !ok {
			return nil, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, k)
		}
		fields = append(fields, f)
	}
	idx, ok := in.FieldByName(index)
	if !ok {
		return nil, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, index)
	}
	if bounds {
		bdt := idx.DType
		if bdt.IsDate() {
			bdt = dtype.Datetime(dtype.Microsecond, "")
		}
		fields = append(fields,
			schema.Field{Name: "_lower_boundary", DType: bdt},
			schema.Field{Name: "_upper_boundary", DType: bdt})
	}
	fields = append(fields, idx)
	for _, e := range aggs {
		dt, err := inferExprDType(e, in)
		if err != nil {
			return nil, err
		}
		fields = append(fields, schema.Field{Name: expr.OutputName(e), DType: dt})
	}
	return schema.New(fields...)
}

func (n DynamicGroupNode) Schema() (*schema.Schema, error) {
	in, err := n.Input.Schema()
	if err != nil {
		return nil, err
	}
	return windowSchema(in, n.Options.GroupBy, n.Options.IncludeBoundaries, n.Index, n.Aggs)
}

func (n RollingGroupNode) Schema() (*schema.Schema, error) {
	in, err := n.Input.Schema()
	if err != nil {
		return nil, err
	}
	return windowSchema(in, n.Options.GroupBy, false, n.Index, n.Aggs)
}

func aggList(aggs []expr.Expr) string {
	parts := make([]string, len(aggs))
	for i, e := range aggs {
		parts[i] = e.String()
	}
	return strings.Join(parts, ", ")
}

func (n DynamicGroupNode) String() string {
	return fmt.Sprintf("GROUP_BY_DYNAMIC index=%q every=%s period=%s offset=%s by=%v [%s]",
		n.Index, n.Options.Every, n.Options.Period, n.Options.Offset, n.Options.GroupBy, aggList(n.Aggs))
}

func (n RollingGroupNode) String() string {
	return fmt.Sprintf("ROLLING index=%q period=%s offset=%s by=%v [%s]",
		n.Index, n.Options.Period, n.Options.Offset, n.Options.GroupBy, aggList(n.Aggs))
}

// LazyDynamicGroupBy is a pending GroupByDynamic. Call Agg.
type LazyDynamicGroupBy struct {
	input Node
	index string
	opts  dataframe.DynamicGroupOptions
}

// LazyRollingGroupBy is a pending Rolling. Call Agg.
type LazyRollingGroupBy struct {
	input Node
	index string
	opts  dataframe.RollingGroupOptions
}

// GroupByDynamic groups rows into time windows of the index column
// (polars' group_by_dynamic). See dataframe.DynamicGroupOptions.
//
// # Examples
//
//	lf.GroupByDynamic("ts", dataframe.DynamicGroupOptions{Every: "1h"}).
//	    Agg(expr.Col("v").Sum())
func (lf LazyFrame) GroupByDynamic(index string, o dataframe.DynamicGroupOptions) LazyDynamicGroupBy {
	return LazyDynamicGroupBy{input: lf.plan, index: index, opts: o}
}

// Rolling computes one window per row ending at its index value
// (polars' rolling). See dataframe.RollingGroupOptions.
func (lf LazyFrame) Rolling(index string, o dataframe.RollingGroupOptions) LazyRollingGroupBy {
	return LazyRollingGroupBy{input: lf.plan, index: index, opts: o}
}

// hoistAggs stages complex aggregation inputs as synthetic columns so
// the window executor only sees bare column references.
func hoistAggs(input Node, exprs []expr.Expr) (Node, []expr.Expr) {
	rewritten := make([]expr.Expr, len(exprs))
	var hoisted []expr.Expr
	nextID := 0
	for i, e := range exprs {
		re, h, ok := rewriteAggInput(e, &nextID)
		rewritten[i] = re
		if ok {
			hoisted = append(hoisted, h)
		}
	}
	if len(hoisted) > 0 {
		input = WithColumns{Input: input, Exprs: hoisted}
	}
	return input, rewritten
}

// Agg closes the dynamic group-by.
func (g LazyDynamicGroupBy) Agg(exprs ...expr.Expr) LazyFrame {
	input, aggs := hoistAggs(g.input, exprs)
	return LazyFrame{plan: DynamicGroupNode{Input: input, Index: g.index, Options: g.opts, Aggs: aggs}}
}

// Agg closes the rolling group-by.
func (g LazyRollingGroupBy) Agg(exprs ...expr.Expr) LazyFrame {
	input, aggs := hoistAggs(g.input, exprs)
	return LazyFrame{plan: RollingGroupNode{Input: input, Index: g.index, Options: g.opts, Aggs: aggs}}
}

// executeTemporalGroup runs DynamicGroupNode and RollingGroupNode. ok is
// false for any other node.
func executeTemporalGroup(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, bool, error) {
	switch node := n.(type) {
	case DynamicGroupNode:
		input, err := executeNode(ctx, cfg, node.Input)
		if err != nil {
			return nil, true, err
		}
		defer input.Release()
		out, err := input.GroupByDynamic(node.Index, node.Options).Agg(ctx, node.Aggs, dataframe.WithGroupByAllocator(cfg.alloc))
		return out, true, err
	case RollingGroupNode:
		input, err := executeNode(ctx, cfg, node.Input)
		if err != nil {
			return nil, true, err
		}
		defer input.Release()
		out, err := input.Rolling(node.Index, node.Options).Agg(ctx, node.Aggs, dataframe.WithGroupByAllocator(cfg.alloc))
		return out, true, err
	}
	return nil, false, nil
}
