package difftest

import (
	"fmt"
	"strings"
)

// Describe renders the plan as polars-like Python text for reports.
func (p *Plan) Describe() string {
	var b strings.Builder
	b.WriteString("df")
	for _, op := range p.Ops {
		b.WriteString("\n  ")
		b.WriteString(op.Describe())
	}
	fmt.Fprintf(&b, "\n  # order: %s %v", p.Order.Mode, p.Order.Keys)
	return b.String()
}

// Describe renders one op.
func (op Op) Describe() string {
	exprs := func(es []*Expr) string {
		parts := make([]string, len(es))
		for i, e := range es {
			parts[i] = e.GLR()
		}
		return strings.Join(parts, ", ")
	}
	switch op.Op {
	case "filter":
		return ".filter(" + op.Pred.GLR() + ")"
	case "select", "with_columns":
		return "." + op.Op + "(" + exprs(op.Exprs) + ")"
	case "group_by":
		return fmt.Sprintf(".group_by(%q, maintain_order=%v).agg(%s)", op.Keys, op.Maintain, exprs(op.Exprs))
	case "sort":
		return fmt.Sprintf(".sort(%q, descending=%v, nulls_last=%v)", op.Keys, op.Desc, op.NullsLast)
	case "join":
		return describeJoin(op)
	case "join_asof":
		return fmt.Sprintf(".join_asof(right, on=%q, by=%q, strategy=%q)", op.Keys, op.By, op.Strategy)
	case "unique":
		return fmt.Sprintf(".unique(maintain_order=%v)", op.Maintain)
	case "slice":
		return fmt.Sprintf(".slice(%d, %d)", op.Offset, op.N)
	case "head", "tail":
		return fmt.Sprintf(".%s(%d)", op.Op, op.N)
	case "explode", "drop", "drop_nulls":
		return fmt.Sprintf(".%s(%q)", op.Op, op.Keys)
	case "unpivot":
		return fmt.Sprintf(".unpivot(on=%q, index=%q)", op.On, op.Index)
	case "pivot":
		return fmt.Sprintf(".pivot(on=%q, on_columns=%v, index=%q, values=%q, aggregate_function=%q)", op.On, op.OnValues, op.Index, op.Values, op.Agg)
	case "with_row_index":
		return fmt.Sprintf(".with_row_index(%q, %d)", op.Name, op.Offset)
	case "rename":
		return fmt.Sprintf(".rename({%q: %q})", op.Keys[0], op.Name)
	case "reverse":
		return ".reverse()"
	}
	return "." + op.Op + "(?)"
}

// DescribeFrame renders a frame's schema and, for small frames, its
// values.
func (f *Frame) Describe(maxRows int) string {
	var b strings.Builder
	for _, c := range f.Cols {
		fmt.Fprintf(&b, "  %s: %s chunks=%v", c.Name, c.T.Name(), c.Chunks)
		if len(c.Values) <= maxRows {
			b.WriteString(" = " + FormatValue(c.Values))
		} else {
			fmt.Fprintf(&b, " (%d rows)", len(c.Values))
		}
		b.WriteByte('\n')
	}
	return b.String()
}
