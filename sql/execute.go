package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Execute compiles the statement into a lazy plan on top of df and
// collects the result. Caller owns the returned DataFrame.
func (s *Statement) Execute(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	lf := lazy.FromDataFrame(df)
	// WHERE: apply predicate before projection so groupby / agg sees
	// the filtered rows.
	if !s.Where.Empty && predHasContent(s.Where.Tree) {
		e, err := predToExpr(s.Where.Tree)
		if err != nil {
			return nil, err
		}
		lf = lf.Filter(e)
	}
	// GROUP BY ... aggregations. GroupBy().Agg() emits the group-key
	// columns automatically; we only pass aggregate-valued
	// projections here. Pure column projections that name a group
	// key are allowed but contribute nothing to the agg list.
	if len(s.GroupBy) > 0 {
		aggOnly, err := aggOnlyExprs(s.Projections)
		if err != nil {
			return nil, err
		}
		lf = lf.GroupBy(s.GroupBy...).Agg(aggOnly...)
	} else if hasAggs(s.Projections) {
		// Implicit full-table aggregate (no GROUP BY).
		aggs, err := aggExprs(s.Projections)
		if err != nil {
			return nil, err
		}
		// Use a constant groupby over a synthetic literal column.
		lf = lf.Select(aggs...)
	} else {
		// Simple projection: list of columns.
		if !isStarOnly(s.Projections) {
			selects := make([]expr.Expr, 0, len(s.Projections))
			for _, p := range s.Projections {
				if p.Kind == ProjCol {
					e := expr.Col(p.Col)
					if p.Name != p.Col {
						e = e.Alias(p.Name)
					}
					selects = append(selects, e)
				}
			}
			if len(selects) > 0 {
				lf = lf.Select(selects...)
			}
		}
	}
	// DISTINCT (post aggregation/projection).
	if s.Distinct {
		lf = lf.Unique()
	}
	// ORDER BY: one multi-key sort, first key primary. Like polars SQL
	// (and PostgreSQL), nulls go last for ASC and first for DESC.
	if len(s.OrderBy) > 0 {
		keys := make([]string, len(s.OrderBy))
		opts := make([]compute.SortOptions, len(s.OrderBy))
		for i, oi := range s.OrderBy {
			keys[i] = oi.Col
			opts[i] = compute.SortOptions{Descending: oi.Descending, Nulls: compute.NullsLast}
			if oi.Descending {
				opts[i].Nulls = compute.NullsFirst
			}
		}
		lf = lf.SortBy(keys, opts)
	}
	// LIMIT.
	if s.Limit > 0 {
		lf = lf.Limit(s.Limit)
	}
	return lf.Collect(ctx)
}

func predHasContent(n predNode) bool {
	if n.Leaf {
		return n.Col != ""
	}
	return n.Left != nil || n.Right != nil
}

func predToExpr(n predNode) (expr.Expr, error) {
	if n.Leaf {
		c := expr.Col(n.Col)
		lit := expr.Lit(n.Val)
		switch n.Op {
		case "=", "==":
			return c.Eq(lit), nil
		case "!=", "<>":
			return c.Ne(lit), nil
		case "<":
			return c.Lt(lit), nil
		case "<=":
			return c.Le(lit), nil
		case ">":
			return c.Gt(lit), nil
		case ">=":
			return c.Ge(lit), nil
		}
		return expr.Expr{}, fmt.Errorf("sql: unknown operator %q", n.Op)
	}
	left, err := predToExpr(*n.Left)
	if err != nil {
		return expr.Expr{}, err
	}
	right, err := predToExpr(*n.Right)
	if err != nil {
		return expr.Expr{}, err
	}
	if strings.EqualFold(n.Combine, "or") {
		return left.Or(right), nil
	}
	return left.And(right), nil
}

func aggExprs(projs []Projection) ([]expr.Expr, error) {
	out := make([]expr.Expr, 0, len(projs))
	for _, p := range projs {
		switch p.Kind {
		case ProjCol:
			out = append(out, expr.Col(p.Col).Alias(p.Name))
		case ProjAgg:
			var e expr.Expr
			col := expr.Col(p.Agg.Col)
			switch strings.ToLower(p.Agg.Op) {
			case "sum":
				e = col.Sum()
			case "min":
				e = col.Min()
			case "max":
				e = col.Max()
			case "avg", "mean":
				e = col.Mean()
			case "count":
				e = col.Count()
			case "first":
				e = col.First()
			case "last":
				e = col.Last()
			default:
				return nil, fmt.Errorf("sql: unknown aggregate %q", p.Agg.Op)
			}
			out = append(out, e.Alias(p.Name))
		}
	}
	return out, nil
}

// aggOnlyExprs returns just the aggregate-shaped projections.
// Group-key columns are implicit in GroupBy.Agg and would otherwise
// error as non-aggregation expressions.
func aggOnlyExprs(projs []Projection) ([]expr.Expr, error) {
	out := make([]expr.Expr, 0, len(projs))
	for _, p := range projs {
		if p.Kind != ProjAgg {
			continue
		}
		col := expr.Col(p.Agg.Col)
		var e expr.Expr
		switch strings.ToLower(p.Agg.Op) {
		case "sum":
			e = col.Sum()
		case "min":
			e = col.Min()
		case "max":
			e = col.Max()
		case "avg", "mean":
			e = col.Mean()
		case "count":
			e = col.Count()
		case "first":
			e = col.First()
		case "last":
			e = col.Last()
		default:
			return nil, fmt.Errorf("sql: unknown aggregate %q", p.Agg.Op)
		}
		out = append(out, e.Alias(p.Name))
	}
	return out, nil
}

func hasAggs(projs []Projection) bool {
	for _, p := range projs {
		if p.Kind == ProjAgg {
			return true
		}
	}
	return false
}

func isStarOnly(projs []Projection) bool {
	for _, p := range projs {
		if p.Kind != ProjStar {
			return false
		}
	}
	return len(projs) > 0
}
