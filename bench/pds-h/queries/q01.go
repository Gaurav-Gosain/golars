package queries

import (
	"path/filepath"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q1: pricing summary report.
//
// Standard TPC-H:
//
//	SELECT l_returnflag, l_linestatus,
//	       SUM(l_quantity), SUM(l_extendedprice),
//	       SUM(l_extendedprice * (1 - l_discount)),
//	       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//	       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
//	       COUNT(*)
//	FROM lineitem
//	WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
//	GROUP BY l_returnflag, l_linestatus
//	ORDER BY l_returnflag, l_linestatus;
//
// TODO: the `l_shipdate <= …` predicate is stubbed out until golars
// supports date-literal comparisons (compute side can already compare
// Date32 values; the Expr layer is missing a LitDate). Landing that
// would cut ~2% of rows on SF=1, affecting the timing only marginally
// but making the answer set match upstream's data/answers/q1.parquet.
func Q1(dataDir string) (lazy.LazyFrame, error) {
	lineitem := golars.ScanParquet(filepath.Join(dataDir, "lineitem.parquet"))

	// Common subexpressions the aggregations reuse. We don't rely on
	// golars having CSE (common subexpression elimination) across
	// aggs, but the optimiser flattens the arithmetic per-agg so it's
	// fine as separate Exprs.
	price := expr.Col("l_extendedprice")
	discount := expr.Col("l_discount")
	tax := expr.Col("l_tax")

	oneMinusDisc := expr.LitFloat64(1.0).Sub(discount)
	onePlusTax := expr.LitFloat64(1.0).Add(tax)
	discPrice := price.Mul(oneMinusDisc)
	charge := discPrice.Mul(onePlusTax)

	// The lazy planner auto-hoists complex agg inputs into a
	// WithColumns stage, so we can write the arithmetic inline the
	// same way polars does. The rewrite synthesises `__agg_i`
	// columns behind the scenes.
	out := lineitem.
		GroupBy("l_returnflag", "l_linestatus").
		Agg(
			expr.Col("l_quantity").Sum().Alias("sum_qty"),
			price.Sum().Alias("sum_base_price"),
			discPrice.Sum().Alias("sum_disc_price"),
			charge.Sum().Alias("sum_charge"),
			expr.Col("l_quantity").Mean().Alias("avg_qty"),
			price.Mean().Alias("avg_price"),
			discount.Mean().Alias("avg_disc"),
			expr.Col("l_quantity").Count().Alias("count_order"),
		).
		SortBy([]string{"l_returnflag", "l_linestatus"},
			[]compute.SortOptions{{}, {}})

	return out, nil
}
