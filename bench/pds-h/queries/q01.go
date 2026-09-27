package queries

import (
	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q1: pricing summary report.
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
func Q1(dataDir string) (lazy.LazyFrame, error) {
	price := col("l_extendedprice")
	out := scan(dataDir, "lineitem").
		Filter(col("l_shipdate").Le(date(1998, 9, 2))).
		GroupBy("l_returnflag", "l_linestatus").
		Agg(
			col("l_quantity").Sum().Alias("sum_qty"),
			price.Sum().Alias("sum_base_price"),
			discPrice().Sum().Alias("sum_disc_price"),
			discPrice().Mul(f64(1).Add(col("l_tax"))).Sum().Alias("sum_charge"),
			col("l_quantity").Mean().Alias("avg_qty"),
			price.Mean().Alias("avg_price"),
			col("l_discount").Mean().Alias("avg_disc"),
			col("l_quantity").Count().Alias("count_order"),
		).
		SortBy([]string{"l_returnflag", "l_linestatus"}, []compute.SortOptions{asc(), asc()})
	return out, nil
}
