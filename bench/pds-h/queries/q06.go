package queries

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q6: forecasting revenue change.
//
//	SELECT SUM(l_extendedprice * l_discount) AS revenue
//	FROM lineitem
//	WHERE l_shipdate >= DATE '1994-01-01'
//	  AND l_shipdate <  DATE '1995-01-01'
//	  AND l_discount BETWEEN 0.05 AND 0.07
//	  AND l_quantity < 24;
func Q6(dataDir string) (lazy.LazyFrame, error) {
	out := scan(dataDir, "lineitem").
		Filter(betweenLeft("l_shipdate", date(1994, 1, 1), date(1995, 1, 1))).
		Filter(col("l_discount").IsBetween(f64(0.05), f64(0.07), expr.ClosedBoth)).
		Filter(col("l_quantity").Lt(i64(24))).
		Select(col("l_extendedprice").Mul(col("l_discount")).Sum().Alias("revenue"))
	return out, nil
}
