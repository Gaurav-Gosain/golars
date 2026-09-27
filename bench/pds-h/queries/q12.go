package queries

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q12: shipping modes and order priority.
//
//	SELECT l_shipmode,
//	       SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END),
//	       SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END)
//	FROM orders, lineitem
//	WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
//	  AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
//	  AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'
//	GROUP BY l_shipmode
//	ORDER BY l_shipmode;
func Q12(dataDir string) (lazy.LazyFrame, error) {
	high := col("o_orderpriority").IsIn("1-URGENT", "2-HIGH")
	out := scan(dataDir, "orders").
		JoinOn(scan(dataDir, "lineitem"), "o_orderkey", "l_orderkey", inner).
		Filter(col("l_shipmode").IsIn("MAIL", "SHIP")).
		Filter(col("l_commitdate").Lt(col("l_receiptdate"))).
		Filter(col("l_shipdate").Lt(col("l_commitdate"))).
		Filter(betweenLeft("l_receiptdate", date(1994, 1, 1), date(1995, 1, 1))).
		WithColumns(
			expr.When(high).Then(i64(1)).Otherwise(i64(0)).Alias("high_line_count"),
			expr.When(high.Not()).Then(i64(1)).Otherwise(i64(0)).Alias("low_line_count"),
		).
		GroupBy("l_shipmode").
		Agg(col("high_line_count").Sum().Alias("high_line_count"), col("low_line_count").Sum().Alias("low_line_count")).
		Sort("l_shipmode", false)
	return out, nil
}
