package queries

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q4: order priority checking.
//
//	SELECT o_orderpriority, COUNT(*) AS order_count
//	FROM orders
//	WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'
//	  AND EXISTS (SELECT * FROM lineitem
//	              WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
//	GROUP BY o_orderpriority
//	ORDER BY o_orderpriority;
//
// polars-benchmark writes the EXISTS as a join followed by
// unique(subset=[o_orderpriority, l_orderkey]). golars has no unique
// with a subset on LazyFrame, so the pair is selected first and made
// unique; nothing after it reads other columns, so the answer is the
// same.
func Q4(dataDir string) (lazy.LazyFrame, error) {
	out := scan(dataDir, "lineitem").
		JoinOn(scan(dataDir, "orders"), "l_orderkey", "o_orderkey", inner).
		Filter(betweenLeft("o_orderdate", date(1993, 7, 1), date(1993, 10, 1))).
		Filter(col("l_commitdate").Lt(col("l_receiptdate"))).
		Select(col("o_orderpriority"), col("l_orderkey")).
		Unique().
		GroupBy("o_orderpriority").
		Agg(expr.Len().Alias("order_count")).
		Sort("o_orderpriority", false)
	return out, nil
}
