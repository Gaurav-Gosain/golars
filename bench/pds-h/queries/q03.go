package queries

import (
	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q3: shipping priority.
//
//	SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//	       o_orderdate, o_shippriority
//	FROM customer, orders, lineitem
//	WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
//	  AND l_orderkey = o_orderkey
//	  AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
//	GROUP BY l_orderkey, o_orderdate, o_shippriority
//	ORDER BY revenue DESC, o_orderdate
//	LIMIT 10;
func Q3(dataDir string) (lazy.LazyFrame, error) {
	cutoff := date(1995, 3, 15)
	out := scan(dataDir, "customer").
		Filter(col("c_mktsegment").Eq(str("BUILDING"))).
		JoinOn(scan(dataDir, "orders"), "c_custkey", "o_custkey", inner).
		JoinOn(scan(dataDir, "lineitem"), "o_orderkey", "l_orderkey", inner).
		Filter(col("o_orderdate").Lt(cutoff)).
		Filter(col("l_shipdate").Gt(cutoff)).
		WithColumns(discPrice().Alias("revenue")).
		GroupBy("o_orderkey", "o_orderdate", "o_shippriority").
		Agg(col("revenue").Sum().Alias("revenue")).
		Select(col("o_orderkey").Alias("l_orderkey"), col("revenue"), col("o_orderdate"), col("o_shippriority")).
		SortBy([]string{"revenue", "o_orderdate"}, []compute.SortOptions{desc(), asc()}).
		Head(10)
	return out, nil
}
