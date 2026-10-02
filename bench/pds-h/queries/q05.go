package queries

import (
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q5: local supplier volume.
//
//	SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
//	FROM customer, orders, lineitem, supplier, nation, region
//	WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
//	  AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
//	  AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
//	  AND r_name = 'ASIA'
//	  AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'
//	GROUP BY n_name
//	ORDER BY revenue DESC;
//
// polars joins supplier on the key pair (l_suppkey, n_nationkey).
// golars joins on one key, so it joins on the supplier key and keeps
// the rows whose supplier nation matches, which is the same inner join.
func Q5(dataDir string) (lazy.LazyFrame, error) {
	out := scan(dataDir, "region").
		JoinOn(scan(dataDir, "nation"), "r_regionkey", "n_regionkey", inner).
		JoinOn(scan(dataDir, "customer"), "n_nationkey", "c_nationkey", inner).
		JoinOn(scan(dataDir, "orders"), "c_custkey", "o_custkey", inner).
		JoinOn(scan(dataDir, "lineitem"), "o_orderkey", "l_orderkey", inner).
		JoinOn(scan(dataDir, "supplier"), "l_suppkey", "s_suppkey", inner).
		Filter(col("n_nationkey").Eq(col("s_nationkey"))).
		Filter(col("r_name").Eq(str("ASIA"))).
		Filter(betweenLeft("o_orderdate", date(1994, 1, 1), date(1995, 1, 1))).
		WithColumns(discPrice().Alias("revenue")).
		GroupBy("n_name").
		Agg(col("revenue").Sum().Alias("revenue")).
		Sort("revenue", true)
	return out, nil
}
