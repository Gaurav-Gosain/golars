package queries

import (
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q10: returned item reporting.
//
//	SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//	       c_acctbal, n_name, c_address, c_phone, c_comment
//	FROM customer, orders, lineitem, nation
//	WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
//	  AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
//	  AND l_returnflag = 'R' AND c_nationkey = n_nationkey
//	GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
//	ORDER BY revenue DESC
//	LIMIT 20;
func Q10(dataDir string) (lazy.LazyFrame, error) {
	out := scan(dataDir, "customer").
		JoinOn(scan(dataDir, "orders"), "c_custkey", "o_custkey", inner).
		JoinOn(scan(dataDir, "lineitem"), "o_orderkey", "l_orderkey", inner).
		JoinOn(scan(dataDir, "nation"), "c_nationkey", "n_nationkey", inner).
		Filter(betweenLeft("o_orderdate", date(1993, 10, 1), date(1994, 1, 1))).
		Filter(col("l_returnflag").Eq(str("R"))).
		GroupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment").
		Agg(discPrice().Sum().Round(2).Alias("revenue")).
		Select(col("c_custkey"), col("c_name"), col("revenue"), col("c_acctbal"),
			col("n_name"), col("c_address"), col("c_phone"), col("c_comment")).
		Sort("revenue", true).
		Head(20)
	return out, nil
}
