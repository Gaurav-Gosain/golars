package queries

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q19: discounted revenue.
//
//	SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
//	FROM lineitem, part
//	WHERE p_partkey = l_partkey
//	  AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
//	  AND (   (p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
//	           AND l_quantity BETWEEN 1 AND 11 AND p_size BETWEEN 1 AND 5)
//	       OR (p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
//	           AND l_quantity BETWEEN 10 AND 20 AND p_size BETWEEN 1 AND 10)
//	       OR (p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
//	           AND l_quantity BETWEEN 20 AND 30 AND p_size BETWEEN 1 AND 15));
func Q19(dataDir string) (lazy.LazyFrame, error) {
	arm := func(brand string, containers []any, qlo, qhi, smax int64) expr.Expr {
		return col("p_brand").Eq(str(brand)).
			And(col("p_container").IsIn(containers...)).
			And(col("l_quantity").IsBetween(i64(qlo), i64(qhi), expr.ClosedBoth)).
			And(col("p_size").IsBetween(i64(1), i64(smax), expr.ClosedBoth))
	}
	out := scan(dataDir, "part").
		JoinOn(scan(dataDir, "lineitem"), "p_partkey", "l_partkey", inner).
		Filter(col("l_shipmode").IsIn("AIR", "AIR REG")).
		Filter(col("l_shipinstruct").Eq(str("DELIVER IN PERSON"))).
		Filter(
			arm("Brand#12", []any{"SM CASE", "SM BOX", "SM PACK", "SM PKG"}, 1, 11, 5).
				Or(arm("Brand#23", []any{"MED BAG", "MED BOX", "MED PKG", "MED PACK"}, 10, 20, 10)).
				Or(arm("Brand#34", []any{"LG CASE", "LG BOX", "LG PACK", "LG PKG"}, 20, 30, 15)),
		).
		Select(discPrice().Sum().Round(2).Alias("revenue"))
	return out, nil
}
