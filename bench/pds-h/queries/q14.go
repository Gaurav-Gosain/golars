package queries

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Q14: promotion effect.
//
//	SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
//	                         THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
//	       / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
//	FROM lineitem, part
//	WHERE l_partkey = p_partkey
//	  AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01';
func Q14(dataDir string) (lazy.LazyFrame, error) {
	promo := expr.When(col("p_type").Str().ContainsRegex("PROMO*")).
		Then(discPrice()).
		Otherwise(f64(0))
	out := scan(dataDir, "lineitem").
		JoinOn(scan(dataDir, "part"), "l_partkey", "p_partkey", inner).
		Filter(betweenLeft("l_shipdate", date(1995, 9, 1), date(1995, 10, 1))).
		Select(f64(100).Mul(promo.Sum()).Div(discPrice().Sum()).Round(2).Alias("promo_revenue"))
	return out, nil
}
