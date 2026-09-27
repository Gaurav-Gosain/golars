package queries

import (
	"path/filepath"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// scan opens dataDir/<table>.parquet lazily.
func scan(dataDir, table string) lazy.LazyFrame {
	return golars.ScanParquet(filepath.Join(dataDir, table+".parquet"))
}

var (
	col  = expr.Col
	date = expr.LitDate
	str  = expr.LitString
	f64  = expr.LitFloat64
	i64  = expr.LitInt64
)

const inner = dataframe.InnerJoin

// discPrice is l_extendedprice * (1 - l_discount), the revenue term
// most queries share.
func discPrice() expr.Expr {
	return col("l_extendedprice").Mul(f64(1).Sub(col("l_discount")))
}

// between is polars is_between with closed="left" (lo <= x < hi).
func betweenLeft(c string, lo, hi expr.Expr) expr.Expr {
	return col(c).IsBetween(lo, hi, expr.ClosedLeft)
}

func asc() compute.SortOptions  { return compute.SortOptions{} }
func desc() compute.SortOptions { return compute.SortOptions{Descending: true} }
