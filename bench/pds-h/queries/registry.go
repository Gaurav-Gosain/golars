// Package queries holds the golars implementations of the PDS-H /
// TPC-H query set. Each query is a function that takes a data
// directory (containing the per-table parquet files) and returns a
// LazyFrame describing the query; the runner collects + times it.
//
// Intentionally mirrors upstream polars-benchmark's queries/polars/
// layout so diffs between the two runners are a direct AST-level
// comparison.
package queries

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/lazy"
)

// QueryFn builds a LazyFrame for a TPC-H query against tables loaded
// from dataDir. Path convention: dataDir/<table>.parquet, where
// <table> is lowercase (lineitem, orders, customer, ...).
type QueryFn func(dataDir string) (lazy.LazyFrame, error)

// registry indexes queries by their 1-based PDS-H number.
var registry = map[int]QueryFn{
	1:  Q1,
	3:  Q3,
	4:  Q4,
	5:  Q5,
	6:  Q6,
	10: Q10,
	12: Q12,
	14: Q14,
	19: Q19,
}

// Get returns the builder for query number n, or an error when the
// query isn't implemented yet.
func Get(n int) (QueryFn, error) {
	fn, ok := registry[n]
	if !ok {
		return nil, fmt.Errorf("q%d: not implemented", n)
	}
	return fn, nil
}

// Available lists the query numbers we can run, in ascending order.
func Available() []int {
	out := make([]int, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	// Small registry; insertion sort keeps it tidy without importing sort.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
