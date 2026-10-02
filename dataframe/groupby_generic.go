package dataframe

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/expr"
)

// GenericAgg evaluates group-by aggregations that the built-in kernels
// cannot handle (anything other than col(name).<agg>()). The eval
// package installs it at init time; dataframe cannot import eval
// directly because eval depends on dataframe. When nil, such
// aggregations return an error.
var GenericAgg func(ctx context.Context, df *DataFrame, keys []string, aggs []expr.Expr, alloc memory.Allocator) (*DataFrame, error)
