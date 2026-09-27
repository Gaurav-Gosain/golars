package dataframe

import (
	"context"
)

// Frame-level aggregates. Each returns a one-row DataFrame with one column
// per input column, mirroring polars' DataFrame.sum(), .mean() and
// friends. Output dtypes follow polars exactly: for example sum of i8 is
// i64, sum of bool is u32, mean of f32 stays f32, mean of a date is a
// datetime[us], and reductions that are undefined for a dtype (mean of a
// string, sum of a date) produce a null of the input dtype rather than
// dropping the column.

// SumAll returns the column-wise sums. Mirrors polars' DataFrame.sum().
// Integer sums wrap on overflow like polars.
func (df *DataFrame) SumAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceSum, reduceArgs{})
}

// MeanAll returns the column-wise means. Mirrors polars' DataFrame.mean().
func (df *DataFrame) MeanAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceMean, reduceArgs{})
}

// MinAll returns the column-wise minima. NaN is ignored unless a column
// holds only NaN. Mirrors polars' DataFrame.min().
func (df *DataFrame) MinAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceMin, reduceArgs{})
}

// MaxAll returns the column-wise maxima. Mirrors polars' DataFrame.max().
func (df *DataFrame) MaxAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceMax, reduceArgs{})
}

// StdAll returns the column-wise standard deviations. ddof defaults to 1
// (sample standard deviation), matching polars' DataFrame.std(ddof=1).
func (df *DataFrame) StdAll(ctx context.Context, ddof ...int) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceStd, reduceArgs{ddof: ddofOr1(ddof)})
}

// VarAll returns the column-wise variances. ddof defaults to 1, matching
// polars' DataFrame.var(ddof=1).
func (df *DataFrame) VarAll(ctx context.Context, ddof ...int) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceVar, reduceArgs{ddof: ddofOr1(ddof)})
}

// MedianAll returns the column-wise medians. Mirrors polars'
// DataFrame.median().
func (df *DataFrame) MedianAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceMedian, reduceArgs{})
}

// ProductAll returns the column-wise products. Mirrors polars'
// DataFrame.product(): integers and booleans multiply in i64 (u64 stays
// u64), floats keep their dtype and other dtypes yield a Null column.
func (df *DataFrame) ProductAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceProduct, reduceArgs{})
}

// QuantileAll returns the column-wise q-quantiles using method. An empty
// method means QuantileNearest, polars' default for DataFrame.quantile.
func (df *DataFrame) QuantileAll(ctx context.Context, q float64, method QuantileMethod) (*DataFrame, error) {
	method = method.orDefault()
	if err := method.validate(); err != nil {
		return nil, err
	}
	if q < 0 || q > 1 {
		return nil, errQuantileRange(q)
	}
	return df.reduceFrame(ctx, reduceQuantile, reduceArgs{q: q, method: method})
}

// CountAll returns the number of non-null values per column as u32.
// Mirrors polars' DataFrame.count().
func (df *DataFrame) CountAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceCount, reduceArgs{})
}

// NullCountAll returns the per-column null counts as u32. Mirrors
// polars' DataFrame.null_count().
func (df *DataFrame) NullCountAll(ctx context.Context) (*DataFrame, error) {
	return df.reduceFrame(ctx, reduceNullCount, reduceArgs{})
}

func ddofOr1(ddof []int) int {
	if len(ddof) > 0 {
		return ddof[0]
	}
	return 1
}
