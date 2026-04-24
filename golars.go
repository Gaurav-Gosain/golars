// Package golars is the top-level facade that re-exports the most
// common types and helpers from the sub-packages so that a single
//
//	import "github.com/Gaurav-Gosain/golars"
//
// brings DataFrame, Series, expression helpers, and I/O entry points
// into scope. The sub-packages stay the canonical homes; this file
// is sugar so users don't need to remember which package holds what.
//
// polars users will recognise most of the names: `golars.Col`,
// `golars.Lit`, `golars.Sum`, `golars.ReadCSV`, `golars.Concat`, etc.
package golars

import (
	"context"
	"io"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// ----- Core type aliases -----
// So users can type `*golars.DataFrame` instead of importing the
// sub-packages. The aliases don't copy anything; they are the same
// types.

type (
	// DataFrame is the eager columnar table type. See package
	// github.com/Gaurav-Gosain/golars/dataframe for the full API.
	DataFrame = dataframe.DataFrame

	// Series is a named, chunked column. See package
	// github.com/Gaurav-Gosain/golars/series.
	Series = series.Series

	// LazyFrame is the deferred-execution pipeline handle. See package
	// github.com/Gaurav-Gosain/golars/lazy.
	LazyFrame = lazy.LazyFrame

	// Expr is the expression AST used to describe computations on
	// columns. Built via Col, Lit, When, and fluent methods.
	Expr = expr.Expr

	// DType is the logical dtype descriptor.
	DType = dtype.DType

	// JoinType enumerates join kinds: golars.InnerJoin,
	// golars.LeftJoin, golars.CrossJoin.
	JoinType = dataframe.JoinType
)

// Join-type constants surfaced at the top level.
const (
	InnerJoin = dataframe.InnerJoin
	LeftJoin  = dataframe.LeftJoin
	CrossJoin = dataframe.CrossJoin
)

// ----- Series / DataFrame constructors -----

// NewDataFrame builds a DataFrame from the given Series columns.
// Equivalent to dataframe.New.
func NewDataFrame(cols ...*Series) (*DataFrame, error) { return dataframe.New(cols...) }

// FromMap builds a DataFrame from a column-name → slice map. order
// determines output column order (nil → alphabetical). Supported
// slice types mirror dataframe.FromMap.
func FromMap(data map[string]any, order []string) (*DataFrame, error) {
	return dataframe.FromMap(data, order)
}

// Concat vertically stacks DataFrames of the same schema.
func Concat(frames ...*DataFrame) (*DataFrame, error) { return dataframe.Concat(frames...) }

// FromInt64 / FromFloat64 / FromString / FromBool are shortcuts to
// construct a Series from a native Go slice without remembering which
// sub-package exports each builder.
func FromInt64(name string, v []int64, valid []bool) (*Series, error) {
	return series.FromInt64(name, v, valid)
}
func FromInt32(name string, v []int32, valid []bool) (*Series, error) {
	return series.FromInt32(name, v, valid)
}
func FromFloat64(name string, v []float64, valid []bool) (*Series, error) {
	return series.FromFloat64(name, v, valid)
}
func FromFloat32(name string, v []float32, valid []bool) (*Series, error) {
	return series.FromFloat32(name, v, valid)
}
func FromString(name string, v []string, valid []bool) (*Series, error) {
	return series.FromString(name, v, valid)
}
func FromBool(name string, v []bool, valid []bool) (*Series, error) {
	return series.FromBool(name, v, valid)
}

// Lazy wraps df as a LazyFrame. polars-style alias for
// `lazy.FromDataFrame(df)`.
func Lazy(df *DataFrame) LazyFrame { return lazy.FromDataFrame(df) }

// SelectExpr evaluates the given expressions against df and returns
// a new DataFrame holding their outputs in order. Equivalent to
// `golars.Lazy(df).Select(exprs...).Collect(ctx)` but reads more
// naturally for one-shot eager use.
func SelectExpr(ctx context.Context, df *DataFrame, exprs ...Expr) (*DataFrame, error) {
	return lazy.FromDataFrame(df).Select(exprs...).Collect(ctx)
}

// WithColumnsExpr evaluates the given expressions and attaches their
// outputs to df as additional columns, returning a new DataFrame.
// Equivalent to `golars.Lazy(df).WithColumns(exprs...).Collect(ctx)`.
func WithColumnsExpr(ctx context.Context, df *DataFrame, exprs ...Expr) (*DataFrame, error) {
	return lazy.FromDataFrame(df).WithColumns(exprs...).Collect(ctx)
}

// ----- Expression builders -----

// Col references a named column.
func Col(name string) Expr { return expr.Col(name) }

// Lit builds a literal with an inferred dtype.
func Lit(v any) Expr { return expr.Lit(v) }

// LitInt64/LitFloat64/LitBool/LitString are typed literal builders.
func LitInt64(v int64) Expr     { return expr.LitInt64(v) }
func LitFloat64(v float64) Expr { return expr.LitFloat64(v) }
func LitBool(v bool) Expr       { return expr.LitBool(v) }
func LitString(v string) Expr   { return expr.LitString(v) }

// When starts a when/then/otherwise conditional expression.
func When(pred Expr) expr.WhenBuilder { return expr.When(pred) }

// Sum / Mean / Min / Max / Count / First / Last / Median / Std / Var
// are sugar for `golars.Col(name).<agg>()`. They match polars' top-
// level helpers: `pl.sum("a")` becomes `golars.Sum("a")`.
func Sum(col string) Expr       { return expr.Col(col).Sum() }
func Mean(col string) Expr      { return expr.Col(col).Mean() }
func Min(col string) Expr       { return expr.Col(col).Min() }
func Max(col string) Expr       { return expr.Col(col).Max() }
func Count(col string) Expr     { return expr.Col(col).Count() }
func First(col string) Expr     { return expr.Col(col).First() }
func Last(col string) Expr      { return expr.Col(col).Last() }
func Median(col string) Expr    { return expr.Col(col).Median() }
func Std(col string) Expr       { return expr.Col(col).Std() }
func Var(col string) Expr       { return expr.Col(col).Var() }
func NullCount(col string) Expr { return expr.Col(col).NullCount() }

// FillNan is sugar for Col(col).FillNan(v).
func FillNan(col string, v float64) Expr { return expr.Col(col).FillNan(v) }

// ForwardFill is sugar for Col(col).ForwardFill(limit).
func ForwardFill(col string, limit int) Expr { return expr.Col(col).ForwardFill(limit) }

// BackwardFill is sugar for Col(col).BackwardFill(limit).
func BackwardFill(col string, limit int) Expr { return expr.Col(col).BackwardFill(limit) }

// Coalesce returns the first non-null value across exprs row-wise.
func Coalesce(exprs ...Expr) Expr { return expr.Coalesce(exprs...) }

// ConcatStr concatenates string forms of exprs using sep row-wise.
func ConcatStr(sep string, exprs ...Expr) Expr { return expr.ConcatStr(sep, exprs...) }

// IntRange produces an int64 sequence [start, end) with step.
func IntRange(start, end, step int64) Expr { return expr.IntRange(start, end, step) }

// Ones produces a float64 Series of length n filled with 1.0.
func Ones(n int) Expr { return expr.Ones(n) }

// Zeros produces a float64 Series of length n filled with 0.0.
func Zeros(n int) Expr { return expr.Zeros(n) }

// ----- I/O shortcuts -----

// ReadCSV reads a CSV file into a [DataFrame].
//
// Inference runs on the header + first rows: numeric columns become
// i64/f64, text becomes utf8. Pass [iocsv.WithNullValues] to treat
// specific tokens as nulls (empty string is a common choice to
// match polars defaults).
//
// Uses [context.Background]; for cancellable reads call
// [iocsv.ReadFile] directly.
//
// Example:
//
//	df, err := golars.ReadCSV("people.csv",
//	    iocsv.WithNullValues(""),          // empty field -> null
//	    iocsv.WithDelimiter(';'),          // European CSV
//	)
//	if err != nil { log.Fatal(err) }
//	defer df.Release()
//	fmt.Println(df.Schema())
func ReadCSV(path string, opts ...iocsv.Option) (*DataFrame, error) {
	return iocsv.ReadFile(context.Background(), path, opts...)
}

// ReadCSVReader reads a CSV stream from an [io.Reader]. Useful for
// http bodies, stdin, or embedded fixtures via [strings.NewReader].
//
// Example:
//
//	resp, _ := http.Get("https://example.com/data.csv")
//	defer resp.Body.Close()
//	df, err := golars.ReadCSVReader(resp.Body)
func ReadCSVReader(r io.Reader, opts ...iocsv.Option) (*DataFrame, error) {
	return iocsv.Read(context.Background(), r, opts...)
}

// WriteCSV serialises a [DataFrame] to a CSV file at path.
//
// Default writer emits a header row and quotes only fields that
// contain the delimiter, a quote, or a newline. Pass
// [iocsv.WithDelimiter] to use TSV (`'\t'`) or regional separators.
func WriteCSV(df *DataFrame, path string, opts ...iocsv.Option) error {
	return iocsv.WriteFile(context.Background(), path, df, opts...)
}

// ReadParquet reads a Parquet file by path.
func ReadParquet(path string, opts ...parquet.Option) (*DataFrame, error) {
	return parquet.ReadFile(context.Background(), path, opts...)
}

// WriteParquet writes df to a Parquet file.
func WriteParquet(df *DataFrame, path string, opts ...parquet.Option) error {
	return parquet.WriteFile(context.Background(), path, df, opts...)
}

// ReadIPC reads an Arrow IPC file.
func ReadIPC(path string, opts ...ipc.Option) (*DataFrame, error) {
	return ipc.ReadFile(context.Background(), path, opts...)
}

// WriteIPC writes df to an Arrow IPC file.
func WriteIPC(df *DataFrame, path string, opts ...ipc.Option) error {
	return ipc.WriteFile(context.Background(), path, df, opts...)
}

// ReadJSON reads a JSON (array-of-object) file.
func ReadJSON(path string, opts ...iojson.Option) (*DataFrame, error) {
	return iojson.ReadFile(context.Background(), path, opts...)
}

// ReadNDJSON reads a newline-delimited JSON file.
func ReadNDJSON(path string, opts ...iojson.Option) (*DataFrame, error) {
	return iojson.ReadNDJSONFile(context.Background(), path, opts...)
}

// WriteJSON writes df as an array of objects to a JSON file.
func WriteJSON(df *DataFrame, path string) error {
	return iojson.WriteFile(context.Background(), path, df)
}

// WriteNDJSON writes df as newline-delimited JSON to a file.
func WriteNDJSON(df *DataFrame, path string) error {
	return iojson.WriteNDJSONFile(context.Background(), path, df)
}

// NewIPCStreamWriter wraps w as a streaming Arrow IPC writer. Use for
// multi-batch pipelines or cross-language streaming over a socket.
func NewIPCStreamWriter(w io.Writer, schemaFrame *DataFrame, opts ...ipc.Option) (*ipc.StreamWriter, error) {
	return ipc.NewStreamWriter(w, schemaFrame, opts...)
}

// NewIPCStreamReader wraps r as a streaming Arrow IPC reader.
func NewIPCStreamReader(r io.Reader, opts ...ipc.Option) (*ipc.StreamReader, error) {
	return ipc.NewStreamReader(r, opts...)
}

// ----- Lazy I/O scanners -----
//
// Scan* returns a LazyFrame that opens the file only on Collect,
// letting the optimiser push projections/filters into the reader.
// Matches polars' pl.scan_csv / pl.scan_parquet family.

// ScanCSV returns a LazyFrame backed by a CSV file on disk.
func ScanCSV(path string, opts ...iocsv.Option) LazyFrame {
	return iocsv.Scan(path, opts...)
}

// ScanParquet returns a LazyFrame backed by a Parquet file.
func ScanParquet(path string, opts ...parquet.Option) LazyFrame {
	return parquet.Scan(path, opts...)
}

// ScanIPC returns a LazyFrame backed by an Arrow IPC file.
func ScanIPC(path string, opts ...ipc.Option) LazyFrame {
	return ipc.Scan(path, opts...)
}

// ScanJSON returns a LazyFrame backed by a JSON array-of-object file.
func ScanJSON(path string, opts ...iojson.Option) LazyFrame {
	return iojson.Scan(path, opts...)
}

// ScanNDJSON returns a LazyFrame backed by a newline-delimited JSON file.
func ScanNDJSON(path string, opts ...iojson.Option) LazyFrame {
	return iojson.ScanNDJSON(path, opts...)
}

// ----- Horizontal aggregates -----
//
// Mirror polars' sum_horizontal / min_horizontal / max_horizontal /
// mean_horizontal / all_horizontal / any_horizontal. Each returns a
// Series equal in length to the input frame, reducing across the
// selected columns (all numeric/bool when cols is empty).

// SumHorizontal returns a Series of row-wise sums. See
// DataFrame.SumHorizontal for details.
func SumHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.SumHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// MeanHorizontal returns a Series of row-wise means.
func MeanHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.MeanHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// MinHorizontal returns a Series of row-wise minima.
func MinHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.MinHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// MaxHorizontal returns a Series of row-wise maxima.
func MaxHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.MaxHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// AllHorizontal returns a boolean Series that is true iff every
// boolean column is true at that row.
func AllHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.AllHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// AnyHorizontal is the disjunctive counterpart of AllHorizontal.
func AnyHorizontal(ctx context.Context, df *DataFrame, cols ...string) (*Series, error) {
	return df.AnyHorizontal(ctx, dataframe.IgnoreNulls, cols...)
}

// ----- dtype helpers -----

// DTypes namespace: golars.Int64(), golars.String(), etc., matching
// polars' `pl.Int64`. Calling the function returns a DType value.
var (
	Int64DType   = dtype.Int64
	Int32DType   = dtype.Int32
	Int16DType   = dtype.Int16
	Int8DType    = dtype.Int8
	UInt64DType  = dtype.Uint64
	UInt32DType  = dtype.Uint32
	UInt16DType  = dtype.Uint16
	UInt8DType   = dtype.Uint8
	Float64DType = dtype.Float64
	Float32DType = dtype.Float32
	StringDType  = dtype.String
	BoolDType    = dtype.Bool
	DateDType    = dtype.Date
	BinaryDType  = dtype.Binary
)

// ----- context-friendly aliases -----

// Filter runs a compute-level filter on a series + mask. Convenience
// that avoids importing compute for one-line use.
func Filter(ctx context.Context, s *Series, mask *Series) (*Series, error) {
	return compute.Filter(ctx, s, mask)
}

// Take picks the rows at indices (see compute.Take).
func Take(ctx context.Context, s *Series, indices []int) (*Series, error) {
	return compute.Take(ctx, s, indices)
}
