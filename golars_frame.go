package golars

import (
	"context"
	"os"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/sql"
)

// DataFrame and LazyFrame conveniences that cannot live on the types
// themselves because of import direction: the io and sql packages import
// dataframe and lazy, so methods like DataFrame.write_csv or
// LazyFrame.sink_parquet are provided here as functions.

// Additional join and group-by aliases surfaced at the top level.
type (
	// AsofOptions configures JoinAsof. See dataframe.AsofOptions.
	AsofOptions = dataframe.AsofOptions
	// QuantileMethod selects quantile interpolation.
	QuantileMethod = dataframe.QuantileMethod
)

// WriteIPCStream writes df to path in the Arrow IPC streaming format.
// Mirrors polars' DataFrame.write_ipc_stream.
func WriteIPCStream(df *DataFrame, path string, opts ...ipc.Option) error {
	return ipc.WriteStreamFile(context.Background(), path, []*dataframe.DataFrame{df}, opts...)
}

// SinkCSV executes lf and writes the result to a CSV file. Mirrors
// polars' LazyFrame.sink_csv.
func SinkCSV(ctx context.Context, lf LazyFrame, path string, opts ...iocsv.Option) error {
	return lf.Sink(ctx, func(ctx context.Context, df *dataframe.DataFrame) error {
		return iocsv.WriteFile(ctx, path, df, opts...)
	})
}

// SinkParquet executes lf and writes the result to a Parquet file.
// Mirrors polars' LazyFrame.sink_parquet.
func SinkParquet(ctx context.Context, lf LazyFrame, path string, opts ...parquet.Option) error {
	return lf.Sink(ctx, func(ctx context.Context, df *dataframe.DataFrame) error {
		return parquet.WriteFile(ctx, path, df, opts...)
	})
}

// SinkIPC executes lf and writes the result to an Arrow IPC file.
// Mirrors polars' LazyFrame.sink_ipc.
func SinkIPC(ctx context.Context, lf LazyFrame, path string, opts ...ipc.Option) error {
	return lf.Sink(ctx, func(ctx context.Context, df *dataframe.DataFrame) error {
		return ipc.WriteFile(ctx, path, df, opts...)
	})
}

// SinkNDJSON executes lf and writes the result as newline-delimited
// JSON. Mirrors polars' LazyFrame.sink_ndjson.
func SinkNDJSON(ctx context.Context, lf LazyFrame, path string) error {
	return lf.Sink(ctx, func(ctx context.Context, df *dataframe.DataFrame) error {
		return iojson.WriteNDJSONFile(ctx, path, df)
	})
}

// SinkIPCStream executes lf and writes the result in the Arrow IPC
// streaming format, one record batch per streamed slice.
func SinkIPCStream(ctx context.Context, lf LazyFrame, path string, opts ...ipc.Option) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	var sw *ipc.StreamWriter
	for batch, err := range lf.CollectBatches(ctx, 0) {
		if err != nil {
			f.Close()
			return err
		}
		if sw == nil {
			sw, err = ipc.NewStreamWriter(f, batch, opts...)
			if err != nil {
				batch.Release()
				f.Close()
				return err
			}
		}
		err = sw.Write(ctx, batch)
		batch.Release()
		if err != nil {
			sw.Close()
			f.Close()
			return err
		}
	}
	if sw != nil {
		if err := sw.Close(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

// SQL runs query against df registered as tableName ("self" when empty).
// Mirrors polars' DataFrame.sql(query, table_name="self").
func SQL(ctx context.Context, df *DataFrame, query string, tableName ...string) (*DataFrame, error) {
	name := "self"
	if len(tableName) > 0 && tableName[0] != "" {
		name = tableName[0]
	}
	sess := sql.NewSession()
	defer sess.Close()
	if err := sess.Register(name, df); err != nil {
		return nil, err
	}
	return sess.Query(ctx, query)
}

// SQLLazy is SQL for a LazyFrame: the query runs when the returned frame
// is collected and its schema is inferred without executing lf. Mirrors
// polars' LazyFrame.sql(query, table_name="self").
func SQLLazy(lf LazyFrame, query string, tableName ...string) LazyFrame {
	return lf.MapBatches(func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return SQL(context.Background(), df, query, tableName...)
	}, lazy.MapBatchesOptions{InferSchema: true})
}

// SelectSeq evaluates exprs against df one after another. Mirrors
// polars' DataFrame.select_seq; results equal SelectExpr.
func SelectSeq(ctx context.Context, df *DataFrame, exprs ...Expr) (*DataFrame, error) {
	return SelectExpr(ctx, df, exprs...)
}

// WithColumnsSeq adds the columns of exprs to df, evaluating them one
// after another. Mirrors polars' DataFrame.with_columns_seq.
func WithColumnsSeq(ctx context.Context, df *DataFrame, exprs ...Expr) (*DataFrame, error) {
	return WithColumnsExpr(ctx, df, exprs...)
}

// GroupByExprs groups df by expression keys, for example
// GroupByExprs(df, golars.Col("a").Gt(golars.Lit(2))), returning the
// lazy group-by so any aggregation (Agg, Sum, Len, ...) can follow;
// finish with Collect. Mirrors polars' DataFrame.group_by with
// expression arguments.
func GroupByExprs(df *DataFrame, keys ...Expr) lazy.LazyGroupBy {
	return lazy.FromDataFrame(df).GroupByExprs(keys...)
}
