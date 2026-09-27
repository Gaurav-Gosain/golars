package lazy

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// This file adds the polars LazyFrame methods that map onto an eager
// DataFrame operation. Each one appends a FrameOpNode (or a
// BinaryFrameOpNode), so the optimizer never moves filters, projections
// or slices across them.

// Explode turns every element of the named list columns into its own
// row; several columns explode together and must have matching list
// lengths per row. Mirrors polars' LazyFrame.explode.
func (lf LazyFrame) Explode(columns ...string) LazyFrame {
	return lf.frameOp("EXPLODE", fmt.Sprint(columns), func(in *schema.Schema) (*schema.Schema, error) {
		out := in
		for _, c := range columns {
			f, ok := in.FieldByName(c)
			if !ok {
				return nil, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, c)
			}
			lt, ok := f.DType.Arrow().(arrow.ListLikeType)
			if !ok {
				return nil, fmt.Errorf("lazy.Explode: column %q has dtype %s, not a list", c, f.DType)
			}
			out = out.WithField(schema.Field{Name: c, DType: dtype.FromArrow(lt.Elem())})
		}
		return out, nil
	}, func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.ExplodeColumns(ctx, columns...)
	})
}

// Unnest replaces each named struct column by its fields. Mirrors
// polars' LazyFrame.unnest.
func (lf LazyFrame) Unnest(columns ...string) LazyFrame {
	return lf.frameOp("UNNEST", fmt.Sprint(columns), nil, func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		cur := df.Clone()
		for _, c := range columns {
			next, err := cur.Unnest(ctx, c)
			cur.Release()
			if err != nil {
				return nil, err
			}
			cur = next
		}
		return cur, nil
	})
}

// UnpivotOptions mirrors polars' LazyFrame.unpivot keyword arguments.
// Empty VariableName and ValueName mean "variable" and "value".
type UnpivotOptions struct {
	On           []string
	Index        []string
	VariableName string
	ValueName    string
}

// Unpivot turns the On columns (every non-index column when empty) into
// variable/value rows. Mirrors polars' LazyFrame.unpivot.
func (lf LazyFrame) Unpivot(opts UnpivotOptions) LazyFrame {
	return lf.frameOp("UNPIVOT", fmt.Sprintf("on=%v index=%v", opts.On, opts.Index), nil,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			out, err := df.Unpivot(ctx, opts.Index, opts.On)
			if err != nil {
				return nil, err
			}
			for from, to := range map[string]string{"variable": opts.VariableName, "value": opts.ValueName} {
				if to == "" || to == from {
					continue
				}
				renamed, err := out.Rename(from, to)
				out.Release()
				if err != nil {
					return nil, err
				}
				out = renamed
			}
			return out, nil
		})
}

// PivotOptions mirrors polars' LazyFrame.pivot. Unlike the eager pivot,
// the lazy pivot needs the output value columns up front (OnColumns) so
// the schema is known before execution, exactly like polars 1.39.
type PivotOptions struct {
	On        string
	OnColumns []string
	Index     []string
	Values    string
	Agg       dataframe.PivotAgg
}

// Pivot spreads the values of On into one column per entry of OnColumns.
// Values of On that are not listed are ignored; listed values that do not
// occur produce a column holding the aggregate of an empty group (0 for
// sum and count, null otherwise). Mirrors polars' LazyFrame.pivot.
func (lf LazyFrame) Pivot(opts PivotOptions) LazyFrame {
	agg := opts.Agg
	if agg == "" {
		agg = dataframe.PivotFirst
	}
	return lf.frameOp("PIVOT", fmt.Sprintf("on=%s columns=%v", opts.On, opts.OnColumns), nil,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			wide, err := df.Pivot(ctx, opts.Index, opts.On, opts.Values, agg)
			if err != nil {
				return nil, err
			}
			defer wide.Release()
			valCol, err := df.Column(opts.Values)
			if err != nil {
				return nil, err
			}
			cols := make([]*series.Series, 0, len(opts.Index)+len(opts.OnColumns))
			for _, k := range opts.Index {
				c, err := wide.Column(k)
				if err != nil {
					releaseSeries(cols)
					return nil, err
				}
				cols = append(cols, c.Clone())
			}
			for _, name := range opts.OnColumns {
				if c, err := wide.Column(name); err == nil {
					cols = append(cols, c.Clone())
					continue
				}
				s, err := emptyPivotColumn(name, valCol.DType(), wide.Height(), agg)
				if err != nil {
					releaseSeries(cols)
					return nil, err
				}
				cols = append(cols, s)
			}
			return dataframe.New(cols...)
		})
}

// emptyPivotColumn is the column for a requested pivot value that never
// occurs: the aggregate of an empty group.
func emptyPivotColumn(name string, valDT dtype.DType, n int, agg dataframe.PivotAgg) (*series.Series, error) {
	mem := memory.DefaultAllocator
	switch agg {
	case dataframe.PivotSum:
		b := array.NewBuilder(mem, valDT.Arrow())
		defer b.Release()
		for range n {
			switch bb := b.(type) {
			case *array.Int64Builder:
				bb.Append(0)
			case *array.Float64Builder:
				bb.Append(0)
			default:
				b.AppendNull()
			}
		}
		return seriesFrom(name, b.NewArray())
	case dataframe.PivotCount, dataframe.PivotLen:
		b := array.NewUint32Builder(mem)
		defer b.Release()
		for range n {
			b.Append(0)
		}
		return seriesFrom(name, b.NewArray())
	case dataframe.PivotMean:
		return seriesFrom(name, array.MakeArrayOfNull(mem, arrow.PrimitiveTypes.Float64, n))
	}
	return seriesFrom(name, array.MakeArrayOfNull(mem, valDT.Arrow(), n))
}

// seriesFrom wraps arr, consuming the reference even on error.
func seriesFrom(name string, arr arrow.Array) (*series.Series, error) {
	s, err := series.New(name, arr)
	if err != nil {
		arr.Release()
		return nil, err
	}
	return s, nil
}

// TopK keeps the k rows with the largest values of by, largest first.
// Mirrors polars' LazyFrame.top_k.
func (lf LazyFrame) TopK(k int, by string) LazyFrame {
	return lf.frameOp("TOP_K", fmt.Sprintf("k=%d by=%s", k, by), sameSchema,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.TopK(ctx, k, by)
		})
}

// BottomK keeps the k rows with the smallest values of by. Mirrors
// polars' LazyFrame.bottom_k.
func (lf LazyFrame) BottomK(k int, by string) LazyFrame {
	return lf.frameOp("BOTTOM_K", fmt.Sprintf("k=%d by=%s", k, by), sameSchema,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.BottomK(ctx, k, by)
		})
}

// FillNullWithStrategy fills nulls with a polars strategy (forward,
// backward, min, max, mean, zero, one). limit bounds forward and backward
// fills; 0 means no limit. Mirrors polars' LazyFrame.fill_null(strategy=).
func (lf LazyFrame) FillNullWithStrategy(strategy dataframe.FillNullStrategy, limit int) LazyFrame {
	return lf.frameOp("FILL_NULL", "strategy="+string(strategy), sameSchema,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.FillNullWithStrategy(ctx, strategy, limit)
		})
}

// DropNans removes rows holding NaN in any float column of subset (all
// columns when empty). Mirrors polars' LazyFrame.drop_nans.
func (lf LazyFrame) DropNans(subset ...string) LazyFrame {
	return lf.frameOp("DROP_NANS", strings.Join(subset, ","), sameSchema,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.DropNans(ctx, subset...)
		})
}

// Shift moves every column by n rows, filling vacated slots with
// fillValue (nil for null). Mirrors polars' LazyFrame.shift.
func (lf LazyFrame) Shift(n int, fillValue any) LazyFrame {
	return lf.frameOp("SHIFT", fmt.Sprintf("n=%d", n), nil,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.Shift(ctx, n, fillValue)
		})
}

// GatherEvery keeps every n-th row starting at offset. Mirrors polars'
// LazyFrame.gather_every.
func (lf LazyFrame) GatherEvery(n, offset int) LazyFrame {
	return lf.frameOp("GATHER_EVERY", fmt.Sprintf("n=%d offset=%d", n, offset), sameSchema,
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.GatherEvery(ctx, n, offset)
		})
}

// First keeps the first row. Mirrors polars' LazyFrame.first.
func (lf LazyFrame) First() LazyFrame { return lf.Slice(0, 1) }

// Last keeps the last row. Mirrors polars' LazyFrame.last.
func (lf LazyFrame) Last() LazyFrame { return lf.Tail(1) }

// reduceOp appends a frame-level aggregate. The schema comes from running
// the aggregate on an empty frame, which applies the same dtype rules.
func (lf LazyFrame) reduceOp(name string, fn func(context.Context, *dataframe.DataFrame) (*dataframe.DataFrame, error)) LazyFrame {
	return lf.frameOp(name, "", nil, fn)
}

// Sum aggregates every column to its sum. Mirrors polars' LazyFrame.sum.
func (lf LazyFrame) Sum() LazyFrame {
	return lf.reduceOp("SUM", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.SumAll(ctx)
	})
}

// Mean aggregates every column to its mean. Mirrors polars'
// LazyFrame.mean.
func (lf LazyFrame) Mean() LazyFrame {
	return lf.reduceOp("MEAN", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.MeanAll(ctx)
	})
}

// Median aggregates every column to its median. Mirrors polars'
// LazyFrame.median.
func (lf LazyFrame) Median() LazyFrame {
	return lf.reduceOp("MEDIAN", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.MedianAll(ctx)
	})
}

// Min aggregates every column to its minimum. Mirrors polars'
// LazyFrame.min.
func (lf LazyFrame) Min() LazyFrame {
	return lf.reduceOp("MIN", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.MinAll(ctx)
	})
}

// Max aggregates every column to its maximum. Mirrors polars'
// LazyFrame.max.
func (lf LazyFrame) Max() LazyFrame {
	return lf.reduceOp("MAX", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.MaxAll(ctx)
	})
}

// Std aggregates every column to its standard deviation; ddof defaults
// to 1. Mirrors polars' LazyFrame.std.
func (lf LazyFrame) Std(ddof ...int) LazyFrame {
	return lf.reduceOp("STD", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.StdAll(ctx, ddof...)
	})
}

// Var aggregates every column to its variance; ddof defaults to 1.
// Mirrors polars' LazyFrame.var.
func (lf LazyFrame) Var(ddof ...int) LazyFrame {
	return lf.reduceOp("VAR", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.VarAll(ctx, ddof...)
	})
}

// Quantile aggregates every column to its q-quantile; an empty method
// means nearest. Mirrors polars' LazyFrame.quantile.
func (lf LazyFrame) Quantile(q float64, method dataframe.QuantileMethod) LazyFrame {
	return lf.reduceOp("QUANTILE", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.QuantileAll(ctx, q, method)
	})
}

// Count returns the non-null count of every column as u32. Mirrors
// polars' LazyFrame.count.
func (lf LazyFrame) Count() LazyFrame {
	return lf.reduceOp("COUNT", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.CountAll(ctx)
	})
}

// NullCount returns the null count of every column as u32. Mirrors
// polars' LazyFrame.null_count.
func (lf LazyFrame) NullCount() LazyFrame {
	return lf.reduceOp("NULL_COUNT", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.NullCountAll(ctx)
	})
}

// ApproxNUnique returns the number of distinct values per column as u32.
// Mirrors polars' LazyFrame.approx_n_unique.
func (lf LazyFrame) ApproxNUnique() LazyFrame {
	return lf.reduceOp("APPROX_N_UNIQUE", func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.ApproxNUnique(ctx)
	})
}

// Describe collects lf and returns summary statistics. Like polars'
// LazyFrame.describe this executes the plan.
func (lf LazyFrame) Describe(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, error) {
	df, err := lf.Collect(ctx, opts...)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	return df.Describe(ctx)
}

// CastColumns casts the named columns. Mirrors polars'
// LazyFrame.cast({name: dtype}, strict=...).
func (lf LazyFrame) CastColumns(casts map[string]dtype.DType, strict bool) LazyFrame {
	return lf.frameOp("CAST", fmt.Sprint(casts), func(in *schema.Schema) (*schema.Schema, error) {
		out := in
		for name, to := range casts {
			if !in.Contains(name) {
				return nil, fmt.Errorf("%w: %q", schema.ErrColumnNotFound, name)
			}
			out = out.WithField(schema.Field{Name: name, DType: to})
		}
		return out, nil
	}, func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.CastColumns(ctx, casts, strict)
	})
}

// CastAll casts every column to to. Mirrors polars' LazyFrame.cast(dtype).
func (lf LazyFrame) CastAll(to dtype.DType, strict bool) LazyFrame {
	return lf.frameOp("CAST", "* -> "+to.String(), func(in *schema.Schema) (*schema.Schema, error) {
		fields := in.Fields()
		out := make([]schema.Field, len(fields))
		for i, f := range fields {
			out[i] = schema.Field{Name: f.Name, DType: to}
		}
		return schema.New(out...)
	}, func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Cast(ctx, to, strict)
	})
}

// CastDTypes casts every column whose dtype matches a From entry.
// Mirrors polars' LazyFrame.cast({dtype: dtype}).
func (lf LazyFrame) CastDTypes(casts []dataframe.DTypeCast, strict bool) LazyFrame {
	return lf.frameOp("CAST", fmt.Sprint(casts), func(in *schema.Schema) (*schema.Schema, error) {
		fields := in.Fields()
		out := make([]schema.Field, len(fields))
		for i, f := range fields {
			out[i] = f
			for _, c := range casts {
				if c.From.Equal(f.DType) {
					out[i].DType = c.To
					break
				}
			}
		}
		return schema.New(out...)
	}, func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.CastDTypes(ctx, casts, strict)
	})
}

// MatchToSchema conforms lf to sch. Mirrors polars'
// LazyFrame.match_to_schema.
func (lf LazyFrame) MatchToSchema(sch *schema.Schema, opts dataframe.MatchToSchemaOptions) LazyFrame {
	return lf.frameOp("MATCH_TO_SCHEMA", sch.String(), func(*schema.Schema) (*schema.Schema, error) { return sch, nil },
		func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return df.MatchToSchema(ctx, sch, opts)
		})
}

// SelectSeq is Select with polars' sequential-evaluation name. Results
// are identical; golars chooses the evaluation strategy itself.
func (lf LazyFrame) SelectSeq(exprs ...expr.Expr) LazyFrame { return lf.Select(exprs...) }

// WithColumnsSeq is WithColumns with polars' sequential-evaluation name.
func (lf LazyFrame) WithColumnsSeq(exprs ...expr.Expr) LazyFrame { return lf.WithColumns(exprs...) }

// Inspect calls fn with the intermediate result every time the plan runs
// and passes the frame through unchanged. fn must not release the frame.
// Mirrors polars' LazyFrame.inspect.
func (lf LazyFrame) Inspect(fn func(*dataframe.DataFrame)) LazyFrame {
	return lf.frameOp("INSPECT", "", sameSchema, func(_ context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		fn(df)
		return df.Clone(), nil
	})
}

// Pipe applies fn to lf. Mirrors polars' LazyFrame.pipe.
func (lf LazyFrame) Pipe(fn func(LazyFrame) LazyFrame) LazyFrame { return fn(lf) }

// Clone returns an independent handle to the same plan. Plans are
// immutable, so this is a cheap copy. Mirrors polars' LazyFrame.clone.
func (lf LazyFrame) Clone() LazyFrame { return LazyFrame{plan: lf.plan} }

// Clear returns a frame with the same schema and n all-null rows (n
// defaults to 0). Mirrors polars' LazyFrame.clear.
func (lf LazyFrame) Clear(n ...int) LazyFrame {
	rows := 0
	if len(n) > 0 {
		rows = n[0]
	}
	return lf.frameOp("CLEAR", fmt.Sprintf("n=%d", rows), sameSchema,
		func(_ context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			cols := make([]*series.Series, df.Width())
			for i, c := range df.Columns() {
				s, err := seriesFrom(c.Name(), array.MakeArrayOfNull(memory.DefaultAllocator, c.DType().Arrow(), rows))
				if err != nil {
					releaseSeries(cols)
					return nil, err
				}
				cols[i] = s
			}
			return dataframe.New(cols...)
		})
}

// MapBatchesOptions configures MapBatches.
type MapBatchesOptions struct {
	// Schema is the output schema of fn. When nil the input schema is
	// assumed, as polars does.
	Schema *schema.Schema
	// InferSchema runs fn on an empty frame of the input schema to learn
	// the output schema. Ignored when Schema is set.
	InferSchema bool
}

// MapBatches applies an arbitrary eager function to the materialised
// input. fn borrows its argument and returns a new frame. Mirrors
// polars' LazyFrame.map_batches.
func (lf LazyFrame) MapBatches(fn func(*dataframe.DataFrame) (*dataframe.DataFrame, error), opts ...MapBatchesOptions) LazyFrame {
	var o MapBatchesOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	var schemaFn func(*schema.Schema) (*schema.Schema, error)
	switch {
	case o.Schema != nil:
		schemaFn = func(*schema.Schema) (*schema.Schema, error) { return o.Schema, nil }
	case !o.InferSchema:
		schemaFn = sameSchema
	}
	return lf.frameOp("MAP_BATCHES", "", schemaFn, func(_ context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return fn(df)
	})
}

// JoinAsof performs an as-of join against other. Mirrors polars'
// LazyFrame.join_asof.
func (lf LazyFrame) JoinAsof(other LazyFrame, opts dataframe.AsofOptions) LazyFrame {
	return LazyFrame{plan: BinaryFrameOpNode{
		Left: lf.plan, Right: other.plan, Op: "JOIN_ASOF",
		Detail: asofDetail(opts),
		Fn: func(ctx context.Context, l, r *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return l.JoinAsof(ctx, r, opts)
		},
	}}
}

func asofDetail(o dataframe.AsofOptions) string {
	on := o.On
	if on == "" {
		on = o.LeftOn + "=" + o.RightOn
	}
	d := "on=" + on + " strategy=" + o.Strategy.String()
	if len(o.By) > 0 {
		d += fmt.Sprintf(" by=%v", o.By)
	}
	return d
}

// JoinWhere joins on arbitrary comparisons between left and right
// columns. Mirrors polars' LazyFrame.join_where.
func (lf LazyFrame) JoinWhere(other LazyFrame, predicates []expr.Expr, opts ...dataframe.JoinOption) LazyFrame {
	parts := make([]string, len(predicates))
	for i, p := range predicates {
		parts[i] = p.String()
	}
	return LazyFrame{plan: BinaryFrameOpNode{
		Left: lf.plan, Right: other.plan, Op: "JOIN_WHERE",
		Detail: "[" + strings.Join(parts, ", ") + "]",
		Fn: func(ctx context.Context, l, r *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return l.JoinWhere(ctx, r, predicates, opts...)
		},
	}}
}

// MergeSorted merges two inputs sorted by key into one sorted output.
// Mirrors polars' LazyFrame.merge_sorted.
func (lf LazyFrame) MergeSorted(other LazyFrame, key string) LazyFrame {
	return LazyFrame{plan: BinaryFrameOpNode{
		Left: lf.plan, Right: other.plan, Op: "MERGE_SORTED", Detail: "key=" + key,
		SchemaFn: func(l, r *schema.Schema) (*schema.Schema, error) {
			if !l.Equal(r) {
				return nil, fmt.Errorf("lazy.MergeSorted: schemas differ: %s vs %s", l, r)
			}
			return l, nil
		},
		Fn: func(ctx context.Context, l, r *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return l.MergeSorted(ctx, r, key)
		},
	}}
}

// Update overwrites values with non-null values from other. Mirrors
// polars' LazyFrame.update.
func (lf LazyFrame) Update(other LazyFrame, opts dataframe.UpdateOptions) LazyFrame {
	return LazyFrame{plan: BinaryFrameOpNode{
		Left: lf.plan, Right: other.plan, Op: "UPDATE", Detail: string(opts.How),
		Fn: func(ctx context.Context, l, r *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			return l.Update(ctx, r, opts)
		},
	}}
}

// Profile collects lf while timing every plan node. It returns the
// result and a timings frame with columns node (str), start and end
// (u64 microseconds since the query started), mirroring polars'
// LazyFrame.profile. The first row covers optimization.
func (lf LazyFrame) Profile(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, *dataframe.DataFrame, error) {
	begin := time.Now()
	optimized, _, err := DefaultOptimizer().Optimize(lf.plan)
	if err != nil {
		return nil, nil, err
	}
	optEnd := time.Now()
	prof := NewProfiler()
	cfg := resolveExec(append(slices.Clone(opts), WithProfiler(prof)))
	df, err := executeMaybeStreaming(ctx, cfg, optimized)
	if err != nil {
		return nil, nil, err
	}
	spans := prof.Spans()
	names := make([]string, 0, len(spans)+1)
	starts := make([]uint64, 0, len(spans)+1)
	ends := make([]uint64, 0, len(spans)+1)
	names = append(names, "optimization")
	starts = append(starts, 0)
	ends = append(ends, uint64(optEnd.Sub(begin).Microseconds()))
	for _, s := range spans {
		names = append(names, s.Detail)
		st := uint64(s.StartedAt.Sub(begin).Microseconds())
		starts = append(starts, st)
		ends = append(ends, st+uint64(s.Duration.Microseconds()))
	}
	nodeCol, err := series.FromString("node", names, nil)
	if err != nil {
		df.Release()
		return nil, nil, err
	}
	startCol, err := series.FromUint64("start", starts, nil)
	if err != nil {
		nodeCol.Release()
		df.Release()
		return nil, nil, err
	}
	endCol, err := series.FromUint64("end", ends, nil)
	if err != nil {
		nodeCol.Release()
		startCol.Release()
		df.Release()
		return nil, nil, err
	}
	timings, err := dataframe.New(nodeCol, startCol, endCol)
	if err != nil {
		df.Release()
		return nil, nil, err
	}
	return df, timings, nil
}

// CollectBatches executes lf and yields the result in slices of at most
// chunkSize rows (the whole result in one batch when chunkSize <= 0).
// Every yielded frame must be released by the caller. Breaking out of
// the loop releases the rest. Mirrors polars' LazyFrame.collect_batches.
func (lf LazyFrame) CollectBatches(ctx context.Context, chunkSize int, opts ...ExecOption) iter.Seq2[*dataframe.DataFrame, error] {
	return func(yield func(*dataframe.DataFrame, error) bool) {
		df, err := lf.Collect(ctx, opts...)
		if err != nil {
			yield(nil, err)
			return
		}
		defer df.Release()
		if chunkSize <= 0 || df.Height() <= chunkSize {
			if !yield(df.Clone(), nil) {
				return
			}
			return
		}
		for off := 0; off < df.Height(); off += chunkSize {
			sl, err := df.Slice(off, min(chunkSize, df.Height()-off))
			if !yield(sl, err) || err != nil {
				return
			}
		}
	}
}

// CollectResult is the outcome delivered by CollectAsync.
type CollectResult struct {
	DataFrame *dataframe.DataFrame
	Err       error
}

// CollectAsync runs Collect on a new goroutine and delivers the result on
// the returned channel, which receives exactly one value and is then
// closed. Cancel ctx to abort. Mirrors polars' LazyFrame.collect_async.
func (lf LazyFrame) CollectAsync(ctx context.Context, opts ...ExecOption) <-chan CollectResult {
	ch := make(chan CollectResult, 1)
	go func() {
		defer close(ch)
		df, err := lf.Collect(ctx, opts...)
		ch <- CollectResult{DataFrame: df, Err: err}
	}()
	return ch
}
