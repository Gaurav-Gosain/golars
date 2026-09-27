package lazy

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// SourceFunc is a lazy DataFrame source. It's evaluated once at
// collect time. Used to implement ScanCSV, ScanParquet, etc. without
// blowing up the plan tree.
type SourceFunc struct {
	// Name is a human-friendly label that shows up in Explain().
	Name string
	// Schema, when non-nil, is the best-effort output schema. Some
	// callers can supply it cheaply (parquet metadata, explicit
	// user-provided schemas); others leave it nil so Collect runs the
	// source once to discover the schema.
	KnownSchema *schema.Schema
	// Load returns the underlying DataFrame. The returned frame is
	// owned by the caller; the lazy executor calls Release when done.
	Load func(context.Context) (*dataframe.DataFrame, error)
	// LoadColumns, when non-nil, reads only the named columns. The
	// projection pushdown pass fills Projection and the executor then
	// calls LoadColumns instead of Load, so formats that store columns
	// separately (parquet, ipc) skip decoding columns nobody reads.
	LoadColumns func(ctx context.Context, cols []string) (*dataframe.DataFrame, error)
	// Projection is the pushed-down column subset. Empty means all.
	Projection []string
}

func (SourceFunc) isLogicalNode()   {}
func (SourceFunc) Children() []Node { return nil }
func (s SourceFunc) WithChildren(children []Node) Node {
	if len(children) != 0 {
		panic("lazy: SourceFunc has no children")
	}
	return s
}

// Schema returns the pre-declared schema if available. When KnownSchema
// is nil, the loader is invoked once and its output schema is cached.
// This is a deliberate tradeoff: it trades one extra I/O at schema-
// discovery time for much better optimiser behaviour (projection
// pushdown needs a schema).
func (s SourceFunc) Schema() (*schema.Schema, error) {
	if s.KnownSchema != nil {
		if len(s.Projection) > 0 {
			return s.KnownSchema.Select(s.Projection...)
		}
		return s.KnownSchema, nil
	}
	// We can't materialise the source here without a ctx; punt to the
	// executor. Callers that need schemas before Collect should
	// supply KnownSchema up front.
	return nil, fmt.Errorf("lazy: SourceFunc[%s] has no declared schema; pass KnownSchema or Collect to observe", s.Name)
}

func (s SourceFunc) String() string {
	if len(s.Projection) > 0 {
		return fmt.Sprintf("SCAN source=%s projection=%v", s.Name, s.Projection)
	}
	return fmt.Sprintf("SCAN source=%s", s.Name)
}

// load materialises the source, honouring a pushed-down projection.
// Sources without LoadColumns read everything and select afterwards so
// the output schema is the same either way.
func (s SourceFunc) load(ctx context.Context) (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame
	var err error
	switch {
	case len(s.Projection) == 0:
		df, err = s.Load(ctx)
	case s.LoadColumns != nil:
		df, err = s.LoadColumns(ctx, s.Projection)
	default:
		var all *dataframe.DataFrame
		all, err = s.Load(ctx)
		if err != nil {
			return nil, err
		}
		df, err = all.Select(s.Projection...)
		all.Release()
	}
	if err != nil {
		return nil, err
	}
	return rechunkFrame(ctx, df)
}

// rechunkFrame makes every column of df a single chunk, consuming df.
// File readers return one chunk per row group or batch, while most
// kernels work on one contiguous array and would otherwise concatenate
// the same column again inside every expression that reads it. Doing it
// once at the scan (in parallel over columns) is the cheaper place.
func rechunkFrame(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	cols := df.Columns()
	multi := 0
	for _, c := range cols {
		if c.NumChunks() > 1 {
			multi++
		}
	}
	if multi == 0 {
		return df, nil
	}
	defer df.Release()
	out := make([]*series.Series, len(cols))
	g := pool.NewGroup(ctx, 0)
	for i, c := range cols {
		if c.NumChunks() <= 1 {
			out[i] = c.Clone()
			continue
		}
		g.Go(func(context.Context) error {
			out[i] = c.Rechunk()
			return nil
		})
	}
	err := g.Wait()
	if err == nil {
		var res *dataframe.DataFrame
		if res, err = dataframe.New(out...); err == nil {
			return res, nil
		}
	}
	for _, c := range out {
		if c != nil {
			c.Release()
		}
	}
	return nil, err
}

// FromSource wraps a loader function into a LazyFrame. Intended for
// io/* packages that want to expose a lazy scan.
func FromSource(name string, known *schema.Schema, load func(context.Context) (*dataframe.DataFrame, error)) LazyFrame {
	return LazyFrame{plan: SourceFunc{Name: name, KnownSchema: known, Load: load}}
}

// FromSourceProjected is FromSource for loaders that can read a column
// subset directly. loadCols receives nil to read every column, or the
// exact columns the rest of the plan references (in the source's
// schema order when the schema is known, otherwise sorted by name).
func FromSourceProjected(name string, known *schema.Schema, loadCols func(ctx context.Context, cols []string) (*dataframe.DataFrame, error)) LazyFrame {
	return LazyFrame{plan: SourceFunc{
		Name:        name,
		KnownSchema: known,
		Load:        func(ctx context.Context) (*dataframe.DataFrame, error) { return loadCols(ctx, nil) },
		LoadColumns: loadCols,
	}}
}
