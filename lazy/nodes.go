package lazy

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// --- Unique ------------------------------------------------------------

// UniqueNode is the plan node for lf.Unique().
type UniqueNode struct{ Input Node }

func (UniqueNode) isLogicalNode() {}

func (u UniqueNode) Children() []Node { return []Node{u.Input} }

func (u UniqueNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: UniqueNode takes one child")
	}
	return UniqueNode{Input: children[0]}
}

func (u UniqueNode) Schema() (*schema.Schema, error) { return u.Input.Schema() }
func (u UniqueNode) String() string                  { return "UNIQUE" }

// Unique returns a LazyFrame whose output contains distinct rows.
// Evaluation materialises upstream and then calls df.Unique(ctx).
func (lf LazyFrame) Unique() LazyFrame {
	return LazyFrame{plan: UniqueNode{Input: lf.plan}}
}

// --- FillNull ----------------------------------------------------------

// FillNullNode carries a scalar fill value that is applied across
// every column whose dtype accepts the value's Go type.
type FillNullNode struct {
	Input Node
	Value any
}

func (FillNullNode) isLogicalNode() {}

func (f FillNullNode) Children() []Node { return []Node{f.Input} }

func (f FillNullNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: FillNullNode takes one child")
	}
	return FillNullNode{Input: children[0], Value: f.Value}
}

func (f FillNullNode) Schema() (*schema.Schema, error) {
	in, err := f.Input.Schema()
	if err != nil {
		return nil, err
	}
	return fillNullSchema(in, f.Value)
}

func (f FillNullNode) String() string { return fmt.Sprintf("FILL_NULL value=%v", f.Value) }

// FillNull returns a LazyFrame where nulls in compatible columns are
// replaced with value. Incompatible columns pass through unchanged.
func (lf LazyFrame) FillNull(value any) LazyFrame {
	return LazyFrame{plan: FillNullNode{Input: lf.plan, Value: value}}
}

// --- DropNulls ---------------------------------------------------------

// DropNullsNode drops rows that contain a null in any of Cols. An
// empty Cols slice checks every column.
type DropNullsNode struct {
	Input Node
	Cols  []string
}

func (DropNullsNode) isLogicalNode() {}

func (d DropNullsNode) Children() []Node { return []Node{d.Input} }

func (d DropNullsNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: DropNullsNode takes one child")
	}
	return DropNullsNode{Input: children[0], Cols: d.Cols}
}

func (d DropNullsNode) Schema() (*schema.Schema, error) { return d.Input.Schema() }
func (d DropNullsNode) String() string {
	if len(d.Cols) == 0 {
		return "DROP_NULLS"
	}
	return "DROP_NULLS cols=[" + strings.Join(d.Cols, ",") + "]"
}

// DropNulls returns a LazyFrame that removes rows whose entries are
// null in any of the given columns. No columns means every column.
func (lf LazyFrame) DropNulls(cols ...string) LazyFrame {
	return LazyFrame{plan: DropNullsNode{Input: lf.plan, Cols: cols}}
}

// --- Cast --------------------------------------------------------------

// CastNode changes the dtype of a single column. Unlike expr.Cast it
// operates at the frame level so the optimiser can reason about
// schema changes without expression introspection.
type CastNode struct {
	Input Node
	Col   string
	To    dtype.DType
}

func (CastNode) isLogicalNode() {}

func (c CastNode) Children() []Node { return []Node{c.Input} }

func (c CastNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: CastNode takes one child")
	}
	return CastNode{Input: children[0], Col: c.Col, To: c.To}
}

func (c CastNode) Schema() (*schema.Schema, error) {
	in, err := c.Input.Schema()
	if err != nil {
		return nil, err
	}
	idx, ok := in.Index(c.Col)
	if !ok {
		return nil, fmt.Errorf("lazy.Cast: column %q not found", c.Col)
	}
	fields := make([]schema.Field, in.Len())
	for i := range in.Len() {
		f := in.Field(i)
		if i == idx {
			fields[i] = schema.Field{Name: f.Name, DType: c.To}
		} else {
			fields[i] = f
		}
	}
	return schema.New(fields...)
}

func (c CastNode) String() string { return fmt.Sprintf("CAST %q -> %s", c.Col, c.To) }

// Cast returns a LazyFrame where column `col` is cast to dtype `to`.
// Fails at execution if the column is missing or the cast is invalid.
func (lf LazyFrame) Cast(col string, to dtype.DType) LazyFrame {
	return LazyFrame{plan: CastNode{Input: lf.plan, Col: col, To: to}}
}

// --- Cache -------------------------------------------------------------

// CacheNode memoises its sub-plan: the first Collect evaluates the
// upstream; subsequent evaluations reuse the cached DataFrame. Useful
// when the same LazyFrame is the basis of several downstream plans.
type CacheNode struct {
	Input Node
	state *cacheState
}

type cacheState struct {
	once sync.Once
	df   *dataframe.DataFrame
	err  error
}

func (CacheNode) isLogicalNode() {}

func (c CacheNode) Children() []Node { return []Node{c.Input} }

func (c CacheNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: CacheNode takes one child")
	}
	return CacheNode{Input: children[0], state: c.state}
}

func (c CacheNode) Schema() (*schema.Schema, error) { return c.Input.Schema() }
func (c CacheNode) String() string                  { return "CACHE" }

// Cache wraps lf so future Collects reuse the first materialised
// result instead of re-running the whole plan. The cached frame is
// retained on first Collect and released automatically when the
// cache state is garbage-collected (via runtime.SetFinalizer), so
// callers do not need an explicit Release on the LazyFrame itself.
func (lf LazyFrame) Cache() LazyFrame {
	st := &cacheState{}
	// The finalizer releases the cached frame when st is collected.
	// runtime.AddCleanup is not a drop-in here: its callback must not
	// receive st, and the frame lives inside st (written by the
	// sync.Once), so it would need a separate holder object.
	runtime.SetFinalizer(st, func(held *cacheState) {
		if held.df != nil {
			held.df.Release()
			held.df = nil
		}
	})
	return LazyFrame{plan: CacheNode{Input: lf.plan, state: st}}
}

// --- WithRowIndex ------------------------------------------------------

// WithRowIndexNode prepends a u32 row index column.
type WithRowIndexNode struct {
	Input  Node
	Name   string
	Offset int64
}

func (WithRowIndexNode) isLogicalNode() {}

func (w WithRowIndexNode) Children() []Node { return []Node{w.Input} }

func (w WithRowIndexNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: WithRowIndexNode takes one child")
	}
	return WithRowIndexNode{Input: children[0], Name: w.Name, Offset: w.Offset}
}

func (w WithRowIndexNode) Schema() (*schema.Schema, error) {
	in, err := w.Input.Schema()
	if err != nil {
		return nil, err
	}
	fields := make([]schema.Field, 0, in.Len()+1)
	fields = append(fields, schema.Field{Name: w.Name, DType: dtype.Uint32()})
	for i := range in.Len() {
		f := in.Field(i)
		fields = append(fields, f)
	}
	return schema.New(fields...)
}

func (w WithRowIndexNode) String() string {
	return fmt.Sprintf("WITH_ROW_INDEX %q offset=%d", w.Name, w.Offset)
}

// WithRowIndex returns a LazyFrame with a new u32 column `name`
// prepended, containing sequential row numbers starting at offset.
func (lf LazyFrame) WithRowIndex(name string, offset int64) LazyFrame {
	return LazyFrame{plan: WithRowIndexNode{Input: lf.plan, Name: name, Offset: offset}}
}

// --- Executor wiring for the new nodes --------------------------------

func executeUnique(ctx context.Context, cfg execConfig, u UniqueNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, u.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Unique(ctx)
}

func executeFillNull(ctx context.Context, cfg execConfig, f FillNullNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, f.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.FillNull(f.Value)
}

func executeDropNulls(ctx context.Context, cfg execConfig, d DropNullsNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, d.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.DropNulls(ctx, d.Cols...)
}

func executeCast(ctx context.Context, cfg execConfig, c CastNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, c.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	col, err := input.Column(c.Col)
	if err != nil {
		return nil, err
	}
	casted, err := compute.Cast(ctx, col, c.To, compute.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	// Rebuild the frame with the cast column substituted.
	names := input.Schema().Names()
	cols := make([]*series.Series, len(names))
	for i, n := range names {
		if n == c.Col {
			cols[i] = casted
			continue
		}
		c0, _ := input.Column(n)
		cols[i] = c0.Clone()
	}
	return dataframe.New(cols...)
}

func executeCache(ctx context.Context, cfg execConfig, c CacheNode) (*dataframe.DataFrame, error) {
	c.state.once.Do(func() {
		df, err := executeNode(ctx, cfg, c.Input)
		c.state.df = df
		c.state.err = err
	})
	if c.state.err != nil {
		return nil, c.state.err
	}
	// Every caller gets a fresh reference they can Release independently.
	return c.state.df.Clone(), nil
}

func executeWithRowIndex(ctx context.Context, cfg execConfig, w WithRowIndexNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, w.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.WithRowIndex(w.Name, w.Offset)
}
