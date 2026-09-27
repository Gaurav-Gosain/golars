package lazy

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// FrameOpNode applies an eager DataFrame operation to its materialised
// input. Most polars LazyFrame methods that have no dedicated operator
// (explode, unpivot, shift, frame aggregates, group-by conveniences, ...)
// are expressed as a FrameOpNode.
//
// The optimizer treats the node as opaque: no pass pushes a predicate, a
// projection or a slice through it, so results never depend on whether
// the plan was optimized. Passes still rewrite the subtree below it.
type FrameOpNode struct {
	Input Node
	// Op is the polars-style operation name shown by Explain.
	Op string
	// Detail is an optional argument summary for Explain.
	Detail string
	// SchemaFn derives the output schema from the input schema. When nil
	// the op is run once on an empty frame of the input schema.
	SchemaFn func(in *schema.Schema) (*schema.Schema, error)
	// Fn computes the output. It borrows df and returns a new frame.
	Fn func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error)
}

func (FrameOpNode) isLogicalNode() {}

func (f FrameOpNode) Children() []Node { return []Node{f.Input} }

func (f FrameOpNode) WithChildren(children []Node) Node {
	if len(children) != 1 {
		panic("lazy: FrameOpNode takes one child")
	}
	out := f
	out.Input = children[0]
	return out
}

func (f FrameOpNode) Schema() (*schema.Schema, error) {
	in, err := f.Input.Schema()
	if err != nil {
		return nil, err
	}
	if f.SchemaFn != nil {
		return f.SchemaFn(in)
	}
	return schemaByEmptyRun(in, func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return f.Fn(context.Background(), df)
	})
}

func (f FrameOpNode) String() string {
	if f.Detail == "" {
		return f.Op
	}
	return f.Op + " " + f.Detail
}

// BinaryFrameOpNode is FrameOpNode for operations with two inputs such as
// join_asof, join_where, merge_sorted and update.
type BinaryFrameOpNode struct {
	Left, Right Node
	Op          string
	Detail      string
	SchemaFn    func(left, right *schema.Schema) (*schema.Schema, error)
	Fn          func(ctx context.Context, left, right *dataframe.DataFrame) (*dataframe.DataFrame, error)
}

func (BinaryFrameOpNode) isLogicalNode() {}

func (b BinaryFrameOpNode) Children() []Node { return []Node{b.Left, b.Right} }

func (b BinaryFrameOpNode) WithChildren(children []Node) Node {
	if len(children) != 2 {
		panic("lazy: BinaryFrameOpNode takes two children")
	}
	out := b
	out.Left, out.Right = children[0], children[1]
	return out
}

func (b BinaryFrameOpNode) Schema() (*schema.Schema, error) {
	ls, err := b.Left.Schema()
	if err != nil {
		return nil, err
	}
	rs, err := b.Right.Schema()
	if err != nil {
		return nil, err
	}
	if b.SchemaFn != nil {
		return b.SchemaFn(ls, rs)
	}
	le, err := emptyFrame(ls)
	if err != nil {
		return nil, err
	}
	defer le.Release()
	re, err := emptyFrame(rs)
	if err != nil {
		return nil, err
	}
	defer re.Release()
	out, err := b.Fn(context.Background(), le, re)
	if err != nil {
		return nil, fmt.Errorf("lazy: schema of %s: %w", b.Op, err)
	}
	defer out.Release()
	return out.Schema(), nil
}

func (b BinaryFrameOpNode) String() string {
	if b.Detail == "" {
		return b.Op
	}
	return b.Op + " " + b.Detail
}

// schemaByEmptyRun runs fn on a zero-row frame of sch and reports the
// schema of the result. It lets eager operations define their lazy schema
// without a hand-written inference rule.
func schemaByEmptyRun(sch *schema.Schema, fn func(*dataframe.DataFrame) (*dataframe.DataFrame, error)) (_ *schema.Schema, err error) {
	df, err := emptyFrame(sch)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("lazy: schema inference failed: %v", r)
		}
	}()
	out, err := fn(df)
	if err != nil {
		return nil, err
	}
	defer out.Release()
	return out.Schema(), nil
}

// emptyFrame returns a zero-row frame whose columns each hold one empty
// chunk, which every kernel accepts (unlike the chunkless columns of
// dataframe.Empty).
func emptyFrame(sch *schema.Schema) (*dataframe.DataFrame, error) {
	cols := make([]*series.Series, sch.Len())
	for i, f := range sch.Fields() {
		arr := array.MakeArrayOfNull(memory.DefaultAllocator, f.DType.Arrow(), 0)
		s, err := series.New(f.Name, arr)
		if err != nil {
			arr.Release()
			for _, c := range cols[:i] {
				c.Release()
			}
			return nil, err
		}
		cols[i] = s
	}
	return dataframe.New(cols...)
}

// executeInput materialises a child, streaming its prefix when the
// caller asked for streaming execution.
func executeInput(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, error) {
	if cfg.streaming {
		return executeMaybeStreaming(ctx, cfg, n)
	}
	return executeNode(ctx, cfg, n)
}

func executeFrameOp(ctx context.Context, cfg execConfig, f FrameOpNode) (*dataframe.DataFrame, error) {
	input, err := executeInput(ctx, cfg, f.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return f.Fn(ctx, input)
}

func executeBinaryFrameOp(ctx context.Context, cfg execConfig, b BinaryFrameOpNode) (*dataframe.DataFrame, error) {
	left, err := executeInput(ctx, cfg, b.Left)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := executeInput(ctx, cfg, b.Right)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	return b.Fn(ctx, left, right)
}

// frameOp is shorthand for appending a FrameOpNode to lf.
func (lf LazyFrame) frameOp(op, detail string, schemaFn func(*schema.Schema) (*schema.Schema, error),
	fn func(context.Context, *dataframe.DataFrame) (*dataframe.DataFrame, error)) LazyFrame {
	return LazyFrame{plan: FrameOpNode{Input: lf.plan, Op: op, Detail: detail, SchemaFn: schemaFn, Fn: fn}}
}

// sameSchema is the SchemaFn of operations that keep the input schema.
func sameSchema(in *schema.Schema) (*schema.Schema, error) { return in, nil }
