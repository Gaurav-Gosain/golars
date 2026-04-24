// Package expr defines the golars expression AST.
//
// Expressions are immutable values. Users build them through package-level
// constructors and fluent methods:
//
//	expr.Col("price").Mul(expr.Col("qty")).Alias("revenue")
//	expr.Col("x").GtLit(int64(10)).And(expr.Col("y").IsNotNull())
//
// Expressions describe a computation on a DataFrame. They are executed either
// eagerly through Eval or indirectly through the lazy planner. The same AST
// drives both paths.
package expr

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// BinaryOp is the set of binary operators the AST carries.
type BinaryOp uint8

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
)

func (o BinaryOp) symbol() string {
	switch o {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpEq:
		return "=="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	}
	return "?"
}

// UnaryOp is the set of unary operators the AST carries.
type UnaryOp uint8

const (
	OpNot UnaryOp = iota
	OpNeg
)

func (o UnaryOp) symbol() string {
	switch o {
	case OpNot:
		return "not"
	case OpNeg:
		return "-"
	}
	return "?"
}

// AggOp names an aggregation.
type AggOp uint8

const (
	AggSum AggOp = iota
	AggMin
	AggMax
	AggMean
	AggCount
	AggNullCount
	AggFirst
	AggLast
)

// String returns the polars-style short name for the aggregation.
func (o AggOp) String() string {
	return o.symbol()
}

func (o AggOp) symbol() string {
	switch o {
	case AggSum:
		return "sum"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggMean:
		return "mean"
	case AggCount:
		return "count"
	case AggNullCount:
		return "null_count"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	}
	return "?"
}

// Node is the tag interface for AST nodes. Users do not construct Nodes
// directly; they work with Expr values returned by constructors.
type Node interface {
	isNode()
	fmt.Stringer
}

// Expr is the public expression value. It wraps an internal Node and carries
// fluent builder methods.
type Expr struct{ node Node }

// Node returns the underlying AST node for pattern matching by internal
// consumers (the evaluator and the optimizer).
func (e Expr) Node() Node { return e.node }

// String returns a polars-style repr: col("a"), col("a") + 1, col("a").sum().
func (e Expr) String() string {
	if e.node == nil {
		return "<nil>"
	}
	return e.node.String()
}

// Hash returns a stable 64-bit hash of the expression structure. Used for
// common subexpression elimination.
func (e Expr) Hash() uint64 { return hashExpr(e) }

// ColNode is a reference to a named column.
type ColNode struct{ Name string }

func (ColNode) isNode()          {}
func (c ColNode) String() string { return fmt.Sprintf("col(%q)", c.Name) }

// LitNode is a typed literal value.
type LitNode struct {
	DType dtype.DType
	// Value is a typed Go value. Supported types: int64, float64, bool,
	// string. Use the Lit* constructors to build a LitNode safely.
	Value any
}

func (LitNode) isNode() {}
func (l LitNode) String() string {
	switch v := l.Value.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return fmt.Sprintf("%v", l.Value)
}

// BinaryNode combines two sub-expressions with an operator.
type BinaryNode struct {
	Op    BinaryOp
	Left  Expr
	Right Expr
}

func (BinaryNode) isNode() {}
func (b BinaryNode) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op.symbol(), b.Right)
}

// UnaryNode applies a unary operator.
type UnaryNode struct {
	Op  UnaryOp
	Arg Expr
}

func (UnaryNode) isNode() {}
func (u UnaryNode) String() string {
	return fmt.Sprintf("(%s %s)", u.Op.symbol(), u.Arg)
}

// AliasNode attaches a name to an expression.
type AliasNode struct {
	Inner Expr
	Name  string
}

func (AliasNode) isNode() {}
func (a AliasNode) String() string {
	return fmt.Sprintf("%s.alias(%q)", a.Inner, a.Name)
}

// CastNode coerces an expression to a target dtype.
type CastNode struct {
	Inner Expr
	To    dtype.DType
}

func (CastNode) isNode() {}
func (c CastNode) String() string {
	return fmt.Sprintf("%s.cast(%s)", c.Inner, c.To)
}

// AggNode aggregates an expression to a single scalar or one value per group.
type AggNode struct {
	Op    AggOp
	Inner Expr
}

func (AggNode) isNode() {}
func (a AggNode) String() string {
	return fmt.Sprintf("%s.%s()", a.Inner, a.Op.symbol())
}

// IsNullNode tests for null, or (with Negate) non-null.
type IsNullNode struct {
	Inner  Expr
	Negate bool
}

func (IsNullNode) isNode() {}
func (n IsNullNode) String() string {
	if n.Negate {
		return fmt.Sprintf("%s.is_not_null()", n.Inner)
	}
	return fmt.Sprintf("%s.is_null()", n.Inner)
}

// WhenThenNode is a conditional: when(pred).then(a).otherwise(b).
type WhenThenNode struct {
	Pred      Expr
	Then      Expr
	Otherwise Expr
}

func (WhenThenNode) isNode() {}
func (w WhenThenNode) String() string {
	return fmt.Sprintf("when(%s).then(%s).otherwise(%s)", w.Pred, w.Then, w.Otherwise)
}

// Constructors ---------------------------------------------------------------

// Col returns an expression that references a column by name.
//
// The name is resolved at execution time against the frame's
// schema, so a typo or missing column surfaces when the lazy plan
// runs, not when the expression is built. [Col] is the starting
// point of almost every expression: arithmetic, predicates, string
// methods, aggregates, and window functions all chain off of it.
//
// # Parameters
//
//   - name: column name in the frame under evaluation.
//
// # Returns
//
// An [Expr] that, at eval time, produces the referenced column.
//
// # Examples
//
// Build a predicate from a column:
//
//	expr.Col("age").Gt(expr.LitInt64(18))
//
// Project and alias:
//
//	expr.Col("salary").Mul(expr.LitFloat64(12)).Alias("annual")
//
// Use in a full pipeline:
//
//	df.Lazy().
//	    Filter(expr.Col("salary").Gt(expr.LitInt64(100_000))).
//	    GroupBy("dept").
//	    Agg(expr.Col("salary").Mean().Alias("avg"))
func Col(name string) Expr { return Expr{ColNode{Name: name}} }

// LitInt64 returns a typed int64 scalar literal.
//
// Use [Lit] for Go-idiomatic type inference; reach for LitInt64
// directly when you need to pin the dtype (e.g. comparing against
// an i64 column where the Go source would otherwise infer int).
//
// Example:
//
//	expr.Col("score").Gt(expr.LitInt64(80))
func LitInt64(v int64) Expr { return Expr{LitNode{DType: dtype.Int64(), Value: v}} }

// LitFloat64 returns a typed float64 scalar literal. See [LitInt64]
// for when to prefer this over [Lit].
func LitFloat64(v float64) Expr { return Expr{LitNode{DType: dtype.Float64(), Value: v}} }

// LitBool returns a typed bool scalar literal.
func LitBool(v bool) Expr { return Expr{LitNode{DType: dtype.Bool(), Value: v}} }

// LitString returns a typed utf8 scalar literal.
func LitString(v string) Expr { return Expr{LitNode{DType: dtype.String(), Value: v}} }

// LitNull returns a null literal carrying dt as its declared dtype.
// Useful inside [When] / [Coalesce] where the branches must agree on
// a concrete type.
//
// Example:
//
//	expr.Coalesce(expr.Col("primary"), expr.LitNull(dtype.String()))
func LitNull(dt dtype.DType) Expr { return Expr{LitNode{DType: dt, Value: nil}} }

// Lit is a type-inferring literal constructor.
//
// The Go type of v decides the dtype: `int` / `int32` / `int64`
// become i64, `float32` / `float64` become f64, plus `bool`,
// `string`, and `nil` (typed null). Anything else panics, so reach
// for [LitInt64] / [LitFloat64] / [LitString] when the exact dtype
// matters for a mixed-type comparison.
//
// # Parameters
//
//   - v: Go scalar value; see above for supported types.
//
// # Returns
//
// An [Expr] producing the literal when evaluated.
//
// # Examples
//
// Scalar literals inline:
//
//	expr.Col("qty").Mul(expr.Lit(2))           // int inferred as i64
//	expr.Col("tag").Eq(expr.Lit("priority"))   // string literal
//	expr.Lit(nil)                              // typed null
//
// Prefer a typed constructor when the RHS dtype matters:
//
//	expr.Col("score_f64").Gt(expr.LitFloat64(80)) // f64 != i64(80)
func Lit(v any) Expr {
	switch x := v.(type) {
	case int:
		return LitInt64(int64(x))
	case int32:
		return LitInt64(int64(x))
	case int64:
		return LitInt64(x)
	case float32:
		return LitFloat64(float64(x))
	case float64:
		return LitFloat64(x)
	case bool:
		return LitBool(x)
	case string:
		return LitString(x)
	case nil:
		return Expr{LitNode{DType: dtype.Null(), Value: nil}}
	}
	panic(fmt.Sprintf("expr.Lit: unsupported literal type %T", v))
}

// When starts a conditional expression, polars-style. Always chain
// exactly one [WhenBuilder.Then] and one [WhenThenBuilder.Otherwise];
// the resulting [Expr] carries the promoted dtype of both branches.
//
// Example:
//
//	// Clamp salary to a grade bucket.
//	bucket := expr.When(expr.Col("salary").Gt(expr.LitInt64(150_000))).
//	    Then(expr.LitString("senior")).
//	    Otherwise(
//	        expr.When(expr.Col("salary").Gt(expr.LitInt64(80_000))).
//	            Then(expr.LitString("mid")).
//	            Otherwise(expr.LitString("junior")),
//	    ).Alias("grade")
func When(pred Expr) WhenBuilder { return WhenBuilder{pred: pred} }

// WhenBuilder captures a predicate awaiting Then().
type WhenBuilder struct{ pred Expr }

// Then records the value taken when the predicate is true.
func (w WhenBuilder) Then(v Expr) WhenThenBuilder {
	return WhenThenBuilder{pred: w.pred, then: v}
}

// WhenThenBuilder captures pred+then awaiting Otherwise().
type WhenThenBuilder struct{ pred, then Expr }

// Otherwise closes the conditional and returns an Expr.
func (w WhenThenBuilder) Otherwise(v Expr) Expr {
	return Expr{WhenThenNode{Pred: w.pred, Then: w.then, Otherwise: v}}
}

// Fluent methods on Expr -----------------------------------------------------

// Add returns `e + other`, computed element-wise.
//
// Null propagation: if either operand is null at a row, the result
// is null. Numeric dtype promotion mirrors polars (i64 + f64 → f64).
//
// # Parameters
//
//   - other: right-hand side [Expr]; must be numeric.
//
// # Returns
//
// An [Expr] producing the sum.
//
// # Examples
//
//	// Two columns:
//	expr.Col("base").Add(expr.Col("bonus"))
//
//	// Column + scalar:
//	expr.Col("price").Add(expr.LitFloat64(1.0))
//
// See also [Expr.AddLit] for scalar RHS, [Expr.Sub], [Expr.Mul],
// [Expr.Div].
func (e Expr) Add(other Expr) Expr { return binary(OpAdd, e, other) }

// AddLit is sugar for e.Add([Lit](v)). Use it when the RHS is a
// plain Go value so you don't have to write the constructor twice.
func (e Expr) AddLit(v any) Expr { return binary(OpAdd, e, Lit(v)) }

// Sub returns e - other. See [Expr.Add] for null semantics.
func (e Expr) Sub(other Expr) Expr { return binary(OpSub, e, other) }

// SubLit is sugar for e.Sub([Lit](v)).
func (e Expr) SubLit(v any) Expr { return binary(OpSub, e, Lit(v)) }

// Mul returns e * other.
func (e Expr) Mul(other Expr) Expr { return binary(OpMul, e, other) }

// MulLit is sugar for e.Mul([Lit](v)).
func (e Expr) MulLit(v any) Expr { return binary(OpMul, e, Lit(v)) }

// Div returns e / other. Integer operands divide as integers; mix a
// float operand to force float output.
func (e Expr) Div(other Expr) Expr { return binary(OpDiv, e, other) }

// DivLit is sugar for e.Div([Lit](v)).
func (e Expr) DivLit(v any) Expr { return binary(OpDiv, e, Lit(v)) }

// Eq returns the element-wise equality test (`e == other`).
//
// The result is a boolean [Expr] suitable for
// [lazy.LazyFrame.Filter] or as the predicate of [When].
//
// # Parameters
//
//   - other: right-hand side [Expr]; dtypes must be compatible.
//
// # Returns
//
// A boolean [Expr].
//
// # Examples
//
//	expr.Col("status").Eq(expr.LitString("active"))
//
//	// Compose predicates:
//	expr.Col("dept").Eq(expr.LitString("eng")).
//	    And(expr.Col("active").Eq(expr.LitBool(true)))
//
// See also [Expr.Ne], [Expr.EqLit].
func (e Expr) Eq(other Expr) Expr { return binary(OpEq, e, other) }

// EqLit is sugar for e.Eq([Lit](v)).
func (e Expr) EqLit(v any) Expr { return binary(OpEq, e, Lit(v)) }

// Ne returns the element-wise inequality test (`e != other`).
func (e Expr) Ne(other Expr) Expr { return binary(OpNe, e, other) }

// NeLit is sugar for e.Ne([Lit](v)).
func (e Expr) NeLit(v any) Expr { return binary(OpNe, e, Lit(v)) }

// Lt returns the element-wise less-than test (`e < other`).
func (e Expr) Lt(other Expr) Expr { return binary(OpLt, e, other) }

// LtLit is sugar for e.Lt([Lit](v)).
func (e Expr) LtLit(v any) Expr { return binary(OpLt, e, Lit(v)) }

// Le returns the element-wise less-than-or-equal test (`e <= other`).
func (e Expr) Le(other Expr) Expr { return binary(OpLe, e, other) }

// LeLit is sugar for e.Le([Lit](v)).
func (e Expr) LeLit(v any) Expr { return binary(OpLe, e, Lit(v)) }

// Gt returns the element-wise greater-than test (`e > other`).
//
// # Parameters
//
//   - other: right-hand side [Expr]; dtypes must be comparable.
//
// # Returns
//
// A boolean [Expr].
//
// # Examples
//
//	// Filter rows where age > 18:
//	df.Lazy().Filter(expr.Col("age").Gt(expr.LitInt64(18)))
//
//	// Chain on another op:
//	expr.Col("salary").Gt(expr.LitFloat64(100_000)).Alias("bulk")
func (e Expr) Gt(other Expr) Expr { return binary(OpGt, e, other) }

// GtLit is sugar for e.Gt([Lit](v)).
func (e Expr) GtLit(v any) Expr { return binary(OpGt, e, Lit(v)) }

// Ge returns the element-wise greater-than-or-equal test (`e >= other`).
func (e Expr) Ge(other Expr) Expr { return binary(OpGe, e, other) }

// GeLit is sugar for e.Ge([Lit](v)).
func (e Expr) GeLit(v any) Expr { return binary(OpGe, e, Lit(v)) }

// And returns the element-wise logical AND of two boolean
// expressions. Null in either operand yields null.
//
// # Parameters
//
//   - other: boolean [Expr].
//
// # Returns
//
// A boolean [Expr].
//
// # Examples
//
//	expr.Col("age").Ge(expr.LitInt64(18)).And(
//	    expr.Col("active").Eq(expr.LitBool(true)),
//	)
//
// See also [Expr.Or], [Expr.Not].
func (e Expr) And(other Expr) Expr { return binary(OpAnd, e, other) }

// Or returns the element-wise logical OR. Operands must be bool.
func (e Expr) Or(other Expr) Expr { return binary(OpOr, e, other) }

// Not returns the element-wise logical negation. Operand must be bool.
func (e Expr) Not() Expr { return Expr{UnaryNode{Op: OpNot, Arg: e}} }

// Neg returns the element-wise arithmetic negation (`-e`).
func (e Expr) Neg() Expr { return Expr{UnaryNode{Op: OpNeg, Arg: e}} }

// Alias renames the expression's output column.
//
// The renamed expression is still a valid right-hand side for
// another op, so aliases can chain. Repeated Alias calls collapse:
// `e.Alias("a").Alias("b")` is equivalent to `e.Alias("b")`.
//
// # Parameters
//
//   - name: new column name for the result.
//
// # Returns
//
// An [Expr] that renames this one's output.
//
// # Examples
//
//	// Derive a yearly salary column:
//	expr.Col("salary").Mul(expr.LitFloat64(12)).Alias("annual")
//
//	// Assign a human-friendly name to a predicate:
//	expr.Col("age").Ge(expr.LitInt64(18)).Alias("adult")
func (e Expr) Alias(name string) Expr {
	// Collapse double alias: col("a").alias("b").alias("c") == col("a").alias("c")
	if a, ok := e.node.(AliasNode); ok {
		return Expr{AliasNode{Inner: a.Inner, Name: name}}
	}
	return Expr{AliasNode{Inner: e, Name: name}}
}

// Cast coerces the expression result to the target dtype.
func (e Expr) Cast(to dtype.DType) Expr {
	// Collapse redundant casts of the same dtype.
	if c, ok := e.node.(CastNode); ok && c.To.Equal(to) {
		return e
	}
	return Expr{CastNode{Inner: e, To: to}}
}

// IsNull returns a boolean expression that is true where the input is null.
func (e Expr) IsNull() Expr { return Expr{IsNullNode{Inner: e, Negate: false}} }

// IsNotNull returns a boolean expression that is true where the input is not null.
func (e Expr) IsNotNull() Expr { return Expr{IsNullNode{Inner: e, Negate: true}} }

// Sum aggregates the expression with sum.
func (e Expr) Sum() Expr { return Expr{AggNode{Op: AggSum, Inner: e}} }

// Min aggregates with min.
func (e Expr) Min() Expr { return Expr{AggNode{Op: AggMin, Inner: e}} }

// Max aggregates with max.
func (e Expr) Max() Expr { return Expr{AggNode{Op: AggMax, Inner: e}} }

// Mean aggregates with arithmetic mean.
func (e Expr) Mean() Expr { return Expr{AggNode{Op: AggMean, Inner: e}} }

// Count returns the number of non-null values.
func (e Expr) Count() Expr { return Expr{AggNode{Op: AggCount, Inner: e}} }

// NullCount returns the number of null values.
func (e Expr) NullCount() Expr { return Expr{AggNode{Op: AggNullCount, Inner: e}} }

// First returns the first value (aggregation).
func (e Expr) First() Expr { return Expr{AggNode{Op: AggFirst, Inner: e}} }

// Last returns the last value (aggregation).
func (e Expr) Last() Expr { return Expr{AggNode{Op: AggLast, Inner: e}} }

// Walk visits every sub-expression depth-first. Returning false from fn
// prunes the subtree.
func Walk(e Expr, fn func(Expr) bool) {
	if !fn(e) {
		return
	}
	for _, c := range Children(e) {
		Walk(c, fn)
	}
}

// Children returns the direct sub-expressions of e, in a stable order.
func Children(e Expr) []Expr {
	switch n := e.node.(type) {
	case BinaryNode:
		return []Expr{n.Left, n.Right}
	case UnaryNode:
		return []Expr{n.Arg}
	case AliasNode:
		return []Expr{n.Inner}
	case CastNode:
		return []Expr{n.Inner}
	case AggNode:
		return []Expr{n.Inner}
	case IsNullNode:
		return []Expr{n.Inner}
	case WhenThenNode:
		return []Expr{n.Pred, n.Then, n.Otherwise}
	case FunctionNode:
		return append([]Expr(nil), n.Args...)
	case OverNode:
		return []Expr{n.Inner}
	}
	return nil
}

// WithChildren constructs a new expression with the given children replacing
// the originals. Panics if len(children) does not match the expected arity of
// the node.
func WithChildren(e Expr, children []Expr) Expr {
	switch n := e.node.(type) {
	case ColNode, LitNode:
		if len(children) != 0 {
			panic(fmt.Sprintf("expr: %T has no children", n))
		}
		return e
	case BinaryNode:
		mustArity(n, children, 2)
		return Expr{BinaryNode{Op: n.Op, Left: children[0], Right: children[1]}}
	case UnaryNode:
		mustArity(n, children, 1)
		return Expr{UnaryNode{Op: n.Op, Arg: children[0]}}
	case AliasNode:
		mustArity(n, children, 1)
		return Expr{AliasNode{Inner: children[0], Name: n.Name}}
	case CastNode:
		mustArity(n, children, 1)
		return Expr{CastNode{Inner: children[0], To: n.To}}
	case AggNode:
		mustArity(n, children, 1)
		return Expr{AggNode{Op: n.Op, Inner: children[0]}}
	case IsNullNode:
		mustArity(n, children, 1)
		return Expr{IsNullNode{Inner: children[0], Negate: n.Negate}}
	case WhenThenNode:
		mustArity(n, children, 3)
		return Expr{WhenThenNode{Pred: children[0], Then: children[1], Otherwise: children[2]}}
	case FunctionNode:
		// Arity fixed by number of Args; Params unchanged.
		mustArity(n, children, len(n.Args))
		newArgs := append([]Expr(nil), children...)
		return Expr{FunctionNode{Name: n.Name, Args: newArgs, Params: n.Params}}
	case OverNode:
		mustArity(n, children, 1)
		return Expr{OverNode{Inner: children[0], Keys: n.Keys}}
	}
	panic(fmt.Sprintf("expr: WithChildren on unknown node %T", e.node))
}

// Columns returns the set of distinct column names referenced by e.
// OverNode partition keys count as references even though they don't
// appear as ColNode children.
func Columns(e Expr) []string {
	seen := map[string]struct{}{}
	var order []string
	add := func(name string) {
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		order = append(order, name)
	}
	Walk(e, func(x Expr) bool {
		switch n := x.node.(type) {
		case ColNode:
			add(n.Name)
		case OverNode:
			for _, k := range n.Keys {
				add(k)
			}
		}
		return true
	})
	return order
}

// OutputName returns the inferred name of the expression's output column.
// An Alias defines the name explicitly; otherwise the first referenced
// column name is used (mirroring polars).
func OutputName(e Expr) string {
	if e.node == nil {
		return ""
	}
	if a, ok := e.node.(AliasNode); ok {
		return a.Name
	}
	cols := Columns(e)
	if len(cols) > 0 {
		return cols[0]
	}
	// For literals, use the literal value as the default name.
	if l, ok := e.node.(LitNode); ok {
		return fmt.Sprintf("literal[%s]", l.DType)
	}
	return "expr"
}

// ContainsAgg reports whether e contains an AggNode anywhere in its subtree.
func ContainsAgg(e Expr) bool {
	found := false
	Walk(e, func(x Expr) bool {
		if _, ok := x.node.(AggNode); ok {
			found = true
			return false
		}
		return true
	})
	return found
}

// Equal reports whether two expressions are structurally identical.
func Equal(a, b Expr) bool {
	if a.node == nil || b.node == nil {
		return a.node == b.node
	}
	if a.String() != b.String() {
		// Quick short-circuit: different repr => different expressions.
		// (String is structural in this package.)
		return false
	}
	// Deep compare for safety.
	return a.Hash() == b.Hash()
}

func binary(op BinaryOp, l, r Expr) Expr {
	return Expr{BinaryNode{Op: op, Left: l, Right: r}}
}

func mustArity(n Node, children []Expr, want int) {
	if len(children) != want {
		panic(fmt.Sprintf("expr: %T expects %d children, got %d", n, want, len(children)))
	}
}

func hashExpr(e Expr) uint64 {
	h := fnv.New64a()
	writeExprHash(h, e)
	return h.Sum64()
}

func writeExprHash(h interface {
	Write(p []byte) (n int, err error)
}, e Expr) {
	if e.node == nil {
		h.Write([]byte("nil"))
		return
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%T|", e.node)
	switch n := e.node.(type) {
	case ColNode:
		sb.WriteString(n.Name)
	case LitNode:
		sb.WriteString(n.DType.String())
		sb.WriteString("=")
		sb.WriteString(n.String())
	case BinaryNode:
		sb.WriteByte(byte(n.Op))
	case UnaryNode:
		sb.WriteByte(byte(n.Op))
	case AliasNode:
		sb.WriteString(n.Name)
	case CastNode:
		sb.WriteString(n.To.String())
	case AggNode:
		sb.WriteByte(byte(n.Op))
	case IsNullNode:
		if n.Negate {
			sb.WriteByte('!')
		}
	case WhenThenNode:
		// no extra scalar fields
	}
	h.Write([]byte(sb.String()))
	for _, c := range Children(e) {
		h.Write([]byte{'['})
		writeExprHash(h, c)
		h.Write([]byte{']'})
	}
}
