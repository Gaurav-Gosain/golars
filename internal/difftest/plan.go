package difftest

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Expr is a node of a query expression. The same tree is rendered to
// glr text for golars (exprparse) and interpreted directly by the
// Python runner for polars.
//
// Kinds:
//
//	col   Name is the column
//	lit   Val is the value (nil for null); F marks a float literal;
//	      DType, when set, casts the literal
//	raw   Val is a plain argument value (number, string, bool, list)
//	dtype DType is a dtype argument (cast targets)
//	bin   Name is one of + - * / // % ** == != < <= > >= & | ; Args
//	      has both operands
//	not, neg  Args[0]
//	call  Name is the method path ("abs", "str.slice"); Args[0] is the
//	      receiver, the rest positional arguments; KW keyword arguments
//	fn    Name is a top-level function (coalesce, concat_str, ...)
//	when  Args holds condition/value pairs; Else the otherwise branch
type Expr struct {
	K     string  `json:"k"`
	Name  string  `json:"name,omitempty"`
	Val   any     `json:"val,omitempty"`
	F     bool    `json:"f,omitempty"`
	DType string  `json:"dtype,omitempty"`
	Args  []*Expr `json:"args,omitempty"`
	KW    []KW    `json:"kw,omitempty"`
	Else  *Expr   `json:"else,omitempty"`
}

// KW is a keyword argument.
type KW struct {
	Name string `json:"name"`
	Val  *Expr  `json:"val"`
}

// Op is one frame-level step of a plan.
type Op struct {
	Op        string   `json:"op"`
	Exprs     []*Expr  `json:"exprs,omitempty"`
	Pred      *Expr    `json:"pred,omitempty"`
	Keys      []string `json:"keys,omitempty"`
	Desc      []bool   `json:"desc,omitempty"`
	NullsLast []bool   `json:"nulls_last,omitempty"`
	Maintain  bool     `json:"maintain_order,omitempty"`
	How       string   `json:"how,omitempty"`
	Offset    int      `json:"offset,omitempty"`
	N         int      `json:"n,omitempty"`
	Name      string   `json:"name,omitempty"`
	Strategy  string   `json:"strategy,omitempty"`
	By        []string `json:"by,omitempty"`
	On        []string `json:"on,omitempty"`
	Index     []string `json:"index,omitempty"`
	Values    string   `json:"values,omitempty"`
	Agg       string   `json:"agg,omitempty"`
	OnValues  []any    `json:"on_values,omitempty"`
	// ResetsOrder marks a select whose result is one row or a single
	// deterministic length-changing expression.
	ResetsOrder bool `json:"resets_order,omitempty"`
}

// Plan is a whole case: the query plus how results are compared.
type Plan struct {
	Seed  uint64 `json:"seed"`
	Ops   []Op   `json:"ops"`
	Order Order  `json:"order"`
	// HasRight says the case has a right-hand input frame.
	HasRight bool `json:"has_right,omitempty"`
}

// OrderMode says which row order the oracle guarantees.
type OrderMode string

const (
	// OrderExact: rows must match in order.
	OrderExact OrderMode = "exact"
	// OrderUnordered: rows compare as a multiset.
	OrderUnordered OrderMode = "unordered"
	// OrderSorted: Keys must match in order; whole rows as a multiset.
	OrderSorted OrderMode = "sorted"
	// OrderKeysOnly: only Keys are comparable, in order (a slice after
	// an unstable ordering).
	OrderKeysOnly OrderMode = "keys_only"
	// OrderCountOnly: only the schema and the height are comparable.
	OrderCountOnly OrderMode = "count_only"
)

// Order is the comparison contract of a plan's result.
type Order struct {
	Mode OrderMode `json:"mode"`
	Keys []string  `json:"keys,omitempty"`
}

// Clone deep-copies an expression.
func (e *Expr) Clone() *Expr {
	if e == nil {
		return nil
	}
	c := *e
	c.Args = make([]*Expr, len(e.Args))
	for i, a := range e.Args {
		c.Args[i] = a.Clone()
	}
	c.KW = make([]KW, len(e.KW))
	for i, k := range e.KW {
		c.KW[i] = KW{Name: k.Name, Val: k.Val.Clone()}
	}
	if len(e.Args) == 0 {
		c.Args = nil
	}
	if len(e.KW) == 0 {
		c.KW = nil
	}
	c.Else = e.Else.Clone()
	return &c
}

// ClonePlan deep-copies a plan through JSON.
func ClonePlan(p *Plan) *Plan {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	var out Plan
	if err := UnmarshalPlan(b, &out); err != nil {
		panic(err)
	}
	return &out
}

// UnmarshalPlan decodes a plan keeping integer literals exact.
func UnmarshalPlan(b []byte, p *Plan) error {
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	if err := dec.Decode(p); err != nil {
		return err
	}
	for i := range p.Ops {
		op := &p.Ops[i]
		for _, e := range op.Exprs {
			fixNumbers(e)
		}
		fixNumbers(op.Pred)
		for j, v := range op.OnValues {
			op.OnValues[j] = fixValue(v)
		}
	}
	return nil
}

func fixNumbers(e *Expr) {
	if e == nil {
		return
	}
	e.Val = fixValue(e.Val)
	for _, a := range e.Args {
		fixNumbers(a)
	}
	for _, k := range e.KW {
		fixNumbers(k.Val)
	}
	fixNumbers(e.Else)
}

func fixValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	case []any:
		for i := range x {
			x[i] = fixValue(x[i])
		}
	}
	return v
}

// Walk visits e and its descendants in pre-order.
func (e *Expr) Walk(fn func(*Expr)) {
	if e == nil {
		return
	}
	fn(e)
	for _, a := range e.Args {
		a.Walk(fn)
	}
	for _, k := range e.KW {
		k.Val.Walk(fn)
	}
	e.Else.Walk(fn)
}

// ---------------------------------------------------------------
// glr rendering.

// GLR renders e as a glr expression for exprparse.Parse.
func (e *Expr) GLR() string {
	var b strings.Builder
	e.glr(&b)
	return b.String()
}

var glrBinOps = map[string]string{"&": "and", "|": "or"}

func (e *Expr) glr(b *strings.Builder) {
	switch e.K {
	case "col":
		b.WriteString("col(" + quote(e.Name) + ")")
	case "lit":
		litExprGLR(b, e.Val, e.F)
		if e.DType != "" {
			b.WriteString(".cast(" + quote(e.DType) + ")")
		}
	case "raw":
		litGLR(b, e.Val, e.F)
	case "dtype":
		b.WriteString(quote(e.DType))
	case "bin":
		op := e.Name
		if g, ok := glrBinOps[op]; ok {
			op = g
		}
		b.WriteString("(")
		e.Args[0].glr(b)
		b.WriteString(" " + op + " ")
		e.Args[1].glr(b)
		b.WriteString(")")
	case "not":
		b.WriteString("(not ")
		e.Args[0].glr(b)
		b.WriteString(")")
	case "neg":
		b.WriteString("(-")
		e.Args[0].glr(b)
		b.WriteString(")")
	case "call":
		r := glrForm(e)
		r.Args[0].glr(b)
		b.WriteString("." + r.Name + "(")
		r.callArgs(b, r.Args[1:])
		b.WriteString(")")
	case "fn":
		b.WriteString(e.Name + "(")
		e.callArgs(b, e.Args)
		b.WriteString(")")
	case "when":
		b.WriteString("(")
		for i := 0; i+1 < len(e.Args); i += 2 {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString("when ")
			e.Args[i].glr(b)
			b.WriteString(" then ")
			e.Args[i+1].glr(b)
		}
		if e.Else != nil {
			b.WriteString(" otherwise ")
			e.Else.glr(b)
		}
		b.WriteString(")")
	default:
		panic("difftest: unknown expr kind " + e.K)
	}
}

func (e *Expr) callArgs(b *strings.Builder, args []*Expr) {
	n := 0
	for _, a := range args {
		if n > 0 {
			b.WriteString(", ")
		}
		a.glr(b)
		n++
	}
	for _, k := range e.KW {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k.Name + "=")
		k.Val.glr(b)
		n++
	}
}

func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// litExprGLR renders a literal expression. glr's lit() only takes a
// plain literal, so negative numbers become a negation and non-finite
// floats a division.
func litExprGLR(b *strings.Builder, v any, isFloat bool) {
	switch x := v.(type) {
	case int64:
		switch {
		case isFloat:
			litExprGLR(b, float64(x), false)
		case x == math.MinInt64:
			b.WriteString("(-lit(9223372036854775807) - lit(1))")
		case x < 0:
			fmt.Fprintf(b, "(-lit(%d))", -x)
		default:
			fmt.Fprintf(b, "lit(%d)", x)
		}
		return
	case float64:
		if math.Signbit(x) {
			b.WriteString("(-")
			litExprGLR(b, -x, false)
			b.WriteString(")")
			return
		}
	case string:
		if isFloat {
			litGLR(b, x, true)
			return
		}
	}
	b.WriteString("lit(")
	litGLR(b, v, isFloat)
	b.WriteString(")")
}

func litGLR(b *strings.Builder, v any, isFloat bool) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		if isFloat {
			switch x {
			case "nan":
				b.WriteString("(0.0 / 0.0)")
			case "inf":
				b.WriteString("(1.0 / 0.0)")
			case "-inf":
				b.WriteString("(-1.0 / 0.0)")
			case "-0.0":
				b.WriteString("(-0.0)")
			}
			return
		}
		b.WriteString(quote(x))
	case int64:
		if isFloat {
			litGLR(b, float64(x), false)
			return
		}
		if x < 0 {
			fmt.Fprintf(b, "(%d)", x)
		} else {
			fmt.Fprintf(b, "%d", x)
		}
	case int:
		litGLR(b, int64(x), false)
	case float64:
		s := strconv.FormatFloat(x, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eEn") {
			s += ".0"
		}
		if math.Signbit(x) {
			b.WriteString("(" + s + ")")
		} else {
			b.WriteString(s)
		}
	case []any:
		b.WriteString("[")
		for i, e := range x {
			if i > 0 {
				b.WriteString(", ")
			}
			litGLR(b, e, isFloat)
		}
		b.WriteString("]")
	default:
		fmt.Fprintf(b, "%v", x)
	}
}

// ---------------------------------------------------------------
// Constructors used by the generator and the regression tests.

func Col(name string) *Expr           { return &Expr{K: "col", Name: name} }
func LitV(v any) *Expr                { return &Expr{K: "lit", Val: v} }
func LitF(v float64) *Expr            { return &Expr{K: "lit", Val: floatVal(v), F: true} }
func Raw(v any) *Expr                 { return &Expr{K: "raw", Val: v} }
func RawF(v float64) *Expr            { return &Expr{K: "raw", Val: floatVal(v), F: true} }
func DT(name string) *Expr            { return &Expr{K: "dtype", DType: name} }
func Bin(op string, l, r *Expr) *Expr { return &Expr{K: "bin", Name: op, Args: []*Expr{l, r}} }
func Not(x *Expr) *Expr               { return &Expr{K: "not", Args: []*Expr{x}} }
func Neg(x *Expr) *Expr               { return &Expr{K: "neg", Args: []*Expr{x}} }

// Call builds recv.name(args...).
func Call(recv *Expr, name string, args ...*Expr) *Expr {
	return &Expr{K: "call", Name: name, Args: append([]*Expr{recv}, args...)}
}

// Fn builds a top-level function call.
func Fn(name string, args ...*Expr) *Expr { return &Expr{K: "fn", Name: name, Args: args} }

// With adds a keyword argument and returns e.
func (e *Expr) With(name string, v *Expr) *Expr {
	e.KW = append(e.KW, KW{Name: name, Val: v})
	return e
}

// Alias wraps e in .alias(name).
func Alias(e *Expr, name string) *Expr { return Call(e, "alias", Raw(name)) }

// floatVal encodes a float for JSON: non-finite values and -0.0 as
// strings, the rest as numbers.
func floatVal(f float64) any {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0 && math.Signbit(f):
		return "-0.0"
	}
	return f
}
