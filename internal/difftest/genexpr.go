package difftest

import (
	"fmt"
	"slices"
)

// Class is a coarse type class the expression generator asks for.
type Class uint8

const (
	CAny Class = iota
	CNum
	CInt
	CFloat
	CBool
	CStr
	CTemporal // date, datetime, duration, time
	CDateLike // date, datetime
	CList
	CStruct
	CCat
)

func (c Class) has(t Type) bool {
	switch c {
	case CAny:
		return true
	case CNum:
		return t.IsNumeric()
	case CInt:
		return t.IsInt()
	case CFloat:
		return t.IsFloat()
	case CBool:
		return t.K == KBool
	case CStr:
		return t.K == KStr
	case CTemporal:
		return t.IsTemporal()
	case CDateLike:
		return t.K == KDate || t.K == KDatetime
	case CList:
		return t.K == KList
	case CStruct:
		return t.K == KStruct
	case CCat:
		return t.K == KCat
	}
	return false
}

// ColInfo is a column visible to the expression generator.
type ColInfo struct {
	Name string
	T    Type
}

// Scope describes where an expression is generated.
type Scope struct {
	Cols []ColInfo
	// Ordered allows functions whose result depends on row order.
	Ordered bool
	// Agg is set inside group_by().agg().
	Agg bool
	// NoAggs forbids aggregations (for example inside over() or a
	// predicate that must stay row aligned).
	NoAggs bool
}

// TE is a typed generated expression.
type TE struct {
	E *Expr
	T Type
	// Scalar marks a one-value result (an aggregation).
	Scalar bool
	// LenChange marks a result whose length differs from the frame.
	LenChange bool
}

// ExprGen generates random expressions.
type ExprGen struct {
	*Gen
	nextAlias int
	// LiteralTrees is the probability of keeping a column-free
	// subexpression (see sub).
	LiteralTrees float64
}

func (g *ExprGen) colsOf(s *Scope, c Class) []ColInfo {
	var out []ColInfo
	for _, ci := range s.Cols {
		if c.has(ci.T) {
			out = append(out, ci)
		}
	}
	return out
}

// Alias returns a fresh output name.
func (g *ExprGen) Alias() string {
	g.nextAlias++
	return fmt.Sprintf("o%d", g.nextAlias)
}

// Leaf draws a column of class c or a literal.
func (g *ExprGen) Leaf(s *Scope, c Class) TE {
	cols := g.colsOf(s, c)
	if len(cols) > 0 && (g.Chance(0.85) || !g.canLit(c)) {
		ci := pick(g.Gen, cols)
		return TE{E: Col(ci.Name), T: ci.T}
	}
	return g.Lit(c)
}

func (g *ExprGen) canLit(c Class) bool {
	switch c {
	case CList, CStruct, CCat, CTemporal:
		return false
	}
	return true
}

// Lit draws a literal of class c.
func (g *ExprGen) Lit(c Class) TE {
	switch c {
	case CAny:
		return g.Lit(pick(g.Gen, []Class{CInt, CFloat, CBool, CStr}))
	case CNum:
		if g.Chance(0.6) {
			return g.Lit(CInt)
		}
		return g.Lit(CFloat)
	case CInt:
		v := int64(g.Intn(11) - 5)
		if g.Chance(0.05) {
			v = pick(g.Gen, []int64{1 << 31, -1 << 31, 1<<63 - 1, -1 << 63, 255, 256, 127, 128, 65535})
		}
		if g.Chance(0.15) {
			dt := pick(g.Gen, intTypes)
			if dt.K == KUint && v < 0 {
				v = -v
			}
			return TE{E: &Expr{K: "lit", Val: v, DType: dt.Name()}, T: dt, Scalar: true}
		}
		return TE{E: LitV(v), T: TI64, Scalar: true}
	case CFloat:
		v := float64(g.Intn(21)-10) / 4
		if g.Chance(0.05) {
			v = pick(g.Gen, specialFloats())
		}
		return TE{E: LitF(v), T: TF64, Scalar: true}
	case CBool:
		if g.Chance(0.1) {
			return TE{E: &Expr{K: "lit", Val: nil, DType: "bool"}, T: TBool, Scalar: true}
		}
		return TE{E: LitV(g.Chance(0.5)), T: TBool, Scalar: true}
	case CStr:
		return TE{E: LitV(pick(g.Gen, strPool)), T: TStr, Scalar: true}
	case CDateLike, CTemporal:
		y, m, d := int64(1990+g.Intn(50)), int64(1+g.Intn(12)), int64(1+g.Intn(28))
		return TE{E: Fn("date", Raw(y), Raw(m), Raw(d)), T: TDate, Scalar: true}
	}
	return TE{E: LitV(int64(0)), T: TI64, Scalar: true}
}

func specialFloats() []float64 {
	nan := 0.0
	return []float64{nan / zero(), 1 / zero(), -1 / zero(), negZero(), 1e308, -1e308, 5e-324, 0.1, 1e-10}
}

func zero() float64 { return 0 }

func negZero() float64 {
	z := zero()
	return -z
}

// Expr draws an expression of class c at the given depth budget.
func (g *ExprGen) Expr(s *Scope, depth int, c Class) TE {
	if c == CAny {
		c = pick(g.Gen, []Class{CNum, CNum, CNum, CBool, CStr, CStr, CTemporal, CDateLike, CList, CAny})
		if c == CAny {
			ci := pick(g.Gen, s.Cols)
			return TE{E: Col(ci.Name), T: ci.T}
		}
	}
	if depth <= 0 || g.Chance(0.2) {
		return g.Leaf(s, c)
	}
	for range 8 {
		var r TE
		var ok bool
		switch c {
		case CNum, CInt, CFloat:
			r, ok = g.numExpr(s, depth, c)
		case CBool:
			r, ok = g.boolExpr(s, depth)
		case CStr:
			r, ok = g.strExpr(s, depth)
		case CTemporal, CDateLike:
			r, ok = g.temporalExpr(s, depth, c)
		case CList:
			r, ok = g.listExpr(s, depth)
		case CCat, CStruct:
			r, ok = g.Leaf(s, c), len(g.colsOf(s, c)) > 0
		}
		if ok {
			if r.LenChange && s.Agg {
				// Inside agg a length-changing expression is fine; the
				// result becomes a list. Keep it rare.
				if g.Chance(0.7) {
					continue
				}
			}
			return r
		}
	}
	return g.Leaf(s, c)
}

// sub draws a row-aligned (non length changing) subexpression.
func (g *ExprGen) sub(s *Scope, depth int, c Class) TE {
	for range 6 {
		r := g.Expr(s, depth-1, c)
		if r.LenChange {
			continue
		}
		// A tree without columns is a literal computation. polars keeps
		// those at length one while golars broadcasts literals early,
		// which is one known bug that would otherwise dominate the
		// results. LiteralTrees sets how often they are kept.
		if !hasCol(r.E) && !g.Chance(g.LiteralTrees) {
			continue
		}
		return r
	}
	return g.Leaf(s, c)
}

func hasCol(e *Expr) bool {
	found := false
	e.Walk(func(x *Expr) {
		if x.K == "col" {
			found = true
		}
	})
	return found
}

// subNoAgg draws a row-aligned expression that is not an aggregate.
func (g *ExprGen) subNoAgg(s *Scope, depth int, c Class) TE {
	inner := *s
	inner.NoAggs = true
	for range 6 {
		r := g.Expr(&inner, depth-1, c)
		if !r.LenChange && !r.Scalar {
			return r
		}
	}
	cols := g.colsOf(s, c)
	if len(cols) > 0 {
		ci := pick(g.Gen, cols)
		return TE{E: Col(ci.Name), T: ci.T}
	}
	return g.Leaf(s, c)
}

func numSuper(a, b Type) Type {
	if a.K == KNull {
		return b
	}
	if b.K == KNull {
		return a
	}
	if a.Equal(b) {
		return a
	}
	if a.IsFloat() || b.IsFloat() {
		if a.IsFloat() && b.IsFloat() {
			return Type{K: KFloat, Bits: max(a.Bits, b.Bits)}
		}
		return TF64
	}
	if a.K == b.K {
		return Type{K: a.K, Bits: max(a.Bits, b.Bits)}
	}
	return TI64
}

func litLike(r TE) Type {
	// A bare int or float literal adopts the other side's type in
	// polars, so the result keeps the column type.
	return r.T
}

func (g *ExprGen) numExpr(s *Scope, depth int, c Class) (TE, bool) {
	switch g.Intn(20) {
	case 0, 1, 2, 3:
		// Arithmetic.
		a := g.sub(s, depth, CNum)
		b := g.sub(s, depth, CNum)
		if g.Chance(0.4) {
			b = g.Lit(CNum)
		}
		op := pick(g.Gen, []string{"+", "-", "*", "/", "//", "%", "+", "-", "*", "**"})
		t := numSuper(a.T, b.T)
		if b.E.K == "lit" && b.E.DType == "" && a.T.IsNumeric() && (!b.T.IsFloat() || a.T.IsFloat()) {
			t = litLike(a)
		}
		if op == "/" && !t.IsFloat() {
			t = TF64
		}
		if op == "**" {
			b = TE{E: LitV(int64(g.Intn(4))), T: TI64, Scalar: true}
			if a.T.IsFloat() && g.Chance(0.5) {
				b = TE{E: LitF(0.5), T: TF64, Scalar: true}
			}
		}
		return TE{E: Bin(op, a.E, b.E), T: t, Scalar: a.Scalar && b.Scalar}, true
	case 4:
		a := g.sub(s, depth, CNum)
		return TE{E: Neg(a.E), T: a.T, Scalar: a.Scalar}, true
	case 5, 6, 7:
		return g.numUnary(s, depth)
	case 8, 9:
		return g.aggregate(s, depth, CNum)
	case 10, 11:
		return g.orderDependent(s, depth)
	case 12:
		return g.countLike(s, depth)
	case 13:
		return g.cast(s, depth, pick(g.Gen, append(slices.Clone(intTypes), TF32, TF64, TF64, TI64)))
	case 14:
		return g.when(s, depth, CNum)
	case 15:
		return g.overExpr(s, depth, CNum)
	case 16:
		return g.horizontal(s, depth)
	case 17:
		return g.fillNull(s, depth, CNum)
	case 18:
		return g.temporalPart(s, depth)
	case 19:
		return g.rank(s, depth)
	}
	return TE{}, false
}

func (g *ExprGen) numUnary(s *Scope, depth int) (TE, bool) {
	a := g.sub(s, depth, CNum)
	ft := TF64
	if a.T.K == KFloat && a.T.Bits == 32 {
		ft = TF32
	}
	type u struct {
		name  string
		out   Type
		args  []*Expr
		float bool // only for float receivers
	}
	lo := int64(g.Intn(5) - 3)
	us := []u{
		{name: "abs", out: a.T},
		{name: "sqrt", out: ft},
		{name: "cbrt", out: ft},
		{name: "exp", out: ft},
		{name: "log", out: ft},
		{name: "log10", out: ft},
		{name: "log1p", out: ft},
		{name: "sin", out: ft},
		{name: "cos", out: ft},
		{name: "tan", out: ft},
		{name: "arctan", out: ft},
		{name: "arcsin", out: ft},
		{name: "tanh", out: ft},
		{name: "sinh", out: ft},
		{name: "degrees", out: ft},
		{name: "sign", out: a.T},
		{name: "floor", out: a.T, float: true},
		{name: "ceil", out: a.T, float: true},
		{name: "round", out: a.T, args: []*Expr{Raw(int64(g.Intn(4)))}, float: true},
		{name: "clip", out: a.T, args: []*Expr{Raw(lo), Raw(lo + int64(g.Intn(6)))}},
		{name: "fill_nan", out: a.T, args: []*Expr{RawF(float64(g.Intn(3)))}, float: true},
	}
	pickU := pick(g.Gen, us)
	if pickU.float && !a.T.IsFloat() {
		pickU = us[0]
	}
	if pickU.name == "sign" && a.T.K == KUint {
		pickU = us[0]
	}
	return TE{E: Call(a.E, pickU.name, pickU.args...), T: pickU.out, Scalar: a.Scalar}, true
}

func (g *ExprGen) aggregate(s *Scope, depth int, c Class) (TE, bool) {
	if s.NoAggs {
		return TE{}, false
	}
	var a TE
	if c == CNum && g.Chance(0.3) {
		a = g.sub(s, depth, CBool) // sum/mean of bools
	} else {
		a = g.subNoAgg(s, depth, pick(g.Gen, []Class{CNum, CNum, CNum, CTemporal, CStr}))
	}
	var names []string
	switch {
	case a.T.IsNumeric() || a.T.K == KBool:
		names = []string{"sum", "mean", "min", "max", "median", "std", "var", "product", "n_unique", "count", "null_count", "quantile", "skew", "kurtosis", "len", "nan_max", "nan_min"}
	case a.T.K == KStr:
		names = []string{"min", "max", "n_unique", "count", "null_count", "len"}
	case a.T.IsTemporal():
		names = []string{"min", "max", "n_unique", "count", "null_count", "median", "mean"}
	default:
		names = []string{"n_unique", "count", "null_count", "len"}
	}
	if s.Ordered {
		names = append(names, "first", "last", "arg_min", "arg_max")
	}
	name := pick(g.Gen, names)
	out := a.T
	e := Call(a.E, name)
	switch name {
	case "mean", "median", "std", "var", "skew", "kurtosis":
		if !a.T.IsTemporal() {
			out = TF64
			if a.T.K == KFloat && a.T.Bits == 32 {
				out = TF32
			}
		}
		if name == "std" || name == "var" {
			if g.Chance(0.3) {
				e.Args = append(e.Args, Raw(int64(g.Intn(3))))
			}
		}
		if (name == "skew" || name == "kurtosis") && g.Chance(0.05) {
			e.With("bias", Raw(false))
		}
	case "quantile":
		e.Args = append(e.Args, RawF(pick(g.Gen, []float64{0, 0.25, 0.5, 0.9, 1})))
		e.With("interpolation", Raw(pick(g.Gen, []string{"nearest", "higher", "lower", "midpoint", "linear"})))
		out = TF64
	case "n_unique", "count", "null_count", "len", "arg_min", "arg_max":
		out = TU32
	case "sum", "product":
		if a.T.K == KBool {
			out = TU32
		}
		if a.T.IsInt() && a.T.Bits < 64 {
			out = TI64
		}
	}
	return TE{E: e, T: out, Scalar: true}, true
}

func (g *ExprGen) orderDependent(s *Scope, depth int) (TE, bool) {
	if !s.Ordered {
		return TE{}, false
	}
	a := g.subNoAgg(s, depth, CNum)
	ft := TF64
	switch g.Intn(12) {
	case 0:
		e := Call(a.E, pick(g.Gen, []string{"cum_sum", "cum_min", "cum_max", "cum_prod", "cum_count"}))
		if g.Chance(0.05) {
			e.With("reverse", Raw(true))
		}
		return TE{E: e, T: a.T}, true
	case 1:
		return TE{E: Call(a.E, "shift", Raw(int64(g.Intn(7)-3))), T: a.T}, true
	case 2:
		return TE{E: Call(a.E, "diff", Raw(int64(g.Intn(3)+1))), T: a.T}, true
	case 3:
		return TE{E: Call(a.E, "pct_change", Raw(int64(g.Intn(2)+1))), T: ft}, true
	case 4, 5, 6:
		name := pick(g.Gen, []string{"rolling_mean", "rolling_sum", "rolling_min", "rolling_max", "rolling_std", "rolling_var", "rolling_median"})
		e := Call(a.E, name, Raw(int64(1+g.Intn(4))))
		if g.Chance(0.5) {
			e.With("min_samples", Raw(int64(1+g.Intn(2))))
		}
		if g.Chance(0.05) {
			e.With("center", Raw(true))
		}
		out := a.T
		if name != "rolling_min" && name != "rolling_max" && name != "rolling_sum" {
			out = ft
		}
		return TE{E: e, T: out}, true
	case 7:
		return TE{E: Call(a.E, pick(g.Gen, []string{"forward_fill", "backward_fill"})), T: a.T}, true
	case 8:
		return TE{E: Call(a.E, "ewm_mean").With("alpha", RawF(0.5)), T: ft}, true
	case 9:
		return TE{E: Call(a.E, "interpolate"), T: ft}, true
	case 10:
		return TE{E: Call(a.E, "reverse"), T: a.T}, true
	case 11:
		return TE{E: Call(a.E, "rle_id"), T: TU32}, true
	}
	return TE{}, false
}

func (g *ExprGen) rank(s *Scope, depth int) (TE, bool) {
	a := g.subNoAgg(s, depth, pick(g.Gen, []Class{CNum, CStr, CTemporal}))
	methods := []string{"average", "min", "max", "dense"}
	if s.Ordered {
		methods = append(methods, "ordinal")
	}
	m := pick(g.Gen, methods)
	out := TU32
	if m == "average" {
		out = TF64
	}
	e := Call(a.E, "rank", Raw(m))
	if g.Chance(0.4) {
		e.With("descending", Raw(true))
	}
	return TE{E: e, T: out}, true
}

func (g *ExprGen) countLike(s *Scope, depth int) (TE, bool) {
	switch g.Intn(4) {
	case 0:
		a := g.sub(s, depth, CStr)
		return TE{E: Call(a.E, pick(g.Gen, []string{"str.len_chars", "str.len_bytes"})), T: TU32, Scalar: a.Scalar}, true
	case 1:
		a := g.sub(s, depth, CList)
		if a.T.K != KList {
			return TE{}, false
		}
		name := pick(g.Gen, []string{"list.len", "list.n_unique"})
		if a.T.Elem.IsNumeric() {
			name = pick(g.Gen, []string{"list.len", "list.sum", "list.min", "list.max", "list.mean", "list.first", "list.last"})
		}
		out := TU32
		switch name {
		case "list.sum", "list.min", "list.max", "list.first", "list.last":
			out = *a.T.Elem
		case "list.mean":
			out = TF64
		}
		return TE{E: Call(a.E, name), T: out, Scalar: a.Scalar}, true
	case 2:
		a := g.sub(s, depth, CStr)
		return TE{E: Call(a.E, "str.count_matches", Raw(pick(g.Gen, []string{"a", ",", "b", "é"}))).With("literal", Raw(true)), T: TU32, Scalar: a.Scalar}, true
	case 3:
		a := g.sub(s, depth, CStr)
		return TE{E: Call(a.E, "str.to_integer").With("strict", Raw(false)), T: TI64, Scalar: a.Scalar}, true
	}
	return TE{}, false
}

func (g *ExprGen) cast(s *Scope, depth int, to Type) (TE, bool) {
	a := g.sub(s, depth, pick(g.Gen, []Class{CNum, CNum, CBool, CStr, CTemporal}))
	e := Call(a.E, "cast", DT(to.Name()))
	if g.Chance(0.9) {
		e.With("strict", Raw(false))
	}
	return TE{E: e, T: to, Scalar: a.Scalar}, true
}

func (g *ExprGen) when(s *Scope, depth int, c Class) (TE, bool) {
	e := &Expr{K: "when"}
	var t Type
	n := 1 + g.Intn(2)
	for i := range n {
		cond := g.sub(s, depth, CBool)
		v := g.sub(s, depth, c)
		if i == 0 {
			t = v.T
		}
		e.Args = append(e.Args, cond.E, v.E)
	}
	if g.Chance(0.75) {
		e.Else = g.sub(s, depth, c).E
	}
	return TE{E: e, T: t}, true
}

func (g *ExprGen) overExpr(s *Scope, depth int, c Class) (TE, bool) {
	if s.Agg || s.NoAggs {
		return TE{}, false
	}
	var keys []*Expr
	for _, ci := range s.Cols {
		if ci.T.K == KStr || ci.T.K == KBool || ci.T.IsInt() || ci.T.K == KCat || ci.T.K == KDate {
			keys = append(keys, Raw(ci.Name))
		}
	}
	if len(keys) == 0 {
		return TE{}, false
	}
	var inner TE
	var ok bool
	if s.Ordered && g.Chance(0.4) {
		inner, ok = g.orderDependent(s, depth)
	} else if g.Chance(0.2) {
		inner, ok = g.rank(s, depth)
	} else {
		inner, ok = g.aggregate(s, depth, c)
		inner.Scalar = false
	}
	if !ok {
		return TE{}, false
	}
	k := []*Expr{pick(g.Gen, keys)}
	if len(keys) > 1 && g.Chance(0.3) {
		k = append(k, pick(g.Gen, keys))
	}
	return TE{E: Call(inner.E, "over", k...), T: inner.T}, true
}

func (g *ExprGen) horizontal(s *Scope, depth int) (TE, bool) {
	a := g.sub(s, depth, CNum)
	b := g.sub(s, depth, CNum)
	name := pick(g.Gen, []string{"sum_horizontal", "min_horizontal", "max_horizontal", "mean_horizontal", "coalesce"})
	t := numSuper(a.T, b.T)
	if name == "mean_horizontal" {
		t = TF64
	}
	return TE{E: Fn(name, a.E, b.E), T: t, Scalar: a.Scalar && b.Scalar}, true
}

func (g *ExprGen) fillNull(s *Scope, depth int, c Class) (TE, bool) {
	a := g.sub(s, depth, c)
	if s.Ordered && g.Chance(0.2) {
		st := pick(g.Gen, []string{"forward", "backward"})
		if g.Chance(0.1) {
			st = pick(g.Gen, []string{"min", "max", "zero", "one", "mean"})
		}
		return TE{E: Call(a.E, "fill_null").With("strategy", Raw(st)), T: a.T}, true
	}
	var v TE
	if g.Chance(0.5) {
		v = g.Lit(c)
	} else {
		v = g.sub(s, depth, c)
	}
	return TE{E: Call(a.E, "fill_null", v.E), T: a.T, Scalar: a.Scalar && v.Scalar}, true
}

func (g *ExprGen) temporalPart(s *Scope, depth int) (TE, bool) {
	cols := g.colsOf(s, CTemporal)
	if len(cols) == 0 {
		return TE{}, false
	}
	a := g.sub(s, depth, CTemporal)
	var names []string
	out := TI32
	switch a.T.K {
	case KDate:
		names = []string{"year", "month", "day", "weekday", "week", "ordinal_day", "quarter", "iso_year"}
	case KDatetime:
		names = []string{"year", "month", "day", "hour", "minute", "second", "millisecond", "microsecond", "nanosecond", "weekday", "ordinal_day", "quarter", "week"}
	case KTime:
		names = []string{"hour", "minute", "second", "nanosecond"}
	case KDuration:
		names = []string{"total_days", "total_hours", "total_minutes", "total_seconds", "total_milliseconds", "total_microseconds"}
		out = TI64
	default:
		return TE{}, false
	}
	name := pick(g.Gen, names)
	switch name {
	case "month", "day", "weekday", "week", "quarter", "hour", "minute", "second":
		out = TI8
	case "ordinal_day":
		out = TI16
	}
	return TE{E: Call(a.E, "dt."+name), T: out, Scalar: a.Scalar}, true
}

func (g *ExprGen) boolExpr(s *Scope, depth int) (TE, bool) {
	switch g.Intn(16) {
	case 0, 1, 2, 3:
		// Comparison between two values of the same class.
		c := pick(g.Gen, []Class{CNum, CNum, CStr, CTemporal, CBool, CCat})
		a := g.sub(s, depth, c)
		var b TE
		switch {
		case c == CCat && a.T.K == KCat:
			b = TE{E: LitV(pick(g.Gen, catPool)), T: TStr, Scalar: true}
		case a.T.K == KDate && g.Chance(0.5):
			b = g.Lit(CDateLike)
		case a.T.IsTemporal() || a.T.K == KCat || a.T.K == KList || a.T.K == KStruct:
			cols := g.colsOf(s, CAny)
			var same []ColInfo
			for _, ci := range cols {
				if ci.T.Equal(a.T) {
					same = append(same, ci)
				}
			}
			if len(same) == 0 {
				return TE{}, false
			}
			ci := pick(g.Gen, same)
			b = TE{E: Col(ci.Name), T: ci.T}
		default:
			b = g.sub(s, depth, c)
			if g.Chance(0.4) && g.canLit(c) {
				b = g.Lit(c)
			}
		}
		op := pick(g.Gen, []string{"==", "!=", "<", "<=", ">", ">="})
		if c == CCat {
			op = pick(g.Gen, []string{"==", "!="})
		}
		if g.Chance(0.1) {
			return TE{E: Call(a.E, pick(g.Gen, []string{"eq_missing", "ne_missing"}), b.E), T: TBool, Scalar: a.Scalar && b.Scalar}, true
		}
		return TE{E: Bin(op, a.E, b.E), T: TBool, Scalar: a.Scalar && b.Scalar}, true
	case 4, 5:
		a := g.sub(s, depth, CBool)
		b := g.sub(s, depth, CBool)
		op := pick(g.Gen, []string{"&", "|", "&", "|", "xor"})
		if op == "xor" {
			return TE{E: Call(a.E, "xor", b.E), T: TBool, Scalar: a.Scalar && b.Scalar}, true
		}
		return TE{E: Bin(op, a.E, b.E), T: TBool, Scalar: a.Scalar && b.Scalar}, true
	case 6:
		a := g.sub(s, depth, CBool)
		return TE{E: Not(a.E), T: TBool, Scalar: a.Scalar}, true
	case 7:
		a := g.sub(s, depth, CAny)
		return TE{E: Call(a.E, pick(g.Gen, []string{"is_null", "is_not_null"})), T: TBool, Scalar: a.Scalar}, true
	case 8:
		a := g.sub(s, depth, pick(g.Gen, []Class{CNum, CStr, CBool}))
		var vals []any
		for range 1 + g.Intn(4) {
			switch {
			case a.T.IsInt():
				vals = append(vals, int64(g.Intn(11)-5))
			case a.T.IsFloat():
				vals = append(vals, float64(g.Intn(11)-5)/2)
			case a.T.K == KStr:
				vals = append(vals, pick(g.Gen, strPool))
			case a.T.K == KBool:
				vals = append(vals, g.Chance(0.5))
			}
		}
		if len(vals) == 0 {
			return TE{}, false
		}
		arg := Raw(vals)
		arg.F = a.T.IsFloat()
		return TE{E: Call(a.E, "is_in", arg), T: TBool, Scalar: a.Scalar}, true
	case 9:
		a := g.sub(s, depth, CNum)
		lo := int64(g.Intn(7) - 3)
		e := Call(a.E, "is_between", LitV(lo), LitV(lo+int64(g.Intn(5))))
		if g.Chance(0.5) {
			e.With("closed", Raw(pick(g.Gen, []string{"both", "left", "right", "none"})))
		}
		return TE{E: e, T: TBool, Scalar: a.Scalar}, true
	case 10:
		a := g.sub(s, depth, CStr)
		pat := pick(g.Gen, []string{"a", "A", "", "é", "ab", " ", "1"})
		name := pick(g.Gen, []string{"str.starts_with", "str.ends_with", "str.contains"})
		e := Call(a.E, name, Raw(pat))
		if name == "str.contains" {
			if g.Chance(0.5) {
				e.With("literal", Raw(true))
			} else {
				e.Args[1] = Raw(pick(g.Gen, []string{"^a", "b$", "[0-9]+", "(?i)abc", "a|b", "\\s", "^$"}))
			}
		}
		return TE{E: e, T: TBool, Scalar: a.Scalar}, true
	case 11:
		a := g.sub(s, depth, CFloat)
		if !a.T.IsFloat() {
			return TE{}, false
		}
		return TE{E: Call(a.E, pick(g.Gen, []string{"is_nan", "is_not_nan", "is_finite", "is_infinite"})), T: TBool, Scalar: a.Scalar}, true
	case 12:
		if s.NoAggs {
			return TE{}, false
		}
		a := g.subNoAgg(s, depth, CBool)
		return TE{E: Call(a.E, pick(g.Gen, []string{"any", "all"})), T: TBool, Scalar: true}, true
	case 13:
		a := g.subNoAgg(s, depth, pick(g.Gen, []Class{CNum, CStr, CBool}))
		names := []string{"is_duplicated", "is_unique"}
		if s.Ordered {
			names = append(names, "is_first_distinct", "is_last_distinct")
		}
		return TE{E: Call(a.E, pick(g.Gen, names)), T: TBool}, true
	case 14:
		a := g.sub(s, depth, CDateLike)
		if !CDateLike.has(a.T) {
			return TE{}, false
		}
		return TE{E: Call(a.E, "dt.is_leap_year"), T: TBool, Scalar: a.Scalar}, true
	case 15:
		return g.when(s, depth, CBool)
	}
	return TE{}, false
}

func (g *ExprGen) strExpr(s *Scope, depth int) (TE, bool) {
	a := g.sub(s, depth, CStr)
	if a.T.K != KStr {
		return TE{}, false
	}
	st := func(e *Expr) (TE, bool) { return TE{E: e, T: TStr, Scalar: a.Scalar}, true }
	switch g.Intn(20) {
	case 0:
		return st(Call(a.E, pick(g.Gen, []string{"str.to_uppercase", "str.to_lowercase", "str.to_titlecase", "str.reverse"})))
	case 1:
		e := Call(a.E, pick(g.Gen, []string{"str.strip_chars", "str.strip_chars_start", "str.strip_chars_end"}))
		if g.Chance(0.4) {
			e.Args = append(e.Args, Raw(pick(g.Gen, []string{"a", " x", "ab"})))
		}
		return st(e)
	case 2:
		off := int64(g.Intn(7) - 3)
		e := Call(a.E, "str.slice", Raw(off))
		if g.Chance(0.9) {
			e.Args = append(e.Args, Raw(int64(g.Intn(4))))
		}
		return st(e)
	case 3:
		return st(Call(a.E, pick(g.Gen, []string{"str.head", "str.tail"}), Raw(int64(g.Intn(7)-2))))
	case 4:
		return st(Call(a.E, pick(g.Gen, []string{"str.pad_start", "str.pad_end"}), Raw(int64(g.Intn(6))), Raw(pick(g.Gen, []string{"*", " ", "0"}))))
	case 5:
		return st(Call(a.E, "str.zfill", Raw(int64(g.Intn(6)))))
	case 6:
		name := pick(g.Gen, []string{"str.replace", "str.replace_all"})
		e := Call(a.E, name, Raw(pick(g.Gen, []string{"a", "b", ",", ""})), Raw(pick(g.Gen, []string{"X", "", "éé"})))
		e.With("literal", Raw(true))
		return st(e)
	case 7:
		if g.Chance(0.8) {
			return TE{}, false
		}
		name := pick(g.Gen, []string{"str.replace", "str.replace_all"})
		return st(Call(a.E, name, Raw(pick(g.Gen, []string{"[ab]", "^.", "(a)(b)", "[0-9]+", "\\s+"})), Raw(pick(g.Gen, []string{"X", "", "$1"}))))
	case 8:
		return st(Call(a.E, pick(g.Gen, []string{"str.strip_prefix", "str.strip_suffix"}), Raw(pick(g.Gen, []string{"a", "ab", " ", "c"}))))
	case 9:
		return st(Call(a.E, "str.extract", Raw(pick(g.Gen, []string{"([a-z]+)", "(\\d+)", "(a)(b)?", "^(.)"})), Raw(int64(1))))
	case 10:
		b := g.sub(s, depth, CStr)
		e := Fn("concat_str", a.E, b.E)
		if g.Chance(0.5) {
			e.With("separator", Raw(pick(g.Gen, []string{"-", "", ", "})))
		}
		if g.Chance(0.05) {
			e.With("ignore_nulls", Raw(true))
		}
		return TE{E: e, T: TStr, Scalar: a.Scalar && b.Scalar}, true
	case 11:
		x := g.sub(s, depth, pick(g.Gen, []Class{CNum, CBool, CTemporal, CCat}))
		if x.T.K == KDatetime && x.T.TZ != "" {
			// polars renders the offset; keep but rare
			if g.Chance(0.5) {
				return TE{}, false
			}
		}
		return TE{E: Call(x.E, "cast", DT("str")), T: TStr, Scalar: x.Scalar}, true
	case 12:
		return g.when(s, depth, CStr)
	case 13:
		return g.fillNull(s, depth, CStr)
	case 14:
		x := g.sub(s, depth, CDateLike)
		if !CDateLike.has(x.T) {
			return TE{}, false
		}
		f := pick(g.Gen, []string{"%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%a %b %e", "%j", "%H%M", "%Y-%m-%dT%H:%M:%S%.f", "%s"})
		if x.T.K == KDate && (f == "%Y/%m/%d %H:%M:%S" || f == "%H%M" || f == "%Y-%m-%dT%H:%M:%S%.f") {
			f = "%Y-%m-%d"
		}
		return TE{E: Call(x.E, "dt.strftime", Raw(f)), T: TStr, Scalar: x.Scalar}, true
	case 15:
		l := g.sub(s, depth, CList)
		if l.T.K != KList || l.T.Elem.K != KStr {
			return TE{}, false
		}
		return TE{E: Call(l.E, "list.join", Raw(pick(g.Gen, []string{",", "", " | "}))), T: TStr, Scalar: l.Scalar}, true
	case 16:
		if s.NoAggs {
			return TE{}, false
		}
		b := g.subNoAgg(s, depth, CStr)
		return TE{E: Call(b.E, pick(g.Gen, []string{"min", "max"})), T: TStr, Scalar: true}, true
	case 17:
		return st(Call(a.E, "str.to_uppercase"))
	case 18:
		if !s.Ordered {
			return TE{}, false
		}
		b := g.subNoAgg(s, depth, CStr)
		return TE{E: Call(b.E, "shift", Raw(int64(g.Intn(5)-2))), T: TStr}, true
	case 19:
		return g.overExpr(s, depth, CStr)
	}
	return TE{}, false
}

func (g *ExprGen) temporalExpr(s *Scope, depth int, c Class) (TE, bool) {
	a := g.sub(s, depth, c)
	if !a.T.IsTemporal() {
		return TE{}, false
	}
	same := func(e *Expr) (TE, bool) { return TE{E: e, T: a.T, Scalar: a.Scalar}, true }
	switch g.Intn(12) {
	case 0, 1:
		if a.T.K != KDate && a.T.K != KDatetime {
			return TE{}, false
		}
		every := pick(g.Gen, []string{"1d", "1h", "15m", "1mo", "1y", "1w", "2d", "3h", "1q"})
		if a.T.K == KDate && (every == "1h" || every == "15m" || every == "3h") {
			every = "1d"
		}
		return same(Call(a.E, pick(g.Gen, []string{"dt.truncate", "dt.truncate", "dt.round"}), Raw(every)))
	case 2, 3:
		if a.T.K != KDate && a.T.K != KDatetime {
			return TE{}, false
		}
		by := pick(g.Gen, []string{"1d", "-1d", "1mo", "-1mo", "1y", "2h", "-30m", "1w", "1mo1d", "1q"})
		if a.T.K == KDate && (by == "2h" || by == "-30m") {
			by = "3d"
		}
		return same(Call(a.E, "dt.offset_by", Raw(by)))
	case 4:
		if a.T.K != KDate && a.T.K != KDatetime {
			return TE{}, false
		}
		return same(Call(a.E, pick(g.Gen, []string{"dt.month_start", "dt.month_end"})))
	case 5:
		if a.T.K != KDatetime {
			return TE{}, false
		}
		return TE{E: Call(a.E, "dt.date"), T: TDate, Scalar: a.Scalar}, true
	case 6:
		if a.T.K != KDatetime {
			return TE{}, false
		}
		if a.T.TZ == "" {
			tz := pick(g.Gen, tzPool[2:])
			e := Call(a.E, "dt.replace_time_zone", Raw(tz))
			if g.Chance(0.5) {
				e.With("ambiguous", Raw(pick(g.Gen, []string{"earliest", "latest", "null"})))
				e.With("non_existent", Raw("null"))
			}
			return TE{E: e, T: Datetime(a.T.Unit, tz), Scalar: a.Scalar}, true
		}
		tz := pick(g.Gen, tzPool[2:])
		return TE{E: Call(a.E, "dt.convert_time_zone", Raw(tz)), T: Datetime(a.T.Unit, tz), Scalar: a.Scalar}, true
	case 7:
		// Difference of two columns of the same temporal type.
		if a.T.K != KDate && a.T.K != KDatetime {
			return TE{}, false
		}
		for _, ci := range s.Cols {
			if ci.T.Equal(a.T) && g.Chance(0.5) {
				unit := a.T.Unit
				if a.T.K == KDate {
					unit = "ms"
				}
				return TE{E: Bin("-", a.E, Col(ci.Name)), T: Duration(unit)}, true
			}
		}
		return TE{}, false
	case 8:
		if a.T.K != KDatetime && a.T.K != KDuration {
			return TE{}, false
		}
		u := pick(g.Gen, []string{"ms", "us", "ns"})
		t := a.T
		t.Unit = u
		return TE{E: Call(a.E, "dt.cast_time_unit", Raw(u)), T: t, Scalar: a.Scalar}, true
	case 9:
		// datetime + duration column
		if a.T.K != KDatetime && a.T.K != KDate {
			return TE{}, false
		}
		for _, ci := range s.Cols {
			if ci.T.K == KDuration {
				t := a.T
				if a.T.K == KDate {
					t = Datetime(ci.T.Unit, "")
				}
				return TE{E: Bin(pick(g.Gen, []string{"+", "-"}), a.E, Col(ci.Name)), T: t, Scalar: false}, true
			}
		}
		return TE{}, false
	case 10:
		return g.when(s, depth, c)
	case 11:
		if s.NoAggs {
			return TE{}, false
		}
		b := g.subNoAgg(s, depth, c)
		if !b.T.IsTemporal() {
			return TE{}, false
		}
		return TE{E: Call(b.E, pick(g.Gen, []string{"min", "max"})), T: b.T, Scalar: true}, true
	}
	return TE{}, false
}

func (g *ExprGen) listExpr(s *Scope, depth int) (TE, bool) {
	switch g.Intn(8) {
	case 0, 1:
		a := g.sub(s, depth, CStr)
		if a.T.K != KStr {
			return TE{}, false
		}
		return TE{E: Call(a.E, "str.split", Raw(pick(g.Gen, []string{",", "", " ", "a"}))), T: ListOf(TStr), Scalar: a.Scalar}, true
	case 2, 3, 4:
		a := g.sub(s, depth, CList)
		if a.T.K != KList {
			return TE{}, false
		}
		var e *Expr
		switch g.Intn(8) {
		case 0:
			e = Call(a.E, "list.sort")
			if g.Chance(0.5) {
				e.With("descending", Raw(true))
			}
			if g.Chance(0.3) {
				e.With("nulls_last", Raw(true))
			}
		case 1:
			e = Call(a.E, "list.reverse")
		case 2:
			e = Call(a.E, "list.unique").With("maintain_order", Raw(true))
		case 3:
			e = Call(a.E, "list.slice", Raw(int64(g.Intn(5)-2)), Raw(int64(g.Intn(3))))
		case 4:
			e = Call(a.E, pick(g.Gen, []string{"list.head", "list.tail"}), Raw(int64(g.Intn(4))))
		case 5:
			e = Call(a.E, "list.drop_nulls")
		case 6:
			e = Call(a.E, "list.shift", Raw(int64(g.Intn(3)-1)))
		case 7:
			return TE{E: Call(a.E, "list.get", Raw(int64(g.Intn(5)-2))).With("null_on_oob", Raw(true)), T: *a.T.Elem, Scalar: a.Scalar}, true
		}
		return TE{E: e, T: a.T, Scalar: a.Scalar}, true
	case 5:
		a := g.sub(s, depth, CList)
		if a.T.K != KList {
			return TE{}, false
		}
		var v *Expr
		switch {
		case a.T.Elem.IsInt():
			v = Raw(int64(g.Intn(5) - 2))
		case a.T.Elem.K == KStr:
			v = Raw(pick(g.Gen, strPool))
		default:
			return TE{}, false
		}
		return TE{E: Call(a.E, "list.contains", v), T: TBool, Scalar: a.Scalar}, true
	case 6:
		if !s.Agg || s.NoAggs {
			return TE{}, false
		}
		a := g.subNoAgg(s, depth, pick(g.Gen, []Class{CNum, CStr}))
		return TE{E: Call(a.E, "implode"), T: ListOf(a.T), Scalar: true}, true
	case 7:
		a := g.sub(s, depth, pick(g.Gen, []Class{CInt, CStr}))
		b := g.sub(s, depth, pick(g.Gen, []Class{CInt, CStr}))
		if !a.T.Equal(b.T) {
			return TE{}, false
		}
		return TE{E: Fn("concat_list", a.E, b.E), T: ListOf(a.T), Scalar: a.Scalar && b.Scalar}, true
	}
	return TE{}, false
}

// LengthChanging draws a top-level expression whose length differs
// from the frame (used in select, where it must stand alone).
func (g *ExprGen) LengthChanging(s *Scope, depth int) TE {
	a := g.subNoAgg(s, depth, pick(g.Gen, []Class{CNum, CStr, CTemporal, CBool}))
	switch g.Intn(8) {
	case 0:
		e := Call(a.E, "sort")
		if g.Chance(0.5) {
			e.With("descending", Raw(true))
		}
		if g.Chance(0.5) {
			e.With("nulls_last", Raw(true))
		}
		return TE{E: e, T: a.T, LenChange: true}
	case 1:
		return TE{E: Call(a.E, "drop_nulls"), T: a.T, LenChange: true}
	case 2:
		return TE{E: Call(Call(a.E, "unique"), "sort"), T: a.T, LenChange: true}
	case 3:
		if !s.Ordered {
			return TE{E: Call(Call(a.E, "unique"), "sort"), T: a.T, LenChange: true}
		}
		return TE{E: Call(a.E, "unique").With("maintain_order", Raw(true)), T: a.T, LenChange: true}
	case 4:
		if !s.Ordered {
			return TE{E: Call(a.E, "sort"), T: a.T, LenChange: true}
		}
		return TE{E: Call(a.E, pick(g.Gen, []string{"head", "tail"}), Raw(int64(g.Intn(6)))), T: a.T, LenChange: true}
	case 5:
		p := g.subNoAgg(s, depth, CBool)
		if !s.Ordered {
			return TE{E: Call(Call(a.E, "filter", p.E), "sort"), T: a.T, LenChange: true}
		}
		return TE{E: Call(a.E, "filter", p.E), T: a.T, LenChange: true}
	case 6:
		if a.T.IsNumeric() {
			return TE{E: Call(Call(a.E, pick(g.Gen, []string{"top_k", "bottom_k"}), Raw(int64(g.Intn(5)))), "sort"), T: a.T, LenChange: true}
		}
		return TE{E: Call(a.E, "drop_nulls"), T: a.T, LenChange: true}
	case 7:
		if a.T.IsFloat() {
			return TE{E: Call(a.E, "drop_nans"), T: a.T, LenChange: true}
		}
		return TE{E: Call(Call(a.E, "unique"), "sort"), T: a.T, LenChange: true}
	}
	return TE{E: Call(a.E, "sort"), T: a.T, LenChange: true}
}
