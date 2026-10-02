package exprparse

import (
	"fmt"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/script"
)

// val is a compiled expression together with the Go source that
// rebuilds it.
type val struct {
	e   expr.Expr
	src string
}

var (
	exprType     = reflect.TypeFor[expr.Expr]()
	exprPtrType  = reflect.TypeFor[*expr.Expr]()
	dtypeType    = reflect.TypeFor[dtype.DType]()
	timeUnitType = reflect.TypeFor[dtype.TimeUnit]()
	runeType     = reflect.TypeFor[rune]()
	rollOptType  = reflect.TypeFor[expr.RollingByOption]()
)

func compile(n *Node) (val, error) {
	v, err := compileNode(n)
	return v, locate(err, n)
}

func compileNode(n *Node) (val, error) {
	switch n.Kind {
	case KindLit:
		return litVal(n.Lit)
	case KindCol:
		return val{expr.Col(n.Name), fmt.Sprintf("expr.Col(%q)", n.Name)}, nil
	case KindList:
		return val{}, errAt(n, "a [list] is only valid as a function argument").
			withHint(`as in x in [1, 2] or cut(x, [0, 10])`)
	case KindNot, KindNeg:
		inner, err := compile(n.Args[0])
		if err != nil {
			return val{}, err
		}
		if n.Kind == KindNot {
			return val{inner.e.Not(), inner.src + ".Not()"}, nil
		}
		return val{inner.e.Neg(), inner.src + ".Neg()"}, nil
	case KindBinary:
		return compileBinary(n)
	case KindWhen:
		return compileWhen(n)
	case KindCall:
		return compileCall(n)
	}
	return val{}, fmt.Errorf("internal: unknown node kind %d", n.Kind)
}

func litVal(v any) (val, error) {
	switch x := v.(type) {
	case int64:
		return val{expr.LitInt(x), fmt.Sprintf("expr.LitInt(%d)", x)}, nil
	case float64:
		return val{expr.LitFloat(x), fmt.Sprintf("expr.LitFloat(%s)", goFloat(x))}, nil
	case string:
		return val{expr.LitString(x), fmt.Sprintf("expr.LitString(%q)", x)}, nil
	case bool:
		return val{expr.LitBool(x), fmt.Sprintf("expr.LitBool(%t)", x)}, nil
	case nil:
		return val{expr.LitNull(dtype.Null()), "expr.LitNull(dtype.Null())"}, nil
	}
	return val{}, fmt.Errorf("internal: unsupported literal %T", v)
}

// binaryMethods maps each infix operator to its expr.Expr method.
var binaryMethods = map[string]string{
	"+": "Add", "-": "Sub", "*": "Mul", "/": "Div", "//": "FloorDiv", "%": "Mod",
	"**": "PowExpr", "==": "Eq", "!=": "Ne", "<": "Lt", "<=": "Le", ">": "Gt",
	">=": "Ge", "and": "And", "or": "Or",
}

func compileBinary(n *Node) (val, error) {
	l, err := compile(n.Args[0])
	if err != nil {
		return val{}, err
	}
	r, err := compile(n.Args[1])
	if err != nil {
		return val{}, err
	}
	m := binaryMethods[n.Name]
	out := reflect.ValueOf(l.e).MethodByName(m).Call([]reflect.Value{reflect.ValueOf(r.e)})[0]
	return val{out.Interface().(expr.Expr), fmt.Sprintf("%s.%s(%s)", l.src, m, r.src)}, nil
}

// compileWhen nests `when a then x when b then y otherwise z` as
// When(a).Then(x).Otherwise(When(b).Then(y).Otherwise(z)). A missing
// otherwise yields null, as in polars.
func compileWhen(n *Node) (val, error) {
	acc, err := litVal(nil)
	if err != nil {
		return val{}, err
	}
	if n.Final != nil {
		if acc, err = compile(n.Final); err != nil {
			return val{}, err
		}
	}
	for i := len(n.Args) - 2; i >= 0; i -= 2 {
		pred, err := compile(n.Args[i])
		if err != nil {
			return val{}, err
		}
		then, err := compile(n.Args[i+1])
		if err != nil {
			return val{}, err
		}
		acc = val{
			expr.When(pred.e).Then(then.e).Otherwise(acc.e),
			fmt.Sprintf("expr.When(%s).Then(%s).Otherwise(%s)", pred.src, then.src, acc.src),
		}
	}
	return acc, nil
}

func compileCall(n *Node) (val, error) {
	if n.Recv != nil {
		recv, err := compile(n.Recv)
		if err != nil {
			return val{}, err
		}
		return compileMethod(n, n.NS, recv, n.Args, 0)
	}
	if n.NS != "" {
		v, err := compileNSFunction(n)
		if err != nil && n.ambig {
			col := val{expr.Col(n.NS), fmt.Sprintf("expr.Col(%q)", n.NS)}
			if v2, err2 := compileMethod(n, "", col, n.Args, 0); err2 == nil {
				return v2, nil
			}
		}
		return v, err
	}
	return compileFree(n)
}

// compileNSFunction handles `dt.year(ts)`: the first argument is the
// receiver of the namespaced method.
func compileNSFunction(n *Node) (val, error) {
	if _, found := lookupMethod(n.NS, n.Name); !found {
		return val{}, unknownFunction(n, "unknown "+n.NS+" function")
	}
	if len(n.Args) == 0 {
		return val{}, errAt(n, "%s.%s needs the value to work on as its first argument", n.NS, n.Name).
			withHint("as in %s.%s(x) or x.%s.%s()", n.NS, n.Name, n.NS, n.Name)
	}
	recv, err := compile(n.Args[0])
	if err != nil {
		return val{}, err
	}
	return compileMethod(n, n.NS, recv, n.Args[1:], 1)
}

func compileFree(n *Node) (val, error) {
	switch n.Name {
	case "col":
		if len(n.Args) != 1 || len(n.Kw) != 0 {
			return val{}, errAt(n, "col takes 1 argument, got %d", len(n.Args)+len(n.Kw)).withHint(`as in col("my column")`)
		}
		s, err := stringLit(n.Args[0], "col", 0)
		if err != nil {
			return val{}, err
		}
		return val{expr.Col(s), fmt.Sprintf("expr.Col(%q)", s)}, nil
	case "lit":
		if len(n.Args) != 1 || len(n.Kw) != 0 {
			return val{}, errAt(n, "lit takes 1 argument, got %d", len(n.Args)+len(n.Kw))
		}
		v := litOf(n.Args[0])
		if v == nil && !isNullLit(n.Args[0]) {
			return val{}, errAt(n.Args[0], "lit: expected a literal")
		}
		return litVal(v)
	}
	// Legacy spelling: `sum("x")` aggregates the column named x.
	if slices.Contains(legacyColumnAggs, n.Name) && len(n.Args) == 1 && len(n.Kw) == 0 &&
		n.Args[0].Kind == KindLit {
		s, isStr := n.Args[0].Lit.(string)
		if !isStr {
			return val{}, errAt(n.Args[0], "%s: expected a column name or expression", n.Name)
		}
		return compileMethod(&Node{Name: n.Name, Pos: n.Pos, End: n.End, NamePos: n.NamePos}, "",
			val{expr.Col(s), fmt.Sprintf("expr.Col(%q)", s)}, nil, 1)
	}
	_, isMethod := lookupMethod("", n.Name)
	if f, found := freeFuncs[n.Name]; found {
		v, err := f.call(n)
		if err == nil || !isMethod || len(n.Args) == 0 {
			return v, err
		}
	}
	if isMethod && len(n.Args) > 0 {
		recv, err := compile(n.Args[0])
		if err != nil {
			return val{}, err
		}
		return compileMethod(n, "", recv, n.Args[1:], 1)
	}
	if isMethod {
		return val{}, errAt(n, "%s needs the value to work on as its first argument", n.Name).
			withHint("as in %s(x) or x.%s()", n.Name, n.Name)
	}
	return val{}, unknownFunction(n, "unknown function")
}

// unknownFunction reports an unresolved call name with the closest
// known spelling.
func unknownFunction(n *Node, what string) *Error {
	e := errSpan(n.NamePos, n.NameEnd(), "%s %q", what, n.Name)
	var pool []string
	switch {
	case n.Recv != nil:
		pool = Functions()[n.NS]
	case n.NS != "":
		pool = Functions()[n.NS]
	default:
		fns := Functions()
		pool = append(append([]string(nil), fns["free"]...), fns[""]...)
	}
	if best := script.Closest(n.Name, pool); best != "" {
		e.Fix = best
		return e.withHint("did you mean %q?", best)
	}
	return e
}

func isNullLit(n *Node) bool { return n.Kind == KindLit && n.Lit == nil }

// compileMethod applies method name of namespace ns (or of expr.Expr
// itself when ns is "") to recv.
//
// call is the call node (for its name, keyword arguments and error
// positions); shift is 1 when the receiver was written as the first
// argument, so arity errors count the arguments the user typed.
func compileMethod(call *Node, ns string, recv val, args []*Node, shift int) (val, error) {
	name, kw := call.Name, call.Kw
	spec, found := lookupMethod(ns, name)
	display := name
	if ns != "" {
		display = ns + "." + name
	}
	if !found {
		probe := *call
		probe.NS = ns
		if ns == "" {
			return val{}, unknownFunction(&probe, "unknown method")
		}
		return val{}, unknownFunction(&probe, "unknown "+ns+" method")
	}
	if spec.custom != nil {
		v, err := spec.custom(recv, args, kw)
		return v, locate(err, call)
	}
	rv := reflect.ValueOf(recv.e)
	prefix := recv.src
	if ns != "" {
		acc := namespaces[ns]
		rv = rv.MethodByName(acc).Call(nil)[0]
		prefix += "." + acc + "()"
	}
	goName := spec.method
	if len(kw) > 0 && spec.kwMethod != "" {
		goName = spec.kwMethod
	}
	m := rv.MethodByName(goName)
	if !m.IsValid() {
		return val{}, fmt.Errorf("internal: %s maps to missing method %s", display, goName)
	}
	return invoke(call, m, display, prefix+"."+goName, args, kw, spec.defaults, spec.names, shift)
}

// invoke calls fn with glr arguments converted to its Go parameter
// types. Struct parameters (option types) and variadic option
// functions are filled from keyword arguments; the other parameters
// are positional, with trailing ones taking defaults when omitted and
// names letting keyword arguments fill them.
func invoke(call *Node, fn reflect.Value, display, goCall string, args []*Node, kw []KwArg, defaults []any, names []string, shift int) (val, error) {
	t := fn.Type()
	nIn := t.NumIn()
	variadic := t.IsVariadic()
	var positional, optStructs []int
	optFuncs := -1
	for i := range nIn {
		pt := t.In(i)
		switch {
		case variadic && i == nIn-1 && pt.Elem() == rollOptType:
			optFuncs = i
		case pt.Kind() == reflect.Struct && pt != exprType && pt != dtypeType:
			optStructs = append(optStructs, i)
		default:
			positional = append(positional, i)
		}
	}
	hasVar := variadic && optFuncs < 0
	nFixed := len(positional)
	if hasVar {
		nFixed--
	}

	in := make([]reflect.Value, nIn)
	srcs := make([]string, nIn)
	filled := make([]bool, nIn)
	used := make([]bool, len(kw))

	// Keyword arguments that name a positional parameter.
	for k, a := range kw {
		for j, name := range names {
			if j < nFixed && normalize(name) == normalize(a.Name) {
				idx := positional[j]
				v, s, err := convert(a.Val, t.In(idx), display, j+1)
				if err != nil {
					return val{}, err
				}
				in[idx], srcs[idx], filled[idx], used[k] = v, s, true, true
			}
		}
	}

	next := 0
	var rest []*Node
	for j := range nFixed {
		idx := positional[j]
		if filled[idx] {
			continue
		}
		if next < len(args) {
			v, s, err := convert(args[next], t.In(idx), display, next+1)
			if err != nil {
				return val{}, err
			}
			in[idx], srcs[idx], filled[idx] = v, s, true
			next++
			continue
		}
		d := len(defaults) - (nFixed - j)
		if d < 0 {
			return val{}, arityError(call, display, nFixed, len(defaults), hasVar, len(args), shift)
		}
		v, s, err := convert(litNode(defaults[d]), t.In(idx), display, j+1)
		if err != nil {
			return val{}, fmt.Errorf("internal: default for %s: %w", display, err)
		}
		in[idx], srcs[idx], filled[idx] = v, s, true
	}
	if next < len(args) {
		if !hasVar {
			return val{}, arityError(call, display, nFixed, len(defaults), hasVar, len(args), shift)
		}
		rest = args[next:]
	}

	var varVals []reflect.Value
	var varSrcs []string
	if hasVar {
		elem := t.In(nIn - 1).Elem()
		if len(rest) == 1 && rest[0].Kind == KindList && elem.Kind() != reflect.Slice {
			rest = rest[0].Args
		}
		for i, a := range rest {
			v, s, err := convert(a, elem, display, next+i+1)
			if err != nil {
				return val{}, err
			}
			varVals = append(varVals, v)
			varSrcs = append(varSrcs, s)
		}
	}

	for _, idx := range optStructs {
		v, s, err := buildOptions(t.In(idx), display, kw, used)
		if err != nil {
			return val{}, err
		}
		in[idx], srcs[idx] = v, s
	}
	if optFuncs >= 0 {
		vals, ss, err := buildRollingOptions(display, kw, used)
		if err != nil {
			return val{}, err
		}
		varVals, varSrcs = vals, ss
	}
	for k, isUsed := range used {
		if !isUsed {
			return val{}, unknownKeyword(display, kw[k], keywordNames(t, names, nFixed, optStructs, optFuncs >= 0))
		}
	}

	callArgs := in
	callSrcs := srcs
	var out []reflect.Value
	switch {
	case variadic && len(varVals) == 0:
		// reflect.Call would pass an empty, non-nil slice. Pass nil so
		// the callee sees the same value as a Go call with no variadic
		// arguments (str.strip_chars() strips whitespace, while an
		// explicit empty set strips nothing).
		callArgs = append(in[:nIn-1:nIn-1], reflect.Zero(t.In(nIn-1)))
		callSrcs = srcs[:nIn-1]
		out = fn.CallSlice(callArgs)
	case variadic:
		callArgs = append(in[:nIn-1:nIn-1], varVals...)
		callSrcs = append(srcs[:nIn-1:nIn-1], varSrcs...)
		out = fn.Call(callArgs)
	default:
		out = fn.Call(callArgs)
	}
	if len(out) != 1 || out[0].Type() != exprType {
		return val{}, fmt.Errorf("internal: %s does not return an expression", display)
	}
	return val{out[0].Interface().(expr.Expr), goCall + "(" + strings.Join(callSrcs, ", ") + ")"}, nil
}

func arityError(call *Node, display string, nFixed, nDefaults int, hasVar bool, got, shift int) error {
	lo := max(nFixed-nDefaults, 0) + shift
	hi := nFixed + shift
	got += shift
	var e *Error
	switch {
	case hasVar:
		e = errAt(call, "%s takes at least %d %s, got %d", display, lo, plural(lo), got)
	case lo == hi:
		e = errAt(call, "%s takes %d %s, got %d", display, hi, plural(hi), got)
	default:
		e = errAt(call, "%s takes %d to %d arguments, got %d", display, lo, hi, got)
	}
	if sig := Signature(nsOf(display), nameOf(display)); sig != "" {
		e.Hint = "usage: " + sig
	}
	return e
}

func nsOf(display string) string {
	if ns, _, found := strings.Cut(display, "."); found {
		return ns
	}
	return ""
}

func nameOf(display string) string {
	if _, name, found := strings.Cut(display, "."); found {
		return name
	}
	return display
}

// keywordNames lists the keyword arguments fn accepts: named
// positional parameters, option struct fields and rolling options.
func keywordNames(t reflect.Type, names []string, nFixed int, optStructs []int, rolling bool) []string {
	var out []string
	for j, n := range names {
		if j < nFixed {
			out = append(out, n)
		}
	}
	for _, idx := range optStructs {
		st := t.In(idx)
		for i := range st.NumField() {
			if f := st.Field(i); f.IsExported() {
				out = append(out, snake(f.Name))
			}
		}
	}
	if rolling {
		out = append(out, "min_periods", "closed", "ddof", "interpolation")
	}
	return out
}

func unknownKeyword(display string, k KwArg, valid []string) *Error {
	e := errSpan(k.NamePos, k.NamePos+len(k.Name), "%s: unknown keyword argument %q", display, k.Name)
	if best := script.Closest(k.Name, valid); best != "" {
		e.Fix = best
		return e.withHint("did you mean %q?", best)
	}
	if len(valid) > 0 {
		return e.withHint("valid: %s", strings.Join(valid, ", "))
	}
	return e.withHint("%s takes no keyword arguments", display)
}

func plural(n int) string {
	if n == 1 {
		return "argument"
	}
	return "arguments"
}

// normalize folds a glr or Go identifier for matching: lower case,
// underscores removed, so `is_nan` finds IsNaN and `ewm_mean` finds
// EWMMean.
func normalize(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// buildOptions fills an option struct (CutOptions, DatetimeArgs, ...)
// from the keyword arguments whose names match its fields.
func buildOptions(st reflect.Type, display string, kw []KwArg, used []bool) (reflect.Value, string, error) {
	out := reflect.New(st).Elem()
	var fields []string
	set := map[int]string{}
	for _, b := range structDefaults[st] {
		fi := fieldIndex(st, b.name)
		v, s, err := convert(litNode(b.lit), st.Field(fi).Type, display, 0)
		if err != nil {
			return reflect.Value{}, "", fmt.Errorf("internal: default %s: %w", b.name, err)
		}
		out.Field(fi).Set(v)
		set[fi] = s
	}
	for k, a := range kw {
		if used[k] {
			continue
		}
		fi := fieldIndex(st, a.Name)
		if fi < 0 {
			continue
		}
		v, s, err := convert(a.Val, st.Field(fi).Type, display+" "+a.Name, 0)
		if err != nil {
			return reflect.Value{}, "", err
		}
		out.Field(fi).Set(v)
		set[fi] = s
		used[k] = true
	}
	for i := range st.NumField() {
		if s, found := set[i]; found {
			fields = append(fields, st.Field(i).Name+": "+s)
		}
	}
	return out, qualified(st) + "{" + strings.Join(fields, ", ") + "}", nil
}

func fieldIndex(st reflect.Type, name string) int {
	want := normalize(name)
	if alias, found := fieldAliases[want]; found {
		want = alias
	}
	for i := range st.NumField() {
		f := st.Field(i)
		if f.IsExported() && strings.ToLower(f.Name) == want {
			return i
		}
	}
	return -1
}

// buildRollingOptions turns keyword arguments into the functional
// options of the rolling_*_by methods.
func buildRollingOptions(display string, kw []KwArg, used []bool) ([]reflect.Value, []string, error) {
	var vals []reflect.Value
	var srcs []string
	for k, a := range kw {
		if used[k] {
			continue
		}
		opt, found := rollingOptions[normalize(a.Name)]
		if !found {
			continue
		}
		fn := reflect.ValueOf(opt.fn)
		v, s, err := convert(a.Val, fn.Type().In(0), display+" "+a.Name, 0)
		if err != nil {
			return nil, nil, err
		}
		vals = append(vals, fn.Call([]reflect.Value{v})[0])
		srcs = append(srcs, "expr."+opt.goName+"("+s+")")
		used[k] = true
	}
	return vals, srcs, nil
}

// convert turns one glr argument into a value of Go type pt, plus the
// Go source for it. pos is the 1-based argument position (0 for a
// keyword argument) used in error messages.
func convert(n *Node, pt reflect.Type, display string, pos int) (reflect.Value, string, error) {
	v, s, err := convertArg(n, pt, display, pos)
	return v, s, locate(err, n)
}

func convertArg(n *Node, pt reflect.Type, display string, pos int) (reflect.Value, string, error) {
	where := display
	if pos > 0 {
		where = fmt.Sprintf("%s: argument %d", display, pos)
	}
	switch pt {
	case exprType:
		v, err := compile(n)
		if err != nil {
			return reflect.Value{}, "", err
		}
		return reflect.ValueOf(v.e), v.src, nil
	case exprPtrType:
		v, err := compile(n)
		if err != nil {
			return reflect.Value{}, "", err
		}
		p := new(expr.Expr)
		*p = v.e
		return reflect.ValueOf(p), "new(" + v.src + ")", nil
	case dtypeType:
		s, err := stringLit(n, where, 0)
		if err != nil {
			return reflect.Value{}, "", err
		}
		dt, src, err := parseDType(s)
		if err != nil {
			return reflect.Value{}, "", fmt.Errorf("%s: %w", where, err)
		}
		return reflect.ValueOf(dt), src, nil
	case timeUnitType:
		s, err := stringLit(n, where, 0)
		if err != nil {
			return reflect.Value{}, "", err
		}
		u, src, err := parseTimeUnit(s)
		if err != nil {
			return reflect.Value{}, "", fmt.Errorf("%s: %w", where, err)
		}
		return reflect.ValueOf(u), src, nil
	}
	switch pt.Kind() {
	case reflect.String:
		s, err := stringLit(n, where, 0)
		if err != nil {
			return reflect.Value{}, "", err
		}
		src := strconv.Quote(s)
		if pt.PkgPath() != "" {
			src = qualified(pt) + "(" + src + ")"
		}
		return reflect.ValueOf(s).Convert(pt), src, nil
	case reflect.Bool:
		b, isBool := litOf(n).(bool)
		if !isBool {
			return reflect.Value{}, "", fmt.Errorf("%s: expected true or false", where)
		}
		return reflect.ValueOf(b), strconv.FormatBool(b), nil
	case reflect.Int32:
		if pt == runeType {
			if s, isStr := litOf(n).(string); isStr {
				r := []rune(s)
				if len(r) != 1 {
					return reflect.Value{}, "", fmt.Errorf("%s: expected a single character", where)
				}
				return reflect.ValueOf(r[0]), strconv.QuoteRune(r[0]), nil
			}
		}
		fallthrough
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, isInt := litOf(n).(int64)
		if !isInt {
			return reflect.Value{}, "", fmt.Errorf("%s: expected an integer literal", where)
		}
		if i < 0 && pt.Kind() >= reflect.Uint && pt.Kind() <= reflect.Uint64 {
			return reflect.Value{}, "", fmt.Errorf("%s: expected a non-negative integer", where)
		}
		return reflect.ValueOf(i).Convert(pt), strconv.FormatInt(i, 10), nil
	case reflect.Float32, reflect.Float64:
		switch x := litOf(n).(type) {
		case float64:
			return reflect.ValueOf(x).Convert(pt), goFloat(x), nil
		case int64:
			return reflect.ValueOf(float64(x)).Convert(pt), strconv.FormatInt(x, 10), nil
		}
		return reflect.Value{}, "", fmt.Errorf("%s: expected a numeric literal", where)
	case reflect.Interface:
		if n.Kind != KindLit {
			return reflect.Value{}, "", fmt.Errorf("%s: expected a literal", where)
		}
		if n.Lit == nil {
			return reflect.Zero(pt), "nil", nil
		}
		return reflect.ValueOf(n.Lit), anySrc(n.Lit), nil
	case reflect.Slice:
		if pt.Elem().Kind() == reflect.Uint8 {
			s, err := stringLit(n, where, 0)
			if err != nil {
				return reflect.Value{}, "", err
			}
			return reflect.ValueOf([]byte(s)), fmt.Sprintf("[]byte(%q)", s), nil
		}
		elems := []*Node{n}
		if n.Kind == KindList {
			elems = n.Args
		}
		out := reflect.MakeSlice(pt, 0, len(elems))
		parts := make([]string, len(elems))
		for i, e := range elems {
			v, s, err := convert(e, pt.Elem(), where, 0)
			if err != nil {
				return reflect.Value{}, "", err
			}
			out = reflect.Append(out, v)
			parts[i] = s
		}
		return out, "[]" + qualified(pt.Elem()) + "{" + strings.Join(parts, ", ") + "}", nil
	}
	return reflect.Value{}, "", fmt.Errorf("%s: a %s argument cannot be written in glr", where, qualified(pt))
}

// litOf returns the literal value of n, or nil when n is not a
// literal.
func litOf(n *Node) any {
	switch n.Kind {
	case KindLit:
		return n.Lit
	case KindNeg:
		// `-(1)` is a negative literal written with parentheses.
		switch v := litOf(n.Args[0]).(type) {
		case int64:
			return -v
		case float64:
			return -v
		}
	}
	return nil
}

func stringLit(n *Node, where string, _ int) (string, error) {
	s, isStr := litOf(n).(string)
	if !isStr {
		return "", fmt.Errorf("%s: expected a string literal", where)
	}
	return s, nil
}

// qualified renders a Go type as source: builtins as is, named types
// as pkg.Name, interfaces as any.
func qualified(t reflect.Type) string {
	switch {
	case t.Kind() == reflect.Interface && t.NumMethod() == 0:
		return "any"
	case t.Kind() == reflect.Slice && t.Name() == "":
		return "[]" + qualified(t.Elem())
	case t.Kind() == reflect.Pointer:
		return "*" + qualified(t.Elem())
	case t.PkgPath() == "":
		return t.String()
	}
	return path.Base(t.PkgPath()) + "." + t.Name()
}

func anySrc(v any) string {
	switch x := v.(type) {
	case int64:
		return fmt.Sprintf("int64(%d)", x)
	case float64:
		return fmt.Sprintf("float64(%s)", goFloat(x))
	case string:
		return strconv.Quote(x)
	case bool:
		return strconv.FormatBool(x)
	}
	return "nil"
}

// goFloat formats f so Go reads it back as a float constant.
func goFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eEn") {
		s += ".0"
	}
	return s
}
