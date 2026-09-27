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

func compile(n *node) (val, error) {
	switch n.kind {
	case nLit:
		return litVal(n.lit)
	case nCol:
		return val{expr.Col(n.name), fmt.Sprintf("expr.Col(%q)", n.name)}, nil
	case nList:
		return val{}, fmt.Errorf("a [list] at byte %d is only valid as a function argument", n.pos)
	case nNot, nNeg:
		inner, err := compile(n.args[0])
		if err != nil {
			return val{}, err
		}
		if n.kind == nNot {
			return val{inner.e.Not(), inner.src + ".Not()"}, nil
		}
		return val{inner.e.Neg(), inner.src + ".Neg()"}, nil
	case nBinary:
		return compileBinary(n)
	case nWhen:
		return compileWhen(n)
	case nCall:
		return compileCall(n)
	}
	return val{}, fmt.Errorf("internal: unknown node kind %d", n.kind)
}

func litVal(v any) (val, error) {
	switch x := v.(type) {
	case int64:
		return val{expr.LitInt64(x), fmt.Sprintf("expr.LitInt64(%d)", x)}, nil
	case float64:
		return val{expr.LitFloat64(x), fmt.Sprintf("expr.LitFloat64(%s)", goFloat(x))}, nil
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

func compileBinary(n *node) (val, error) {
	l, err := compile(n.args[0])
	if err != nil {
		return val{}, err
	}
	r, err := compile(n.args[1])
	if err != nil {
		return val{}, err
	}
	m := binaryMethods[n.name]
	out := reflect.ValueOf(l.e).MethodByName(m).Call([]reflect.Value{reflect.ValueOf(r.e)})[0]
	return val{out.Interface().(expr.Expr), fmt.Sprintf("%s.%s(%s)", l.src, m, r.src)}, nil
}

// compileWhen nests `when a then x when b then y otherwise z` as
// When(a).Then(x).Otherwise(When(b).Then(y).Otherwise(z)). A missing
// otherwise yields null, as in polars.
func compileWhen(n *node) (val, error) {
	acc, err := litVal(nil)
	if err != nil {
		return val{}, err
	}
	if n.final != nil {
		if acc, err = compile(n.final); err != nil {
			return val{}, err
		}
	}
	for i := len(n.args) - 2; i >= 0; i -= 2 {
		pred, err := compile(n.args[i])
		if err != nil {
			return val{}, err
		}
		then, err := compile(n.args[i+1])
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

func compileCall(n *node) (val, error) {
	if n.recv != nil {
		recv, err := compile(n.recv)
		if err != nil {
			return val{}, err
		}
		return compileMethod(n.ns, n.name, recv, n.args, n.kw)
	}
	if n.ns != "" {
		v, err := compileNSFunction(n)
		if err != nil && n.ambig {
			col := val{expr.Col(n.ns), fmt.Sprintf("expr.Col(%q)", n.ns)}
			if v2, err2 := compileMethod("", n.name, col, n.args, n.kw); err2 == nil {
				return v2, nil
			}
		}
		return v, err
	}
	return compileFree(n)
}

// compileNSFunction handles `dt.year(ts)`: the first argument is the
// receiver of the namespaced method.
func compileNSFunction(n *node) (val, error) {
	if len(n.args) == 0 {
		return val{}, fmt.Errorf("%s.%s needs the value to work on as its first argument, as in %s.%s(x)",
			n.ns, n.name, n.ns, n.name)
	}
	if _, found := lookupMethod(n.ns, n.name); !found {
		return val{}, fmt.Errorf("unknown %s function %q", n.ns, n.name)
	}
	recv, err := compile(n.args[0])
	if err != nil {
		return val{}, err
	}
	return compileMethod(n.ns, n.name, recv, n.args[1:], n.kw)
}

func compileFree(n *node) (val, error) {
	switch n.name {
	case "col":
		if len(n.args) != 1 || len(n.kw) != 0 {
			return val{}, fmt.Errorf("col takes 1 argument, got %d", len(n.args))
		}
		s, err := stringLit(n.args[0], "col", 0)
		if err != nil {
			return val{}, err
		}
		return val{expr.Col(s), fmt.Sprintf("expr.Col(%q)", s)}, nil
	case "lit":
		if len(n.args) != 1 || len(n.kw) != 0 {
			return val{}, fmt.Errorf("lit takes 1 argument, got %d", len(n.args))
		}
		if n.args[0].kind != nLit {
			return val{}, fmt.Errorf("lit: expected a literal")
		}
		return litVal(n.args[0].lit)
	}
	// Legacy spelling: `sum("x")` aggregates the column named x.
	if slices.Contains(legacyColumnAggs, n.name) && len(n.args) == 1 && len(n.kw) == 0 &&
		n.args[0].kind == nLit {
		s, isStr := n.args[0].lit.(string)
		if !isStr {
			return val{}, fmt.Errorf("%s: expected a column name or expression", n.name)
		}
		return compileMethod("", n.name, val{expr.Col(s), fmt.Sprintf("expr.Col(%q)", s)}, nil, nil)
	}
	_, isMethod := lookupMethod("", n.name)
	if f, found := freeFuncs[n.name]; found {
		v, err := f.call(n.name, n.args, n.kw)
		if err == nil || !isMethod || len(n.args) == 0 {
			return v, err
		}
	}
	if isMethod && len(n.args) > 0 {
		recv, err := compile(n.args[0])
		if err != nil {
			return val{}, err
		}
		return compileMethod("", n.name, recv, n.args[1:], n.kw)
	}
	return val{}, fmt.Errorf("unknown function %q", n.name)
}

// compileMethod applies method name of namespace ns (or of expr.Expr
// itself when ns is "") to recv.
func compileMethod(ns, name string, recv val, args []*node, kw []kwarg) (val, error) {
	spec, found := lookupMethod(ns, name)
	display := name
	if ns != "" {
		display = ns + "." + name
	}
	if !found {
		if ns == "" {
			return val{}, fmt.Errorf("unknown method %q", name)
		}
		return val{}, fmt.Errorf("unknown %s method %q", ns, name)
	}
	if spec.custom != nil {
		return spec.custom(recv, args, kw)
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
	return invoke(m, display, prefix+"."+goName, args, kw, spec.defaults, spec.names)
}

// invoke calls fn with glr arguments converted to its Go parameter
// types. Struct parameters (option types) and variadic option
// functions are filled from keyword arguments; the other parameters
// are positional, with trailing ones taking defaults when omitted and
// names letting keyword arguments fill them.
func invoke(fn reflect.Value, display, goCall string, args []*node, kw []kwarg, defaults []any, names []string) (val, error) {
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
			if j < nFixed && normalize(name) == normalize(a.name) {
				idx := positional[j]
				v, s, err := convert(a.val, t.In(idx), display, j+1)
				if err != nil {
					return val{}, err
				}
				in[idx], srcs[idx], filled[idx], used[k] = v, s, true, true
			}
		}
	}

	next := 0
	var rest []*node
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
			return val{}, arityError(display, nFixed, len(defaults), hasVar, len(args), len(kw))
		}
		v, s, err := convert(litNode(defaults[d]), t.In(idx), display, j+1)
		if err != nil {
			return val{}, fmt.Errorf("internal: default for %s: %w", display, err)
		}
		in[idx], srcs[idx], filled[idx] = v, s, true
	}
	if next < len(args) {
		if !hasVar {
			return val{}, arityError(display, nFixed, len(defaults), hasVar, len(args), len(kw))
		}
		rest = args[next:]
	}

	var varVals []reflect.Value
	var varSrcs []string
	if hasVar {
		elem := t.In(nIn - 1).Elem()
		if len(rest) == 1 && rest[0].kind == nList && elem.Kind() != reflect.Slice {
			rest = rest[0].args
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
			return val{}, fmt.Errorf("%s: unknown keyword argument %q", display, kw[k].name)
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

func arityError(display string, nFixed, nDefaults int, hasVar bool, got, gotKw int) error {
	lo := max(nFixed-nDefaults, 0)
	switch {
	case hasVar:
		return fmt.Errorf("%s takes at least %d %s, got %d", display, lo, plural(lo), got)
	case lo == nFixed:
		return fmt.Errorf("%s takes %d %s, got %d", display, nFixed, plural(nFixed), got)
	}
	return fmt.Errorf("%s takes %d to %d arguments, got %d", display, lo, nFixed, got)
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
func buildOptions(st reflect.Type, display string, kw []kwarg, used []bool) (reflect.Value, string, error) {
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
		fi := fieldIndex(st, a.name)
		if fi < 0 {
			continue
		}
		v, s, err := convert(a.val, st.Field(fi).Type, display+" "+a.name, 0)
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
func buildRollingOptions(display string, kw []kwarg, used []bool) ([]reflect.Value, []string, error) {
	var vals []reflect.Value
	var srcs []string
	for k, a := range kw {
		if used[k] {
			continue
		}
		opt, found := rollingOptions[normalize(a.name)]
		if !found {
			continue
		}
		fn := reflect.ValueOf(opt.fn)
		v, s, err := convert(a.val, fn.Type().In(0), display+" "+a.name, 0)
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
func convert(n *node, pt reflect.Type, display string, pos int) (reflect.Value, string, error) {
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
		if n.kind != nLit {
			return reflect.Value{}, "", fmt.Errorf("%s: expected a literal", where)
		}
		if n.lit == nil {
			return reflect.Zero(pt), "nil", nil
		}
		return reflect.ValueOf(n.lit), anySrc(n.lit), nil
	case reflect.Slice:
		if pt.Elem().Kind() == reflect.Uint8 {
			s, err := stringLit(n, where, 0)
			if err != nil {
				return reflect.Value{}, "", err
			}
			return reflect.ValueOf([]byte(s)), fmt.Sprintf("[]byte(%q)", s), nil
		}
		elems := []*node{n}
		if n.kind == nList {
			elems = n.args
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
func litOf(n *node) any {
	if n.kind != nLit {
		return nil
	}
	return n.lit
}

func stringLit(n *node, where string, _ int) (string, error) {
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
