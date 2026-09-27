package exprparse

import (
	"fmt"
	"reflect"
	"strings"
)

// goDoc is one entry of the generated goDocs table.
type goDoc struct {
	params []string
	doc    string
}

// Param describes one argument of a glr function for editors.
type Param struct {
	// Name is the parameter name; keyword arguments use it too.
	Name string
	// Type is a short glr type: expr, int, float, str, bool, dtype,
	// value, list[...].
	Type string
	// Default is the glr literal used when the argument is left out,
	// "" when it is required.
	Default string
	// Variadic marks a trailing parameter that takes any number of
	// values (or one list).
	Variadic bool
}

// FuncInfo documents a glr function or method for hover, completion
// and signature help.
type FuncInfo struct {
	// NS is the namespace ("" for plain methods and free functions).
	NS string
	// Name is the glr spelling.
	Name string
	// Free is true for top-level functions with no receiver, such as
	// coalesce or date_range.
	Free bool
	// Params lists the arguments after the receiver.
	Params []Param
	// Keywords lists keyword-only options (option struct fields).
	Keywords []string
	// Doc is the Go doc comment of the underlying API.
	Doc string
	// GoName is the Go API it calls, as in Expr.Round or
	// StrOps.ToUpper or Coalesce.
	GoName string
}

// Label renders the call shape. Methods show their receiver as the
// first argument, the spelling that works in every position:
//
//	round(x, decimals: int = 0)
//	str.to_date(x, format: str = "")
//	coalesce(exprs: expr...)
func (f FuncInfo) Label() string {
	var parts []string
	if !f.Free {
		parts = append(parts, "x")
	}
	for _, p := range f.Params {
		parts = append(parts, p.String())
	}
	for _, k := range f.Keywords {
		parts = append(parts, k+"=...")
	}
	name := f.Name
	if f.NS != "" {
		name = f.NS + "." + name
	}
	return name + "(" + strings.Join(parts, ", ") + ")"
}

// String renders one parameter as `name: type = default`.
func (p Param) String() string {
	s := p.Name + ": " + p.Type
	if p.Variadic {
		s += "..."
	}
	if p.Default != "" {
		s += " = " + p.Default
	}
	return s
}

// Markdown renders hover documentation for the function.
func (f FuncInfo) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "```glr\n%s\n```\n", f.Label())
	if !f.Free {
		name := f.Name
		if f.NS != "" {
			name = f.NS + "." + name
		}
		fmt.Fprintf(&b, "\nAlso written `x.%s(...)`.\n", name)
	}
	if f.Doc != "" {
		b.WriteString("\n")
		b.WriteString(f.Doc)
		b.WriteString("\n")
	}
	if f.GoName != "" {
		fmt.Fprintf(&b, "\nGo: `%s`\n", f.GoName)
	}
	return strings.TrimRight(b.String(), "\n")
}

// Signature returns the call label of a function, or "" when unknown.
func Signature(ns, name string) string {
	if f, found := Lookup(ns, name); found {
		return f.Label()
	}
	return ""
}

// Lookup describes the glr function ns.name. For ns "" it finds plain
// methods and top-level functions.
func Lookup(ns, name string) (FuncInfo, bool) {
	if ns == "" {
		if name == "col" {
			return FuncInfo{Name: "col", Free: true, Params: []Param{{Name: "name", Type: "str"}},
				Doc: "Reference a column by name, including names with spaces or symbols."}, true
		}
		if name == "lit" {
			return FuncInfo{Name: "lit", Free: true, Params: []Param{{Name: "value", Type: "value"}},
				Doc: "A literal value broadcast to every row."}, true
		}
		if f, found := freeFuncs[name]; found {
			if f.custom != nil {
				return customInfo("", name), true
			}
			return describeFunc(FuncInfo{Name: name, Free: true, GoName: "expr." + f.goName},
				reflect.TypeOf(f.fn), goDocs[f.goName], f.defaults, f.names), true
		}
	}
	spec, found := lookupMethod(ns, name)
	if !found {
		return FuncInfo{}, false
	}
	if spec.custom != nil {
		return customInfo(ns, name), true
	}
	t := nsTypes[ns]
	goName := spec.method
	m, found := t.MethodByName(goName)
	if !found {
		return FuncInfo{}, false
	}
	info := FuncInfo{NS: ns, Name: name, GoName: t.Name() + "." + goName}
	// Method types from reflect.Type include the receiver first.
	info = describeFunc(info, dropReceiver(m.Type), goDocs[info.GoName], spec.defaults, spec.names)
	if spec.kwMethod != "" {
		if km, found := t.MethodByName(spec.kwMethod); found {
			kinfo := describeFunc(FuncInfo{}, dropReceiver(km.Type), goDoc{}, nil, nil)
			info.Keywords = kinfo.Keywords
		}
	}
	return info, true
}

// dropReceiver returns the function type of a method without its
// receiver parameter.
func dropReceiver(mt reflect.Type) reflect.Type {
	in := make([]reflect.Type, 0, mt.NumIn()-1)
	for i := 1; i < mt.NumIn(); i++ {
		in = append(in, mt.In(i))
	}
	out := make([]reflect.Type, mt.NumOut())
	for i := range out {
		out[i] = mt.Out(i)
	}
	return reflect.FuncOf(in, out, mt.IsVariadic())
}

// describeFunc fills the parameters of info from the Go function type
// ft, the doc table entry and the glr defaults and names.
func describeFunc(info FuncInfo, ft reflect.Type, d goDoc, defaults []any, names []string) FuncInfo {
	info.Doc = d.doc
	var positional []int
	for i := range ft.NumIn() {
		pt := ft.In(i)
		switch {
		case ft.IsVariadic() && i == ft.NumIn()-1 && pt.Elem() == rollOptType:
			info.Keywords = append(info.Keywords, "min_periods", "closed", "ddof", "interpolation")
		case pt.Kind() == reflect.Struct && pt != exprType && pt != dtypeType:
			for j := range pt.NumField() {
				if f := pt.Field(j); f.IsExported() {
					info.Keywords = append(info.Keywords, snake(f.Name))
				}
			}
		default:
			positional = append(positional, i)
		}
	}
	nFixed := len(positional)
	for j, idx := range positional {
		pt := ft.In(idx)
		p := Param{Name: fmt.Sprintf("arg%d", j+1)}
		if j < len(names) {
			p.Name = names[j]
		} else if idx < len(d.params) && d.params[idx] != "_" {
			p.Name = snake(d.params[idx])
		}
		if ft.IsVariadic() && idx == ft.NumIn()-1 {
			p.Variadic = true
			pt = pt.Elem()
		} else if k := len(defaults) - (nFixed - j); k >= 0 && k < len(defaults) {
			p.Default = glrLiteral(defaults[k])
		}
		p.Type = glrType(pt)
		info.Params = append(info.Params, p)
	}
	return info
}

// glrType names a Go parameter type the way a glr user writes it.
func glrType(t reflect.Type) string {
	switch t {
	case exprType, exprPtrType:
		return "expr"
	case dtypeType:
		return "dtype"
	case timeUnitType:
		return "time_unit"
	}
	switch t.Kind() {
	case reflect.String:
		return "str"
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Interface:
		return "value"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "str"
		}
		return "list[" + glrType(t.Elem()) + "]"
	}
	return t.String()
}

// glrLiteral renders a table default as glr source.
func glrLiteral(v any) string {
	switch x := v.(type) {
	case string:
		return fmt.Sprintf("%q", x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = glrLiteral(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case nil:
		return "null"
	case float64:
		return goFloat(x)
	}
	return fmt.Sprint(v)
}

// customInfo documents the functions built by hand in funcs.go.
func customInfo(ns, name string) FuncInfo {
	info := FuncInfo{NS: ns, Name: name}
	p := func(name, typ, def string) Param { return Param{Name: name, Type: typ, Default: def} }
	switch ns + "." + name {
	case ".log":
		info.Params = []Param{p("base", "float", "e")}
		info.Doc = "Natural logarithm, or the logarithm in the given base."
		info.GoName = "Expr.Log / Expr.LogBase"
	case ".replace_strict":
		info.Params = []Param{p("old", "list", ""), p("new", "list", "")}
		info.Keywords = []string{"default", "return_dtype"}
		info.Doc = "Replace every value by the mapping old to new; values without a mapping take default, or fail when there is none."
		info.GoName = "Expr.ReplaceStrict"
	case ".qcut":
		info.Params = []Param{p("quantiles", "list[float] | int", "")}
		info.Keywords = []string{"labels", "left_closed", "allow_duplicates", "include_breaks"}
		info.Doc = "Bin values into quantile buckets: a list of quantiles or a bucket count."
		info.GoName = "Expr.QCut / Expr.QCutN"
	case ".sort_by":
		info.Params = []Param{{Name: "by", Type: "expr", Variadic: true}}
		info.Keywords = []string{"descending", "nulls_last", "maintain_order"}
		info.Doc = "Sort the values by one or more other expressions."
		info.GoName = "Expr.SortBy"
	case ".top_k_by", ".bottom_k_by":
		info.Params = []Param{p("by", "expr | list", ""), p("k", "int", "5")}
		info.Keywords = []string{"reverse"}
		info.Doc = "The k values with the largest (top) or smallest (bottom) keys in by."
		info.GoName = "Expr.TopKBy / Expr.BottomKBy"
	case "dt.offset_by":
		info.Params = []Param{p("by", "str | expr", "")}
		info.Doc = "Shift each timestamp by a calendar duration such as \"1mo\" or \"-2d\"."
		info.GoName = "DtOps.OffsetBy"
	case ".concat_str":
		info.Free = true
		info.Params = []Param{{Name: "exprs", Type: "expr", Variadic: true}}
		info.Keywords = []string{"separator"}
		info.Doc = "Concatenate the string forms of the expressions row by row."
		info.GoName = "ConcatStr"
	}
	return info
}

// AllFunctions describes every glr function, grouped as Functions
// groups them.
func AllFunctions() []FuncInfo {
	var out []FuncInfo
	for ns, names := range Functions() {
		lookupNS := ns
		if ns == "free" {
			lookupNS = ""
		}
		for _, name := range names {
			if f, found := Lookup(lookupNS, name); found {
				out = append(out, f)
			}
		}
	}
	return out
}
