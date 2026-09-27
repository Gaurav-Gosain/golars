package exprparse

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
)

// methodSpec says how a glr method name maps onto the Go API.
type methodSpec struct {
	// method is the Go method on the namespace type.
	method string
	// kwMethod replaces method when keyword arguments are present
	// (ToDate becomes ToDateWith so options can be passed).
	kwMethod string
	// defaults fill trailing positional parameters left out in glr,
	// written as glr literals. They follow polars' defaults.
	defaults []any
	// names label the positional parameters so keyword arguments can
	// fill them, as in `date_range(a, b, interval="1h")`.
	names []string
	// custom builds the call by hand when the Go signature does not
	// map onto glr arguments.
	custom func(recv val, args []*node, kw []kwarg) (val, error)
}

// overrides adjusts the automatic mapping per namespace ("" is
// expr.Expr itself). Keys are glr names. An entry without a method
// keeps the automatically resolved Go method and only adds defaults.
// Set in init because the custom builders refer back to the compiler.
var overrides map[string]map[string]methodSpec

func init() {
	overrides = map[string]map[string]methodSpec{
		"": {
			"fill_null":      {method: "FillNullExpr"},
			"shift":          {defaults: []any{1}},
			"diff":           {defaults: []any{1}},
			"pct_change":     {defaults: []any{1}},
			"round":          {defaults: []any{0}},
			"forward_fill":   {defaults: []any{0}},
			"backward_fill":  {defaults: []any{0}},
			"sort":           {method: "SortWith", defaults: []any{false, false}, names: []string{"descending", "nulls_last"}},
			"arg_sort":       {defaults: []any{false, false}, names: []string{"descending", "nulls_last"}},
			"rank":           {method: "RankWith", defaults: []any{"average", false}, names: []string{"method", "descending"}},
			"top_k":          {defaults: []any{5}},
			"bottom_k":       {defaults: []any{5}},
			"head":           {defaults: []any{10}},
			"tail":           {defaults: []any{10}},
			"limit":          {defaults: []any{10}},
			"interpolate":    {defaults: []any{"linear"}, names: []string{"method"}},
			"is_between":     {defaults: []any{"both"}, names: []string{"lower_bound", "upper_bound", "closed"}},
			"search_sorted":  {defaults: []any{"any"}, names: []string{"element", "side"}},
			"entropy":        {method: "EntropyWith", defaults: []any{2.718281828459045, true}, names: []string{"base", "normalize"}},
			"value_counts":   {defaults: []any{false, false}, names: []string{"sort", "normalize"}},
			"gather_every":   {defaults: []any{0}, names: []string{"n", "offset"}},
			"is_close":       {defaults: []any{0.0, 1e-9, false}, names: []string{"other", "abs_tol", "rel_tol", "nans_equal"}},
			"sample":         {defaults: []any{false, false, 0}, names: []string{"n", "with_replacement", "shuffle", "seed"}},
			"shuffle":        {defaults: []any{0}, names: []string{"seed"}},
			"log":            {custom: logCall},
			"replace_strict": {custom: replaceStrict},
			"qcut":           {custom: qcut},
			"sort_by":        {custom: sortBy},
			"top_k_by":       {custom: topKBy("TopKBy", "top_k_by")},
			"bottom_k_by":    {custom: topKBy("BottomKBy", "bottom_k_by")},
			"hash":           {method: "HashValues"},
			"pow":            {method: "PowExpr"},
		},
		"str": {
			"upper":         {method: "ToUpper"},
			"to_upper":      {method: "ToUpper"},
			"to_uppercase":  {method: "ToUpper"},
			"lower":         {method: "ToLower"},
			"to_lower":      {method: "ToLower"},
			"to_lowercase":  {method: "ToLower"},
			"strip":         {method: "StripChars"},
			"strip_start":   {method: "StripCharsStart"},
			"strip_end":     {method: "StripCharsEnd"},
			"len":           {method: "LenChars"},
			"to_date":       {kwMethod: "ToDateWith", defaults: []any{""}, names: []string{"format"}},
			"to_datetime":   {kwMethod: "ToDatetimeWith", defaults: []any{""}, names: []string{"format"}},
			"to_time":       {kwMethod: "ToTimeWith", defaults: []any{""}, names: []string{"format"}},
			"strptime":      {kwMethod: "StrptimeWith", defaults: []any{""}, names: []string{"dtype", "format"}},
			"to_integer":    {defaults: []any{10, "i64", true}, names: []string{"base", "dtype", "strict"}},
			"pad_start":     {defaults: []any{" "}, names: []string{"length", "fill_char"}},
			"pad_end":       {defaults: []any{" "}, names: []string{"length", "fill_char"}},
			"contains_any":  {defaults: []any{false}, names: []string{"patterns", "ascii_case_insensitive"}},
			"extract":       {defaults: []any{1}, names: []string{"pattern", "group_index"}},
			"extract_many":  {defaults: []any{false, false}, names: []string{"patterns", "ascii_case_insensitive", "overlapping"}},
			"find_many":     {defaults: []any{false, false}, names: []string{"patterns", "ascii_case_insensitive", "overlapping"}},
			"replace_many":  {defaults: []any{false}, names: []string{"patterns", "replace_with", "ascii_case_insensitive"}},
			"join":          {defaults: []any{"", true}, names: []string{"delimiter", "ignore_nulls"}},
			"decode":        {defaults: []any{true}, names: []string{"encoding", "strict"}},
			"split_exact_n": {defaults: []any{false}, names: []string{"by", "n", "inclusive"}},
		},
		"dt": {
			"epoch":     {defaults: []any{"us"}, names: []string{"time_unit"}},
			"timestamp": {defaults: []any{"us"}, names: []string{"time_unit"}},
			"combine":   {defaults: []any{"us"}, names: []string{"time", "time_unit"}},
			"offset_by": {custom: offsetBy},
			"strftime":  {names: []string{"format"}},
		},
		"list": {
			"join":         {method: "JoinWith", defaults: []any{false}, names: []string{"separator", "ignore_nulls"}},
			"sort":         {defaults: []any{false, false}, names: []string{"descending", "nulls_last"}},
			"unique":       {defaults: []any{false}, names: []string{"maintain_order"}},
			"head":         {defaults: []any{5}},
			"tail":         {defaults: []any{5}},
			"shift":        {defaults: []any{1}},
			"diff":         {defaults: []any{1}},
			"std":          {defaults: []any{1}, names: []string{"ddof"}},
			"var":          {defaults: []any{1}, names: []string{"ddof"}},
			"gather":       {defaults: []any{false}, names: []string{"indices", "null_on_oob"}},
			"gather_every": {defaults: []any{0}, names: []string{"n", "offset"}},
			"sample":       {defaults: []any{false, false, 0}, names: []string{"n", "with_replacement", "shuffle", "seed"}},
			"to_struct":    {names: []string{"fields"}},
		},
		"arr": {
			"get":    {defaults: []any{false}, names: []string{"index", "null_on_oob"}},
			"join":   {defaults: []any{false}, names: []string{"separator", "ignore_nulls"}},
			"sort":   {defaults: []any{false, false}, names: []string{"descending", "nulls_last"}},
			"unique": {defaults: []any{false}, names: []string{"maintain_order"}},
			"head":   {defaults: []any{5}},
			"tail":   {defaults: []any{5}},
			"shift":  {defaults: []any{1}},
			"std":    {defaults: []any{1}, names: []string{"ddof"}},
			"var":    {defaults: []any{1}, names: []string{"ddof"}},
		},
		"name": {
			"replace": {defaults: []any{false}, names: []string{"pattern", "value", "literal"}},
		},
		"bin": {
			"decode": {defaults: []any{true}, names: []string{"encoding", "strict"}},
			"size":   {defaults: []any{"b"}, names: []string{"unit"}},
			"get":    {defaults: []any{false}, names: []string{"index", "null_on_oob"}},
		},
	}
}

// structDefaults seed option structs before keyword arguments apply,
// so `str.to_date(s, "%d", strict=false)` keeps the other polars
// defaults.
var structDefaults = map[reflect.Type][]struct {
	name string
	lit  any
}{
	reflect.TypeFor[expr.StrptimeOptions](): {{"strict", true}, {"exact", true}, {"ambiguous", "raise"}},
}

// fieldAliases maps polars keyword names onto option struct fields
// whose Go name differs (normalized on both sides).
var fieldAliases = map[string]string{
	"minperiods": "minsamples",
}

// rollingOptions maps keyword arguments of the rolling_*_by methods
// to their functional options.
var rollingOptions = map[string]struct {
	fn     any
	goName string
}{
	"minperiods":    {expr.WithMinPeriods, "WithMinPeriods"},
	"minsamples":    {expr.WithMinPeriods, "WithMinPeriods"},
	"closed":        {expr.WithClosed, "WithClosed"},
	"ddof":          {expr.WithDdof, "WithDdof"},
	"interpolation": {expr.WithInterpolation, "WithInterpolation"},
}

// legacyColumnAggs accept a quoted column name: `sum("amount")`.
var legacyColumnAggs = []string{"sum", "mean", "min", "max", "count", "first", "last",
	"median", "std", "var", "n_unique"}

// nsTypes are the Go types behind each namespace.
var nsTypes = map[string]reflect.Type{
	"":       exprType,
	"str":    reflect.TypeFor[expr.StrOps](),
	"dt":     reflect.TypeFor[expr.DtOps](),
	"list":   reflect.TypeFor[expr.ListOps](),
	"arr":    reflect.TypeFor[expr.ArrOps](),
	"struct": reflect.TypeFor[expr.StructOps](),
	"name":   reflect.TypeFor[expr.NameOps](),
	"bin":    reflect.TypeFor[expr.BinOps](),
	"cat":    reflect.TypeFor[expr.CatOps](),
}

// hidden are Go methods that should not surface in glr: they take Go
// callbacks, duplicate an operator, or are no-ops.
var hidden = map[string]bool{
	"AddLit": true, "SubLit": true, "MulLit": true, "DivLit": true, "EqLit": true,
	"NeLit": true, "LtLit": true, "LeLit": true, "GtLit": true, "GeLit": true,
	"Pipe": true, "MapBatches": true, "RollingMap": true, "Map": true, "MapFields": true,
	"ShrinkDtype": true, "SetSorted": true, "Rechunk": true, "Node": true,
}

var (
	methodIndexOnce sync.Once
	methodIndex     map[string]map[string]string // ns -> normalized -> Go name
)

// goMethods indexes the methods of each namespace type that return an
// expression.
func goMethods() map[string]map[string]string {
	methodIndexOnce.Do(func() {
		methodIndex = map[string]map[string]string{}
		for ns, t := range nsTypes {
			m := map[string]string{}
			for i := range t.NumMethod() {
				meth := t.Method(i)
				ft := meth.Type
				if hidden[meth.Name] || ft.NumOut() != 1 || ft.Out(0) != exprType {
					continue
				}
				m[strings.ToLower(meth.Name)] = meth.Name
			}
			methodIndex[ns] = m
		}
	})
	return methodIndex
}

// lookupMethod resolves a glr method name in namespace ns.
func lookupMethod(ns, name string) (methodSpec, bool) {
	spec := overrides[ns][name]
	if spec.custom != nil {
		return spec, true
	}
	if spec.method == "" {
		spec.method = goMethods()[ns][normalize(name)]
	}
	return spec, spec.method != ""
}

// freeFunc is a top-level function such as coalesce or date_range.
type freeFunc struct {
	fn       any
	goName   string
	defaults []any
	names    []string
	minArgs  int
	custom   func(name string, args []*node, kw []kwarg) (val, error)
}

func (f freeFunc) call(name string, args []*node, kw []kwarg) (val, error) {
	if f.custom != nil {
		return f.custom(name, args, kw)
	}
	if len(args) < f.minArgs {
		return val{}, fmt.Errorf("%s requires at least %d %s", name, f.minArgs, plural(f.minArgs))
	}
	return invoke(reflect.ValueOf(f.fn), name, "expr."+f.goName, args, kw, f.defaults, f.names)
}

var freeFuncs map[string]freeFunc

func init() {
	freeFuncs = map[string]freeFunc{
		"coalesce":           {fn: expr.Coalesce, goName: "Coalesce", minArgs: 1},
		"concat_str":         {custom: concatStr},
		"format":             {fn: expr.Format, goName: "Format"},
		"sum_horizontal":     {fn: expr.SumHorizontal, goName: "SumHorizontal", minArgs: 1},
		"mean_horizontal":    {fn: expr.MeanHorizontal, goName: "MeanHorizontal", minArgs: 1},
		"min_horizontal":     {fn: expr.MinHorizontal, goName: "MinHorizontal", minArgs: 1},
		"max_horizontal":     {fn: expr.MaxHorizontal, goName: "MaxHorizontal", minArgs: 1},
		"all_horizontal":     {fn: expr.AllHorizontal, goName: "AllHorizontal", minArgs: 1},
		"any_horizontal":     {fn: expr.AnyHorizontal, goName: "AnyHorizontal", minArgs: 1},
		"cum_sum_horizontal": {fn: expr.CumSumHorizontal, goName: "CumSumHorizontal", minArgs: 1},
		"struct":             {fn: expr.Struct, goName: "Struct", minArgs: 1},
		"concat_list":        {fn: expr.ConcatList, goName: "ConcatList", minArgs: 1},
		"int_range":          {fn: expr.IntRange, goName: "IntRange", defaults: []any{1}, names: []string{"start", "end", "step"}},
		"date":               {fn: expr.Date, goName: "Date"},
		"datetime":           {fn: expr.Datetime, goName: "Datetime"},
		"duration":           {fn: expr.Duration, goName: "Duration"},
		"date_range":         {fn: expr.DateRange, goName: "DateRange", defaults: []any{"1d", "both"}, names: []string{"start", "end", "interval", "closed"}},
		"datetime_range":     {fn: expr.DatetimeRange, goName: "DatetimeRange", defaults: []any{"1d", "both", "us", ""}, names: []string{"start", "end", "interval", "closed", "time_unit", "time_zone"}},
		"time_range":         {fn: expr.TimeRange, goName: "TimeRange", defaults: []any{"1h", "both"}, names: []string{"start", "end", "interval", "closed"}},
		"repeat":             {fn: expr.Repeat, goName: "Repeat", names: []string{"value", "n"}},
		"arctan2":            {fn: expr.Arctan2, goName: "Arctan2"},
		"corr":               {fn: expr.Corr, goName: "Corr", defaults: []any{"pearson"}, names: []string{"a", "b", "method"}},
		"cov":                {fn: expr.Cov, goName: "Cov", defaults: []any{1}, names: []string{"a", "b", "ddof"}},
		"len":                {fn: expr.Len, goName: "Len"},
		"element":            {fn: expr.Element, goName: "Element"},
		"arg_where":          {fn: expr.ArgWhere, goName: "ArgWhere"},
		"field":              {fn: expr.Field, goName: "Field"},
		"exclude":            {fn: expr.Exclude, goName: "Exclude", minArgs: 1},
		"all":                {fn: expr.AllCols, goName: "AllCols"},
	}
}

// --- custom builders --------------------------------------------

// logCall maps `log(x)` to the natural log and `log(x, base)` to
// LogBase, as polars' Expr.log does.
func logCall(recv val, args []*node, kw []kwarg) (val, error) {
	if len(args) == 0 && len(kw) == 0 {
		return val{recv.e.Log(), recv.src + ".Log()"}, nil
	}
	return invoke(reflect.ValueOf(recv.e.LogBase), "log", recv.src+".LogBase", args, kw, nil, []string{"base"})
}

// replaceStrict takes `replace_strict(x, old, new, default=v,
// return_dtype="str")`.
func replaceStrict(recv val, args []*node, kw []kwarg) (val, error) {
	return invoke(reflect.ValueOf(recv.e.ReplaceStrict), "replace_strict", recv.src+".ReplaceStrict", args, kw, nil, []string{"old", "new"})
}

// qcut accepts a list of quantiles or a bin count.
func qcut(recv val, args []*node, kw []kwarg) (val, error) {
	if len(args) > 0 {
		if _, isInt := litOf(args[0]).(int64); isInt {
			return invoke(reflect.ValueOf(recv.e.QCutN), "qcut", recv.src+".QCutN", args, kw, nil, nil)
		}
	}
	return invoke(reflect.ValueOf(recv.e.QCut), "qcut", recv.src+".QCut", args, kw, nil, nil)
}

// sortBy takes `sort_by(x, key...)` plus descending= and nulls_last=.
func sortBy(recv val, args []*node, kw []kwarg) (val, error) {
	var opts expr.SortByOptions
	var optSrc []string
	for _, a := range kw {
		switch normalize(a.name) {
		case "descending":
			v, s, err := convert(a.val, reflect.TypeFor[[]bool](), "sort_by descending", 0)
			if err != nil {
				return val{}, err
			}
			opts.Descending = v.Interface().([]bool)
			optSrc = append(optSrc, "Descending: "+s)
		case "nullslast":
			b, isBool := litOf(a.val).(bool)
			if !isBool {
				return val{}, fmt.Errorf("sort_by nulls_last: expected true or false")
			}
			opts.NullsLast = b
			optSrc = append(optSrc, fmt.Sprintf("NullsLast: %t", b))
		case "maintainorder":
			b, isBool := litOf(a.val).(bool)
			if !isBool {
				return val{}, fmt.Errorf("sort_by maintain_order: expected true or false")
			}
			opts.MaintainOrder = b
			optSrc = append(optSrc, fmt.Sprintf("MaintainOrder: %t", b))
		default:
			return val{}, fmt.Errorf("sort_by: unknown keyword argument %q", a.name)
		}
	}
	keys, keySrc, err := exprList("sort_by", args)
	if err != nil {
		return val{}, err
	}
	return val{recv.e.SortBy(keys, opts), fmt.Sprintf("%s.SortBy([]expr.Expr{%s}, expr.SortByOptions{%s})",
		recv.src, strings.Join(keySrc, ", "), strings.Join(optSrc, ", "))}, nil
}

// topKBy takes `top_k_by(x, by, k)` or `top_k_by(x, [a, b], k,
// reverse=[...])`.
func topKBy(goName, display string) func(recv val, args []*node, kw []kwarg) (val, error) {
	return func(recv val, args []*node, kw []kwarg) (val, error) {
		m := reflect.ValueOf(recv.e).MethodByName(goName)
		return invoke(m, display, recv.src+"."+goName, args, kw, []any{5, []any{}}, []string{"by", "k", "reverse"})
	}
}

// offsetBy accepts a duration string or an expression.
func offsetBy(recv val, args []*node, kw []kwarg) (val, error) {
	dt := reflect.ValueOf(recv.e.Dt())
	if len(args) == 1 && len(kw) == 0 {
		if _, isStr := litOf(args[0]).(string); !isStr {
			return invoke(dt.MethodByName("OffsetByExpr"), "dt.offset_by", recv.src+".Dt().OffsetByExpr", args, kw, nil, nil)
		}
	}
	return invoke(dt.MethodByName("OffsetBy"), "dt.offset_by", recv.src+".Dt().OffsetBy", args, kw, nil, []string{"by"})
}

// concatStr takes `concat_str(a, b, ..., separator="-")`: the
// separator is keyword-only, as in polars.
func concatStr(name string, args []*node, kw []kwarg) (val, error) {
	sep := litNode("")
	for _, a := range kw {
		if normalize(a.name) != "separator" {
			return val{}, fmt.Errorf("%s: unknown keyword argument %q", name, a.name)
		}
		sep = a.val
	}
	if len(args) == 0 {
		return val{}, fmt.Errorf("%s requires at least 1 argument", name)
	}
	return invoke(reflect.ValueOf(expr.ConcatStr), name, "expr.ConcatStr", append([]*node{sep}, args...), nil, nil, nil)
}

func exprList(display string, args []*node) ([]expr.Expr, []string, error) {
	if len(args) == 1 && args[0].kind == nList {
		args = args[0].args
	}
	out := make([]expr.Expr, len(args))
	srcs := make([]string, len(args))
	for i, a := range args {
		v, err := compile(a)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", display, err)
		}
		out[i], srcs[i] = v.e, v.src
	}
	return out, srcs, nil
}

// --- dtypes ------------------------------------------------------

type dtypeEntry struct {
	names []string
	dt    func() dtype.DType
	src   string
}

var dtypeTable = []dtypeEntry{
	{[]string{"i8", "int8"}, dtype.Int8, "dtype.Int8()"},
	{[]string{"i16", "int16"}, dtype.Int16, "dtype.Int16()"},
	{[]string{"i32", "int32"}, dtype.Int32, "dtype.Int32()"},
	{[]string{"i64", "int64", "int"}, dtype.Int64, "dtype.Int64()"},
	{[]string{"u8", "uint8"}, dtype.Uint8, "dtype.Uint8()"},
	{[]string{"u16", "uint16"}, dtype.Uint16, "dtype.Uint16()"},
	{[]string{"u32", "uint32"}, dtype.Uint32, "dtype.Uint32()"},
	{[]string{"u64", "uint64"}, dtype.Uint64, "dtype.Uint64()"},
	{[]string{"f32", "float32"}, dtype.Float32, "dtype.Float32()"},
	{[]string{"f64", "float64", "float"}, dtype.Float64, "dtype.Float64()"},
	{[]string{"bool", "boolean"}, dtype.Bool, "dtype.Bool()"},
	{[]string{"str", "string", "utf8"}, dtype.String, "dtype.String()"},
	{[]string{"binary"}, dtype.Binary, "dtype.Binary()"},
	{[]string{"date"}, dtype.Date, "dtype.Date()"},
	{[]string{"categorical", "cat"}, dtype.Categorical, "dtype.Categorical()"},
	{[]string{"null"}, dtype.Null, "dtype.Null()"},
}

// parseDType resolves a dtype name. Temporal types take an optional
// unit: datetime (microseconds), datetime[ms], duration[ns], time.
func parseDType(name string) (dtype.DType, string, error) {
	key := strings.ToLower(strings.TrimSpace(name))
	for _, e := range dtypeTable {
		for _, n := range e.names {
			if n == key {
				return e.dt(), e.src, nil
			}
		}
	}
	base, unit, hasUnit := strings.Cut(key, "[")
	unitName := "us"
	if hasUnit {
		unitName = strings.TrimSuffix(unit, "]")
	}
	u, usrc, err := parseTimeUnit(unitName)
	if err != nil {
		return dtype.DType{}, "", fmt.Errorf("unknown dtype %q", name)
	}
	switch base {
	case "datetime":
		return dtype.Datetime(u, ""), fmt.Sprintf("dtype.Datetime(%s, \"\")", usrc), nil
	case "duration":
		return dtype.Duration(u), fmt.Sprintf("dtype.Duration(%s)", usrc), nil
	case "time":
		if !hasUnit {
			u, usrc = dtype.Nanosecond, "dtype.Nanosecond"
		}
		return dtype.Time(u), fmt.Sprintf("dtype.Time(%s)", usrc), nil
	}
	return dtype.DType{}, "", fmt.Errorf("unknown dtype %q", name)
}

// ParseDType resolves a glr dtype name such as "i64", "str", "date",
// "datetime[ms]" or "categorical".
func ParseDType(name string) (dtype.DType, error) {
	dt, _, err := parseDType(name)
	return dt, err
}

// DTypeNames lists the dtype spellings ParseDType accepts, for help
// text and completion.
func DTypeNames() []string {
	var out []string
	for _, e := range dtypeTable {
		out = append(out, e.names[0])
	}
	return append(out, "datetime", "datetime[ms]", "datetime[ns]", "duration", "time")
}

func parseTimeUnit(s string) (dtype.TimeUnit, string, error) {
	switch strings.ToLower(s) {
	case "s":
		return dtype.Second, "dtype.Second", nil
	case "ms":
		return dtype.Millisecond, "dtype.Millisecond", nil
	case "us", "μs":
		return dtype.Microsecond, "dtype.Microsecond", nil
	case "ns":
		return dtype.Nanosecond, "dtype.Nanosecond", nil
	}
	return 0, "", fmt.Errorf("unknown time unit %q: want s, ms, us or ns", s)
}

// Functions lists every function name glr expressions accept, grouped
// by namespace ("" for plain methods, "free" for top-level
// functions). Used by editor completion and the docs drift test.
func Functions() map[string][]string {
	out := map[string][]string{}
	for ns, index := range goMethods() {
		seen := map[string]bool{}
		for _, goName := range index {
			seen[snake(goName)] = true
		}
		for name := range overrides[ns] {
			if _, found := lookupMethod(ns, name); found {
				seen[name] = true
			}
		}
		for name := range seen {
			out[ns] = append(out[ns], name)
		}
		sort.Strings(out[ns])
	}
	for name := range freeFuncs {
		out["free"] = append(out["free"], name)
	}
	out["free"] = append(out["free"], "col", "lit")
	sort.Strings(out["free"])
	return out
}

// snake converts a Go method name to glr snake_case, keeping runs of
// capitals together (EWMMean becomes ewm_mean, IsNaN becomes is_nan).
func snake(s string) string {
	var b strings.Builder
	r := []rune(s)
	for i, c := range r {
		upper := c >= 'A' && c <= 'Z'
		if upper && i > 0 {
			prevLower := r[i-1] >= 'a' && r[i-1] <= 'z'
			nextLower := i+1 < len(r) && r[i+1] >= 'a' && r[i+1] <= 'z'
			prevUpper := r[i-1] >= 'A' && r[i-1] <= 'Z'
			if prevLower || (prevUpper && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteString(strings.ToLower(string(c)))
	}
	return b.String()
}
