package analysis

import (
	"context"
	"os"
	"strconv"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

var bgCtx = context.Background()

// parts returns the parts of st of the given kind.
func parts(st *syntax.Stmt, kind syntax.PartKind) []syntax.Part {
	var out []syntax.Part
	for _, p := range st.Parts {
		if p.Kind == kind {
			out = append(out, p)
		}
	}
	return out
}

// columns flattens the column parts of st: plain columns and list
// items.
func columns(st *syntax.Stmt) []syntax.Part {
	var out []syntax.Part
	for _, p := range st.Parts {
		switch p.Kind {
		case syntax.PartColumn:
			out = append(out, p)
		case syntax.PartList:
			out = append(out, p.Items...)
		}
	}
	return out
}

func values(ps []syntax.Part) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Value
	}
	return out
}

func firstCount(st *syntax.Stmt) (int, bool) {
	for _, p := range st.Parts {
		if p.Kind == syntax.PartCount {
			n, err := strconv.Atoi(p.Value)
			return n, err == nil
		}
	}
	return 0, false
}

// limitRows caps the focus at n rows. An exact count stays exact;
// an unknown count becomes the upper bound n.
func (a *analyzer) limitRows(n int) {
	switch {
	case a.focus.Rows < 0:
		a.focus.Rows, a.focus.Exact = n, false
	case n < a.focus.Rows:
		a.focus.Rows = n
	}
}

// checkCols verifies every column part against the focus.
func (a *analyzer) checkCols(ps []syntax.Part) bool {
	good := true
	for _, p := range ps {
		if !a.refColumn(p, a.focus, true) {
			good = false
		}
	}
	return good
}

func (a *analyzer) apply(st *syntax.Stmt) {
	spec := st.Spec
	if needsFocus(spec) && !a.requireFocus(st) {
		// Still record the references for navigation.
		a.applyBroken(st)
		return
	}
	switch name := spec.Name; name {
	case "load", "scan_csv", "scan_parquet", "scan_ipc", "scan_json", "scan_ndjson", "scan_auto":
		a.applyLoad(st)
	case "use":
		p := st.Parts[0]
		if s, found := a.refFrame(p, false); found {
			a.focus, a.base = s.frame, s.frame
			a.focus.Source = p.Value
		} else {
			a.focus, a.base = unknownFrame(), unknownFrame()
		}
		a.hasFocus = true
	case "stash":
		p := st.Parts[0]
		l := a.loc(p)
		prev := a.staged[p.Value]
		s := &staged{frame: a.focus, def: &l, stash: true}
		if prev != nil && !prev.stash {
			s.stash = false
		}
		a.staged[p.Value] = s
		a.symbol(Symbol{Kind: SymFrame, Name: p.Value, Loc: l, Def: true})
		a.base = a.focus
	case "drop_frame":
		p := st.Parts[0]
		if _, found := a.refFrame(p, false); found {
			delete(a.staged, p.Value)
		}
	case "source":
		a.applySource(st)
	case "collect":
		a.base = a.focus
	case "reset":
		a.focus = a.base
	case "filter":
		a.applyFilter(st)
	case "with":
		a.applyWith(st)
	case "select":
		a.applySelect(st)
	case "drop":
		cols := columns(st)
		if a.checkCols(cols) && a.focus.Known() {
			a.focus.Schema = a.focus.Schema.Drop(values(cols)...)
		}
	case "sort":
		a.applySort(st)
	case "limit":
		if n, ok := firstCount(st); ok {
			a.limitRows(n)
		}
	case "groupby", "group_by_dynamic":
		a.applyGroupBy(st)
	case "join":
		a.applyJoin(st)
	case "join_asof":
		a.applyJoinAsof(st)
	case "unique", "drop_null":
		a.checkCols(columns(st))
		a.focus.Exact = false
	case "cast":
		cols := columns(st)
		dt := parts(st, syntax.PartDType)
		if a.checkCols(cols) && len(dt) == 1 && a.focus.Known() {
			to, err := exprparse.ParseDType(dt[0].Value)
			if err == nil {
				a.lazyStep(st.Parts[0], func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Cast(cols[0].Value, to) }, nil)
			}
		}
	case "rename":
		cols := columns(st)
		nc := parts(st, syntax.PartNewColumn)
		if a.checkCols(cols) && len(nc) == 1 {
			l := a.defColumn(nc[0])
			if a.focus.Known() {
				if sch, err := a.focus.Schema.Rename(cols[0].Value, nc[0].Value); err == nil {
					a.focus.Schema = sch
					a.focus.Origins = withOrigins(a.focus, map[string]Loc{nc[0].Value: l})
				}
			}
		}
	case "with_row_index":
		nc := parts(st, syntax.PartNewColumn)[0]
		l := a.defColumn(nc)
		a.lazyStep(nc, func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.WithRowIndex(nc.Value, 0) },
			map[string]Loc{nc.Value: l})
	case "sum_horizontal", "mean_horizontal", "min_horizontal", "max_horizontal", "all_horizontal", "any_horizontal":
		nc := parts(st, syntax.PartNewColumn)[0]
		cols := columns(st)
		l := a.defColumn(nc)
		if a.checkCols(cols) {
			names := values(cols)
			a.lazyStep(nc, func(lf lazy.LazyFrame) lazy.LazyFrame {
				switch name {
				case "sum_horizontal":
					return lf.SumHorizontal(nc.Value, names...)
				case "mean_horizontal":
					return lf.MeanHorizontal(nc.Value, names...)
				case "min_horizontal":
					return lf.MinHorizontal(nc.Value, names...)
				case "max_horizontal":
					return lf.MaxHorizontal(nc.Value, names...)
				case "all_horizontal":
					return lf.AllHorizontal(nc.Value, names...)
				}
				return lf.AnyHorizontal(nc.Value, names...)
			}, map[string]Loc{nc.Value: l})
		}
	case "fill_null", "fill_nan", "forward_fill", "backward_fill", "reverse":
		// Shape and schema are unchanged.
	case "sample", "top_k", "bottom_k":
		a.checkCols(columns(st))
		if n, ok := firstCount(st); ok {
			a.limitRows(n)
		}
		a.base = a.focus
	case "shuffle":
		a.base = a.focus
	case "unnest", "explode", "upsample", "unpivot", "transpose", "pivot", "to_dummies":
		a.applyReshape(st)
	default:
		// Inspect, aggregate, plan and session commands leave the
		// focus alone; check their column arguments.
		a.checkCols(columns(st))
	}
}

// lazyStep applies a lazy operation to the focus schema, reporting an
// engine error at p.
func (a *analyzer) lazyStep(p syntax.Part, fn func(lazy.LazyFrame) lazy.LazyFrame, add map[string]Loc) {
	if !a.focus.Known() {
		if add != nil {
			a.focus.Origins = withOrigins(a.focus, add)
		}
		return
	}
	out, err := runLazy(a.focus, !a.emptyProbe, fn)
	if err != nil {
		a.engineError(p, err, a.focus)
		a.focus.Schema = nil
		return
	}
	a.setFocus(out, a.focus.Rows, a.focus.Exact, add)
}

func (a *analyzer) applyLoad(st *syntax.Stmt) {
	path := parts(st, syntax.PartPath)[0]
	f := a.sourceFrame(path)
	if nf := parts(st, syntax.PartNewFrame); len(nf) == 1 {
		l := a.loc(nf[0])
		a.staged[nf[0].Value] = &staged{frame: f, def: &l}
		a.symbol(Symbol{Kind: SymFrame, Name: nf[0].Value, Loc: l, Def: true})
		return
	}
	a.focus, a.base, a.hasFocus = f, f, true
}

func (a *analyzer) applySource(st *syntax.Stmt) {
	p := parts(st, syntax.PartPath)[0]
	abs := Resolve(p.Value, a.r.opts.Dir)
	src, err := os.ReadFile(abs)
	if err != nil {
		a.errAt(p, "missing-file", "cannot read %q: %v", p.Value, err)
		return
	}
	if a.depth >= 8 {
		return
	}
	f := syntax.Parse(string(src))
	cur, quiet := a.cur, a.quiet
	a.depth++
	a.quiet = true
	for _, s := range f.Stmts {
		a.cur = s
		if s.Spec != nil && len(s.Diags) == 0 {
			a.apply(s)
		}
	}
	a.depth--
	a.cur, a.quiet = cur, quiet
	a.hasFocus = true
}

func (a *analyzer) applyFilter(st *syntax.Stmt) {
	p := st.Parts[0]
	e, ok := a.checkExpr(p, a.focus)
	a.focus.Exact = false
	if !ok || !a.focus.Known() {
		return
	}
	sch, err := evalExprs(a.focus.Schema, e.Alias("__predicate"))
	if err != nil {
		a.engineError(p, err, a.focus)
		return
	}
	if dt := sch.Field(0).DType; dt.String() != "bool" && dt.String() != "null" {
		d := a.errAt(p, "not-boolean", "filter predicate is %s, not bool", dt)
		d.Hint = "compare it, as in x > 0, or test it with is_null"
	}
}

// namedExprs walks `name = expr` items (with, select, groupby).
type namedExpr struct {
	name *syntax.Part
	expr syntax.Part
}

func namedExprs(st *syntax.Stmt) []namedExpr {
	var out []namedExpr
	for i := 0; i < len(st.Parts); i++ {
		p := st.Parts[i]
		switch {
		case p.Kind == syntax.PartNewColumn && i+2 < len(st.Parts) && st.Parts[i+2].Kind == syntax.PartExpr:
			name := st.Parts[i]
			out = append(out, namedExpr{name: &name, expr: st.Parts[i+2]})
			i += 2
		case p.Kind == syntax.PartExpr:
			out = append(out, namedExpr{expr: p})
		}
	}
	return out
}

// compileItems checks and compiles named expressions against f. It
// returns the expressions (aliased) and the origins of the named
// outputs; ok is false when an item did not compile.
func (a *analyzer) compileItems(items []namedExpr, f Frame) (exprs []expr.Expr, add map[string]Loc, ok bool) {
	exprs, _, add, ok = a.compileItemsFields(items, f)
	return exprs, add, ok
}

// compileItemsFields is compileItems that also returns the output
// field of each expression; fields is nil when an expression expands
// to several columns or the frame is unknown.
func (a *analyzer) compileItemsFields(items []namedExpr, f Frame) (exprs []expr.Expr, fields []schema.Field, add map[string]Loc, ok bool) {
	add = map[string]Loc{}
	ok = true
	single := f.Known()
	for _, it := range items {
		e, good := a.checkExpr(it.expr, f)
		if it.name != nil {
			add[it.name.Value] = a.defColumn(*it.name)
			e = e.Alias(it.name.Value)
		}
		if !good {
			ok = false
			continue
		}
		if f.Known() {
			sch, err := evalExprs(f.Schema, e)
			if err != nil {
				a.engineError(it.expr, err, f)
				ok = false
				continue
			}
			if it.name != nil && a.step0 != nil {
				if a.step0.Items == nil {
					a.step0.Items = map[string]string{}
				}
				a.step0.Items[it.name.Value] = sch.Field(0).DType.String()
			}
			if sch.Len() == 1 {
				fields = append(fields, sch.Field(0))
			} else {
				single = false
			}
		}
		exprs = append(exprs, e)
	}
	if !single {
		fields = nil
	}
	return exprs, fields, add, ok
}

func (a *analyzer) applyWith(st *syntax.Stmt) {
	exprs, fields, add, ok := a.compileItemsFields(namedExprs(st), a.focus)
	if !ok {
		a.focus.Origins = withOrigins(a.focus, add)
		if a.focus.Known() {
			// Keep the names so later statements do not cascade.
			a.addUnknownColumns(add)
		}
		return
	}
	if fields != nil {
		// with_columns appends or replaces each output in order.
		sch := a.focus.Schema
		for _, f := range fields {
			sch = sch.WithField(f)
		}
		a.focus.Schema = sch
		a.focus.Origins = withOrigins(a.focus, add)
		return
	}
	a.lazyStep(st.Parts[0], func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.WithColumns(exprs...) }, add)
}

// addUnknownColumns adds columns of unknown dtype (after an error) so
// later references to them are not reported again.
func (a *analyzer) addUnknownColumns(add map[string]Loc) {
	sch := a.focus.Schema
	for name := range add {
		if !sch.Contains(name) {
			sch = sch.WithField(nullField(name))
		}
	}
	a.focus.Schema = sch
}

func (a *analyzer) applySelect(st *syntax.Stmt) {
	if lists := parts(st, syntax.PartList); len(lists) > 0 {
		cols := columns(st)
		if a.checkCols(cols) && a.focus.Known() {
			if sch, err := a.focus.Schema.Select(values(cols)...); err == nil {
				a.focus.Schema = sch
			}
		}
		return
	}
	exprs, fields, add, ok := a.compileItemsFields(namedExprs(st), a.focus)
	if !ok {
		a.focus.Schema = nil
		a.focus.Origins = withOrigins(a.focus, add)
		return
	}
	if fields != nil {
		if sch, err := schema.New(fields...); err == nil {
			a.focus.Schema = sch
			a.focus.Origins = withOrigins(a.focus, add)
			return
		}
	}
	a.lazyStep(st.Parts[0], func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Select(exprs...) }, add)
}

func (a *analyzer) applySort(st *syntax.Stmt) {
	var cols []syntax.Part
	for _, p := range st.Parts {
		if p.Kind == syntax.PartColumn {
			cols = append(cols, p)
		}
	}
	a.checkCols(cols)
}

// aggItems returns the aggregations of a groupby statement as
// compiled expressions, the key parts, and output origins.
func (a *analyzer) aggItems(st *syntax.Stmt) (aggs []expr.Expr, keys []syntax.Part, add map[string]Loc, ok bool) {
	ok = true
	add = map[string]Loc{}
	for _, p := range st.Parts {
		if p.Kind != syntax.PartAgg {
			continue
		}
		col, op := p.Items[0], p.Items[1]
		out := col
		if len(p.Items) == 3 {
			out = p.Items[2]
			add[out.Value] = a.defColumn(out)
		}
		if !a.refColumn(col, a.focus, true) {
			ok = false
			continue
		}
		aggs = append(aggs, aggExpr(col.Value, op.Value).Alias(out.Value))
	}
	exprs, named, good := a.compileItems(namedExprs(st), a.focus)
	for k, v := range named {
		add[k] = v
	}
	aggs = append(aggs, exprs...)
	return aggs, keys, add, ok && good
}

// aggExpr builds the expression of a col:op shorthand.
func aggExpr(col, op string) expr.Expr {
	e := expr.Col(col)
	switch op {
	case "sum":
		return e.Sum()
	case "mean", "avg":
		return e.Mean()
	case "min":
		return e.Min()
	case "max":
		return e.Max()
	case "count":
		return e.Count()
	case "null_count":
		return e.NullCount()
	case "first":
		return e.First()
	case "last":
		return e.Last()
	case "median":
		return e.Median()
	case "std":
		return e.Std()
	case "var":
		return e.Var()
	}
	return e.NUnique()
}

func (a *analyzer) applyGroupBy(st *syntax.Stmt) {
	var keys []syntax.Part
	var index syntax.Part
	opts := dataframe.DynamicGroupOptions{}
	dynamic := st.Spec.Name == "group_by_dynamic"
	for i, p := range st.Parts {
		switch {
		case !dynamic && i == 0 && p.Kind == syntax.PartList:
			keys = p.Items
		case dynamic && i == 0:
			index = p
		case dynamic && p.Kind == syntax.PartKeyword && i+1 < len(st.Parts):
			v := st.Parts[i+1]
			switch p.Value {
			case "every":
				opts.Every = v.Value
			case "period":
				opts.Period = v.Value
			case "offset":
				opts.Offset = v.Value
			case "by":
				keys = v.Items
				opts.GroupBy = values(v.Items)
			case "closed":
				opts.Closed = v.Value
			case "label":
				opts.Label = v.Value
			case "start_by":
				opts.StartBy = v.Value
			}
		}
	}
	good := a.checkCols(keys)
	if dynamic {
		good = a.checkCols([]syntax.Part{index}) && good
	}
	aggs, _, add, ok := a.aggItems(st)
	a.focus.Exact = false
	for _, k := range keys {
		add[k.Value] = a.focus.Origins[k.Value]
	}
	if !good || !ok || !a.focus.Known() {
		a.focus.Schema = nil
		a.focus.Origins = withOrigins(a.focus, add)
		return
	}
	a.emptyProbe = dynamic
	defer func() { a.emptyProbe = false }()
	a.lazyStep(st.Parts[0], func(lf lazy.LazyFrame) lazy.LazyFrame {
		if dynamic {
			return lf.GroupByDynamic(index.Value, opts).Agg(aggs...)
		}
		return lf.GroupBy(values(keys)...).Agg(aggs...)
	}, add)
	a.base = a.focus
}

// otherFrame resolves the right side of a join: a staged frame, or a
// file.
func (a *analyzer) otherFrame(p syntax.Part) Frame {
	if s, found := a.refFrame(p, true); found {
		return s.frame
	}
	abs := Resolve(p.Value, a.r.opts.Dir)
	if _, err := os.Stat(abs); err != nil {
		d := a.errAt(p, "unknown-frame", "%q is neither a staged frame nor a file", p.Value)
		if best := closestName(p.Value, a.stagedNames()); best != "" {
			d.Hint = "did you mean \"" + best + "\"?"
			d.Fix = &syntax.Fix{Title: "Change to " + best, Text: best}
		}
		return unknownFrame()
	}
	return a.sourceFrame(p)
}

func (a *analyzer) applyJoin(st *syntax.Stmt) {
	target := parts(st, syntax.PartTarget)[0]
	var keys []syntax.Part
	for _, l := range parts(st, syntax.PartList) {
		keys = append(keys, l.Items...)
	}
	keys = append(keys, parts(st, syntax.PartColumn)...)
	how := dataframe.InnerJoin
	if o := parts(st, syntax.PartOption); len(o) == 1 {
		if t, err := dataframe.ParseJoinType(o[0].Value); err == nil {
			how = t
		}
	}
	var opts []dataframe.JoinOption
	if w := parts(st, syntax.PartWord); len(w) == 1 {
		opts = append(opts, dataframe.WithJoinSuffix(w[0].Value))
	}
	right := a.otherFrame(target)
	leftRows := a.focus.Rows
	good := len(keys) > 0
	for _, key := range keys {
		if !a.refColumn(key, a.focus, true) {
			good = false
		}
		if right.Known() && !right.Schema.Contains(key.Value) {
			d := a.errAt(key, "unknown-column", "join key %q is not a column of %s", key.Value, target.Value)
			if best := closestName(key.Value, right.Schema.Names()); best != "" {
				d.Hint = "did you mean \"" + best + "\"?"
				d.Fix = &syntax.Fix{Title: "Change to " + best, Text: best}
			}
			good = false
		}
	}
	add := map[string]Loc{}
	if how != dataframe.SemiJoin && how != dataframe.AntiJoin {
		for k, v := range right.Origins {
			if _, taken := a.focus.Origins[k]; !taken {
				add[k] = v
			}
		}
	}
	if good && a.focus.Known() && right.Known() {
		errAt := target
		if len(keys) > 0 {
			errAt = keys[0]
		}
		out, err := runEager(a.focus, true, func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			r := probe(right, true)
			defer r.Release()
			return df.Join(bgCtx, r, values(keys), how, opts...)
		})
		if err != nil {
			a.engineError(errAt, err, a.focus)
			a.focus.Schema = nil
		} else {
			a.setFocus(out, a.focus.Rows, a.focus.Exact, add)
		}
	} else {
		a.focus.Schema = nil
		a.focus.Origins = withOrigins(a.focus, add)
	}
	switch how {
	case dataframe.LeftJoin:
		a.focus.Rows = leftRows
	case dataframe.RightJoin:
		a.focus.Rows = right.Rows
	case dataframe.CrossJoin:
		a.focus.Rows = -1
		if leftRows >= 0 && right.Rows >= 0 {
			a.focus.Rows = leftRows * right.Rows
		}
	default:
		// Inner, full, semi and anti joins change the row count in
		// ways only the data decides.
		a.focus.Rows = -1
		a.focus.Exact = false
	}
	a.base = a.focus
}

func (a *analyzer) applyJoinAsof(st *syntax.Stmt) {
	target := parts(st, syntax.PartTarget)[0]
	key := parts(st, syntax.PartColumn)[0]
	opts := dataframe.AsofOptions{On: key.Value}
	var by []syntax.Part
	for _, p := range st.Parts {
		switch {
		case p.Kind == syntax.PartList:
			by = p.Items
			opts.By = values(p.Items)
		case p.Kind == syntax.PartOption:
			switch p.Value {
			case "forward":
				opts.Strategy = dataframe.AsofForward
			case "nearest":
				opts.Strategy = dataframe.AsofNearest
			}
		}
	}
	right := a.otherFrame(target)
	good := a.checkCols(append([]syntax.Part{key}, by...))
	for _, c := range append([]syntax.Part{key}, by...) {
		if right.Known() && !right.Schema.Contains(c.Value) {
			a.errAt(c, "unknown-column", "%q is not a column of %s", c.Value, target.Value)
			good = false
		}
	}
	add := map[string]Loc{}
	for k, v := range right.Origins {
		if _, taken := a.focus.Origins[k]; !taken {
			add[k] = v
		}
	}
	rows, exact := a.focus.Rows, a.focus.Exact
	if good && a.focus.Known() && right.Known() {
		out, err := runEager(a.focus, false, func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			r := probe(right, false)
			defer r.Release()
			return df.JoinAsof(bgCtx, r, opts)
		})
		if err != nil {
			a.engineError(target, err, a.focus)
			a.focus.Schema = nil
		} else {
			a.setFocus(out, rows, exact, add)
		}
	} else {
		a.focus.Schema = nil
		a.focus.Origins = withOrigins(a.focus, add)
	}
	a.base = a.focus
}

func (a *analyzer) applyReshape(st *syntax.Stmt) {
	name := st.Spec.Name
	cols := columns(st)
	good := a.checkCols(cols)
	rows := a.focus.Rows
	exact := a.focus.Exact
	switch name {
	case "explode", "upsample":
		rows, exact = -1, false
	case "unpivot":
		rows, exact = -1, false
	case "transpose", "pivot":
		// The output columns come from the data.
		a.focus = Frame{Rows: -1, Origins: map[string]Loc{}, Source: a.focus.Source}
		a.base = a.focus
		return
	case "to_dummies":
		if len(cols) == 0 && a.focus.Known() && a.focus.Schema.Len() > 0 || len(cols) > 0 {
			a.focus.Schema = nil
		}
		a.base = a.focus
		return
	}
	if !good || !a.focus.Known() {
		a.focus.Schema = nil
		a.focus.Rows, a.focus.Exact = rows, exact
		a.base = a.focus
		return
	}
	names := values(cols)
	dur := parts(st, syntax.PartDuration)
	out, err := runEager(a.focus, name != "upsample", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		switch name {
		case "unnest":
			return df.Unnest(bgCtx, names[0])
		case "explode":
			return df.ExplodeColumns(bgCtx, names...)
		case "upsample":
			return df.Upsample(bgCtx, names[0], dur[0].Value)
		}
		lists := parts(st, syntax.PartList)
		var ids, vals []string
		if len(lists) > 0 {
			ids = values(lists[0].Items)
		}
		if len(lists) > 1 {
			vals = values(lists[1].Items)
		}
		return df.Unpivot(bgCtx, ids, vals)
	})
	if err != nil {
		p := st.Cmd
		if len(cols) > 0 {
			p = cols[0]
		}
		a.engineError(p, err, a.focus)
		a.focus.Schema = nil
		a.base = a.focus
		return
	}
	a.setFocus(out, rows, exact, nil)
	a.base = a.focus
}

// closestName is script.Closest for names that may be empty.
func closestName(want string, names []string) string {
	if len(names) == 0 {
		return ""
	}
	return script.Closest(want, names)
}

func nullField(name string) schema.Field { return schema.Field{Name: name, DType: dtype.Null()} }
