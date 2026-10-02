package difftest

import (
	"slices"
)

// planState tracks the generator's view of the frame between ops.
type planState struct {
	cols  []ColInfo
	order Order
	n     int // approximate row count, for guarding cross joins
}

func (st *planState) ordered() bool { return st.order.Mode == OrderExact }

func (st *planState) scope() *Scope {
	return &Scope{Cols: slices.Clone(st.cols), Ordered: st.ordered()}
}

func (st *planState) has(name string) bool {
	return slices.ContainsFunc(st.cols, func(c ColInfo) bool { return c.Name == name })
}

// dropKeysIfGone degrades the order contract when a select or
// with_columns replaced or removed the columns it relies on.
func (st *planState) touch(names []string, keep bool) {
	if len(st.order.Keys) == 0 {
		return
	}
	for _, k := range st.order.Keys {
		if (keep && !slices.Contains(names, k)) || (!keep && slices.Contains(names, k)) {
			switch st.order.Mode {
			case OrderSorted:
				st.order = Order{Mode: OrderUnordered}
			case OrderKeysOnly:
				st.order = Order{Mode: OrderCountOnly}
			}
			return
		}
	}
}

// LiteralTrees is the default ExprGen.LiteralTrees.
var LiteralTrees = 0.1

// NewCase draws a complete random case from seed.
func NewCase(seed uint64) *Case {
	g := &ExprGen{Gen: NewGen(seed), LiteralTrees: LiteralTrees}
	n := g.RandomSize()
	ncols := 2 + g.Intn(5)
	if n > 10000 {
		ncols = 2 + g.Intn(2)
	}
	left := g.RandomFrame("c", n, ncols)
	c := &Case{Left: left, Plan: &Plan{Seed: seed}}
	st := &planState{order: Order{Mode: OrderExact}, n: n}
	for _, col := range left.Cols {
		st.cols = append(st.cols, ColInfo{Name: col.Name, T: col.T})
	}
	nops := 1 + g.Intn(3)
	if g.Chance(0.03) {
		nops = 0
	}
	joined := false
	for range nops {
		op, ok := g.op(c, st, &joined)
		if ok {
			c.Plan.Ops = append(c.Plan.Ops, op)
		}
	}
	c.Plan.Order = DeriveOrder(c.Plan.Ops)
	return c
}

func (g *ExprGen) op(c *Case, st *planState, joined *bool) (Op, bool) {
	if len(st.cols) == 0 {
		return Op{}, false
	}
	depth := 1 + g.Intn(3)
	switch g.Intn(26) {
	case 0, 1, 2:
		s := st.scope()
		s.NoAggs = g.Chance(0.7)
		p := g.Expr(s, depth, CBool)
		return Op{Op: "filter", Pred: p.E}, true
	case 3, 4, 5, 6:
		return g.withColumns(st, depth), true
	case 7, 8, 9:
		return g.selectOp(st, depth), true
	case 10, 11, 12, 13:
		return g.groupBy(st, depth)
	case 14, 15:
		return g.sortOp(st), true
	case 16, 17:
		if *joined {
			return Op{}, false
		}
		*joined = true
		return g.joinOp(c, st)
	case 18:
		if !g.Chance(0.5) {
			return g.sortOp(st), true
		}
		op := Op{Op: "unique", Maintain: st.ordered() && g.Chance(0.7)}
		if !op.Maintain {
			st.order = Order{Mode: OrderUnordered}
		}
		// Float NaN and list columns make full-row unique ambiguous
		// between engines only through hashing, which is exact; keep.
		return op, true
	case 19, 20:
		return g.sliceOp(st), true
	case 21:
		var lists []ColInfo
		for _, ci := range st.cols {
			if ci.T.K == KList {
				lists = append(lists, ci)
			}
		}
		if len(lists) == 0 {
			return g.sliceOp(st), true
		}
		ci := pick(g.Gen, lists)
		for i := range st.cols {
			if st.cols[i].Name == ci.Name {
				st.cols[i].T = *ci.T.Elem
			}
		}
		st.touch([]string{ci.Name}, false)
		return Op{Op: "explode", Keys: []string{ci.Name}}, true
	case 22:
		return g.unpivotOp(st)
	case 23:
		return g.pivotOp(c, st)
	case 24:
		switch g.Intn(4) {
		case 0:
			if !st.ordered() {
				return Op{}, false
			}
			name := g.Alias()
			st.cols = append([]ColInfo{{Name: name, T: TU32}}, st.cols...)
			return Op{Op: "with_row_index", Name: name, Offset: g.Intn(3)}, true
		case 1:
			var sub []string
			for _, ci := range st.cols {
				if g.Chance(0.3) {
					sub = append(sub, ci.Name)
				}
			}
			return Op{Op: "drop_nulls", Keys: sub}, true
		case 2:
			if !st.ordered() {
				return Op{}, false
			}
			return Op{Op: "reverse"}, true
		case 3:
			if len(st.cols) < 2 {
				return Op{}, false
			}
			i := g.Intn(len(st.cols))
			name := st.cols[i].Name
			st.cols = slices.Delete(st.cols, i, i+1)
			st.touch([]string{name}, false)
			return Op{Op: "drop", Keys: []string{name}}, true
		}
	case 25:
		if *joined {
			return Op{}, false
		}
		*joined = true
		return g.asofOp(c, st)
	}
	return Op{}, false
}

func (g *ExprGen) withColumns(st *planState, depth int) Op {
	s := st.scope()
	var exprs []*Expr
	var names []string
	var newCols []ColInfo
	k := 1 + g.Intn(3)
	for range k {
		te := g.Expr(s, depth, CAny)
		if te.LenChange {
			continue
		}
		name := g.Alias()
		switch {
		case g.Chance(0.15) && len(st.cols) > 0:
			// Overwrite an existing column.
			name = pick(g.Gen, st.cols).Name
			exprs = append(exprs, Alias(te.E, name))
		case te.E.K == "col" && g.Chance(0.5):
			name = te.E.Name
			exprs = append(exprs, te.E)
		default:
			exprs = append(exprs, Alias(te.E, name))
		}
		if slices.Contains(names, name) {
			exprs = exprs[:len(exprs)-1]
			continue
		}
		names = append(names, name)
		newCols = append(newCols, ColInfo{Name: name, T: te.T})
	}
	if len(exprs) == 0 {
		ci := pick(g.Gen, st.cols)
		exprs = append(exprs, Col(ci.Name))
		names = append(names, ci.Name)
		newCols = append(newCols, ci)
	}
	for _, nc := range newCols {
		replaced := false
		for i := range st.cols {
			if st.cols[i].Name == nc.Name {
				st.cols[i] = nc
				replaced = true
			}
		}
		if !replaced {
			st.cols = append(st.cols, nc)
		}
	}
	st.touch(names, false)
	return Op{Op: "with_columns", Exprs: exprs}
}

func (g *ExprGen) selectOp(st *planState, depth int) Op {
	s := st.scope()
	var exprs []*Expr
	var cols []ColInfo
	used := map[string]bool{}
	add := func(e *Expr, name string, t Type) {
		if used[name] {
			return
		}
		used[name] = true
		exprs = append(exprs, e)
		cols = append(cols, ColInfo{Name: name, T: t})
	}
	mode := g.Intn(10)
	switch {
	case mode < 2:
		// All aggregations: one row.
		for range 1 + g.Intn(3) {
			te, ok := g.aggregate(s, depth, pick(g.Gen, []Class{CNum, CNum, CStr, CTemporal}))
			if !ok {
				continue
			}
			name := g.Alias()
			add(Alias(te.E, name), name, te.T)
		}
		if len(exprs) > 0 {
			st.cols = cols
			st.order = Order{Mode: OrderExact}
			return Op{Op: "select", Exprs: exprs, ResetsOrder: true}
		}
	case mode < 3:
		// One length-changing expression, optionally with scalars.
		te := g.LengthChanging(s, depth)
		name := g.Alias()
		add(Alias(te.E, name), name, te.T)
		if g.Chance(0.3) {
			if ag, ok := g.aggregate(s, depth, CNum); ok {
				n2 := g.Alias()
				add(Alias(ag.E, n2), n2, ag.T)
			}
		}
		st.cols = cols
		st.order = Order{Mode: OrderExact}
		return Op{Op: "select", Exprs: exprs, ResetsOrder: true}
	}
	for range 1 + g.Intn(4) {
		if g.Chance(0.4) {
			ci := pick(g.Gen, st.cols)
			add(Col(ci.Name), ci.Name, ci.T)
			continue
		}
		te := g.Expr(s, depth, CAny)
		if te.LenChange {
			continue
		}
		name := g.Alias()
		add(Alias(te.E, name), name, te.T)
	}
	if len(exprs) == 0 {
		ci := pick(g.Gen, st.cols)
		add(Col(ci.Name), ci.Name, ci.T)
	}
	var names []string
	for _, c := range cols {
		names = append(names, c.Name)
	}
	st.touch(names, true)
	st.cols = cols
	return Op{Op: "select", Exprs: exprs}
}

func groupable(t Type) bool {
	switch t.K {
	case KList, KStruct:
		return false
	}
	return true
}

func (g *ExprGen) groupBy(st *planState, depth int) (Op, bool) {
	var cands []ColInfo
	for _, ci := range st.cols {
		if groupable(ci.T) {
			cands = append(cands, ci)
		}
	}
	if len(cands) == 0 {
		return Op{}, false
	}
	nk := 1
	if len(cands) > 1 && g.Chance(0.35) {
		nk = 2
	}
	var keys []ColInfo
	for _, i := range g.R.Perm(len(cands))[:nk] {
		keys = append(keys, cands[i])
	}
	var rest []ColInfo
	for _, ci := range st.cols {
		if !slices.ContainsFunc(keys, func(k ColInfo) bool { return k.Name == ci.Name }) {
			rest = append(rest, ci)
		}
	}
	s := &Scope{Cols: rest, Ordered: st.ordered(), Agg: true}
	if len(rest) == 0 {
		s.Cols = keys
	}
	op := Op{Op: "group_by", Maintain: st.ordered() && g.Chance(0.5)}
	out := slices.Clone(keys)
	used := map[string]bool{}
	for _, k := range keys {
		op.Keys = append(op.Keys, k.Name)
		used[k.Name] = true
	}
	for range 1 + g.Intn(4) {
		var te TE
		var ok bool
		switch g.Intn(10) {
		case 0:
			te, ok = TE{E: Fn("len"), T: TU32, Scalar: true}, true
		case 1, 2:
			// Non-aggregated expressions become lists.
			te = g.Expr(s, depth, CAny)
			ok = true
			if !te.Scalar {
				te.T = ListOf(te.T)
			}
		default:
			te, ok = g.aggregate(s, depth, pick(g.Gen, []Class{CNum, CNum, CStr, CTemporal}))
		}
		if !ok {
			continue
		}
		name := g.Alias()
		if used[name] {
			continue
		}
		used[name] = true
		op.Exprs = append(op.Exprs, Alias(te.E, name))
		out = append(out, ColInfo{Name: name, T: te.T})
	}
	st.cols = out
	if !op.Maintain {
		st.order = Order{Mode: OrderUnordered}
	}
	return op, true
}

func (g *ExprGen) sortOp(st *planState) Op {
	var cands []ColInfo
	for _, ci := range st.cols {
		if ci.T.Orderable() || ci.T.K == KCat {
			cands = append(cands, ci)
		}
	}
	if len(cands) == 0 {
		cands = st.cols
	}
	nk := 1 + g.Intn(min(3, len(cands)))
	op := Op{Op: "sort"}
	for _, i := range g.R.Perm(len(cands))[:nk] {
		op.Keys = append(op.Keys, cands[i].Name)
		op.Desc = append(op.Desc, g.Chance(0.4))
		op.NullsLast = append(op.NullsLast, g.Chance(0.4))
	}
	if st.order.Mode != OrderExact {
		st.order = Order{Mode: OrderSorted, Keys: slices.Clone(op.Keys)}
	}
	return op
}

func (g *ExprGen) sliceOp(st *planState) Op {
	var op Op
	switch g.Intn(3) {
	case 0:
		op = Op{Op: "head", N: g.Intn(20)}
	case 1:
		op = Op{Op: "tail", N: g.Intn(20)}
	case 2:
		op = Op{Op: "slice", Offset: g.Intn(40) - 10, N: g.Intn(30)}
	}
	switch st.order.Mode {
	case OrderSorted:
		st.order = Order{Mode: OrderKeysOnly, Keys: st.order.Keys}
	case OrderUnordered:
		st.order = Order{Mode: OrderCountOnly}
	}
	return op
}

func (g *ExprGen) asofOp(c *Case, st *planState) (Op, bool) {
	// The key must be a sorted, null-free numeric or temporal column.
	// Sort the left side first; the right side is generated sorted.
	var cands []ColInfo
	for _, ci := range st.cols {
		orig := c.Left.Col(ci.Name)
		if orig == nil || !orig.T.Equal(ci.T) {
			continue
		}
		if ci.T.IsInt() || ci.T.K == KDate || (ci.T.K == KDatetime && ci.T.TZ == "") {
			cands = append(cands, ci)
		}
	}
	if len(cands) == 0 || !st.ordered() {
		return Op{}, false
	}
	key := pick(g.Gen, cands)
	c.Plan.Ops = append(c.Plan.Ops,
		Op{Op: "drop_nulls", Keys: []string{key.Name}},
		Op{Op: "sort", Keys: []string{key.Name}, Desc: []bool{false}, NullsLast: []bool{false}})
	nr := pick(g.Gen, []int{0, 1, 3, 10, 50})
	src := c.Left.Col(key.Name)
	var vals []any
	for range nr {
		if len(src.Values) > 0 && g.Chance(0.6) {
			if v := src.Values[g.Intn(len(src.Values))]; v != nil {
				vals = append(vals, v)
				continue
			}
		}
		vals = append(vals, g.value(key.T, profile{small: true}))
	}
	slices.SortFunc(vals, func(a, b any) int {
		switch x := a.(type) {
		case int64:
			y := b.(int64)
			if x < y {
				return -1
			} else if x > y {
				return 1
			}
		case uint64:
			y := b.(uint64)
			if x < y {
				return -1
			} else if x > y {
				return 1
			}
		}
		return 0
	})
	right := &Frame{Cols: []Column{{Name: key.Name, T: key.T, Values: vals}}}
	op := Op{Op: "join_asof", Keys: []string{key.Name}, Strategy: pick(g.Gen, []string{"backward", "forward", "nearest"})}
	// Optional by column: a small-domain string or bool column.
	for _, ci := range st.cols {
		if orig := c.Left.Col(ci.Name); (ci.T.K == KStr || ci.T.K == KBool) && g.Chance(0.3) && orig != nil && orig.T.Equal(ci.T) && ci.Name != key.Name {
			bv := make([]any, nr)
			src := c.Left.Col(ci.Name)
			for r := range bv {
				if len(src.Values) > 0 {
					bv[r] = src.Values[g.Intn(len(src.Values))]
				}
			}
			right.Cols = append(right.Cols, Column{Name: ci.Name, T: ci.T, Values: bv})
			op.By = []string{ci.Name}
			break
		}
	}
	extra := g.RandomColumn("rv", g.RandomType(0), nr)
	right.Cols = append(right.Cols, extra)
	c.Right = right
	name := "rv"
	if st.has(name) {
		name += "_right"
	}
	st.cols = append(st.cols, ColInfo{Name: name, T: extra.T})
	return op, true
}

func (g *ExprGen) unpivotOp(st *planState) (Op, bool) {
	// Group columns by dtype and unpivot one group.
	byType := map[string][]ColInfo{}
	for _, ci := range st.cols {
		byType[ci.T.Name()] = append(byType[ci.T.Name()], ci)
	}
	var groups [][]ColInfo
	for _, cs := range byType {
		groups = append(groups, cs)
	}
	slices.SortFunc(groups, func(a, b []ColInfo) int { return len(b) - len(a) })
	if len(groups) == 0 {
		return Op{}, false
	}
	on := groups[0]
	if len(groups) > 1 && g.Chance(0.5) {
		on = groups[g.Intn(len(groups))]
	}
	op := Op{Op: "unpivot"}
	valT := on[0].T
	for _, ci := range on {
		op.On = append(op.On, ci.Name)
	}
	var index []ColInfo
	for _, ci := range st.cols {
		if !slices.Contains(op.On, ci.Name) && g.Chance(0.5) {
			op.Index = append(op.Index, ci.Name)
			index = append(index, ci)
		}
	}
	st.cols = append(index, ColInfo{Name: "variable", T: TStr}, ColInfo{Name: "value", T: valT})
	if st.order.Mode != OrderExact {
		st.order = Order{Mode: OrderUnordered}
	}
	return op, true
}

func (g *ExprGen) pivotOp(c *Case, st *planState) (Op, bool) {
	if !st.ordered() {
		return Op{}, false
	}
	var onCands, idxCands, valCands []ColInfo
	for _, ci := range st.cols {
		orig := c.Left.Col(ci.Name)
		if orig != nil && orig.T.Equal(ci.T) && (ci.T.K == KStr || ci.T.IsInt() || ci.T.K == KBool) {
			onCands = append(onCands, ci)
		}
		if groupable(ci.T) {
			idxCands = append(idxCands, ci)
		}
		if ci.T.IsNumeric() {
			valCands = append(valCands, ci)
		}
	}
	if len(onCands) == 0 || len(valCands) == 0 {
		return Op{}, false
	}
	on := pick(g.Gen, onCands)
	val := pick(g.Gen, valCands)
	var idx ColInfo
	found := false
	for _, i := range g.R.Perm(len(idxCands)) {
		if idxCands[i].Name != on.Name && idxCands[i].Name != val.Name {
			idx, found = idxCands[i], true
			break
		}
	}
	if !found || val.Name == on.Name {
		return Op{}, false
	}
	// Distinct values of the on column, in first-appearance order,
	// possibly with one value that does not occur.
	seen := map[any]bool{}
	var onVals []any
	for _, v := range c.Left.Col(on.Name).Values {
		if v == nil || seen[v] {
			continue
		}
		seen[v] = true
		onVals = append(onVals, v)
		if len(onVals) >= 4 {
			break
		}
	}
	if len(onVals) == 0 || on.T.K == KBool {
		return Op{}, false
	}
	for i, v := range onVals {
		if u, ok := v.(uint64); ok {
			onVals[i] = int64(u)
		}
	}
	agg := pick(g.Gen, []string{"first", "last", "sum", "mean", "min", "max", "len", "median"})
	op := Op{Op: "pivot", On: []string{on.Name}, Index: []string{idx.Name}, Values: val.Name, Agg: agg, OnValues: onVals}
	out := []ColInfo{idx}
	for _, v := range onVals {
		t := val.T
		switch agg {
		case "mean", "median":
			t = TF64
		case "len":
			t = TU32
		}
		out = append(out, ColInfo{Name: FormatPivotName(v), T: t})
	}
	st.cols = out
	return op, true
}

// FormatPivotName is the column name polars gives a pivot value.
func FormatPivotName(v any) string {
	switch x := v.(type) {
	case string:
		return x
	}
	return FormatValue(v)
}
