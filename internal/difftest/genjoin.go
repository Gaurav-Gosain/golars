package difftest

import (
	"fmt"
	"slices"
	"strings"
)

// joinOp builds a right frame that shares one or two key columns with
// the current left schema and joins on them, with a random mix of the
// polars join options (how, left_on/right_on, suffix, coalesce,
// nulls_equal, validate, maintain_order).
func (g *ExprGen) joinOp(c *Case, st *planState) (Op, bool) {
	// Keys must be columns of the original left frame so the right
	// frame can reuse their values; the op must come before any op that
	// changed them, so only join when the state still has them intact.
	var cands []ColInfo
	for _, ci := range st.cols {
		if orig := c.Left.Col(ci.Name); orig != nil && orig.T.Equal(ci.T) && groupable(ci.T) {
			cands = append(cands, ci)
		}
	}
	how := pick(g.Gen, []string{"inner", "inner", "left", "left", "cross"})
	if g.Chance(0.4) {
		how = pick(g.Gen, []string{"full", "semi", "anti", "right"})
	}
	if len(cands) == 0 {
		how = "cross"
	}
	nr := pick(g.Gen, []int{0, 1, 2, 5, 16, 17, 64, 100})
	if how == "cross" && st.n*nr > 50000 {
		nr = 2
	}
	right := &Frame{}
	op := Op{Op: "join", How: how}
	renameKeys := false
	if how != "cross" {
		nk := 1
		if len(cands) > 1 && g.Chance(0.3) {
			nk = 2
		}
		renameKeys = g.Chance(0.25)
		for i, ci := range g.R.Perm(len(cands))[:nk] {
			ci := cands[ci]
			op.Keys = append(op.Keys, ci.Name)
			rname := ci.Name
			if renameKeys {
				rname = fmt.Sprintf("rk%d", i)
				op.RightOn = append(op.RightOn, rname)
			}
			src := c.Left.Col(ci.Name)
			vals := make([]any, nr)
			for r := range vals {
				if len(src.Values) > 0 && g.Chance(0.8) {
					vals[r] = src.Values[g.Intn(len(src.Values))]
				} else if g.Chance(0.5) {
					vals[r] = nil
				} else {
					vals[r] = g.value(ci.T, g.profile())
				}
			}
			right.Cols = append(right.Cols, Column{Name: rname, T: ci.T, Values: vals, Chunks: g.chunks(nr)})
		}
		if g.Chance(0.3) {
			op.Coalesce = pick(g.Gen, []string{"true", "false"})
		}
		op.NullsEqual = g.Chance(0.2)
		if g.Chance(0.15) {
			op.Validate = pick(g.Gen, []string{"1:1", "1:m", "m:1", "m:m"})
		}
		if g.Chance(0.5) {
			op.JoinOrder = pick(g.Gen, []string{"none", "left", "right", "left_right", "right_left"})
		}
	}
	if g.Chance(0.2) {
		op.Suffix = "_x"
	}
	nextra := 1 + g.Intn(2)
	for i := range nextra {
		name := "r" + string(rune('0'+i))
		if g.Chance(0.3) && len(st.cols) > 0 {
			// Collide with a left name to exercise the suffix.
			name = pick(g.Gen, st.cols).Name
			if slices.Contains(op.Keys, name) && !renameKeys {
				name = "r" + string(rune('0'+i))
			}
		}
		if right.Col(name) != nil {
			continue
		}
		right.Cols = append(right.Cols, g.RandomColumn(name, g.RandomType(1), nr))
	}
	c.Right = right
	rcols := make([]ColInfo, len(right.Cols))
	for i, rc := range right.Cols {
		rcols[i] = ColInfo{Name: rc.Name, T: rc.T}
	}
	st.cols = JoinSchema(st.cols, rcols, op)
	if !(st.order.Mode == OrderExact && JoinKeepsOrder(op)) {
		st.order = Order{Mode: OrderUnordered}
	}
	st.n *= max(nr, 1)
	return op, true
}

// JoinCoalesce reports whether a join op coalesces its key columns,
// applying the polars default (true except for full joins).
func JoinCoalesce(op Op) bool {
	switch op.Coalesce {
	case "true":
		return true
	case "false":
		return false
	}
	return op.How != "full"
}

// JoinSchema is the output schema of a join op, following the polars
// rules: right columns whose name collides with an output left column
// get the suffix, coalesced joins drop the right keys (a right join
// drops the left keys instead), and a coalesced full join keeps the
// left key names.
func JoinSchema(left, right []ColInfo, op Op) []ColInfo {
	suffix := op.Suffix
	if suffix == "" {
		suffix = "_right"
	}
	rightKeys := op.RightOn
	if len(rightKeys) == 0 {
		rightKeys = op.Keys
	}
	var out []ColInfo
	switch op.How {
	case "semi", "anti":
		return slices.Clone(left)
	case "cross":
		out = slices.Clone(left)
		rightKeys = nil
	case "right":
		if JoinCoalesce(op) {
			for _, c := range left {
				if !slices.Contains(op.Keys, c.Name) {
					out = append(out, c)
				}
			}
			rightKeys = nil
		} else {
			out = slices.Clone(left)
			rightKeys = nil
		}
	default:
		out = slices.Clone(left)
		if !JoinCoalesce(op) {
			rightKeys = nil
		}
	}
	nLeft := len(out)
	for _, c := range right {
		if slices.Contains(rightKeys, c.Name) {
			continue
		}
		if slices.ContainsFunc(out[:nLeft], func(l ColInfo) bool { return l.Name == c.Name }) {
			c.Name += suffix
		}
		out = append(out, c)
	}
	return out
}

// JoinKeepsOrder reports whether polars guarantees the row order of a
// join op's result (given an ordered left input).
func JoinKeepsOrder(op Op) bool {
	switch op.How {
	case "semi", "anti":
		return op.JoinOrder == "left" || op.JoinOrder == "left_right"
	case "inner", "full":
		return op.JoinOrder == "left_right" || op.JoinOrder == "right_left"
	case "left":
		return op.JoinOrder == "left_right"
	case "right":
		return op.JoinOrder == "right_left"
	}
	return false
}

// describeJoin spells a join op as a polars call.
func describeJoin(op Op) string {
	var b strings.Builder
	if len(op.RightOn) > 0 {
		fmt.Fprintf(&b, ".join(right, left_on=%q, right_on=%q, how=%q", op.Keys, op.RightOn, op.How)
	} else {
		fmt.Fprintf(&b, ".join(right, on=%q, how=%q", op.Keys, op.How)
	}
	if op.Suffix != "" {
		fmt.Fprintf(&b, ", suffix=%q", op.Suffix)
	}
	if op.Coalesce != "" {
		fmt.Fprintf(&b, ", coalesce=%s", op.Coalesce)
	}
	if op.NullsEqual {
		b.WriteString(", nulls_equal=True")
	}
	if op.Validate != "" {
		fmt.Fprintf(&b, ", validate=%q", op.Validate)
	}
	if op.JoinOrder != "" {
		fmt.Fprintf(&b, ", maintain_order=%q", op.JoinOrder)
	}
	b.WriteString(")")
	return b.String()
}
