package difftest

import (
	"regexp"
	"slices"
	"strings"
)

// sameFailure decides whether a shrink candidate still shows the
// original disagreement.
func sameFailure(orig, got Verdict) bool {
	if got.Kind != orig.Kind {
		return false
	}
	switch orig.Kind {
	case MPanic, MGolarsError, MMissingError:
		return failureKey(orig.Detail) == failureKey(got.Detail)
	}
	return true
}

var digitsRe = regexp.MustCompile(`\d+`)

func failureKey(s string) string {
	s = firstLine(s)
	if i := strings.Index(s, "golars panicked:"); i >= 0 {
		s = s[i:]
	}
	return digitsRe.ReplaceAllString(s, "N")
}

// Shrink reduces c to a smaller case that still fails like v. It uses
// greedy delta debugging over ops, rows, columns, expression subtrees,
// keyword arguments, chunking and values. Each probe is a full oracle
// round trip, so the budget caps the work.
func Shrink(c *Case, v Verdict, dir string, o *Oracle) *Case {
	budget := 1500
	try := func(cand *Case) bool {
		if budget <= 0 {
			return false
		}
		budget--
		cand.Plan.Order = DeriveOrder(cand.Plan.Ops)
		out := Check(cand, dir, o)
		return sameFailure(v, out.Verdict)
	}
	cur := c.Clone()
	for progress := true; progress && budget > 0; {
		progress = false
		// Ops.
		for i := len(cur.Plan.Ops) - 1; i >= 0; i-- {
			cand := cur.Clone()
			cand.Plan.Ops = slices.Delete(cand.Plan.Ops, i, i+1)
			if !planUsesJoin(cand.Plan) {
				cand.Right = nil
			}
			if try(cand) {
				cur, progress = cand, true
			}
		}
		// Rows of both frames.
		if next, ok := shrinkRows(cur, false, try); ok {
			cur, progress = next, true
		}
		if cur.Right != nil {
			if next, ok := shrinkRows(cur, true, try); ok {
				cur, progress = next, true
			}
		}
		// Columns.
		for _, right := range []bool{false, true} {
			f := cur.Left
			if right {
				f = cur.Right
			}
			if f == nil {
				continue
			}
			for i := len(f.Cols) - 1; i >= 0 && len(f.Cols) > 1; i-- {
				cand := cur.Clone()
				tf := cand.Left
				if right {
					tf = cand.Right
				}
				tf.Cols = slices.Delete(tf.Cols, i, i+1)
				if try(cand) {
					cur, progress = cand, true
					f = cur.Left
					if right {
						f = cur.Right
					}
				}
			}
		}
		// Expression lists and subtrees.
		if next, ok := shrinkExprs(cur, try); ok {
			cur, progress = next, true
		}
		// Chunking.
		// The side is tracked by flag: cur (and so its frames) changes
		// whenever a candidate is accepted.
		for _, right := range []bool{false, true} {
			side := func(c *Case) *Frame {
				if right {
					return c.Right
				}
				return c.Left
			}
			if side(cur) == nil {
				continue
			}
			for i := 0; i < len(side(cur).Cols); i++ {
				if len(side(cur).Cols[i].Chunks) == 0 {
					continue
				}
				cand := cur.Clone()
				side(cand).Cols[i].Chunks = nil
				if try(cand) {
					cur, progress = cand, true
				}
			}
		}
	}
	// Values: try nulling, then simplifying, each remaining cell of a
	// small frame.
	if cur.Left.Height() <= 12 {
		for ci := range cur.Left.Cols {
			for r := range cur.Left.Cols[ci].Values {
				if cur.Left.Cols[ci].Values[r] == nil {
					continue
				}
				cand := cur.Clone()
				cand.Left.Cols[ci].Values[r] = nil
				if try(cand) {
					cur = cand
					continue
				}
				if s, ok := simpler(cur.Left.Cols[ci].T, cur.Left.Cols[ci].Values[r]); ok {
					cand := cur.Clone()
					cand.Left.Cols[ci].Values[r] = s
					if try(cand) {
						cur = cand
					}
				}
			}
		}
	}
	cur.Plan.Order = DeriveOrder(cur.Plan.Ops)
	return cur
}

func simpler(t Type, v any) (any, bool) {
	switch x := v.(type) {
	case int64:
		if x != 0 && x != 1 && t.K == KInt {
			return int64(1), true
		}
	case uint64:
		if x > 1 {
			return uint64(1), true
		}
	case float64:
		if x != 1 && x == x {
			return 1.0, true
		}
	case string:
		if x != "a" && t.K == KStr {
			return "a", true
		}
	}
	return nil, false
}

// PlanUsesOp reports whether any op of p is one of ops.
func PlanUsesOp(p *Plan, ops []string) bool {
	return slices.ContainsFunc(p.Ops, func(op Op) bool { return slices.Contains(ops, op.Op) })
}

func planUsesJoin(p *Plan) bool {
	return slices.ContainsFunc(p.Ops, func(op Op) bool { return op.Op == "join" || op.Op == "join_asof" })
}

// shrinkRows removes row blocks, halving the block size down to one.
func shrinkRows(c *Case, right bool, try func(*Case) bool) (*Case, bool) {
	cur := c
	progress := false
	frame := func(x *Case) *Frame {
		if right {
			return x.Right
		}
		return x.Left
	}
	n := frame(cur).Height()
	for block := max(n/2, 1); block >= 1; block /= 2 {
		for start := 0; start < frame(cur).Height(); {
			h := frame(cur).Height()
			keep := make([]bool, h)
			for i := range keep {
				keep[i] = i < start || i >= start+block
			}
			cand := cur.Clone()
			if right {
				cand.Right = cur.Right.KeepRows(keep)
			} else {
				cand.Left = cur.Left.KeepRows(keep)
			}
			if try(cand) {
				cur, progress = cand, true
				continue
			}
			start += block
		}
		if block == 1 {
			break
		}
	}
	return cur, progress
}

// exprSlot addresses one expression in a plan.
type exprSlot struct {
	get func(p *Plan) *Expr
	set func(p *Plan, e *Expr)
}

func planSlots(p *Plan) []exprSlot {
	var slots []exprSlot
	for i := range p.Ops {
		if p.Ops[i].Pred != nil {
			slots = append(slots, exprSlot{
				get: func(p *Plan) *Expr { return p.Ops[i].Pred },
				set: func(p *Plan, e *Expr) { p.Ops[i].Pred = e },
			})
		}
		for j := range p.Ops[i].Exprs {
			slots = append(slots, exprSlot{
				get: func(p *Plan) *Expr { return p.Ops[i].Exprs[j] },
				set: func(p *Plan, e *Expr) { p.Ops[i].Exprs[j] = e },
			})
		}
	}
	return slots
}

// nodes lists pointers to every Expr-valued child position under e,
// keyed by a path, so a node can be replaced in a clone.
func exprPaths(e *Expr, prefix []int, out *[][]int) {
	*out = append(*out, slices.Clone(prefix))
	for i, a := range e.Args {
		exprPaths(a, append(prefix, i), out)
	}
	if e.Else != nil {
		exprPaths(e.Else, append(prefix, -1), out)
	}
}

func atPath(e *Expr, path []int) *Expr {
	for _, i := range path {
		if i == -1 {
			e = e.Else
		} else {
			e = e.Args[i]
		}
	}
	return e
}

func replaceAt(root *Expr, path []int, repl *Expr) *Expr {
	if len(path) == 0 {
		return repl
	}
	parent := atPath(root, path[:len(path)-1])
	last := path[len(path)-1]
	if last == -1 {
		parent.Else = repl
	} else {
		parent.Args[last] = repl
	}
	return root
}

func isValueExpr(e *Expr) bool {
	switch e.K {
	case "raw", "dtype":
		return false
	}
	return true
}

func shrinkExprs(c *Case, try func(*Case) bool) (*Case, bool) {
	cur := c
	progress := false
	// Drop whole expressions from select, with_columns and agg lists.
	for i := range cur.Plan.Ops {
		for j := len(cur.Plan.Ops[i].Exprs) - 1; j >= 0; j-- {
			if len(cur.Plan.Ops[i].Exprs) <= 1 && cur.Plan.Ops[i].Op != "group_by" {
				break
			}
			cand := cur.Clone()
			cand.Plan.Ops[i].Exprs = slices.Delete(cand.Plan.Ops[i].Exprs, j, j+1)
			if try(cand) {
				cur, progress = cand, true
			}
		}
	}
	// Hoist a value-expression child over its parent, drop keyword
	// arguments, drop when branches.
	for si := 0; si < len(planSlots(cur.Plan)); si++ {
		for changed := true; changed; {
			changed = false
			slot := planSlots(cur.Plan)[si]
			root := slot.get(cur.Plan)
			var paths [][]int
			exprPaths(root, nil, &paths)
			for _, path := range paths {
				node := atPath(root, path)
				var cands []*Expr
				for _, a := range node.Args {
					if isValueExpr(a) {
						cands = append(cands, a)
					}
				}
				if node.Else != nil {
					cands = append(cands, node.Else)
				}
				for k := range node.KW {
					n2 := node.Clone()
					n2.KW = slices.Delete(n2.KW, k, k+1)
					cands = append(cands, n2)
				}
				if node.K == "when" && len(node.Args) > 2 {
					n2 := node.Clone()
					n2.Args = n2.Args[:len(n2.Args)-2]
					cands = append(cands, n2)
				}
				if node.K == "when" && node.Else != nil {
					n2 := node.Clone()
					n2.Else = nil
					cands = append(cands, n2)
				}
				for _, repl := range cands {
					cand := cur.Clone()
					croot := slot.get(cand.Plan).Clone()
					newRoot := replaceAt(croot, path, repl.Clone())
					// Keep top-level aliases so output names stay stable.
					if len(path) == 0 && root.K == "call" && root.Name == "alias" {
						continue
					}
					slot.set(cand.Plan, newRoot)
					if try(cand) {
						cur, progress, changed = cand, true, true
						break
					}
				}
				if changed {
					break
				}
			}
		}
	}
	return cur, progress
}
