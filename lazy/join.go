package lazy

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// Join is a two-source join. Opts carries the dataframe join options
// (left_on/right_on, suffix, coalesce, nulls_equal, validate,
// maintain_order); Spec resolves them.
type Join struct {
	Left  Node
	Right Node
	On    []string
	How   dataframe.JoinType
	Opts  []dataframe.JoinOption
}

func (Join) isLogicalNode() {}

func (j Join) Children() []Node { return []Node{j.Left, j.Right} }

func (j Join) WithChildren(children []Node) Node {
	if len(children) != 2 {
		panic("lazy: Join takes two children")
	}
	return Join{Left: children[0], Right: children[1], On: j.On, How: j.How, Opts: j.Opts}
}

// Spec returns the resolved join arguments.
func (j Join) Spec() dataframe.JoinSpec {
	return dataframe.ResolveJoinSpec(j.On, j.How, j.Opts...)
}

// options returns the options for dataframe.Join with the executor's
// allocator last.
func (j Join) options(alloc memory.Allocator) []dataframe.JoinOption {
	opts := make([]dataframe.JoinOption, 0, len(j.Opts)+1)
	opts = append(opts, j.Opts...)
	return append(opts, dataframe.WithJoinAllocator(alloc))
}

func (j Join) Schema() (*schema.Schema, error) {
	ls, err := j.Left.Schema()
	if err != nil {
		return nil, err
	}
	rs, err := j.Right.Schema()
	if err != nil {
		return nil, err
	}
	return dataframe.JoinOutputSchema(ls, rs, j.Spec())
}

func (j Join) String() string {
	spec := j.Spec()
	var b strings.Builder
	if slices.Equal(spec.LeftOn, spec.RightOn) {
		fmt.Fprintf(&b, "JOIN %s on=%v", j.How, spec.LeftOn)
	} else {
		fmt.Fprintf(&b, "JOIN %s left_on=%v right_on=%v", j.How, spec.LeftOn, spec.RightOn)
	}
	def := dataframe.ResolveJoinSpec(j.On, j.How)
	if spec.Suffix != def.Suffix {
		fmt.Fprintf(&b, " suffix=%q", spec.Suffix)
	}
	if spec.Coalesce != def.Coalesce {
		fmt.Fprintf(&b, " coalesce=%v", spec.Coalesce)
	}
	if spec.NullsEqual {
		b.WriteString(" nulls_equal=true")
	}
	if spec.Validate != dataframe.ValidateManyToMany {
		fmt.Fprintf(&b, " validate=%s", spec.Validate)
	}
	if spec.Order != dataframe.JoinOrderNone {
		fmt.Fprintf(&b, " maintain_order=%s", spec.Order)
	}
	return b.String()
}

// Join merges lf with other on a set of key columns.
//
// # Parameters
//
//   - other: right-hand side [LazyFrame].
//   - on: shared key column names (must exist in both frames). Use
//     [dataframe.WithJoinKeys] to join differently named columns.
//   - how: [dataframe.InnerJoin], [dataframe.LeftJoin],
//     [dataframe.RightJoin], [dataframe.FullJoin], [dataframe.SemiJoin],
//     [dataframe.AntiJoin] or [dataframe.CrossJoin] (which ignores on).
//   - opts: the options of [dataframe.DataFrame.Join]: suffix,
//     coalesce, nulls_equal, validate and maintain_order.
//
// # Returns
//
// A [LazyFrame] with the polars join output: the left columns, then
// the right columns, key columns coalesced unless disabled, colliding
// right names suffixed.
//
// # Examples
//
//	// Merge salaries onto people by name:
//	people := lazy.FromDataFrame(peopleDF)
//	salaries := lazy.FromDataFrame(salariesDF)
//	out, _ := people.Join(salaries, []string{"name"}, dataframe.InnerJoin).
//	    Filter(expr.Col("salary").Gt(expr.LitFloat64(50_000))).
//	    Collect(ctx)
func (lf LazyFrame) Join(other LazyFrame, on []string, how dataframe.JoinType, opts ...dataframe.JoinOption) LazyFrame {
	return LazyFrame{plan: Join{Left: lf.plan, Right: other.plan, On: on, How: how, Opts: opts}}
}

// joinSide says which inputs a filter on join output may move into.
type joinSide uint8

const (
	pushLeft joinSide = 1 << iota
	pushRight
)

// joinPushTargets decides where the conjunct pred of a filter above j
// may move. A conjunct moves into the left input when every column it
// reads is an unrenamed left column and the join never null-extends the
// left side (inner, left, semi, anti, cross); likewise into the right
// input for inner, right and cross joins. A conjunct that reads only
// coalesced key columns with the same name and dtype on both sides moves
// into both inputs of an inner or full join: equal keys evaluate it the
// same way on either side, so a row survives above the join exactly when
// its source rows survive below it.
func joinPushTargets(j Join, spec dataframe.JoinSpec, ls, rs *schema.Schema, pred expr.Expr) joinSide {
	if !expr.IsElementwise(pred) || referencesAllColumns(pred) {
		return 0
	}
	// Filtering below a validated join could hide the duplicate keys
	// the validation is meant to report.
	if spec.Validate != dataframe.ValidateManyToMany {
		return 0
	}
	refs := expr.Columns(pred)
	if len(refs) == 0 {
		return 0
	}
	out, err := j.Schema()
	if err != nil {
		return 0
	}
	leftOK, rightOK, keysOnly := true, true, true
	for _, r := range refs {
		if !out.Contains(r) {
			return 0
		}
		inLeft := ls.Contains(r) && leftColumnSurvives(spec, r)
		inRight := rs.Contains(r) && rightColumnSurvives(spec, ls, r)
		leftOK = leftOK && inLeft
		rightOK = rightOK && inRight
		k := slices.Index(spec.LeftOn, r)
		if k < 0 || spec.RightOn[k] != r || !sameDType(ls, rs, r) {
			keysOnly = false
		}
	}
	var side joinSide
	switch spec.How {
	case dataframe.InnerJoin, dataframe.CrossJoin:
		if leftOK {
			side |= pushLeft
		}
		if rightOK {
			side |= pushRight
		}
		if spec.How == dataframe.InnerJoin && keysOnly && spec.Coalesce {
			side |= pushLeft | pushRight
		}
	case dataframe.LeftJoin, dataframe.SemiJoin, dataframe.AntiJoin:
		if leftOK {
			side |= pushLeft
		}
	case dataframe.RightJoin:
		if rightOK {
			side |= pushRight
		}
	case dataframe.FullJoin:
		if keysOnly && spec.Coalesce {
			side |= pushLeft | pushRight
		}
	}
	return side
}

// leftColumnSurvives reports whether output column name is the left
// input column of the same name, unchanged.
func leftColumnSurvives(spec dataframe.JoinSpec, name string) bool {
	switch spec.How {
	case dataframe.RightJoin:
		return !(spec.Coalesce && slices.Contains(spec.LeftOn, name))
	case dataframe.FullJoin:
		return !(spec.Coalesce && slices.Contains(spec.LeftOn, name))
	}
	return true
}

// rightColumnSurvives reports whether output column name is the right
// input column of the same name, unchanged (not suffixed, not dropped).
func rightColumnSurvives(spec dataframe.JoinSpec, ls *schema.Schema, name string) bool {
	switch spec.How {
	case dataframe.SemiJoin, dataframe.AntiJoin:
		return false
	case dataframe.RightJoin:
		if spec.Coalesce {
			// Left keys are dropped, so a right column only collides
			// with a left non-key column.
			return !ls.Contains(name) || slices.Contains(spec.LeftOn, name)
		}
		return !ls.Contains(name)
	}
	if spec.Coalesce && slices.Contains(spec.RightOn, name) {
		return false
	}
	return !ls.Contains(name)
}

func sameDType(ls, rs *schema.Schema, name string) bool {
	lf, ok1 := ls.FieldByName(name)
	rf, ok2 := rs.FieldByName(name)
	return ok1 && ok2 && lf.DType.Equal(rf.DType)
}

// pushFilterIntoJoin moves the conjuncts of a filter above a join into
// the join inputs where that keeps the result unchanged (see
// joinPushTargets). The rest stays above the join.
func pushFilterIntoJoin(filter Filter, j Join) (Node, bool) {
	ls, err := j.Left.Schema()
	if err != nil {
		return filter, false
	}
	rs, err := j.Right.Schema()
	if err != nil {
		return filter, false
	}
	spec := j.Spec()
	var toLeft, toRight, keep []expr.Expr
	for _, c := range splitAnd(filter.Predicate) {
		side := joinPushTargets(j, spec, ls, rs, c)
		if side&pushLeft != 0 {
			toLeft = append(toLeft, c)
		}
		if side&pushRight != 0 {
			toRight = append(toRight, c)
		}
		if side == 0 {
			keep = append(keep, c)
		}
	}
	if len(toLeft) == 0 && len(toRight) == 0 {
		return filter, false
	}
	nj := j
	if len(toLeft) > 0 {
		nj.Left = Filter{Input: j.Left, Predicate: joinAnd(toLeft)}
	}
	if len(toRight) > 0 {
		nj.Right = Filter{Input: j.Right, Predicate: joinAnd(toRight)}
	}
	if len(keep) == 0 {
		return nj, true
	}
	return Filter{Input: nj, Predicate: joinAnd(keep)}, true
}
