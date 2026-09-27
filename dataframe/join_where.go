package dataframe

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// JoinWhere joins df with right on arbitrary comparison predicates, the
// polars join_where operation (an inequality or range join).
//
// Each predicate is a comparison (Lt, Le, Gt, Ge, Eq, Ne) whose operands
// are column references or literals; predicates combined with And are
// split. Column names resolve against the output schema: every left
// column keeps its name, and a right column whose name collides with a
// left one is referenced (and emitted) with the join suffix, "_right" by
// default (see WithJoinSuffix). So Col("x") means the left x and
// Col("x_right") the right x when both sides have an x.
//
// The output has every left column followed by every right column. Rows
// with a null in any compared column never match. Floats compare with a
// total order in which NaN equals NaN and is greater than every number.
// Like polars, JoinWhere does not guarantee any output row order.
//
// Algorithm: equality predicates are hash joined; otherwise two
// inequalities run through IEJoin (a sweep over the first inequality with
// a bitmap over the second) and a single inequality through a sort and
// binary search. Remaining predicates filter the candidate pairs. Only
// predicates made of != alone fall back to scanning the cross product.
func (df *DataFrame) JoinWhere(ctx context.Context, right *DataFrame, predicates []expr.Expr, opts ...JoinOption) (*DataFrame, error) {
	cfg := resolveJoin(opts)
	if len(predicates) == 0 {
		return nil, fmt.Errorf("dataframe.JoinWhere: expected join keys/predicates")
	}
	jw := &joinWhere{left: df, right: right, suffix: cfg.suffix, views: map[jwColRef]*cmpCol{}}
	if err := jw.resolveNames(); err != nil {
		return nil, err
	}
	var flat []expr.Expr
	for _, p := range predicates {
		flat = splitAnd(p, flat)
	}
	for _, p := range flat {
		if err := jw.addPredicate(p); err != nil {
			return nil, err
		}
	}

	var li, ri []int
	if !jw.alwaysFalse {
		lrows := jw.candidates(0)
		rrows := jw.candidates(1)
		li, ri = jw.pairs(ctx, lrows, rrows)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return jw.buildOutput(ctx, li, ri, compute.PoolingMem(cfg.alloc))
}

type jwColRef struct {
	side int // 0 left, 1 right
	idx  int
}

type jwOperand struct {
	isLit bool
	ref   jwColRef
	lit   *cmpCol
}

// jwPred is one comparison. For cross predicates a is on the left frame
// and b on the right frame.
type jwPred struct {
	a, b *cmpCol
	op   expr.BinaryOp
}

type jwSidePred struct {
	a, b       *cmpCol
	aLit, bLit bool
	op         expr.BinaryOp
}

type joinWhere struct {
	left, right *DataFrame
	suffix      string
	names       map[string]jwColRef
	views       map[jwColRef]*cmpCol
	alwaysFalse bool

	sideFilters [2][]jwSidePred
	// nonNull lists the columns that must be non-null on each side
	// because a cross predicate reads them.
	nonNull [2][]*cmpCol
	eq      []jwPred
	ineq    []jwPred
	ne      []jwPred
}

func (jw *joinWhere) resolveNames() error {
	jw.names = make(map[string]jwColRef, jw.left.Width()+jw.right.Width())
	for i, c := range jw.left.cols {
		jw.names[c.Name()] = jwColRef{0, i}
	}
	for i, c := range jw.right.cols {
		name := c.Name()
		if jw.left.sch.Contains(name) {
			name += jw.suffix
		}
		if _, dup := jw.names[name]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateColumn, name)
		}
		jw.names[name] = jwColRef{1, i}
	}
	return nil
}

func splitAnd(e expr.Expr, out []expr.Expr) []expr.Expr {
	if b, ok := e.Node().(expr.BinaryNode); ok && b.Op == expr.OpAnd {
		out = splitAnd(b.Left, out)
		return splitAnd(b.Right, out)
	}
	return append(out, e)
}

func (jw *joinWhere) view(ref jwColRef) (*cmpCol, error) {
	if v, ok := jw.views[ref]; ok {
		return v, nil
	}
	frame := jw.left
	if ref.side == 1 {
		frame = jw.right
	}
	c, err := newCmpCol(frame.cols[ref.idx])
	if err != nil {
		return nil, fmt.Errorf("dataframe.JoinWhere: %w", err)
	}
	jw.views[ref] = &c
	return &c, nil
}

func (jw *joinWhere) operand(e expr.Expr) (jwOperand, error) {
	for {
		a, ok := e.Node().(expr.AliasNode)
		if !ok {
			break
		}
		e = a.Inner
	}
	switch n := e.Node().(type) {
	case expr.ColNode:
		ref, ok := jw.names[n.Name]
		if !ok {
			return jwOperand{}, fmt.Errorf("%w: unable to find column %q in join_where predicate", ErrColumnNotFound, n.Name)
		}
		return jwOperand{ref: ref}, nil
	case expr.LitNode:
		if n.Value == nil {
			return jwOperand{isLit: true}, nil
		}
		c, err := litCmpCol(n.Value)
		if err != nil {
			return jwOperand{}, err
		}
		return jwOperand{isLit: true, lit: &c}, nil
	}
	return jwOperand{}, fmt.Errorf("dataframe.JoinWhere: predicate operand %s must be a column or literal", e)
}

func isCompare(op expr.BinaryOp) bool {
	switch op {
	case expr.OpLt, expr.OpLe, expr.OpGt, expr.OpGe, expr.OpEq, expr.OpNe:
		return true
	}
	return false
}

// flipOp mirrors a comparison so that `a op b` equals `b flipOp(op) a`.
func flipOp(op expr.BinaryOp) expr.BinaryOp {
	switch op {
	case expr.OpLt:
		return expr.OpGt
	case expr.OpLe:
		return expr.OpGe
	case expr.OpGt:
		return expr.OpLt
	case expr.OpGe:
		return expr.OpLe
	}
	return op
}

func checkComparable(a, b *cmpCol) error {
	if (a.kind == cmpStr) != (b.kind == cmpStr) {
		return fmt.Errorf("'join_where' cannot compare %s with %s", kindName(a), kindName(b))
	}
	if a.dt != nil && b.dt != nil && a.kind != b.kind {
		return fmt.Errorf("'join_where' cannot compare %s with %s", kindName(a), kindName(b))
	}
	return nil
}

func kindName(c *cmpCol) string {
	if c.dt != nil {
		return c.dt.String()
	}
	switch c.kind {
	case cmpFloat:
		return "float literal"
	case cmpStr:
		return "string literal"
	}
	return "integer literal"
}

func (jw *joinWhere) addPredicate(p expr.Expr) error {
	b, ok := p.Node().(expr.BinaryNode)
	if !ok || !isCompare(b.Op) {
		return fmt.Errorf("dataframe.JoinWhere: predicate %s must be a comparison (<, <=, >, >=, ==, !=)", p)
	}
	x, err := jw.operand(b.Left)
	if err != nil {
		return err
	}
	y, err := jw.operand(b.Right)
	if err != nil {
		return err
	}
	// A null literal never compares true.
	if (x.isLit && x.lit == nil) || (y.isLit && y.lit == nil) {
		jw.alwaysFalse = true
		return nil
	}
	resolve := func(o jwOperand) (*cmpCol, error) {
		if o.isLit {
			return o.lit, nil
		}
		return jw.view(o.ref)
	}
	xa, err := resolve(x)
	if err != nil {
		return err
	}
	ya, err := resolve(y)
	if err != nil {
		return err
	}
	if err := checkComparable(xa, ya); err != nil {
		return err
	}
	switch {
	case x.isLit && y.isLit:
		if !compareOp(compareTotal(xa, 0, ya, 0), b.Op) {
			jw.alwaysFalse = true
		}
	case x.isLit || y.isLit || x.ref.side == y.ref.side:
		side := x.ref.side
		if x.isLit {
			side = y.ref.side
		}
		jw.sideFilters[side] = append(jw.sideFilters[side], jwSidePred{a: xa, b: ya, aLit: x.isLit, bLit: y.isLit, op: b.Op})
	default:
		op := b.Op
		if x.ref.side == 1 {
			xa, ya = ya, xa
			op = flipOp(op)
		}
		jw.nonNull[0] = append(jw.nonNull[0], xa)
		jw.nonNull[1] = append(jw.nonNull[1], ya)
		pr := jwPred{a: xa, b: ya, op: op}
		switch op {
		case expr.OpEq:
			jw.eq = append(jw.eq, pr)
		case expr.OpNe:
			jw.ne = append(jw.ne, pr)
		default:
			jw.ineq = append(jw.ineq, pr)
		}
	}
	return nil
}

func compareOp(c int, op expr.BinaryOp) bool {
	switch op {
	case expr.OpLt:
		return c < 0
	case expr.OpLe:
		return c <= 0
	case expr.OpGt:
		return c > 0
	case expr.OpGe:
		return c >= 0
	case expr.OpEq:
		return c == 0
	}
	return c != 0
}

// candidates returns the rows of one side that pass the single-side
// filters and have no null in any cross-predicate column.
func (jw *joinWhere) candidates(side int) []int {
	frame := jw.left
	if side == 1 {
		frame = jw.right
	}
	n := frame.Height()
	out := make([]int, 0, n)
rows:
	for i := range n {
		for _, c := range jw.nonNull[side] {
			if c.isNull(i) {
				continue rows
			}
		}
		for _, f := range jw.sideFilters[side] {
			ai, bi := i, i
			if f.aLit {
				ai = 0
			} else if f.a.isNull(i) {
				continue rows
			}
			if f.bLit {
				bi = 0
			} else if f.b.isNull(i) {
				continue rows
			}
			if !compareOp(compareTotal(f.a, ai, f.b, bi), f.op) {
				continue rows
			}
		}
		out = append(out, i)
	}
	return out
}

// residual holds the cross predicates a pair generator did not enforce.
type residual []jwPred

func (r residual) ok(i, j int) bool {
	for k := range r {
		p := &r[k]
		if !compareOp(compareTotal(p.a, i, p.b, j), p.op) {
			return false
		}
	}
	return true
}

// jwParallelMin is the work size at which pair generation fans out.
const jwParallelMin = 32 * 1024

func jwWorkers() int { return min(runtime.GOMAXPROCS(0), 8) }

func (jw *joinWhere) pairs(ctx context.Context, lrows, rrows []int) ([]int, []int) {
	if len(lrows) == 0 || len(rrows) == 0 {
		return nil, nil
	}
	switch {
	case len(jw.eq) > 0:
		rest := slices.Concat(jw.eq[1:], jw.ineq, jw.ne)
		return jw.hashPairs(ctx, lrows, rrows, jw.eq[0:1], rest)
	case len(jw.ineq) >= 2:
		rest := slices.Concat(jw.ineq[2:], jw.ne)
		return ieJoin(ctx, lrows, rrows, jw.ineq[0], jw.ineq[1], rest)
	case len(jw.ineq) == 1:
		return sortPairs(ctx, lrows, rrows, jw.ineq[0], jw.ne)
	}
	return crossPairs(ctx, lrows, rrows, jw.ne)
}

// runRowChunks splits [0, n) into worker ranges when n is large and
// concatenates each worker's pair output in range order.
func runRowChunks(ctx context.Context, n int, work func(lo, hi int) ([]int, []int)) ([]int, []int) {
	if n < jwParallelMin {
		return work(0, n)
	}
	w := jwWorkers()
	chunk := (n + w - 1) / w
	type part struct{ l, r []int }
	parts := make([]part, 0, w)
	for lo := 0; lo < n; lo += chunk {
		parts = append(parts, part{})
	}
	var wg sync.WaitGroup
	for k := range parts {
		lo := k * chunk
		hi := min(lo+chunk, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			parts[k].l, parts[k].r = work(lo, hi)
		}()
	}
	wg.Wait()
	total := 0
	for _, p := range parts {
		total += len(p.l)
	}
	li := make([]int, 0, total)
	ri := make([]int, 0, total)
	for _, p := range parts {
		li = append(li, p.l...)
		ri = append(ri, p.r...)
	}
	return li, ri
}

func crossPairs(ctx context.Context, lrows, rrows []int, rest residual) ([]int, []int) {
	return runRowChunks(ctx, len(lrows), func(lo, hi int) ([]int, []int) {
		var li, ri []int
		for _, i := range lrows[lo:hi] {
			for _, j := range rrows {
				if rest.ok(i, j) {
					li = append(li, i)
					ri = append(ri, j)
				}
			}
		}
		return li, ri
	})
}

// jointOrdKeys maps a.values at arows and b.values at brows to int64
// keys whose order and equality match compareTotal across both sides.
// Integers map to themselves, floats to an order-preserving bit pattern
// (NaN greatest, -0 equal to +0) and strings to dense ranks over the
// union of both sides. Sorting and searching plain int64 keys avoids a
// comparator call per comparison.
func jointOrdKeys(a *cmpCol, arows []int, b *cmpCol, brows []int) ([]int64, []int64) {
	if a.kind == cmpStr {
		return stringRanks(a, arows, b, brows)
	}
	return ordKeys(a, arows), ordKeys(b, brows)
}

func ordKeys(c *cmpCol, rows []int) []int64 {
	out := make([]int64, len(rows))
	if c.kind == cmpInt {
		for k, i := range rows {
			out[k] = c.ints[i]
		}
		return out
	}
	for k, i := range rows {
		out[k] = floatOrdKey(c.floatAt(i))
	}
	return out
}

// floatOrdKey maps a float to an int64 with the same total order.
func floatOrdKey(f float64) int64 {
	if f != f {
		return math.MaxInt64
	}
	if f == 0 {
		return 0
	}
	u := math.Float64bits(f)
	if u>>63 != 0 {
		return int64(u ^ 0x7fffffffffffffff)
	}
	return int64(u)
}

func stringRanks(a *cmpCol, arows []int, b *cmpCol, brows []int) ([]int64, []int64) {
	type ref struct {
		s    string
		side bool
		k    int
	}
	all := make([]ref, 0, len(arows)+len(brows))
	for k, i := range arows {
		all = append(all, ref{a.strs.Value(i), false, k})
	}
	for k, j := range brows {
		all = append(all, ref{b.strs.Value(j), true, k})
	}
	slices.SortFunc(all, func(x, y ref) int { return strings.Compare(x.s, y.s) })
	ka := make([]int64, len(arows))
	kb := make([]int64, len(brows))
	rank := int64(-1)
	for n, r := range all {
		if n == 0 || r.s != all[n-1].s {
			rank++
		}
		if r.side {
			kb[r.k] = rank
		} else {
			ka[r.k] = rank
		}
	}
	return ka, kb
}

// lowerBound returns the first position with keys[p] >= x and
// upperBound the first with keys[p] > x.
func lowerBound(keys []int64, x int64) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if keys[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBound(keys []int64, x int64) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if keys[mid] <= x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// sortedByKey returns positions 0..len(keys)-1 ordered by key plus the
// keys in that order. Equal keys keep their input order.
func sortedByKey(keys []int64) ([]int32, []int64) {
	pos := make([]int32, len(keys))
	for p := range pos {
		pos[p] = int32(p)
	}
	sk := slices.Clone(keys)
	radixSortKeyed(sk, pos)
	return pos, sk
}

// radixSortKeyed stably sorts keys ascending and applies the same
// permutation to payload. It is an LSD radix sort over 8-bit digits;
// digits that are identical across every key are skipped, so narrow key
// ranges take only a few passes.
func radixSortKeyed(keys []int64, payload []int32) {
	n := len(keys)
	if n < 2 {
		return
	}
	if n <= 64 {
		// Insertion sort is cheaper for tiny inputs and is stable.
		for i := 1; i < n; i++ {
			k, p := keys[i], payload[i]
			j := i
			for j > 0 && keys[j-1] > k {
				keys[j], payload[j] = keys[j-1], payload[j-1]
				j--
			}
			keys[j], payload[j] = k, p
		}
		return
	}
	const flip = uint64(1) << 63
	var and, or uint64 = ^uint64(0), 0
	for _, k := range keys {
		u := uint64(k) ^ flip
		and &= u
		or |= u
	}
	diff := and ^ or
	ktmp := make([]int64, n)
	ptmp := make([]int32, n)
	src, dst := keys, ktmp
	psrc, pdst := payload, ptmp
	var count [256]int
	for shift := uint(0); shift < 64; shift += 8 {
		if (diff>>shift)&0xff == 0 {
			continue
		}
		count = [256]int{}
		for _, k := range src {
			count[((uint64(k)^flip)>>shift)&0xff]++
		}
		sum := 0
		for d := range count {
			c := count[d]
			count[d] = sum
			sum += c
		}
		for i, k := range src {
			d := ((uint64(k) ^ flip) >> shift) & 0xff
			dst[count[d]] = k
			pdst[count[d]] = psrc[i]
			count[d]++
		}
		src, dst = dst, src
		psrc, pdst = pdst, psrc
	}
	if &src[0] != &keys[0] {
		copy(keys, src)
		copy(payload, psrc)
	}
}

// hashPairs joins on the first equality predicate, then filters rest.
func (jw *joinWhere) hashPairs(ctx context.Context, lrows, rrows []int, eq []jwPred, rest residual) ([]int, []int) {
	p := eq[0]
	lk, rk := jointOrdKeys(p.a, lrows, p.b, rrows)
	heads := make(map[int64]int32, len(rrows))
	next := make([]int32, len(rrows))
	for q := len(rrows) - 1; q >= 0; q-- {
		h, ok := heads[rk[q]]
		if !ok {
			h = -1
		}
		next[q] = h
		heads[rk[q]] = int32(q)
	}
	return runRowChunks(ctx, len(lrows), func(lo, hi int) ([]int, []int) {
		var li, ri []int
		for k := lo; k < hi; k++ {
			h, ok := heads[lk[k]]
			if !ok {
				continue
			}
			i := lrows[k]
			for q := h; q >= 0; q = next[q] {
				j := rrows[q]
				if len(rest) == 0 || rest.ok(i, j) {
					li = append(li, i)
					ri = append(ri, j)
				}
			}
		}
		return li, ri
	})
}

// sortPairs handles one inequality l.a op r.b: sort the right candidates
// by b, then each left row selects a contiguous range by binary search.
func sortPairs(ctx context.Context, lrows, rrows []int, p jwPred, rest residual) ([]int, []int) {
	la, rb := jointOrdKeys(p.a, lrows, p.b, rrows)
	rpos, rkeys := sortedByKey(rb)
	return runRowChunks(ctx, len(lrows), func(lo, hi int) ([]int, []int) {
		var li, ri []int
		for k := lo; k < hi; k++ {
			x := la[k]
			var from, to int
			switch p.op {
			case expr.OpLt:
				from, to = upperBound(rkeys, x), len(rkeys)
			case expr.OpLe:
				from, to = lowerBound(rkeys, x), len(rkeys)
			case expr.OpGt:
				from, to = 0, lowerBound(rkeys, x)
			case expr.OpGe:
				from, to = 0, upperBound(rkeys, x)
			}
			i := lrows[k]
			for _, q := range rpos[from:to] {
				j := rrows[q]
				if len(rest) == 0 || rest.ok(i, j) {
					li = append(li, i)
					ri = append(ri, j)
				}
			}
		}
		return li, ri
	})
}

// ieRightBit marks sweep entries that refer to right candidates; the
// remaining bits are the position in the candidate list.
const ieRightBit = int32(-1 << 31)

// ieJoin evaluates l.a op1 r.b AND l.c op2 r.d without a nested loop.
//
// Rows of both sides are swept in the order that makes op1 a prefix
// condition: a left row is inserted into a bitmap when the sweep passes
// it, and every right row then sees exactly the inserted left rows that
// satisfy op1. The bitmap is indexed by each left row's position in c
// order, so op2 becomes a contiguous position range found by binary
// search. A second bitmap level (one bit per 64-bit word) skips empty
// stretches. The sweep is split across workers, each rebuilding the
// bitmap state for its starting point.
func ieJoin(ctx context.Context, lrows, rrows []int, p1, p2 jwPred, rest residual) ([]int, []int) {
	nl := len(lrows)
	desc := p1.op == expr.OpGt || p1.op == expr.OpGe
	strict := p1.op == expr.OpLt || p1.op == expr.OpGt
	la, rb := jointOrdKeys(p1.a, lrows, p1.b, rrows)
	lc, rd := jointOrdKeys(p2.a, lrows, p2.b, rrows)

	// Ties: for a strict op1 the right row must come first so equal left
	// rows are not yet inserted; for a non-strict op1 the left row comes
	// first. The radix sort is stable, so ties keep this input order.
	total := nl + len(rrows)
	sweepKeys := make([]int64, 0, total)
	sweep := make([]int32, 0, total) // right rows carry ieRightBit
	addLeft := func() {
		for k, v := range la {
			if desc {
				v = ^v
			}
			sweepKeys = append(sweepKeys, v)
			sweep = append(sweep, int32(k))
		}
	}
	addRight := func() {
		for q, v := range rb {
			if desc {
				v = ^v
			}
			sweepKeys = append(sweepKeys, v)
			sweep = append(sweep, int32(q)|ieRightBit)
		}
	}
	if strict {
		addRight()
		addLeft()
	} else {
		addLeft()
		addRight()
	}
	radixSortKeyed(sweepKeys, sweep)

	cOrder, cKeys := sortedByKey(lc)
	slotOf := make([]int32, nl)
	for s, k := range cOrder {
		slotOf[k] = int32(s)
	}

	work := func(lo, hi int) ([]int, []int) {
		bm := newBitmap2(nl)
		for _, it := range sweep[:lo] {
			if it&ieRightBit == 0 {
				bm.set(int(slotOf[it]))
			}
		}
		var li, ri []int
		for _, it := range sweep[lo:hi] {
			if it&ieRightBit == 0 {
				bm.set(int(slotOf[it]))
				continue
			}
			q := it &^ ieRightBit
			d := rd[q]
			var from, to int
			switch p2.op {
			case expr.OpLt:
				from, to = 0, lowerBound(cKeys, d)
			case expr.OpLe:
				from, to = 0, upperBound(cKeys, d)
			case expr.OpGt:
				from, to = upperBound(cKeys, d), nl
			case expr.OpGe:
				from, to = lowerBound(cKeys, d), nl
			}
			if from >= to {
				continue
			}
			j := rrows[q]
			// Walk the set bits in [from, to), skipping zero words via
			// the summary level.
			wlo, whi := from>>6, (to-1)>>6
			for sw := wlo >> 6; sw <= whi>>6; sw++ {
				s := bm.summary[sw]
				base := sw << 6
				if base < wlo {
					s &= ^uint64(0) << uint(wlo-base)
				}
				if base+63 > whi {
					s &= ^uint64(0) >> uint(63-(whi-base))
				}
				for s != 0 {
					w := base + bits.TrailingZeros64(s)
					s &= s - 1
					word := bm.words[w]
					if w == wlo {
						word &= ^uint64(0) << (uint(from) & 63)
					}
					if w == whi {
						word &= ^uint64(0) >> (63 - (uint(to-1) & 63))
					}
					for word != 0 {
						slot := w<<6 + bits.TrailingZeros64(word)
						word &= word - 1
						i := lrows[cOrder[slot]]
						if len(rest) == 0 || rest.ok(i, j) {
							li = append(li, i)
							ri = append(ri, j)
						}
					}
				}
			}
		}
		return li, ri
	}
	return runRowChunks(ctx, len(sweep), work)
}

// bitmap2 is a two-level bitset: words holds the bits and summary holds
// one bit per non-zero word so sparse ranges are skipped quickly.
type bitmap2 struct {
	words   []uint64
	summary []uint64
}

func newBitmap2(n int) *bitmap2 {
	nw := (n + 63) >> 6
	return &bitmap2{words: make([]uint64, nw), summary: make([]uint64, (nw+63)>>6)}
}

func (b *bitmap2) set(i int) {
	w := i >> 6
	b.words[w] |= 1 << (uint(i) & 63)
	b.summary[w>>6] |= 1 << (uint(w) & 63)
}

func (jw *joinWhere) buildOutput(ctx context.Context, li, ri []int, mem memory.Allocator) (*DataFrame, error) {
	if li == nil {
		li, ri = []int{}, []int{}
	}
	out := make([]*series.Series, 0, jw.left.Width()+jw.right.Width())
	release := func() {
		for _, c := range out {
			c.Release()
		}
	}
	for _, c := range jw.left.cols {
		g, err := takeOptional(ctx, c, li, mem)
		if err != nil {
			release()
			return nil, err
		}
		out = append(out, g)
	}
	for _, c := range jw.right.cols {
		g, err := takeOptional(ctx, c, ri, mem)
		if err != nil {
			release()
			return nil, err
		}
		if jw.left.sch.Contains(c.Name()) {
			r := g.Rename(c.Name() + jw.suffix)
			g.Release()
			g = r
		}
		out = append(out, g)
	}
	res, err := New(out...)
	if err != nil {
		release()
		return nil, err
	}
	return res, nil
}
