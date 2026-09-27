package expr

// list_ns.go completes the polars `list` namespace. Scalar arguments
// travel in FunctionNode.Params; other list columns (concat, the set
// operations) are extra Args so projection pushdown sees them. The
// inner expression of Eval and Filter is a Param, not an Arg: it runs
// against the exploded elements (see Element), not the frame.

// Element refers to the current element inside ListOps.Eval,
// ListOps.Filter and Expr.CumulativeEval. Mirrors polars
// `pl.element()`, which is the column with the empty name.
func Element() Expr { return Col("") }

// Median returns the median of each list.
func (o ListOps) Median() Expr { return fn1("list.median", o.e) }

// Std returns the standard deviation of each list (ddof delta degrees
// of freedom; polars defaults to 1).
func (o ListOps) Std(ddof int) Expr { return fn1p("list.std", o.e, ddof) }

// Var returns the variance of each list.
func (o ListOps) Var(ddof int) Expr { return fn1p("list.var", o.e, ddof) }

// NUnique counts distinct values per list (null counts as a value).
func (o ListOps) NUnique() Expr { return fn1("list.n_unique", o.e) }

// ArgMin returns the index of each list's minimum as UInt32.
func (o ListOps) ArgMin() Expr { return fn1("list.arg_min", o.e) }

// ArgMax returns the index of each list's maximum as UInt32.
func (o ListOps) ArgMax() Expr { return fn1("list.arg_max", o.e) }

// Any reports whether any element of a boolean list is true.
func (o ListOps) Any() Expr { return fn1("list.any", o.e) }

// All reports whether every non-null element of a boolean list is true.
func (o ListOps) All() Expr { return fn1("list.all", o.e) }

// GetWithOOB is Get with polars' null_on_oob switch: with nullOnOOB
// false an out-of-range index is an error.
func (o ListOps) GetWithOOB(idx int, nullOnOOB bool) Expr {
	return fn1p("list.get", o.e, idx, nullOnOOB)
}

// Item returns the single element of each list; lists with another
// length are an error. Polars `list.item`.
func (o ListOps) Item() Expr { return fn1("list.item", o.e) }

// CountMatches counts occurrences of v (nil counts nulls) per list.
func (o ListOps) CountMatches(v any) Expr { return fn1p("list.count_matches", o.e, v) }

// JoinWith joins string lists with sep; with ignoreNulls false a null
// element makes the row null. Polars `list.join(sep, ignore_nulls)`.
func (o ListOps) JoinWith(sep string, ignoreNulls bool) Expr {
	return fn1p("list.join", o.e, sep, ignoreNulls)
}

// DropNulls removes null elements from each list.
func (o ListOps) DropNulls() Expr { return fn1("list.drop_nulls", o.e) }

// Reverse reverses each list.
func (o ListOps) Reverse() Expr { return fn1("list.reverse", o.e) }

// Sort sorts each list (nulls first unless nullsLast).
func (o ListOps) Sort(descending, nullsLast bool) Expr {
	return fn1p("list.sort", o.e, descending, nullsLast)
}

// Unique removes duplicates from each list. Numeric and boolean lists
// come back sorted unless maintainOrder.
func (o ListOps) Unique(maintainOrder bool) Expr { return fn1p("list.unique", o.e, maintainOrder) }

// Head keeps the first n elements of each list.
func (o ListOps) Head(n int) Expr { return fn1p("list.head", o.e, n) }

// Tail keeps the last n elements of each list.
func (o ListOps) Tail(n int) Expr { return fn1p("list.tail", o.e, n) }

// Slice keeps elements [offset, offset+length) of each list; a negative
// offset counts from the end and length < 0 takes the rest.
func (o ListOps) Slice(offset, length int) Expr { return fn1p("list.slice", o.e, offset, length) }

// Shift shifts the elements of each list by n, filling with null.
func (o ListOps) Shift(n int) Expr { return fn1p("list.shift", o.e, n) }

// Diff computes x[i] - x[i-n] inside each list.
func (o ListOps) Diff(n int) Expr { return fn1p("list.diff", o.e, n) }

// Gather picks positions out of every list.
func (o ListOps) Gather(indices []int, nullOnOOB bool) Expr {
	return fn1p("list.gather", o.e, append([]int(nil), indices...), nullOnOOB)
}

// GatherEvery takes every n-th element starting at offset.
func (o ListOps) GatherEvery(n, offset int) Expr { return fn1p("list.gather_every", o.e, n, offset) }

// Explode flattens the lists into rows (empty and null lists give one
// null row each). The output length differs from the input.
func (o ListOps) Explode() Expr { return fn1("list.explode", o.e) }

// ToStruct converts each list into a struct. With names the fields are
// those names; otherwise field_0.. up to the first non-null list's
// length. Polars `list.to_struct`.
func (o ListOps) ToStruct(names ...string) Expr {
	var p []string
	if len(names) > 0 {
		p = append([]string(nil), names...)
	}
	return fn1p("list.to_struct", o.e, p, false)
}

// ToStructMaxWidth is ToStruct sized by the longest list
// (n_field_strategy="max_width").
func (o ListOps) ToStructMaxWidth() Expr {
	return fn1p("list.to_struct", o.e, []string(nil), true)
}

// ToArray converts each list to a fixed-size array of width.
func (o ListOps) ToArray(width int) Expr { return fn1p("list.to_array", o.e, width) }

// Concat appends the lists (or scalar values) of others row by row.
func (o ListOps) Concat(others ...Expr) Expr {
	return Expr{FunctionNode{Name: "list.concat", Args: append([]Expr{o.e}, others...)}}
}

func (o ListOps) setOp(name string, other Expr) Expr {
	return Expr{FunctionNode{Name: name, Args: []Expr{o.e, other}}}
}

// SetUnion is the per-row union with another list expression.
func (o ListOps) SetUnion(other Expr) Expr { return o.setOp("list.set_union", other) }

// SetIntersection is the per-row intersection.
func (o ListOps) SetIntersection(other Expr) Expr {
	return o.setOp("list.set_intersection", other)
}

// SetDifference keeps elements not present in other.
func (o ListOps) SetDifference(other Expr) Expr { return o.setOp("list.set_difference", other) }

// SetSymmetricDifference keeps elements present in exactly one side.
func (o ListOps) SetSymmetricDifference(other Expr) Expr {
	return o.setOp("list.set_symmetric_difference", other)
}

// Sample draws n elements from each list.
func (o ListOps) Sample(n int, withReplacement, shuffle bool, seed uint64) Expr {
	return fn1p("list.sample", o.e, n, withReplacement, shuffle, seed)
}

// SampleFraction draws fraction * len elements from each list.
func (o ListOps) SampleFraction(fraction float64, withReplacement, shuffle bool, seed uint64) Expr {
	return fn1p("list.sample_fraction", o.e, fraction, withReplacement, shuffle, seed)
}

// Eval runs e against the elements of every list and collects the
// results back into a list per row. Inside e, Element() is the current
// element. Element-wise expressions run once over all elements;
// anything else (aggregations, sorts, ranks) runs per list.
func (o ListOps) Eval(e Expr) Expr { return fn1p("list.eval", o.e, e) }

// Filter keeps the elements for which pred (written with Element())
// is true.
func (o ListOps) Filter(pred Expr) Expr { return fn1p("list.filter", o.e, pred) }
