package expr

// ArrOps is the expression-level `arr` namespace for fixed-size list
// (polars Array) columns. Reach it via Expr.Arr().
type ArrOps struct{ e Expr }

// Arr opens the array namespace, mirroring polars `col("x").arr.*`.
func (e Expr) Arr() ArrOps { return ArrOps{e: e} }

// Len returns the width of each array as UInt32.
func (o ArrOps) Len() Expr { return fn1("arr.len", o.e) }

// Sum returns the sum of each array.
func (o ArrOps) Sum() Expr { return fn1("arr.sum", o.e) }

// Mean returns the mean of each array.
func (o ArrOps) Mean() Expr { return fn1("arr.mean", o.e) }

// Median returns the median of each array.
func (o ArrOps) Median() Expr { return fn1("arr.median", o.e) }

// Std returns the standard deviation of each array.
func (o ArrOps) Std(ddof int) Expr { return fn1p("arr.std", o.e, ddof) }

// Var returns the variance of each array.
func (o ArrOps) Var(ddof int) Expr { return fn1p("arr.var", o.e, ddof) }

// Min returns the minimum of each array.
func (o ArrOps) Min() Expr { return fn1("arr.min", o.e) }

// Max returns the maximum of each array.
func (o ArrOps) Max() Expr { return fn1("arr.max", o.e) }

// ArgMin returns the index of each array's minimum.
func (o ArrOps) ArgMin() Expr { return fn1("arr.arg_min", o.e) }

// ArgMax returns the index of each array's maximum.
func (o ArrOps) ArgMax() Expr { return fn1("arr.arg_max", o.e) }

// NUnique counts distinct values per array.
func (o ArrOps) NUnique() Expr { return fn1("arr.n_unique", o.e) }

// Any reports whether any element of a boolean array is true.
func (o ArrOps) Any() Expr { return fn1("arr.any", o.e) }

// All reports whether every non-null element of a boolean array is true.
func (o ArrOps) All() Expr { return fn1("arr.all", o.e) }

// Get returns element idx of each array; out-of-range is null with
// nullOnOOB and an error otherwise.
func (o ArrOps) Get(idx int, nullOnOOB bool) Expr { return fn1p("arr.get", o.e, idx, nullOnOOB) }

// First returns the first element of each array.
func (o ArrOps) First() Expr { return fn1("arr.first", o.e) }

// Last returns the last element of each array.
func (o ArrOps) Last() Expr { return fn1("arr.last", o.e) }

// Contains reports whether each array holds v (nil looks for null).
func (o ArrOps) Contains(v any) Expr { return fn1p("arr.contains", o.e, v) }

// CountMatches counts occurrences of v per array.
func (o ArrOps) CountMatches(v any) Expr { return fn1p("arr.count_matches", o.e, v) }

// Join joins string arrays with sep.
func (o ArrOps) Join(sep string, ignoreNulls bool) Expr {
	return fn1p("arr.join", o.e, sep, ignoreNulls)
}

// Reverse reverses each array.
func (o ArrOps) Reverse() Expr { return fn1("arr.reverse", o.e) }

// Sort sorts each array.
func (o ArrOps) Sort(descending, nullsLast bool) Expr {
	return fn1p("arr.sort", o.e, descending, nullsLast)
}

// Shift shifts each array by n, filling with null.
func (o ArrOps) Shift(n int) Expr { return fn1p("arr.shift", o.e, n) }

// Unique returns the distinct values of each array as a List.
func (o ArrOps) Unique(maintainOrder bool) Expr { return fn1p("arr.unique", o.e, maintainOrder) }

// Head returns the first n elements of each array as a List.
func (o ArrOps) Head(n int) Expr { return fn1p("arr.head", o.e, n) }

// Tail returns the last n elements of each array as a List.
func (o ArrOps) Tail(n int) Expr { return fn1p("arr.tail", o.e, n) }

// Slice returns [offset, offset+length) of each array as a List
// (length < 0 takes the rest).
func (o ArrOps) Slice(offset, length int) Expr { return fn1p("arr.slice", o.e, offset, length) }

// Explode flattens the arrays into rows.
func (o ArrOps) Explode() Expr { return fn1("arr.explode", o.e) }

// ToList converts the column to a List.
func (o ArrOps) ToList() Expr { return fn1("arr.to_list", o.e) }

// ToStruct converts each array to a struct (field_0.. or names).
func (o ArrOps) ToStruct(names ...string) Expr {
	var p []string
	if len(names) > 0 {
		p = append([]string(nil), names...)
	}
	return fn1p("arr.to_struct", o.e, p)
}
