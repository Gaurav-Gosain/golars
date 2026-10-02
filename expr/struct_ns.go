package expr

// struct_ns.go completes the polars `struct` namespace.

// Field refers to a field of the struct being updated inside
// StructOps.WithFields. Mirrors polars `pl.field(name)`.
func Field(name string) Expr {
	return Expr{FunctionNode{Name: "struct.field_ref", Params: []any{name}}}
}

// Fields projects several fields at once; in a select or with_columns
// it expands into one output column per field. Polars
// `struct.field(*names)`.
func (o StructOps) Fields(names ...string) Expr {
	p := make([]any, len(names))
	for i, n := range names {
		p[i] = n
	}
	return Expr{FunctionNode{Name: "struct.field", Args: []Expr{o.e}, Params: p}}
}

// RenameFields renames the struct fields positionally. Polars
// `struct.rename_fields(names)`.
func (o StructOps) RenameFields(names ...string) Expr {
	return fn1p("struct.rename_fields", o.e, append([]string(nil), names...))
}

// WithFields adds or replaces struct fields. Inside exprs, Field(name)
// refers to the struct's current fields and Col(name) to frame
// columns; each result is stored under its output name. Polars
// `struct.with_fields(*exprs)`.
func (o StructOps) WithFields(exprs ...Expr) Expr {
	return Expr{FunctionNode{Name: "struct.with_fields", Args: append([]Expr{o.e}, exprs...)}}
}

// Unnest expands the struct into one output column per field. It is
// resolved when the expression enters a lazy plan (select or
// with_columns). Polars `struct.unnest()`.
func (o StructOps) Unnest() Expr { return fn1("struct.unnest", o.e) }

// OutputName returns the output column name of the expression rooted
// at f (see the package-level OutputName).
func (f FunctionNode) OutputName() string { return OutputName(Expr{f}) }
