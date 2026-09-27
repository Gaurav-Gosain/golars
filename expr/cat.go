package expr

// CatOps is the expression-level categorical namespace, reached via
// Expr.Cat(). It mirrors polars' `col("x").cat.*` and works on both
// Categorical and Enum columns. Every name is prefixed with "cat." so
// the evaluator can route it.
type CatOps struct{ e Expr }

// Cat opens the categorical namespace.
//
// # Examples
//
//	expr.Col("color").Cat().GetCategories()
//	expr.Col("color").Cat().StartsWith("bl")
func (e Expr) Cat() CatOps { return CatOps{e: e} }

// GetCategories returns the categories as a String column. Its length
// is the number of categories, not the frame height.
func (o CatOps) GetCategories() Expr { return fn1("cat.get_categories", o.e) }

// LenBytes returns the byte length of each value as UInt32.
func (o CatOps) LenBytes() Expr { return fn1("cat.len_bytes", o.e) }

// LenChars returns the character count of each value as UInt32.
func (o CatOps) LenChars() Expr { return fn1("cat.len_chars", o.e) }

// StartsWith tests each value for a literal prefix.
func (o CatOps) StartsWith(prefix string) Expr { return fn1p("cat.starts_with", o.e, prefix) }

// EndsWith tests each value for a literal suffix.
func (o CatOps) EndsWith(suffix string) Expr { return fn1p("cat.ends_with", o.e, suffix) }

// Slice returns a character slice of each value as a String column.
// A negative offset counts from the end; a negative length means to
// the end of the string (polars' length=None).
func (o CatOps) Slice(offset, length int) Expr {
	return fn1p("cat.slice", o.e, offset, length)
}
