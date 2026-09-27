package expr

// scalarLit builds the literal for the *Lit sugar methods, which
// mirror polars' python operators with a scalar (col + 1, col * 2.5):
// every Go integer or float is an untyped literal there and takes the
// dtype of the column it meets.
func scalarLit(v any) Expr {
	switch x := v.(type) {
	case int:
		return LitInt(int64(x))
	case int8:
		return LitInt(int64(x))
	case int16:
		return LitInt(int64(x))
	case int32:
		return LitInt(int64(x))
	case int64:
		return LitInt(x)
	case uint8:
		return LitInt(int64(x))
	case uint16:
		return LitInt(int64(x))
	case uint32:
		return LitInt(int64(x))
	case float32:
		return LitFloat(float64(x))
	case float64:
		return LitFloat(x)
	}
	return Lit(v)
}
