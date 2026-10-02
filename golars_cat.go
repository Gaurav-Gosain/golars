package golars

import "github.com/Gaurav-Gosain/golars/dtype"

// Categorical and Enum dtype helpers, matching polars' pl.Categorical
// and pl.Enum(categories).
var (
	CategoricalDType = dtype.Categorical
	EnumDType        = dtype.Enum
)
