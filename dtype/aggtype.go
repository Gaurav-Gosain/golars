package dtype

import "github.com/apache/arrow-go/v18/arrow"

// AggResultDType returns the dtype polars gives the aggregation op
// ("sum", "min", "max", "mean", "median", "std", "var", "product",
// "quantile", "first", "last") of a numeric or bool input. ok is false
// for other inputs, which keep their own rules.
//
//   - min, max, first, last keep the input dtype;
//   - sum widens i8, i16, u8 and u16 to i64 and counts bools as u32;
//   - product is i64 for integers narrower than 64 bits and bool;
//   - mean, median, std, var and quantile are f64, except f32 stays f32
//     (and quantile of a bool stays bool).
func AggResultDType(op string, in DType) (DType, bool) {
	if !in.IsNumeric() && !in.IsBool() {
		return DType{}, false
	}
	switch op {
	case "min", "max", "first", "last":
		return in, true
	case "sum":
		switch in.ID() {
		case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16:
			return Int64(), true
		case arrow.BOOL:
			return Uint32(), true
		}
		return in, true
	case "product":
		switch in.ID() {
		case arrow.INT64, arrow.UINT64, arrow.FLOAT32, arrow.FLOAT64:
			return in, true
		}
		return Int64(), true
	case "mean", "median", "std", "var", "quantile":
		if in.ID() == arrow.FLOAT32 {
			return in, true
		}
		if op == "quantile" && in.IsBool() {
			return in, true
		}
		return Float64(), true
	}
	return DType{}, false
}
