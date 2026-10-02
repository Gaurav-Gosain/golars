package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// Encode renders the UTF-8 bytes of each string as "hex" or "base64"
// text. Mirrors polars' str.encode.
func (o StrOps) Encode(encoding string) Expr { return fn1p("str.encode", o.e, encoding) }

// Decode parses each string as "hex" or "base64" and returns the raw
// bytes as a Binary column. With strict, invalid input is an error;
// otherwise it becomes null. Mirrors polars' str.decode.
func (o StrOps) Decode(encoding string, strict bool) Expr {
	return fn1p("str.decode", o.e, encoding, strict)
}

// JSONDecode parses each string as JSON and converts it to dtype to.
// Mirrors polars' str.json_decode(dtype).
//
// # Examples
//
//	expr.Col("payload").Str().JSONDecode(dtype.Struct(
//	    dtype.StructField{Name: "id", DType: dtype.Int64()},
//	    dtype.StructField{Name: "tags", DType: dtype.List(dtype.String())},
//	))
func (o StrOps) JSONDecode(to dtype.DType) Expr { return fn1p("str.json_decode", o.e, to) }

// JSONPathMatch extracts the first value matching a JSONPath such as
// "$.a.b[0]" as a string. Mirrors polars' str.json_path_match.
func (o StrOps) JSONPathMatch(path string) Expr {
	return fn1p("str.json_path_match", o.e, path)
}

// JSONEncode serialises each struct row to a JSON object string.
// Mirrors polars' struct.json_encode.
func (o StructOps) JSONEncode() Expr { return fn1("struct.json_encode", o.e) }
