package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// BinOps is the expression-level mirror of series.BinOps: the
// namespace of binary-column operations reached via Expr.Bin().
// Mirrors polars' `pl.col("x").bin.*`.
type BinOps struct{ e Expr }

// Bin opens the binary namespace.
func (e Expr) Bin() BinOps { return BinOps{e: e} }

// Contains tests each value for the byte sequence needle.
func (o BinOps) Contains(needle []byte) Expr { return fn1p("bin.contains", o.e, needle) }

// StartsWith tests each value for the byte prefix.
func (o BinOps) StartsWith(prefix []byte) Expr { return fn1p("bin.starts_with", o.e, prefix) }

// EndsWith tests each value for the byte suffix.
func (o BinOps) EndsWith(suffix []byte) Expr { return fn1p("bin.ends_with", o.e, suffix) }

// Encode renders each value as text using "hex" or "base64". Output
// dtype is String.
func (o BinOps) Encode(encoding string) Expr { return fn1p("bin.encode", o.e, encoding) }

// Decode parses each value as "hex" or "base64" text and returns the
// raw bytes. With strict, invalid input is an error; otherwise it
// becomes null.
func (o BinOps) Decode(encoding string, strict bool) Expr {
	return fn1p("bin.decode", o.e, encoding, strict)
}

// Size returns the byte length of each value. unit is "b" (UInt32
// output) or "kb", "mb", "gb", "tb" (Float64 output, powers of 1024).
func (o BinOps) Size(unit string) Expr { return fn1p("bin.size", o.e, unit) }

// Get returns the byte at index idx (negative counts from the end) as
// UInt8. Out-of-range indices are an error unless nullOnOOB is set.
func (o BinOps) Get(idx int, nullOnOOB bool) Expr {
	return fn1p("bin.get", o.e, idx, nullOnOOB)
}

// Head takes the first n bytes of each value (negative n: all but the
// last |n|).
func (o BinOps) Head(n int) Expr { return fn1p("bin.head", o.e, n) }

// Tail takes the last n bytes of each value (negative n: all but the
// first |n|).
func (o BinOps) Tail(n int) Expr { return fn1p("bin.tail", o.e, n) }

// Slice takes length bytes from offset (negative offset counts from
// the end). A negative length means "to the end", like polars'
// length=None.
func (o BinOps) Slice(offset, length int) Expr {
	return fn1p("bin.slice", o.e, offset, length)
}

// Reinterpret reads each value as one fixed-width number of dtype to
// (integers and floats). Values of the wrong byte length become null.
func (o BinOps) Reinterpret(to dtype.DType, littleEndian bool) Expr {
	return fn1p("bin.reinterpret", o.e, to, littleEndian)
}
