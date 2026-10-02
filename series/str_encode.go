package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// strView returns the backing String chunk of o plus a zero-copy
// byte view. The caller must Release the returned array.
func (o StrOps) strView(op string, opts []Option) (arrow.Array, binView, error) {
	cfg := resolve(opts)
	if !o.s.DType().IsString() {
		return nil, binView{}, o.unsupported(op)
	}
	a := binChunk(o.s, cfg.alloc)
	v, ok := binViewOf(a)
	if !ok {
		a.Release()
		return nil, binView{}, fmt.Errorf("series.Str.%s: unsupported array type %T", op, a)
	}
	return a, v, nil
}

// Encode renders the UTF-8 bytes of each string with the given
// transfer encoding ("hex" or "base64"). Output dtype is String.
// Mirrors polars' str.encode.
func (o StrOps) Encode(encoding string, opts ...Option) (*Series, error) {
	a, v, err := o.strView("Encode", opts)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binEncode(o.s.Name(), v, encoding, resolve(opts).alloc)
}

// Decode parses each string in the given transfer encoding ("hex" or
// "base64") and returns the raw bytes as a Binary Series. With strict
// set, invalid input is an error; otherwise invalid rows become null.
// Mirrors polars' str.decode.
func (o StrOps) Decode(encoding string, strict bool, opts ...Option) (*Series, error) {
	a, v, err := o.strView("Decode", opts)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binDecode(o.s.Name(), v, encoding, strict, resolve(opts).alloc)
}
