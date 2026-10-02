package parquet

import (
	"encoding/binary"
	"errors"
	"math"
)

// This file holds a minimal reader and writer for the thrift compact
// protocol, enough to parse and emit parquet page headers. arrow-go keeps
// its generated thrift types internal, and the native reader and writer
// handle pages directly.

var errThrift = errors.New("parquet: corrupt page header")

// Compact protocol type ids.
const (
	tBoolTrue  = 1
	tBoolFalse = 2
	tByte      = 3
	tI16       = 4
	tI32       = 5
	tI64       = 6
	tDouble    = 7
	tBinary    = 8
	tList      = 9
	tSet       = 10
	tMap       = 11
	tStruct    = 12
)

// Parquet page types.
const (
	pageData       = 0
	pageIndex      = 1
	pageDictionary = 2
	pageDataV2     = 3
)

// Parquet encodings (the thrift enum values).
const (
	encPlain          = 0
	encPlainDict      = 2
	encRLE            = 3
	encBitPacked      = 4
	encDeltaBinary    = 5
	encDeltaLength    = 6
	encDeltaByteArray = 7
	encRLEDict        = 8
	encByteStreamSplt = 9
)

// pageHeader is the subset of the thrift PageHeader the native reader
// needs. Fields of the data, dictionary and v2 headers are flattened.
type pageHeader struct {
	typ          int32
	uncompressed int32
	compressed   int32
	numValues    int32
	encoding     int32
	defEncoding  int32
	repEncoding  int32
	numNulls     int32
	numRows      int32
	defLen       int32
	repLen       int32
	v2Compressed bool
}

type thriftReader struct {
	b   []byte
	pos int
	err error
}

func (r *thriftReader) byte() byte {
	if r.pos >= len(r.b) {
		r.err = errThrift
		return 0
	}
	c := r.b[r.pos]
	r.pos++
	return c
}

func (r *thriftReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b[min(r.pos, len(r.b)):])
	if n <= 0 {
		r.err = errThrift
		return 0
	}
	r.pos += n
	return v
}

func (r *thriftReader) varint() int64 {
	u := r.uvarint()
	return int64(u>>1) ^ -int64(u&1)
}

func (r *thriftReader) i32() int32 {
	v := r.varint()
	if v < math.MinInt32 || v > math.MaxInt32 {
		r.err = errThrift
	}
	return int32(v)
}

// fieldHeader returns the next field id and type, or type 0 at the end
// of a struct.
func (r *thriftReader) fieldHeader(last int16) (int16, byte) {
	c := r.byte()
	if c == 0 || r.err != nil {
		return 0, 0
	}
	typ := c & 0x0f
	if d := c >> 4; d != 0 {
		return last + int16(d), typ
	}
	return int16(r.varint()), typ
}

// skip consumes one value of the given type.
func (r *thriftReader) skip(typ byte, depth int) {
	if depth > 32 {
		r.err = errThrift
		return
	}
	switch typ {
	case tBoolTrue, tBoolFalse:
	case tByte:
		r.byte()
	case tI16, tI32, tI64:
		r.uvarint()
	case tDouble:
		r.pos += 8
	case tBinary:
		n := r.uvarint()
		if n > uint64(len(r.b)) {
			r.err = errThrift
			return
		}
		r.pos += int(n)
	case tList, tSet:
		c := r.byte()
		n := uint64(c >> 4)
		if n == 15 {
			n = r.uvarint()
		}
		et := c & 0x0f
		for i := uint64(0); i < n && r.err == nil; i++ {
			if et == tBoolTrue || et == tBoolFalse {
				r.byte()
				continue
			}
			r.skip(et, depth+1)
		}
	case tMap:
		n := r.uvarint()
		if n == 0 {
			break
		}
		kv := r.byte()
		for i := uint64(0); i < n && r.err == nil; i++ {
			r.skip(kv>>4, depth+1)
			r.skip(kv&0x0f, depth+1)
		}
	case tStruct:
		var last int16
		for r.err == nil {
			id, t := r.fieldHeader(last)
			if t == 0 {
				return
			}
			r.skip(t, depth+1)
			last = id
		}
	default:
		r.err = errThrift
	}
	if r.pos > len(r.b) {
		r.err = errThrift
	}
}

// readPageHeader parses a PageHeader at the start of b and returns it
// with the number of bytes it occupies.
func readPageHeader(b []byte) (pageHeader, int, error) {
	r := thriftReader{b: b}
	h := pageHeader{v2Compressed: true, typ: -1}
	var last int16
	for r.err == nil {
		id, t := r.fieldHeader(last)
		if t == 0 {
			break
		}
		switch {
		case id == 1 && t == tI32:
			h.typ = r.i32()
		case id == 2 && t == tI32:
			h.uncompressed = r.i32()
		case id == 3 && t == tI32:
			h.compressed = r.i32()
		case id == 5 && t == tStruct:
			r.dataPageHeader(&h)
		case id == 7 && t == tStruct:
			r.dictPageHeader(&h)
		case id == 8 && t == tStruct:
			r.dataPageHeaderV2(&h)
		default:
			r.skip(t, 0)
		}
		last = id
	}
	if r.err != nil {
		return h, 0, r.err
	}
	if h.typ < 0 || h.uncompressed < 0 || h.compressed < 0 || h.numValues < 0 || h.defLen < 0 || h.repLen < 0 {
		return h, 0, errThrift
	}
	return h, r.pos, nil
}

func (r *thriftReader) dataPageHeader(h *pageHeader) {
	var last int16
	for r.err == nil {
		id, t := r.fieldHeader(last)
		if t == 0 {
			return
		}
		switch {
		case id == 1 && t == tI32:
			h.numValues = r.i32()
		case id == 2 && t == tI32:
			h.encoding = r.i32()
		case id == 3 && t == tI32:
			h.defEncoding = r.i32()
		case id == 4 && t == tI32:
			h.repEncoding = r.i32()
		default:
			r.skip(t, 1)
		}
		last = id
	}
}

func (r *thriftReader) dictPageHeader(h *pageHeader) {
	var last int16
	for r.err == nil {
		id, t := r.fieldHeader(last)
		if t == 0 {
			return
		}
		switch {
		case id == 1 && t == tI32:
			h.numValues = r.i32()
		case id == 2 && t == tI32:
			h.encoding = r.i32()
		default:
			r.skip(t, 1)
		}
		last = id
	}
}

func (r *thriftReader) dataPageHeaderV2(h *pageHeader) {
	var last int16
	for r.err == nil {
		id, t := r.fieldHeader(last)
		if t == 0 {
			return
		}
		switch {
		case id == 1 && t == tI32:
			h.numValues = r.i32()
		case id == 2 && t == tI32:
			h.numNulls = r.i32()
		case id == 3 && t == tI32:
			h.numRows = r.i32()
		case id == 4 && t == tI32:
			h.encoding = r.i32()
		case id == 5 && t == tI32:
			h.defLen = r.i32()
		case id == 6 && t == tI32:
			h.repLen = r.i32()
		case id == 7 && (t == tBoolTrue || t == tBoolFalse):
			h.v2Compressed = t == tBoolTrue
		default:
			r.skip(t, 1)
		}
		last = id
	}
}
