package parquet

import "encoding/binary"

// thriftWriter appends thrift compact protocol values to a buffer.
type thriftWriter struct {
	b    []byte
	last []int16 // last field id per open struct
}

func (w *thriftWriter) field(id int16, typ byte) {
	top := &w.last[len(w.last)-1]
	if d := id - *top; d > 0 && d <= 15 {
		w.b = append(w.b, byte(d)<<4|typ)
	} else {
		w.b = append(w.b, typ)
		w.b = binary.AppendUvarint(w.b, uint64(int64(id)<<1^int64(id)>>15))
	}
	*top = id
}

func (w *thriftWriter) i32(id int16, v int32) {
	w.field(id, tI32)
	w.b = binary.AppendUvarint(w.b, uint64(uint32(v<<1^v>>31)))
}

func (w *thriftWriter) boolean(id int16, v bool) {
	if v {
		w.field(id, tBoolTrue)
	} else {
		w.field(id, tBoolFalse)
	}
}

func (w *thriftWriter) begin(id int16) {
	w.field(id, tStruct)
	w.last = append(w.last, 0)
}

func (w *thriftWriter) end() {
	w.b = append(w.b, 0)
	w.last = w.last[:len(w.last)-1]
}

// appendDataPageHeader appends a v1 data page header whose definition
// and repetition levels are RLE encoded.
func appendDataPageHeader(b []byte, uncompressed, compressed, numValues int, enc int32) []byte {
	w := thriftWriter{b: b, last: make([]int16, 1, 4)}
	w.i32(1, pageData)
	w.i32(2, int32(uncompressed))
	w.i32(3, int32(compressed))
	w.begin(5)
	w.i32(1, int32(numValues))
	w.i32(2, enc)
	w.i32(3, encRLE)
	w.i32(4, encRLE)
	w.end()
	w.b = append(w.b, 0)
	return w.b
}

// appendDictPageHeader appends a PLAIN dictionary page header.
func appendDictPageHeader(b []byte, uncompressed, compressed, numValues int) []byte {
	w := thriftWriter{b: b, last: make([]int16, 1, 4)}
	w.i32(1, pageDictionary)
	w.i32(2, int32(uncompressed))
	w.i32(3, int32(compressed))
	w.begin(7)
	w.i32(1, int32(numValues))
	w.i32(2, encPlain)
	w.boolean(3, false)
	w.end()
	w.b = append(w.b, 0)
	return w.b
}
