package dataframe

import (
	"encoding/binary"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// rowKeyCol is one column prepared for row-key encoding.
type rowKeyCol struct {
	arr      arrow.Array
	kind     uint8 // see rowKey* constants
	width    int   // byte width for fixed-width kinds
	values   []byte
	offset   int
	hasNulls bool
	f32      []float32
	f64      []float64
	bytesAt  func(int) []byte
}

const (
	rkFixed uint8 = iota
	rkF32
	rkF64
	rkBool
	rkBytes
	rkNull
	rkOther
)

// rowKeyEncoder turns a row of several columns into a byte string that
// is equal for two rows exactly when polars considers the rows equal for
// grouping and deduplication: nulls equal nulls, all NaNs are equal and
// -0.0 equals +0.0.
type rowKeyEncoder struct {
	cols []rowKeyCol
}

func newRowKeyEncoder(cols []*series.Series) *rowKeyEncoder {
	e := &rowKeyEncoder{cols: make([]rowKeyCol, len(cols))}
	for i, s := range cols {
		a := firstChunk(s)
		c := rowKeyCol{arr: a, hasNulls: a.NullN() > 0, offset: a.Data().Offset()}
		switch t := a.DataType().(type) {
		case *arrow.NullType:
			c.kind = rkNull
		case *arrow.BooleanType:
			c.kind = rkBool
			c.values = a.Data().Buffers()[1].Bytes()
		case *arrow.Float32Type:
			c.kind = rkF32
			c.f32 = rawValues[float32](a)
		case *arrow.Float64Type:
			c.kind = rkF64
			c.f64 = rawValues[float64](a)
		case arrow.FixedWidthDataType:
			if t.BitWidth()%8 == 0 && t.BitWidth() > 0 && len(a.Data().Buffers()) > 1 && a.Data().Buffers()[1] != nil {
				c.kind = rkFixed
				c.width = t.BitWidth() / 8
				c.values = a.Data().Buffers()[1].Bytes()
			} else {
				c.kind = rkOther
			}
		default:
			switch v := a.(type) {
			case *array.String:
				c.kind = rkBytes
				c.bytesAt = func(i int) []byte { return unsafeStringBytes(v.Value(i)) }
			case *array.LargeString:
				c.kind = rkBytes
				c.bytesAt = func(i int) []byte { return unsafeStringBytes(v.Value(i)) }
			case *array.Binary:
				c.kind = rkBytes
				c.bytesAt = v.Value
			case *array.LargeBinary:
				c.kind = rkBytes
				c.bytesAt = v.Value
			default:
				c.kind = rkOther
			}
		}
		e.cols[i] = c
	}
	return e
}

// append appends the key of row to buf and returns the extended slice.
func (e *rowKeyEncoder) append(buf []byte, row int) []byte {
	for i := range e.cols {
		c := &e.cols[i]
		if c.kind == rkNull || (c.hasNulls && c.arr.IsNull(row)) {
			buf = append(buf, 0)
			continue
		}
		buf = append(buf, 1)
		switch c.kind {
		case rkFixed:
			s := (c.offset + row) * c.width
			buf = append(buf, c.values[s:s+c.width]...)
		case rkF32:
			v := c.f32[row]
			var bits uint32
			switch {
			case v != v:
				bits = 0x7fc00000
			case v == 0:
				bits = 0
			default:
				bits = math.Float32bits(v)
			}
			buf = binary.LittleEndian.AppendUint32(buf, bits)
		case rkF64:
			v := c.f64[row]
			var bits uint64
			switch {
			case v != v:
				bits = 0x7ff8000000000000
			case v == 0:
				bits = 0
			default:
				bits = math.Float64bits(v)
			}
			buf = binary.LittleEndian.AppendUint64(buf, bits)
		case rkBool:
			if bitutil.BitIsSet(c.values, c.offset+row) {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case rkBytes:
			b := c.bytesAt(row)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
			buf = append(buf, b...)
		default:
			s := c.arr.ValueStr(row)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)))
			buf = append(buf, s...)
		}
	}
	return buf
}

// rowGroupIDs assigns every row of cols a dense id in first-appearance
// order. Equal rows (per rowKeyEncoder) share an id. It returns the ids
// and the first row index of each id. Hashable dtypes go through the
// group-by assignment engine; nested dtypes use rowKeyEncoder.
func rowGroupIDs(cols []*series.Series, n int) (ids []int, first []int) {
	if len(cols) == 0 {
		ids = make([]int, n)
		if n > 0 {
			first = []int{0}
		}
		return ids, first
	}
	for _, c := range cols {
		if c.NumChunks() > 1 {
			// Rows index the whole column: make the keys contiguous.
			one := make([]*series.Series, len(cols))
			for i, k := range cols {
				one[i] = k.Rechunk()
			}
			defer releaseAll(one)
			cols = one
			break
		}
	}
	arrs := make([]arrow.Array, len(cols))
	fast := true
	for i, c := range cols {
		arrs[i] = firstChunk(c)
		if !encodableKey(arrs[i].DataType()) {
			fast = false
		}
	}
	if fast {
		if gids, ng, err := assignGroupIDs(arrs, n, memory.DefaultAllocator); err == nil {
			first = make([]int, ng)
			seen := make([]bool, ng)
			for r, g := range gids {
				if !seen[g] {
					seen[g] = true
					first[g] = r
				}
			}
			if gids == nil {
				gids = []int{}
			}
			return gids, first
		}
	}
	ids = make([]int, n)
	enc := newRowKeyEncoder(cols)
	table := make(map[string]int, 64)
	buf := make([]byte, 0, 64)
	for r := range n {
		buf = enc.append(buf[:0], r)
		if id, ok := table[string(buf)]; ok {
			ids[r] = id
			continue
		}
		id := len(first)
		table[string(buf)] = id
		first = append(first, r)
		ids[r] = id
	}
	return ids, first
}
