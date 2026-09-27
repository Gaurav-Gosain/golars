package dataframe

import (
	"context"
	"math"
	"slices"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// joinCodes numbers the key tuples of both join sides in one dense code
// space: two rows share a code exactly when their keys are equal, so
// the join itself only compares small integers. A code of -1 marks a
// row that can never match (a null key while nulls are not equal).
type joinCodes struct {
	left, right []int32
	card        int
	// all backs left and right; it goes back to int32Scratch.
	all []int32
	// rawNulls holds one side's codes before null keys were set to -1
	// (the left side when rawNullsLeft), kept only when asked for:
	// full-join validation needs it.
	rawNulls     []int32
	rawNullsLeft bool
}

func (c *joinCodes) release() {
	int32Scratch.put(c.all)
	c.all, c.left, c.right, c.rawNulls = nil, nil, nil, nil
}

// encodeJoinKeys assigns joint codes to the key columns. Each key pair
// is normalized to one physical type, the two sides are concatenated,
// and each column is dictionary encoded with the group-by key encoders
// (keycodes.go, strcodes.go). Several columns are folded into one code
// per row with the mixed-radix combine of assignGroupsFromCodes; only
// when that overflows, or for nested keys, does it fall back to hashing
// a byte encoding of the whole tuple.
func encodeJoinKeys(ctx context.Context, lkeys, rkeys []*series.Series, types joinKeyTypes, nullsEqual bool, keepNulls int) (joinCodes, error) {
	nl, nr := lkeys[0].Len(), rkeys[0].Len()
	n := nl + nr
	cats := make([]arrow.Array, len(lkeys))
	defer func() {
		for _, a := range cats {
			if a != nil {
				a.Release()
			}
		}
	}()
	for i := range lkeys {
		a, err := concatJoinKey(ctx, lkeys[i], rkeys[i], types[i])
		if err != nil {
			return joinCodes{}, err
		}
		cats[i] = a
	}

	all := int32Scratch.get(n)
	card, ok := codesFromEncoders(cats, n, all)
	if !ok {
		card = codesFromTuples(cats, n, all)
	}
	var rawNulls []int32
	if !nullsEqual {
		switch keepNulls {
		case keepNullsLeft:
			rawNulls = slices.Clone(all[:nl])
		case keepNullsRight:
			rawNulls = slices.Clone(all[nl:])
		}
		for _, a := range cats {
			markNullCodes(a, all)
		}
	}
	return joinCodes{left: all[:nl:nl], right: all[nl:], card: card, all: all,
		rawNulls: rawNulls, rawNullsLeft: keepNulls == keepNullsLeft}, nil
}

// keepNulls values for encodeJoinKeys.
const (
	keepNullsNone = iota
	keepNullsLeft
	keepNullsRight
)

// concatJoinKey returns left and right keys as one array in a physical
// type the encoders understand.
func concatJoinKey(ctx context.Context, l, r *series.Series, t dtype.DType) (arrow.Array, error) {
	la, err := normalizeJoinKey(ctx, l, t)
	if err != nil {
		return nil, err
	}
	defer la.Release()
	ra, err := normalizeJoinKey(ctx, r, t)
	if err != nil {
		return nil, err
	}
	defer ra.Release()
	return array.Concatenate([]arrow.Array{la, ra}, memory.DefaultAllocator)
}

// normalizeJoinKey casts s to the comparison dtype t and then to a
// physical encoding: categoricals decode to strings, small integers
// widen to int64, floats become canonical int64 bit patterns (all NaNs
// equal, -0.0 equal to 0.0), and temporal types are viewed as their
// integer storage.
func normalizeJoinKey(ctx context.Context, s *series.Series, t dtype.DType) (arrow.Array, error) {
	if !s.DType().Equal(t) {
		c, err := compute.Cast(ctx, s, t)
		if err != nil {
			return nil, err
		}
		defer c.Release()
		s = c
	}
	a, err := s.Consolidated()
	if err != nil {
		return nil, err
	}
	switch a.DataType().ID() {
	case arrow.DICTIONARY:
		defer a.Release()
		return catDecodeArray(a)
	case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16, arrow.UINT32:
		defer a.Release()
		return widenToInt64(a), nil
	case arrow.FLOAT32, arrow.FLOAT64:
		defer a.Release()
		return joinFloatKeyBits(a), nil
	case arrow.TIMESTAMP, arrow.DURATION, arrow.TIME64, arrow.DATE64, arrow.UINT64:
		defer a.Release()
		return reinterpretArray(a, arrow.PrimitiveTypes.Int64), nil
	case arrow.DATE32, arrow.TIME32:
		defer a.Release()
		return reinterpretArray(a, arrow.PrimitiveTypes.Int32), nil
	}
	return a, nil
}

// reinterpretArray views a fixed-width array as another type of the
// same width, sharing its buffers.
func reinterpretArray(a arrow.Array, to arrow.DataType) arrow.Array {
	d := a.Data()
	nd := array.NewData(to, d.Len(), d.Buffers(), nil, d.NullN(), d.Offset())
	defer nd.Release()
	return array.MakeFromData(nd)
}

// newInt64Array builds an int64 array from vals and the validity of
// src (which must have the same length).
func newInt64Array(vals []int64, src arrow.Array) arrow.Array {
	n := len(vals)
	var valid *memory.Buffer
	nulls := src.NullN()
	if nulls > 0 {
		valid = memory.NewResizableBuffer(memory.DefaultAllocator)
		valid.Resize(int(bitutil.BytesForBits(int64(n))))
		bits := valid.Bytes()
		clear(bits)
		for i := range n {
			if src.IsValid(i) {
				bitutil.SetBit(bits, i)
			}
		}
	}
	buf := memory.NewBufferBytes(int64sAsBytes(vals))
	d := array.NewData(arrow.PrimitiveTypes.Int64, n, []*memory.Buffer{valid, buf}, nil, nulls, 0)
	defer d.Release()
	if valid != nil {
		valid.Release()
	}
	return array.NewInt64Data(d)
}

func widenToInt64(a arrow.Array) arrow.Array {
	vals := make([]int64, a.Len())
	switch t := a.(type) {
	case *array.Int8:
		for i, v := range t.Int8Values() {
			vals[i] = int64(v)
		}
	case *array.Int16:
		for i, v := range t.Int16Values() {
			vals[i] = int64(v)
		}
	case *array.Uint8:
		for i, v := range t.Uint8Values() {
			vals[i] = int64(v)
		}
	case *array.Uint16:
		for i, v := range t.Uint16Values() {
			vals[i] = int64(v)
		}
	case *array.Uint32:
		for i, v := range t.Uint32Values() {
			vals[i] = int64(v)
		}
	}
	return newInt64Array(vals, a)
}

func joinFloatKeyBits(a arrow.Array) arrow.Array {
	vals := make([]int64, a.Len())
	canon := func(v float64) int64 {
		switch {
		case v != v:
			return 0x7ff8000000000000
		case v == 0:
			return 0
		}
		return int64(math.Float64bits(v))
	}
	switch t := a.(type) {
	case *array.Float32:
		for i, v := range t.Float32Values() {
			vals[i] = canon(float64(v))
		}
	case *array.Float64:
		for i, v := range t.Float64Values() {
			vals[i] = canon(v)
		}
	}
	return newInt64Array(vals, a)
}

// codesFromEncoders encodes each column with the typed group-by
// encoders and combines them into out. ok=false means a column type has
// no typed encoder or the combined key space overflowed.
func codesFromEncoders(cols []arrow.Array, n int, out []int32) (int, bool) {
	kcs := make([]keyCodes, len(cols))
	defer func() {
		for _, kc := range kcs {
			int32Scratch.put(kc.codes)
		}
	}()
	for i, a := range cols {
		switch t := a.(type) {
		case *array.Int64:
			kcs[i] = encodeInt64Codes(t)
		case *array.Int32:
			kcs[i] = encodeInt32Codes(t)
		case *array.String:
			kcs[i] = encodeStringCodes(t)
		case *array.Boolean:
			kcs[i] = encodeBoolCodes(t)
		case *array.Null:
			codes := int32Scratch.get(n)
			clear(codes)
			var first []int32
			if n > 0 {
				first = []int32{0}
			}
			kcs[i] = keyCodes{codes: codes, firstRows: first}
		default:
			return 0, false
		}
	}
	if len(kcs) == 1 {
		copy(out, kcs[0].codes)
		return kcs[0].card(), true
	}
	ids, first, ok := assignGroupsFromCodes(kcs, n)
	if !ok {
		return 0, false
	}
	for i, g := range ids {
		out[i] = int32(g)
	}
	intScratch.put(ids)
	return len(first), true
}

// codesFromTuples numbers rows by a byte encoding of the whole key
// tuple. It handles every dtype, including nested keys.
func codesFromTuples(cols []arrow.Array, n int, out []int32) int {
	ss := make([]*series.Series, len(cols))
	for i, a := range cols {
		a.Retain()
		// New only fails on an empty chunk list or mixed chunk types.
		ss[i], _ = series.New("", a)
	}
	defer func() {
		for _, s := range ss {
			s.Release()
		}
	}()
	ids, first := rowGroupIDs(ss, n)
	for i, g := range ids {
		out[i] = int32(g)
	}
	return len(first)
}

// int64sAsBytes views vals as bytes without copying.
func int64sAsBytes(vals []int64) []byte {
	if len(vals) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), len(vals)*8)
}

// markNullCodes sets the code of every row where a is null to -1.
func markNullCodes(a arrow.Array, codes []int32) {
	if a.DataType().ID() == arrow.NULL {
		for i := range codes {
			codes[i] = -1
		}
		return
	}
	if a.NullN() == 0 {
		return
	}
	for i := range codes {
		if a.IsNull(i) {
			codes[i] = -1
		}
	}
}
