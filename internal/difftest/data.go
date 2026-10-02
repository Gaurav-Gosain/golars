package difftest

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Column is a generated input column. Values use one Go representation
// per kind: int64 (KInt and every temporal kind, in physical units),
// uint64 (KUint), float64 (KFloat, already rounded to float32 for
// f32), string (KStr, KCat), bool (KBool), []any (KList elements and
// KStruct fields in field order). nil is a null.
type Column struct {
	Name   string
	T      Type
	Values []any
	// Chunks holds the chunk boundaries (exclusive end offsets, the last
	// one equal to len(Values)). Empty means a single chunk.
	Chunks []int
}

// Frame is a generated input frame.
type Frame struct {
	Cols []Column
}

// Height is the row count.
func (f *Frame) Height() int {
	if len(f.Cols) == 0 {
		return 0
	}
	return len(f.Cols[0].Values)
}

// Col returns the column named name, or nil.
func (f *Frame) Col(name string) *Column {
	for i := range f.Cols {
		if f.Cols[i].Name == name {
			return &f.Cols[i]
		}
	}
	return nil
}

// Clone deep-copies the frame's slices (values are immutable).
func (f *Frame) Clone() *Frame {
	out := &Frame{Cols: make([]Column, len(f.Cols))}
	for i, c := range f.Cols {
		out.Cols[i] = Column{Name: c.Name, T: c.T, Values: slices.Clone(c.Values), Chunks: slices.Clone(c.Chunks)}
	}
	return out
}

// KeepRows returns a copy restricted to the rows where keep is true.
// Chunking is recomputed proportionally.
func (f *Frame) KeepRows(keep []bool) *Frame {
	out := &Frame{Cols: make([]Column, len(f.Cols))}
	for i, c := range f.Cols {
		vals := []any{}
		prefix := make([]int, len(c.Values)+1)
		for r, v := range c.Values {
			prefix[r+1] = prefix[r]
			if keep[r] {
				vals = append(vals, v)
				prefix[r+1]++
			}
		}
		var chunks []int
		for _, b := range c.Chunks {
			nb := prefix[b]
			if nb > 0 && (len(chunks) == 0 || chunks[len(chunks)-1] != nb) {
				chunks = append(chunks, nb)
			}
		}
		if len(chunks) <= 1 {
			chunks = nil
		}
		out.Cols[i] = Column{Name: c.Name, T: c.T, Values: vals, Chunks: chunks}
	}
	return out
}

// ---------------------------------------------------------------
// Building golars frames.

// ToDataFrame builds a golars DataFrame holding the frame's values.
func (f *Frame) ToDataFrame(mem memory.Allocator) (*dataframe.DataFrame, error) {
	cols := make([]*series.Series, 0, len(f.Cols))
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}
	for _, c := range f.Cols {
		s, err := c.ToSeries(mem)
		if err != nil {
			release()
			return nil, err
		}
		cols = append(cols, s)
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		release()
		return nil, err
	}
	return df, nil
}

// ToSeries builds the column as a (possibly chunked) golars Series.
func (c *Column) ToSeries(mem memory.Allocator) (*series.Series, error) {
	bounds := c.Chunks
	if len(bounds) == 0 {
		bounds = []int{len(c.Values)}
	}
	var chunks []arrow.Array
	start := 0
	for _, end := range bounds {
		arr, err := buildArray(mem, c.T, c.Values[start:end])
		if err != nil {
			for _, a := range chunks {
				a.Release()
			}
			return nil, err
		}
		chunks = append(chunks, arr)
		start = end
	}
	s, err := series.New(c.Name, chunks...)
	if err != nil {
		for _, a := range chunks {
			a.Release()
		}
		return nil, err
	}
	return s, nil
}

func buildArray(mem memory.Allocator, t Type, vals []any) (arrow.Array, error) {
	if t.K == KCat {
		return buildCat(mem, vals)
	}
	b := array.NewBuilder(mem, t.Arrow())
	defer b.Release()
	for _, v := range vals {
		if err := appendValue(b, t, v); err != nil {
			return nil, err
		}
	}
	return b.NewArray(), nil
}

// buildCat builds a dictionary array whose dictionary holds the
// categories in order of first appearance, which is how golars and
// polars both lay out a Categorical.
func buildCat(mem memory.Allocator, vals []any) (arrow.Array, error) {
	idx := array.NewUint32Builder(mem)
	defer idx.Release()
	dict := array.NewStringBuilder(mem)
	defer dict.Release()
	seen := map[string]uint32{}
	for _, v := range vals {
		if v == nil {
			idx.AppendNull()
			continue
		}
		s := v.(string)
		code, ok := seen[s]
		if !ok {
			code = uint32(len(seen))
			seen[s] = code
			dict.Append(s)
		}
		idx.Append(code)
	}
	ia := idx.NewArray()
	defer ia.Release()
	da := dict.NewArray()
	defer da.Release()
	return array.NewDictionaryArray(TCat.Arrow(), ia, da), nil
}

func appendValue(b array.Builder, t Type, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		bb.Append(v.(bool))
	case *array.Int8Builder:
		bb.Append(int8(v.(int64)))
	case *array.Int16Builder:
		bb.Append(int16(v.(int64)))
	case *array.Int32Builder:
		bb.Append(int32(v.(int64)))
	case *array.Int64Builder:
		bb.Append(v.(int64))
	case *array.Uint8Builder:
		bb.Append(uint8(v.(uint64)))
	case *array.Uint16Builder:
		bb.Append(uint16(v.(uint64)))
	case *array.Uint32Builder:
		bb.Append(uint32(v.(uint64)))
	case *array.Uint64Builder:
		bb.Append(v.(uint64))
	case *array.Float32Builder:
		bb.Append(float32(v.(float64)))
	case *array.Float64Builder:
		bb.Append(v.(float64))
	case *array.StringBuilder:
		bb.Append(v.(string))
	case *array.Date32Builder:
		bb.Append(arrow.Date32(v.(int64)))
	case *array.Time64Builder:
		bb.Append(arrow.Time64(v.(int64)))
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(v.(int64)))
	case *array.DurationBuilder:
		bb.Append(arrow.Duration(v.(int64)))
	case *array.ListBuilder:
		bb.Append(true)
		vb := bb.ValueBuilder()
		for _, e := range v.([]any) {
			if err := appendValue(vb, *t.Elem, e); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		bb.Append(true)
		fv := v.([]any)
		for i, f := range t.Fields {
			if err := appendValue(bb.FieldBuilder(i), f.T, fv[i]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("difftest: no builder for %s", t.Name())
	}
	return nil
}

// ---------------------------------------------------------------
// Value generation.

// Sizes are the row counts cases draw from. Small sizes dominate; the
// large ones sit on the parallel cutoffs used by golars kernels.
var smallSizes = []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 255, 256, 257, 1000, 1023, 1024, 1025}
var largeSizes = []int{4095, 4096, 4097, 32767, 32768, 32769, 65535, 65536, 65537, 131071, 131072, 131073, 262143, 262144, 262145}

var strPool = []string{
	"", "a", "A", "b", "B", "ab", "abc", "ABC", "aBc", "  pad  ", " x", "y ", "a,b", "a,b,c", ",", "a.b",
	"123", "-4", "0", "1.5", "007", "2024-01-02", "1999-12-31", "2024-02-29 13:45:00", "12:30:00",
	"é", "é", "ß", "straße", "日本語", "Αβγ", "\U00010348x",
	"hello world", "Hello World", "foo_bar", "FOO-BAR", "tab\tsep", "new\nline", "quote\"d", "back\\slash",
	"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "aaa", "aa", "zz", "z",
}

var catPool = []string{"red", "green", "blue", "", "Red", "grün", "x"}

var tzPool = []string{"", "", "UTC", "Asia/Kolkata", "America/New_York", "Europe/London", "Australia/Lord_Howe"}

// Gen draws random values. The zero value is not usable; use NewGen.
type Gen struct {
	R *rand.Rand
}

// NewGen seeds a generator.
func NewGen(seed uint64) *Gen {
	return &Gen{R: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

func (g *Gen) Intn(n int) int { return g.R.IntN(n) }
func (g *Gen) Chance(p float64) bool {
	return g.R.Float64() < p
}

func pick[T any](g *Gen, xs []T) T { return xs[g.R.IntN(len(xs))] }

// profile controls the value distribution of one column.
type profile struct {
	nullFrac float64
	small    bool    // draw from a small domain so keys collide
	special  float64 // probability of an edge value
}

func (g *Gen) profile() profile {
	p := profile{}
	switch g.Intn(8) {
	case 0, 1, 2:
		p.nullFrac = 0
	case 3, 4:
		p.nullFrac = 0.1
	case 5:
		p.nullFrac = 0.5
	case 6:
		p.nullFrac = 0.02
	case 7:
		if g.Chance(0.3) {
			p.nullFrac = 1
		} else {
			p.nullFrac = 0.3
		}
	}
	p.small = g.Chance(0.6)
	switch g.Intn(4) {
	case 0:
		p.special = 0
	case 1, 2:
		p.special = 0.05
	case 3:
		p.special = 0.3
	}
	return p
}

// RandomType draws a column type. depth limits nesting.
func (g *Gen) RandomType(depth int) Type {
	for {
		switch g.Intn(22) {
		case 0, 1, 2:
			return TI64
		case 3:
			return pick(g, intTypes)
		case 4:
			return pick(g, []Type{TI8, TI16, TI32})
		case 5:
			return pick(g, []Type{TU8, TU16, TU32, TU64})
		case 6, 7:
			return TF64
		case 8:
			return TF32
		case 9, 10:
			return TStr
		case 11, 12:
			return TBool
		case 13:
			return TDate
		case 14:
			return Datetime(pick(g, []string{"ms", "us", "ns"}), "")
		case 15:
			return Datetime(pick(g, []string{"ms", "us", "ns"}), pick(g, tzPool))
		case 16:
			return Duration(pick(g, []string{"ms", "us", "ns"}))
		case 17:
			return TTime
		case 18:
			return TCat
		case 19, 20:
			if depth > 0 {
				return ListOf(pick(g, []Type{TI64, TI64, TStr, TF64, TBool, TI32}))
			}
		case 21:
			if depth > 0 {
				return Type{K: KStruct, Fields: []Field{{"a", TI64}, {"b", pick(g, []Type{TStr, TF64, TBool})}}}
			}
		}
	}
}

// RandomColumn draws n values of type t.
func (g *Gen) RandomColumn(name string, t Type, n int) Column {
	p := g.profile()
	vals := make([]any, n)
	for i := range vals {
		if p.nullFrac > 0 && g.Chance(p.nullFrac) {
			continue
		}
		vals[i] = g.value(t, p)
	}
	c := Column{Name: name, T: t, Values: vals}
	c.Chunks = g.chunks(n)
	return c
}

func (g *Gen) chunks(n int) []int {
	if n < 2 || g.Chance(0.6) {
		return nil
	}
	k := 2 + g.Intn(3)
	cuts := map[int]bool{n: true}
	for range k - 1 {
		cuts[g.Intn(n+1)] = true
	}
	var out []int
	for c := range cuts {
		out = append(out, c)
	}
	slices.Sort(out)
	// Zero-length chunks are legal arrow and worth exercising, but a
	// leading 0 boundary adds nothing new.
	if out[0] == 0 && len(out) > 1 && g.Chance(0.5) {
		out = out[1:]
	}
	if len(out) <= 1 {
		return nil
	}
	return out
}

func (g *Gen) intRange(bits int, signed bool) (int64, uint64) {
	if signed {
		return -(int64(1) << (bits - 1)), uint64(1)<<(bits-1) - 1
	}
	if bits == 64 {
		return 0, math.MaxUint64
	}
	return 0, uint64(1)<<bits - 1
}

func (g *Gen) value(t Type, p profile) any {
	special := p.special > 0 && g.Chance(p.special)
	switch t.K {
	case KBool:
		return g.Chance(0.5)
	case KInt:
		lo, hi := g.intRange(t.Bits, true)
		if special {
			return pick(g, []int64{lo, int64(hi), 0, -1, 1, lo + 1, int64(hi) - 1})
		}
		if p.small {
			return int64(g.Intn(11) - 5)
		}
		if t.Bits == 64 {
			return int64(g.R.Uint64() >> uint(g.Intn(64)))
		}
		span := uint64(int64(hi)-lo) + 1
		return lo + int64(g.R.Uint64N(span))
	case KUint:
		_, hi := g.intRange(t.Bits, false)
		if special {
			return pick(g, []uint64{0, 1, hi, hi - 1, hi / 2, hi/2 + 1})
		}
		if p.small {
			return uint64(g.Intn(10))
		}
		if hi == math.MaxUint64 {
			return g.R.Uint64() >> uint(g.Intn(64))
		}
		return g.R.Uint64N(hi + 1)
	case KFloat:
		var f float64
		switch {
		case special:
			f = pick(g, []float64{math.NaN(), math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1),
				math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, 1e-300, 1e300, 0.5, -0.5, 2.5,
				math.MaxFloat32, 9007199254740993})
		case p.small:
			f = float64(g.Intn(11)-5) / pick(g, []float64{1, 1, 2, 4})
		default:
			f = g.R.NormFloat64() * math.Pow(10, float64(g.Intn(7)-2))
		}
		if t.Bits == 32 {
			f = float64(float32(f))
		}
		return f
	case KStr:
		if p.small || special {
			return pick(g, strPool)
		}
		n := g.Intn(8)
		const alpha = "abcxyzABCXYZ 019_-,.:é日"
		rs := []rune(alpha)
		out := make([]rune, n)
		for i := range out {
			out[i] = rs[g.Intn(len(rs))]
		}
		return string(out)
	case KCat:
		return pick(g, catPool)
	case KDate:
		if special {
			// 1970-01-01, 1969-12-31, 2000-02-29, 2024-02-29, 1900-03-01, 2038-01-19, 1600-01-01.
			return pick(g, []int64{0, -1, 11016, 19782, -25508, 24855, -135140, 1, 59})
		}
		if p.small {
			return int64(19700 + g.Intn(90))
		}
		return int64(g.Intn(200000) - 100000)
	case KDatetime:
		return g.datetime(t.Unit, p, special)
	case KDuration:
		scale := unitScale(t.Unit)
		if special {
			return pick(g, []int64{0, -1, 1, 86400 * scale, -86400 * scale, 3600 * scale, math.MaxInt64 / 4, math.MinInt64 / 4})
		}
		if p.small {
			return int64(g.Intn(11)-5) * pick(g, []int64{scale, 3600 * scale, 86400 * scale})
		}
		return g.R.Int64N(20*86400*scale) - 10*86400*scale
	case KTime:
		if special {
			return pick(g, []int64{0, 1, 86399999999999, 43200000000000, 1000})
		}
		if p.small {
			return int64(g.Intn(24)) * 3600e9
		}
		return g.R.Int64N(86400e9)
	case KList:
		n := g.Intn(5)
		if special {
			n = 0
		}
		out := make([]any, n)
		ep := profile{nullFrac: 0.1, small: true, special: 0.05}
		for i := range out {
			if g.Chance(ep.nullFrac) {
				continue
			}
			out[i] = g.value(*t.Elem, ep)
		}
		return out
	case KStruct:
		out := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			if g.Chance(0.1) {
				continue
			}
			out[i] = g.value(f.T, profile{small: p.small, special: p.special})
		}
		return out
	}
	panic("difftest: value for " + t.Name())
}

func unitScale(unit string) int64 {
	switch unit {
	case "ms":
		return 1e3
	case "ns":
		return 1e9
	}
	return 1e6
}

func (g *Gen) datetime(unit string, p profile, special bool) int64 {
	scale := unitScale(unit)
	if special {
		// Epoch, pre-epoch, US and EU DST transitions in 2024, leap day,
		// and a Lord Howe half-hour transition.
		secs := pick(g, []int64{0, -1, 1710054000, 1710050400, 1730613600, 1730610000, 1711846800, 1709164800, 1712419200, -86400 * 365})
		return secs*scale + int64(g.Intn(3)-1)
	}
	if p.small {
		base := int64(1704067200) // 2024-01-01
		return (base + int64(g.Intn(72))*3600*pick(g, []int64{1, 6, 24})) * scale
	}
	days := int64(g.Intn(60000) - 20000)
	return days*86400*scale + g.R.Int64N(86400*scale)
}

// RandomFrame draws a frame with ncols columns of random types.
func (g *Gen) RandomFrame(prefix string, n, ncols int) *Frame {
	f := &Frame{}
	for i := range ncols {
		name := fmt.Sprintf("%s%d", prefix, i)
		f.Cols = append(f.Cols, g.RandomColumn(name, g.RandomType(1), n))
	}
	return f
}

// RandomSize draws a row count.
func (g *Gen) RandomSize() int {
	if g.Chance(0.02) {
		return pick(g, largeSizes)
	}
	return pick(g, smallSizes)
}
