package series

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// JSONDecode parses each string as a JSON value and converts it to
// the target dtype. Mirrors polars' str.json_decode(dtype); as in
// polars 1.39 the dtype is required (no inference).
//
// Supported targets: every integer and float dtype, Boolean, String,
// List of a supported dtype, and Struct of supported dtypes, nested
// to any depth. Conversion rules follow polars:
//
//   - JSON null and null input rows become null. Blank strings become
//     null.
//   - Integers accept JSON integers, floats (truncated toward zero)
//     and booleans (0/1). Values outside the target range become null.
//   - Floats accept numbers and booleans.
//   - Booleans accept only true/false.
//   - Strings accept strings, and render numbers and booleans as text.
//   - A List accepts an array; any other non-null scalar becomes a
//     one-element list.
//   - A Struct accepts an object. Missing keys become null, unknown
//     keys are ignored, and the first occurrence of a duplicate key
//     wins.
//
// Invalid JSON and type mismatches (for example a string where a
// number is expected) are errors.
func (o StrOps) JSONDecode(to dtype.DType, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.strView("JSONDecode", opts)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	bld := array.NewBuilder(cfg.alloc, to.Arrow())
	defer bld.Release()
	dec, err := jsonNewDecoder(bld, to.Arrow())
	if err != nil {
		return nil, err
	}
	sa := a.(*array.String)
	n := v.Len()
	for i := range n {
		if sa.IsNull(i) {
			dec.null()
			continue
		}
		s := sa.Value(i)
		vs, ve, empty, err := jsonValueSpan(s)
		if err != nil {
			return nil, fmt.Errorf("error deserializing JSON: %w", err)
		}
		if empty {
			dec.null()
			continue
		}
		if err := dec.decode(s, vs, ve); err != nil {
			return nil, fmt.Errorf("error deserializing JSON: %w", err)
		}
	}
	return New(o.s.Name(), bld.NewArray())
}

// jsonDecoder appends one decoded value (or a null) to its builder.
// decode receives a validated value span.
type jsonDecoder interface {
	decode(s string, vs, ve int) error
	null()
}

func jsonNewDecoder(b array.Builder, dt arrow.DataType) (jsonDecoder, error) {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return &jsonIntDecoder{b: b, id: dt.ID()}, nil
	case arrow.FLOAT32, arrow.FLOAT64:
		return &jsonFloatDecoder{b: b}, nil
	case arrow.BOOL:
		return &jsonBoolDecoder{b: b.(*array.BooleanBuilder)}, nil
	case arrow.STRING:
		return &jsonStringDecoder{b: b.(*array.StringBuilder)}, nil
	case arrow.LIST:
		lb := b.(*array.ListBuilder)
		child, err := jsonNewDecoder(lb.ValueBuilder(), dt.(*arrow.ListType).Elem())
		if err != nil {
			return nil, err
		}
		return &jsonListDecoder{b: lb, child: child}, nil
	case arrow.STRUCT:
		sb := b.(*array.StructBuilder)
		st := dt.(*arrow.StructType)
		d := &jsonStructDecoder{b: sb}
		for i, f := range st.Fields() {
			child, err := jsonNewDecoder(sb.FieldBuilder(i), f.Type)
			if err != nil {
				return nil, err
			}
			d.names = append(d.names, f.Name)
			d.fields = append(d.fields, child)
		}
		d.spans = make([][2]int, len(d.fields))
		return d, nil
	}
	return nil, fmt.Errorf("str.json_decode: unsupported target dtype %s", dtype.FromArrow(dt))
}

// jsonDescribe renders a short description of a JSON value for error
// messages, in the style polars uses.
func jsonDescribe(s string, vs, ve int) string {
	switch s[vs] {
	case '{':
		return "Object(" + jsonTrunc(s[vs:ve]) + ")"
	case '[':
		return "Array(" + jsonTrunc(s[vs:ve]) + ")"
	case '"':
		return "String(" + jsonTrunc(s[vs:ve]) + ")"
	case 't', 'f':
		return "Bool(" + s[vs:ve] + ")"
	}
	return "Number(" + s[vs:ve] + ")"
}

func jsonTrunc(s string) string {
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}

func jsonMismatch(s string, vs, ve int, as string) error {
	return fmt.Errorf("error deserializing value %q as %s", jsonDescribe(s, vs, ve), as)
}

// jsonIntDecoder converts to any integer dtype.
type jsonIntDecoder struct {
	b  array.Builder
	id arrow.Type
}

func (d *jsonIntDecoder) null() { d.b.AppendNull() }

func (d *jsonIntDecoder) decode(s string, vs, ve int) error {
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
		return nil
	case 't':
		return d.put(false, 1)
	case 'f':
		return d.put(false, 0)
	case '"', '{', '[':
		return jsonMismatch(s, vs, ve, "numeric")
	}
	num := s[vs:ve]
	_, isFloat, _ := jsonScanNumber(s, vs)
	if !isFloat {
		if num[0] == '-' {
			if v, err := strconv.ParseInt(num, 10, 64); err == nil {
				return d.put(true, uint64(-v))
			}
		} else if v, err := strconv.ParseUint(num, 10, 64); err == nil {
			return d.put(false, v)
		}
		// Out of 64-bit range: fall through to the float path, which
		// reports overflow as null.
	}
	f, _ := strconv.ParseFloat(num, 64)
	f = math.Trunc(f)
	if math.IsNaN(f) || math.IsInf(f, 0) || f >= 1<<64 || f < -(1<<63) {
		d.b.AppendNull()
		return nil
	}
	if f < 0 {
		return d.put(true, uint64(-int64(f)))
	}
	return d.put(false, uint64(f))
}

// put appends the value with magnitude mag (negated when neg), or a
// null when it does not fit the target dtype.
func (d *jsonIntDecoder) put(neg bool, mag uint64) error {
	var lo, hi uint64 // magnitude bounds for negative and positive
	switch d.id {
	case arrow.INT8:
		lo, hi = 1<<7, 1<<7-1
	case arrow.INT16:
		lo, hi = 1<<15, 1<<15-1
	case arrow.INT32:
		lo, hi = 1<<31, 1<<31-1
	case arrow.INT64:
		lo, hi = 1<<63, 1<<63-1
	case arrow.UINT8:
		hi = 1<<8 - 1
	case arrow.UINT16:
		hi = 1<<16 - 1
	case arrow.UINT32:
		hi = 1<<32 - 1
	case arrow.UINT64:
		hi = math.MaxUint64
	}
	if (neg && mag > lo) || (!neg && mag > hi) {
		d.b.AppendNull()
		return nil
	}
	sv := int64(mag)
	if neg {
		sv = -int64(mag)
	}
	switch b := d.b.(type) {
	case *array.Int8Builder:
		b.Append(int8(sv))
	case *array.Int16Builder:
		b.Append(int16(sv))
	case *array.Int32Builder:
		b.Append(int32(sv))
	case *array.Int64Builder:
		b.Append(sv)
	case *array.Uint8Builder:
		b.Append(uint8(mag))
	case *array.Uint16Builder:
		b.Append(uint16(mag))
	case *array.Uint32Builder:
		b.Append(uint32(mag))
	case *array.Uint64Builder:
		b.Append(mag)
	}
	return nil
}

// jsonFloatDecoder converts to Float32 or Float64.
type jsonFloatDecoder struct{ b array.Builder }

func (d *jsonFloatDecoder) null() { d.b.AppendNull() }

func (d *jsonFloatDecoder) decode(s string, vs, ve int) error {
	var f float64
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
		return nil
	case 't':
		f = 1
	case 'f':
		f = 0
	case '"', '{', '[':
		return jsonMismatch(s, vs, ve, "numeric")
	default:
		f, _ = strconv.ParseFloat(s[vs:ve], 64)
	}
	switch b := d.b.(type) {
	case *array.Float32Builder:
		b.Append(float32(f))
	case *array.Float64Builder:
		b.Append(f)
	}
	return nil
}

type jsonBoolDecoder struct{ b *array.BooleanBuilder }

func (d *jsonBoolDecoder) null() { d.b.AppendNull() }

func (d *jsonBoolDecoder) decode(s string, vs, ve int) error {
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
	case 't':
		d.b.Append(true)
	case 'f':
		d.b.Append(false)
	default:
		return jsonMismatch(s, vs, ve, "boolean")
	}
	return nil
}

type jsonStringDecoder struct {
	b       *array.StringBuilder
	scratch []byte
}

func (d *jsonStringDecoder) null() { d.b.AppendNull() }

func (d *jsonStringDecoder) decode(s string, vs, ve int) error {
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
		return nil
	case '{', '[':
		return jsonMismatch(s, vs, ve, "string")
	case '"':
		d.scratch = jsonUnescape(d.scratch[:0], s[vs:ve])
	case 't', 'f':
		d.scratch = append(d.scratch[:0], s[vs:ve]...)
	default:
		d.scratch = jsonAppendDisplayNumber(d.scratch[:0], s, vs, ve)
	}
	// BinaryBuilder.Append takes the bytes directly, which avoids the
	// string-to-[]byte conversion StringBuilder.Append would do.
	d.b.BinaryBuilder.Append(d.scratch)
	return nil
}

// jsonAppendDisplayNumber renders a JSON number the way polars casts
// it to a string: integers verbatim, floats in plain decimal notation
// with no exponent and no trailing ".0".
func jsonAppendDisplayNumber(dst []byte, s string, vs, ve int) []byte {
	num := s[vs:ve]
	_, isFloat, _ := jsonScanNumber(s, vs)
	if !isFloat {
		if num[0] == '-' {
			if v, err := strconv.ParseInt(num, 10, 64); err == nil {
				return strconv.AppendInt(dst, v, 10)
			}
		} else if v, err := strconv.ParseUint(num, 10, 64); err == nil {
			return strconv.AppendUint(dst, v, 10)
		}
	}
	f, _ := strconv.ParseFloat(num, 64)
	return strconv.AppendFloat(dst, f, 'f', -1, 64)
}

type jsonListDecoder struct {
	b     *array.ListBuilder
	child jsonDecoder
}

func (d *jsonListDecoder) null() { d.b.AppendNull() }

func (d *jsonListDecoder) decode(s string, vs, ve int) error {
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
		return nil
	case '{':
		return jsonMismatch(s, vs, ve, "list")
	case '[':
		d.b.Append(true)
		var err error
		jsonEachElem(s, vs, func(cvs, cve int) bool {
			err = d.child.decode(s, cvs, cve)
			return err == nil
		})
		return err
	}
	// A scalar becomes a one-element list, as in polars.
	d.b.Append(true)
	return d.child.decode(s, vs, ve)
}

type jsonStructDecoder struct {
	b       *array.StructBuilder
	names   []string
	fields  []jsonDecoder
	spans   [][2]int
	scratch []byte
}

func (d *jsonStructDecoder) null() { d.b.AppendNull() }

func (d *jsonStructDecoder) decode(s string, vs, ve int) error {
	switch s[vs] {
	case 'n':
		d.b.AppendNull()
		return nil
	case '{':
	default:
		return jsonMismatch(s, vs, ve, "struct")
	}
	for i := range d.spans {
		d.spans[i] = [2]int{-1, -1}
	}
	jsonEachMember(s, vs, func(ks, ke int, kesc bool, cvs, cve int) bool {
		for i, name := range d.names {
			if d.spans[i][0] < 0 && jsonKeyEquals(s, ks, ke, kesc, name, &d.scratch) {
				d.spans[i] = [2]int{cvs, cve}
				break
			}
		}
		return true
	})
	d.b.Append(true)
	for i, f := range d.fields {
		sp := d.spans[i]
		if sp[0] < 0 {
			f.null()
			continue
		}
		if err := f.decode(s, sp[0], sp[1]); err != nil {
			return err
		}
	}
	return nil
}

// JSONPathMatch extracts the first value matching a JSONPath
// expression from each string. Mirrors polars' str.json_path_match.
//
// Supported path syntax: the root `$`, child access `.name` and
// `['name']`, array indices `[n]` (negative counts from the end),
// wildcards `.*` and `[*]`, slices `[a:b]`, index unions `[a,b]`, and
// recursive descent `..name` / `..*`. Filter expressions are not
// supported.
//
// Output is a String column: strings are returned unquoted, numbers
// and booleans as text, objects and arrays as compact JSON. Rows that
// are null, are not valid JSON, have no match, or match a JSON null
// come back null. An invalid path is an error.
func (o StrOps) JSONPathMatch(path string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	segs, err := jsonParsePath(path)
	if err != nil {
		return nil, err
	}
	a, v, err := o.strView("JSONPathMatch", opts)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	sa := a.(*array.String)
	n := v.Len()
	out := newBinOut(cfg.alloc, n, 0)
	m := jsonMatcher{segs: segs}
	var buf []byte
	for i := range n {
		if sa.IsNull(i) {
			out.appendNull()
			continue
		}
		s := sa.Value(i)
		vs, ve, empty, err := jsonValueSpan(s)
		if err != nil || empty {
			out.appendNull()
			continue
		}
		ms, me, ok := m.first(s, vs, ve, 0)
		if !ok || s[ms] == 'n' {
			out.appendNull()
			continue
		}
		switch s[ms] {
		case '"':
			buf = jsonUnescape(buf[:0], s[ms:me])
		case 't', 'f':
			buf = append(buf[:0], s[ms:me]...)
		case '{', '[':
			buf = jsonAppendCompact(buf[:0], s, ms, me, &m.scratch)
		default:
			_, isFloat, _ := jsonScanNumber(s, ms)
			buf = jsonAppendNumber(buf[:0], s[ms:me], isFloat)
		}
		out.appendBytes(buf)
		out.endRow()
	}
	return New(o.s.Name(), out.finish(arrow.BinaryTypes.String))
}

type jsonSegKind uint8

const (
	jsonSegKey jsonSegKind = iota
	jsonSegWild
	jsonSegIndex
	jsonSegSlice
	jsonSegUnion
)

type jsonSeg struct {
	kind      jsonSegKind
	recursive bool
	key       string
	idx       []int // index or union members
	lo, hi    int
	hasLo     bool
	hasHi     bool
}

func jsonPathErr(path string) error {
	return fmt.Errorf("error compiling JSON path expression path error: %q", path)
}

// jsonParsePath compiles the supported JSONPath subset.
func jsonParsePath(path string) ([]jsonSeg, error) {
	p := strings.TrimSpace(path)
	if !strings.HasPrefix(p, "$") {
		return nil, jsonPathErr(path)
	}
	i := 1
	var segs []jsonSeg
	for i < len(p) {
		switch p[i] {
		case '.':
			rec := false
			i++
			if i < len(p) && p[i] == '.' {
				rec = true
				i++
			}
			if i < len(p) && p[i] == '[' {
				if !rec {
					return nil, jsonPathErr(path)
				}
				seg, next, err := jsonParseBracket(p, i, path)
				if err != nil {
					return nil, err
				}
				seg.recursive = true
				segs = append(segs, seg)
				i = next
				continue
			}
			if i < len(p) && p[i] == '*' {
				segs = append(segs, jsonSeg{kind: jsonSegWild, recursive: rec})
				i++
				continue
			}
			j := i
			for j < len(p) && p[j] != '.' && p[j] != '[' {
				j++
			}
			if j == i {
				return nil, jsonPathErr(path)
			}
			segs = append(segs, jsonSeg{kind: jsonSegKey, recursive: rec, key: p[i:j]})
			i = j
		case '[':
			seg, next, err := jsonParseBracket(p, i, path)
			if err != nil {
				return nil, err
			}
			segs = append(segs, seg)
			i = next
		default:
			return nil, jsonPathErr(path)
		}
	}
	return segs, nil
}

// jsonParseBracket parses a `[...]` selector starting at p[i] == '['.
func jsonParseBracket(p string, i int, path string) (jsonSeg, int, error) {
	end := strings.IndexByte(p[i:], ']')
	if end < 0 {
		return jsonSeg{}, 0, jsonPathErr(path)
	}
	body := strings.TrimSpace(p[i+1 : i+end])
	next := i + end + 1
	if body == "" {
		return jsonSeg{}, 0, jsonPathErr(path)
	}
	if body == "*" {
		return jsonSeg{kind: jsonSegWild}, next, nil
	}
	if q := body[0]; q == '\'' || q == '"' {
		// Quoted key; the closing bracket search above is naive, so
		// look for the matching quote followed by ']'.
		close := strings.Index(p[i+2:], string(q)+"]")
		if close < 0 {
			return jsonSeg{}, 0, jsonPathErr(path)
		}
		key := p[i+2 : i+2+close]
		return jsonSeg{kind: jsonSegKey, key: key}, i + 2 + close + 2, nil
	}
	if strings.Contains(body, ":") {
		parts := strings.Split(body, ":")
		if len(parts) > 3 {
			return jsonSeg{}, 0, jsonPathErr(path)
		}
		seg := jsonSeg{kind: jsonSegSlice}
		if s := strings.TrimSpace(parts[0]); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil {
				return jsonSeg{}, 0, jsonPathErr(path)
			}
			seg.lo, seg.hasLo = v, true
		}
		if s := strings.TrimSpace(parts[1]); s != "" {
			v, err := strconv.Atoi(s)
			if err != nil {
				return jsonSeg{}, 0, jsonPathErr(path)
			}
			seg.hi, seg.hasHi = v, true
		}
		return seg, next, nil
	}
	parts := strings.Split(body, ",")
	idx := make([]int, 0, len(parts))
	for _, s := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return jsonSeg{}, 0, jsonPathErr(path)
		}
		idx = append(idx, v)
	}
	if len(idx) == 1 {
		return jsonSeg{kind: jsonSegIndex, idx: idx}, next, nil
	}
	return jsonSeg{kind: jsonSegUnion, idx: idx}, next, nil
}

// jsonMatcher walks a validated document for the first value that
// matches its path. The candidate order follows jsonpath_lib (used by
// polars): at each node, direct matches come first, then descendants
// in document order.
type jsonMatcher struct {
	segs    []jsonSeg
	scratch []byte
}

func (m *jsonMatcher) first(s string, vs, ve, k int) (int, int, bool) {
	if k == len(m.segs) {
		return vs, ve, true
	}
	seg := m.segs[k]
	if ms, me, ok := m.step(s, vs, ve, k); ok {
		return ms, me, true
	}
	if !seg.recursive {
		return 0, 0, false
	}
	// Recursive descent: retry this segment on every child.
	var rs, re int
	var found bool
	visit := func(cvs, cve int) bool {
		rs, re, found = m.first(s, cvs, cve, k)
		return !found
	}
	switch s[vs] {
	case '{':
		jsonEachMember(s, vs, func(_, _ int, _ bool, cvs, cve int) bool { return visit(cvs, cve) })
	case '[':
		jsonEachElem(s, vs, visit)
	}
	return rs, re, found
}

// step applies segment k at the node s[vs:ve] (non-recursively) and
// continues with the remaining segments.
func (m *jsonMatcher) step(s string, vs, ve, k int) (int, int, bool) {
	seg := m.segs[k]
	var rs, re int
	var found bool
	try := func(cvs, cve int) bool {
		rs, re, found = m.first(s, cvs, cve, k+1)
		return !found
	}
	switch seg.kind {
	case jsonSegKey:
		if s[vs] != '{' {
			return 0, 0, false
		}
		jsonEachMember(s, vs, func(ks, ke int, kesc bool, cvs, cve int) bool {
			if jsonKeyEquals(s, ks, ke, kesc, seg.key, &m.scratch) {
				try(cvs, cve)
				return false
			}
			return true
		})
	case jsonSegWild:
		switch s[vs] {
		case '{':
			jsonEachMember(s, vs, func(_, _ int, _ bool, cvs, cve int) bool { return try(cvs, cve) })
		case '[':
			jsonEachElem(s, vs, try)
		}
	case jsonSegIndex, jsonSegUnion, jsonSegSlice:
		if s[vs] != '[' {
			return 0, 0, false
		}
		var spans [][2]int
		jsonEachElem(s, vs, func(cvs, cve int) bool {
			spans = append(spans, [2]int{cvs, cve})
			return true
		})
		l := len(spans)
		if seg.kind == jsonSegSlice {
			lo, hi := 0, l
			if seg.hasLo {
				lo = seg.lo
				if lo < 0 {
					lo += l
				}
			}
			if seg.hasHi {
				hi = seg.hi
				if hi < 0 {
					hi += l
				}
			}
			lo, hi = max(lo, 0), min(hi, l)
			for j := lo; j < hi; j++ {
				if !try(spans[j][0], spans[j][1]) {
					break
				}
			}
			break
		}
		for _, j := range seg.idx {
			if j < 0 {
				j += l
			}
			if j < 0 || j >= l {
				continue
			}
			if !try(spans[j][0], spans[j][1]) {
				break
			}
		}
	}
	return rs, re, found
}
