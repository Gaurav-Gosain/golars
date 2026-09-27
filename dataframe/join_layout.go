package dataframe

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
)

// joinColSource says where an output column of a join comes from.
type joinColSource uint8

const (
	joinFromLeft joinColSource = iota
	joinFromRight
	// joinFromBoth is a coalesced full-join key: the left value where
	// the left row exists, the right value otherwise.
	joinFromBoth
)

// joinOutCol is one output column of a join.
type joinOutCol struct {
	name  string
	src   joinColSource
	col   string // source column name on its side
	key   int    // key pair index for joinFromBoth
	dtype dtype.DType
}

// JoinOutputSchema returns the schema Join would produce for frames
// with schemas ls and rs, including the errors polars raises before
// looking at any data (missing or mistyped keys, duplicate names).
func JoinOutputSchema(ls, rs *schema.Schema, spec JoinSpec) (*schema.Schema, error) {
	if _, err := checkJoinKeys(ls, rs, spec); err != nil {
		return nil, err
	}
	cols, err := joinLayout(ls, rs, spec)
	if err != nil {
		return nil, err
	}
	fields := make([]schema.Field, len(cols))
	for i, c := range cols {
		fields[i] = schema.Field{Name: c.name, DType: c.dtype}
	}
	return schema.New(fields...)
}

// joinKeyTypes holds, per key pair, the dtype both sides are compared
// in.
type joinKeyTypes []dtype.DType

// checkJoinKeys validates the key lists against both schemas and
// returns the comparison dtype of each key pair.
func checkJoinKeys(ls, rs *schema.Schema, spec JoinSpec) (joinKeyTypes, error) {
	if spec.How == CrossJoin {
		return nil, nil
	}
	if len(spec.LeftOn) == 0 && len(spec.RightOn) == 0 {
		return nil, fmt.Errorf("dataframe.Join: expected join keys/predicates")
	}
	if len(spec.LeftOn) != len(spec.RightOn) {
		return nil, fmt.Errorf("dataframe.Join: the number of columns given as join key (left: %d, right:%d) should be equal",
			len(spec.LeftOn), len(spec.RightOn))
	}
	for i, k := range spec.LeftOn {
		for _, prev := range spec.LeftOn[:i] {
			if prev == k {
				return nil, fmt.Errorf("dataframe.Join: joining with repeated key names; already joined on %s and %s", prev, k)
			}
		}
	}
	types := make(joinKeyTypes, len(spec.LeftOn))
	for i := range spec.LeftOn {
		lf, ok := ls.FieldByName(spec.LeftOn[i])
		if !ok {
			return nil, missingColumnError(spec.LeftOn[i], ls)
		}
		rf, ok := rs.FieldByName(spec.RightOn[i])
		if !ok {
			return nil, missingColumnError(spec.RightOn[i], rs)
		}
		t, ok := joinKeySupertype(lf.DType, rf.DType)
		if !ok {
			return nil, fmt.Errorf("%w: datatypes of join keys don't match - `%s`: %s on left does not match `%s`: %s on right (and no other type was available to cast to)",
				ErrJoinKeyDTypeMismatch, lf.Name, lf.DType, rf.Name, rf.DType)
		}
		if !joinableKey(t) {
			return nil, fmt.Errorf("%w: %s", ErrJoinUnsupportedKey, t)
		}
		types[i] = t
	}
	if spec.Validate != ValidateManyToMany {
		switch spec.How {
		case RightJoin, SemiJoin, AntiJoin:
			return nil, fmt.Errorf("dataframe.Join: %s validation on a %s join is not supported",
				spec.Validate, strings.ToUpper(spec.How.String()))
		}
	}
	return types, nil
}

func missingColumnError(name string, s *schema.Schema) error {
	names := s.Names()
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = fmt.Sprintf("%q", n)
	}
	return fmt.Errorf("dataframe.Join: unable to find column %q; valid columns: [%s]", name, strings.Join(quoted, ", "))
}

// joinKeySupertype returns the dtype a key pair is compared in. Equal
// dtypes compare as themselves; integers of different widths or
// signedness compare in their common supertype, and so do f32 and f64.
// Every other mix is an error, as in polars.
func joinKeySupertype(l, r dtype.DType) (dtype.DType, bool) {
	if l.Equal(r) {
		return l, true
	}
	if l.IsInteger() && r.IsInteger() {
		return intSupertype(l.ID(), r.ID())
	}
	if l.IsFloating() && r.IsFloating() {
		return dtype.Float64(), true
	}
	return dtype.DType{}, false
}

func intWidth(id arrow.Type) int {
	switch id {
	case arrow.INT8, arrow.UINT8:
		return 8
	case arrow.INT16, arrow.UINT16:
		return 16
	case arrow.INT32, arrow.UINT32:
		return 32
	}
	return 64
}

func signedOfWidth(w int) dtype.DType {
	switch w {
	case 8:
		return dtype.Int8()
	case 16:
		return dtype.Int16()
	case 32:
		return dtype.Int32()
	}
	return dtype.Int64()
}

func unsignedOfWidth(w int) dtype.DType {
	switch w {
	case 8:
		return dtype.Uint8()
	case 16:
		return dtype.Uint16()
	case 32:
		return dtype.Uint32()
	}
	return dtype.Uint64()
}

// intSupertype is the polars integer supertype. A u64 mixed with a
// signed type needs i128, which golars does not have.
func intSupertype(a, b arrow.Type) (dtype.DType, bool) {
	wa, wb := intWidth(a), intWidth(b)
	ua, ub := isUnsigned(a), isUnsigned(b)
	switch {
	case ua == ub && ua:
		return unsignedOfWidth(max(wa, wb)), true
	case ua == ub:
		return signedOfWidth(max(wa, wb)), true
	}
	wu, ws := wa, wb
	if ub {
		wu, ws = wb, wa
	}
	if ws > wu {
		return signedOfWidth(ws), true
	}
	if wu == 64 {
		return dtype.DType{}, false
	}
	return signedOfWidth(2 * wu), true
}

// joinableKey reports whether golars can hash keys of dtype t.
func joinableKey(t dtype.DType) bool {
	switch t.ID() {
	case arrow.NULL, arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME32, arrow.TIME64,
		arrow.DICTIONARY, arrow.DECIMAL128:
		return true
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.STRUCT:
		// Nested keys go through the byte encoder, which compares
		// their rendered values.
		return true
	}
	return false
}

// joinLayout lists the output columns of a join in order, following the
// polars rules for coalescing and suffixes.
func joinLayout(ls, rs *schema.Schema, spec JoinSpec) ([]joinOutCol, error) {
	var out []joinOutCol
	leftField := func(f schema.Field) joinOutCol {
		return joinOutCol{name: f.Name, src: joinFromLeft, col: f.Name, dtype: f.DType}
	}
	var dropRight []string
	switch spec.How {
	case SemiJoin, AntiJoin:
		for _, f := range ls.Fields() {
			out = append(out, leftField(f))
		}
		return out, nil
	case CrossJoin:
		for _, f := range ls.Fields() {
			out = append(out, leftField(f))
		}
	case RightJoin:
		for _, f := range ls.Fields() {
			if spec.Coalesce && slices.Contains(spec.LeftOn, f.Name) {
				continue
			}
			out = append(out, leftField(f))
		}
	default:
		for _, f := range ls.Fields() {
			c := leftField(f)
			if spec.How == FullJoin && spec.Coalesce {
				if k := slices.Index(spec.LeftOn, f.Name); k >= 0 {
					rf, _ := rs.FieldByName(spec.RightOn[k])
					t, _ := joinKeySupertype(f.DType, rf.DType)
					c = joinOutCol{name: f.Name, src: joinFromBoth, col: f.Name, key: k, dtype: t}
				}
			}
			out = append(out, c)
		}
		if spec.Coalesce {
			dropRight = spec.RightOn
		}
	}
	nLeft := len(out)
	for _, f := range rs.Fields() {
		if slices.Contains(dropRight, f.Name) {
			continue
		}
		name := f.Name
		if slices.ContainsFunc(out[:nLeft], func(c joinOutCol) bool { return c.name == name }) {
			name += spec.Suffix
		}
		out = append(out, joinOutCol{name: name, src: joinFromRight, col: f.Name, dtype: f.DType})
	}
	seen := make(map[string]struct{}, len(out))
	for _, c := range out {
		if _, dup := seen[c.name]; dup {
			return nil, fmt.Errorf("dataframe.Join: column with name '%s' already exists\n\n"+
				"You may want to try:\n- renaming the column prior to joining\n"+
				"- using the `suffix` parameter to specify a suffix different to the default one ('_right')", c.name)
		}
		seen[c.name] = struct{}{}
	}
	return out, nil
}

// JoinColumnSource names where one output column of a join comes from:
// the left and/or right input column feeding it ("" for a side that
// does not). A coalesced full-join key has both.
type JoinColumnSource struct {
	Name, Left, Right string
}

// JoinOutputSources lists the output columns of a join with their
// source columns, for planners that prune join inputs.
func JoinOutputSources(ls, rs *schema.Schema, spec JoinSpec) ([]JoinColumnSource, error) {
	cols, err := joinLayout(ls, rs, spec)
	if err != nil {
		return nil, err
	}
	out := make([]JoinColumnSource, len(cols))
	for i, c := range cols {
		out[i].Name = c.name
		switch c.src {
		case joinFromLeft:
			out[i].Left = c.col
		case joinFromRight:
			out[i].Right = c.col
		case joinFromBoth:
			out[i].Left, out[i].Right = spec.LeftOn[c.key], spec.RightOn[c.key]
		}
	}
	return out, nil
}
