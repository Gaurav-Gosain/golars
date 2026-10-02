package difftest

import (
	"fmt"
	"strings"
)

// ParseType is the inverse of Type.Name.
func ParseType(s string) (Type, error) {
	p := &typeParser{s: s}
	t, err := p.parse()
	if err != nil {
		return Type{}, err
	}
	if p.i != len(p.s) {
		return Type{}, fmt.Errorf("difftest: trailing input in type %q", s)
	}
	return t, nil
}

type typeParser struct {
	s string
	i int
}

func (p *typeParser) ident() string {
	j := p.i
	for j < len(p.s) && (p.s[j] == '_' || p.s[j] >= 'a' && p.s[j] <= 'z' || p.s[j] >= 'A' && p.s[j] <= 'Z' || p.s[j] >= '0' && p.s[j] <= '9') {
		j++
	}
	out := p.s[p.i:j]
	p.i = j
	return out
}

func (p *typeParser) eat(tok string) bool {
	if strings.HasPrefix(p.s[p.i:], tok) {
		p.i += len(tok)
		return true
	}
	return false
}

func (p *typeParser) parse() (Type, error) {
	name := p.ident()
	switch name {
	case "null":
		return TNull, nil
	case "bool":
		return TBool, nil
	case "str":
		return TStr, nil
	case "date":
		return TDate, nil
	case "time":
		return TTime, nil
	case "cat":
		return TCat, nil
	case "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64":
		var bits int
		fmt.Sscanf(name[1:], "%d", &bits)
		switch name[0] {
		case 'i':
			return Type{K: KInt, Bits: bits}, nil
		case 'u':
			return Type{K: KUint, Bits: bits}, nil
		}
		return Type{K: KFloat, Bits: bits}, nil
	case "datetime", "duration":
		if !p.eat("[") {
			return Type{}, fmt.Errorf("difftest: %s needs a unit", name)
		}
		unit := p.ident()
		tz := ""
		if p.eat(", ") {
			j := strings.IndexByte(p.s[p.i:], ']')
			if j < 0 {
				return Type{}, fmt.Errorf("difftest: unterminated %s", name)
			}
			tz = p.s[p.i : p.i+j]
			p.i += j
		}
		if !p.eat("]") {
			return Type{}, fmt.Errorf("difftest: unterminated %s", name)
		}
		if name == "duration" {
			return Duration(unit), nil
		}
		return Datetime(unit, tz), nil
	case "list":
		if !p.eat("[") {
			return Type{}, fmt.Errorf("difftest: list needs an element type")
		}
		e, err := p.parse()
		if err != nil {
			return Type{}, err
		}
		if !p.eat("]") {
			return Type{}, fmt.Errorf("difftest: unterminated list")
		}
		return ListOf(e), nil
	case "struct":
		if !p.eat("{") {
			return Type{}, fmt.Errorf("difftest: struct needs fields")
		}
		var fs []Field
		for !p.eat("}") {
			if len(fs) > 0 && !p.eat(", ") {
				return Type{}, fmt.Errorf("difftest: bad struct field list")
			}
			fname := p.ident()
			if !p.eat(": ") {
				return Type{}, fmt.Errorf("difftest: bad struct field")
			}
			ft, err := p.parse()
			if err != nil {
				return Type{}, err
			}
			fs = append(fs, Field{Name: fname, T: ft})
		}
		return Type{K: KStruct, Fields: fs}, nil
	}
	return Type{}, fmt.Errorf("difftest: unknown type %q", name)
}
