package difftest

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
)

// RegressCase is a self-contained regression test: a shrunk case plus
// the result polars produced for it, so `go test` needs no Python.
type RegressCase struct {
	Name string `json:"name"`
	// Status is "fixed" (must pass) or "known" (skipped with Note as
	// the reason until the bug is fixed).
	Status string    `json:"status"`
	Note   string    `json:"note,omitempty"`
	Plan   *Plan     `json:"plan"`
	Left   []ColJSON `json:"left"`
	Right  []ColJSON `json:"right,omitempty"`
	Want   WantJSON  `json:"want"`
}

// ColJSON is a column with JSON-safe values.
type ColJSON struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Values []any  `json:"values"`
	Chunks []int  `json:"chunks,omitempty"`
}

// WantJSON is the polars outcome.
type WantJSON struct {
	OK    bool      `json:"ok"`
	Error string    `json:"error,omitempty"`
	Cols  []ColJSON `json:"cols,omitempty"`
}

// NewRegressCase packages a case and its polars result.
func NewRegressCase(name string, c *Case, pr *OracleResult) *RegressCase {
	rc := &RegressCase{Name: name, Status: "known", Plan: c.Plan, Left: framesJSON(c.Left)}
	if c.Right != nil {
		rc.Right = framesJSON(c.Right)
	}
	rc.Want.OK = pr.OK
	if !pr.OK {
		rc.Want.Error = pr.EType + ": " + firstLine(pr.Error)
	}
	for _, col := range pr.Cols {
		cj := ColJSON{Name: col.Name, Type: col.DType, Values: make([]any, len(col.Values))}
		t, err := ParseType(col.DType)
		for i, v := range col.Values {
			if err == nil {
				cj.Values[i] = encodeValue(t, v)
			} else {
				cj.Values[i] = v
			}
		}
		rc.Want.Cols = append(rc.Want.Cols, cj)
	}
	return rc
}

func framesJSON(f *Frame) []ColJSON {
	out := make([]ColJSON, len(f.Cols))
	for i, c := range f.Cols {
		cj := ColJSON{Name: c.Name, Type: c.T.Name(), Values: make([]any, len(c.Values)), Chunks: c.Chunks}
		for r, v := range c.Values {
			cj.Values[r] = encodeValue(c.T, v)
		}
		out[i] = cj
	}
	return out
}

func encodeValue(t Type, v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case float64:
		return floatVal(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			switch t.K {
			case KList:
				out[i] = encodeValue(*t.Elem, e)
			case KStruct:
				out[i] = encodeValue(t.Fields[i].T, e)
			default:
				out[i] = e
			}
		}
		return out
	}
	return v
}

func decodeValue(t Type, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t.K {
	case KBool:
		return v.(bool), nil
	case KStr, KCat:
		return v.(string), nil
	case KInt, KDate, KDatetime, KDuration, KTime:
		return v.(json.Number).Int64()
	case KUint:
		var u uint64
		_, err := fmt.Sscan(string(v.(json.Number)), &u)
		return u, err
	case KFloat:
		switch x := v.(type) {
		case string:
			switch x {
			case "nan":
				return math.NaN(), nil
			case "inf":
				return math.Inf(1), nil
			case "-inf":
				return math.Inf(-1), nil
			case "-0.0":
				return math.Copysign(0, -1), nil
			}
			return nil, fmt.Errorf("difftest: bad float %q", x)
		case json.Number:
			return x.Float64()
		}
	case KList, KStruct:
		xs := v.([]any)
		out := make([]any, len(xs))
		for i, e := range xs {
			et := t.Elem
			if t.K == KStruct {
				et = &t.Fields[i].T
			}
			d, err := decodeValue(*et, e)
			if err != nil {
				return nil, err
			}
			out[i] = d
		}
		return out, nil
	case KNull:
		return nil, nil
	}
	return nil, fmt.Errorf("difftest: cannot decode %v as %s", v, t.Name())
}

func decodeCols(cs []ColJSON) (*Frame, error) {
	f := &Frame{}
	for _, cj := range cs {
		t, err := ParseType(cj.Type)
		if err != nil {
			return nil, err
		}
		c := Column{Name: cj.Name, T: t, Values: make([]any, len(cj.Values)), Chunks: cj.Chunks}
		for i, v := range cj.Values {
			if c.Values[i], err = decodeValue(t, v); err != nil {
				return nil, err
			}
		}
		f.Cols = append(f.Cols, c)
	}
	return f, nil
}

// LoadRegressCase reads a regression file.
func LoadRegressCase(path string) (*RegressCase, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	var rc RegressCase
	if err := dec.Decode(&rc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Re-decode the plan with exact integer handling.
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	rc.Plan = &Plan{}
	if err := UnmarshalPlan(raw["plan"], rc.Plan); err != nil {
		return nil, err
	}
	return &rc, nil
}

// Case rebuilds the case and the expected polars result.
func (rc *RegressCase) Case() (*Case, *OracleResult, error) {
	left, err := decodeCols(rc.Left)
	if err != nil {
		return nil, nil, err
	}
	c := &Case{Plan: rc.Plan, Left: left}
	if len(rc.Right) > 0 || rc.Plan.HasRight {
		if c.Right, err = decodeCols(rc.Right); err != nil {
			return nil, nil, err
		}
	}
	pr := &OracleResult{OK: rc.Want.OK, Error: rc.Want.Error}
	if i := strings.Index(rc.Want.Error, ": "); i >= 0 {
		pr.EType = rc.Want.Error[:i]
	}
	for _, cj := range rc.Want.Cols {
		col := ResultCol{Name: cj.Name, DType: cj.Type, Values: make([]any, len(cj.Values))}
		t, terr := ParseType(cj.Type)
		for i, v := range cj.Values {
			if terr != nil {
				col.Values[i] = v
				continue
			}
			if col.Values[i], err = decodeValue(t, v); err != nil {
				return nil, nil, err
			}
		}
		pr.Cols = append(pr.Cols, col)
	}
	return c, pr, nil
}

// Save writes the regression file.
func (rc *RegressCase) Save(path string) error {
	b, err := json.MarshalIndent(rc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
