// Parity tests for DataFrame.Join against polars 1.39.3. The cases and
// the expected frames live in testdata/join_parity.json, produced by
// testdata/join_parity_gen.py: every join type with every combination
// of coalesce, nulls_equal and maintain_order, multi-key and mixed-dtype
// keys, left_on/right_on, suffixes, validation, empty sides, nested
// payload columns and multi-chunk inputs.

package dataframe_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

type joinParityCol struct {
	Name   string `json:"name"`
	DType  string `json:"dtype"`
	Values []any  `json:"values"`
}

type joinParityCase struct {
	Name          string   `json:"name"`
	Left          string   `json:"left"`
	Right         string   `json:"right"`
	How           string   `json:"how"`
	On            []string `json:"on"`
	LeftOn        []string `json:"left_on"`
	RightOn       []string `json:"right_on"`
	Suffix        string   `json:"suffix"`
	Coalesce      *bool    `json:"coalesce"`
	NullsEqual    bool     `json:"nulls_equal"`
	Validate      string   `json:"validate"`
	MaintainOrder string   `json:"maintain_order"`
	Chunked       bool     `json:"chunked"`
	Ordered       bool     `json:"ordered"`
	Want          string   `json:"want"`
	Error         string   `json:"error"`
}

type joinParityFile struct {
	Frames map[string][]joinParityCol `json:"frames"`
	Cases  []joinParityCase           `json:"cases"`
}

func TestJoinParityGenerated(t *testing.T) {
	raw, err := os.ReadFile("testdata/join_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var file joinParityFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, c := range file.Cases {
		t.Run(c.Name, func(t *testing.T) {
			alloc := testutil.NewCheckedAllocator(t)
			left := buildParityFrame(t, file.Frames[c.Left], c.Chunked, alloc)
			defer left.Release()
			right := buildParityFrame(t, file.Frames[c.Right], c.Chunked, alloc)
			defer right.Release()

			how, err := dataframe.ParseJoinType(c.How)
			if err != nil {
				t.Fatal(err)
			}
			opts := []dataframe.JoinOption{dataframe.WithJoinAllocator(alloc)}
			if c.LeftOn != nil {
				opts = append(opts, dataframe.WithJoinKeys(c.LeftOn, c.RightOn))
			}
			if c.Suffix != "" {
				opts = append(opts, dataframe.WithJoinSuffix(c.Suffix))
			}
			if c.Coalesce != nil {
				opts = append(opts, dataframe.WithJoinCoalesce(*c.Coalesce))
			}
			if c.NullsEqual {
				opts = append(opts, dataframe.WithJoinNullsEqual(true))
			}
			if c.Validate != "" {
				v, err := dataframe.ParseJoinValidation(c.Validate)
				if err != nil {
					t.Fatal(err)
				}
				opts = append(opts, dataframe.WithJoinValidate(v))
			}
			if c.MaintainOrder != "" {
				o, err := dataframe.ParseJoinOrder(c.MaintainOrder)
				if err != nil {
					t.Fatal(err)
				}
				opts = append(opts, dataframe.WithJoinMaintainOrder(o))
			}
			out, err := left.Join(context.Background(), right, c.On, how, opts...)
			if c.Error != "" {
				if err == nil {
					out.Release()
					t.Fatalf("want error %q, got none", c.Error)
				}
				want := strings.ReplaceAll(c.Error, "μs", "us")
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error mismatch\n got: %v\nwant: %s", err, want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			got := renderParityFrame(out)
			want := c.Want
			if !c.Ordered {
				got, want = sortedBody(got), sortedBody(want)
			}
			if !framerepr.Match(got, want) {
				t.Fatalf("frame mismatch\n got:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

func sortedBody(s string) string {
	lines := strings.Split(s, "\n")
	slices.Sort(lines[1:])
	return strings.Join(lines, "\n")
}

// renderParityFrame is framerepr.Frame over consolidated columns, with
// categorical cells shown as their quoted string.
func renderParityFrame(df *dataframe.DataFrame) string {
	var b strings.Builder
	cols := df.Columns()
	arrs := make([]arrow.Array, len(cols))
	for i, c := range cols {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(c.Name() + ":" + c.DType().String())
		a, err := c.Consolidated()
		if err != nil {
			panic(err)
		}
		defer a.Release()
		arrs[i] = a
	}
	for r := range df.Height() {
		b.WriteByte('\n')
		for i, a := range arrs {
			if i > 0 {
				b.WriteByte('|')
			}
			if d, ok := a.(*array.Dictionary); ok && d.IsValid(r) {
				b.WriteString(`"` + d.Dictionary().(*array.String).Value(d.GetValueIndex(r)) + `"`)
				continue
			}
			b.WriteString(framerepr.Cell(a, r))
		}
	}
	return b.String()
}

func buildParityFrame(t *testing.T, cols []joinParityCol, chunked bool, alloc memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	out := make([]*series.Series, 0, len(cols))
	for _, c := range cols {
		s := buildParityColumn(t, c, alloc)
		if chunked && s.Len() >= 2 {
			a, err := s.Consolidated()
			if err != nil {
				t.Fatal(err)
			}
			mid := s.Len() / 2
			p1 := array.NewSlice(a, 0, int64(mid))
			p2 := array.NewSlice(a, int64(mid), int64(s.Len()))
			a.Release()
			s.Release()
			s, err = series.New(c.Name, p1, p2)
			if err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, s)
	}
	df, err := dataframe.New(out...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func buildParityColumn(t *testing.T, c joinParityCol, alloc memory.Allocator) *series.Series {
	t.Helper()
	n := len(c.Values)
	valid := make([]bool, n)
	for i, v := range c.Values {
		valid[i] = v != nil
	}
	num := func(v any) float64 {
		switch x := v.(type) {
		case float64:
			return x
		case string:
			if x == "nan" {
				return math.NaN()
			}
		}
		return 0
	}
	ints := func() *series.Series {
		vals := make([]int64, n)
		for i, v := range c.Values {
			vals[i] = int64(num(v))
		}
		s, err := series.FromInt64(c.Name, vals, valid, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	cast := func(s *series.Series, to dtype.DType) *series.Series {
		defer s.Release()
		out, err := compute.Cast(context.Background(), s, to, compute.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	switch c.DType {
	case "i64":
		return ints()
	case "i32":
		return cast(ints(), dtype.Int32())
	case "i8":
		return cast(ints(), dtype.Int8())
	case "u32":
		return cast(ints(), dtype.Uint32())
	case "f64", "f32":
		vals := make([]float64, n)
		for i, v := range c.Values {
			vals[i] = num(v)
		}
		s, err := series.FromFloat64(c.Name, vals, valid, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		if c.DType == "f32" {
			return cast(s, dtype.Float32())
		}
		return s
	case "str", "cat":
		vals := make([]string, n)
		for i, v := range c.Values {
			vals[i], _ = v.(string)
		}
		s, err := series.FromString(c.Name, vals, valid, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		if c.DType == "cat" {
			return cast(s, dtype.Categorical())
		}
		return s
	case "bool":
		vals := make([]bool, n)
		for i, v := range c.Values {
			vals[i], _ = v.(bool)
		}
		s, err := series.FromBool(c.Name, vals, valid, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		return s
	case "null":
		return seriesFrom(t, c.Name, array.NewNull(n))
	case "date":
		b := array.NewDate32Builder(alloc)
		defer b.Release()
		for _, v := range c.Values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(arrow.Date32(int32(num(v))))
			}
		}
		return seriesFrom(t, c.Name, b.NewArray())
	case "datetime[us]":
		b := array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Microsecond})
		defer b.Release()
		for _, v := range c.Values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(arrow.Timestamp(int64(num(v))))
			}
		}
		return seriesFrom(t, c.Name, b.NewArray())
	case "list[i64]":
		b := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int64Builder)
		for _, v := range c.Values {
			if v == nil {
				b.AppendNull()
				continue
			}
			b.Append(true)
			for _, x := range v.([]any) {
				if x == nil {
					vb.AppendNull()
				} else {
					vb.Append(int64(num(x)))
				}
			}
		}
		return seriesFrom(t, c.Name, b.NewArray())
	case "struct":
		st := arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		)
		b := array.NewStructBuilder(alloc, st)
		defer b.Release()
		ab := b.FieldBuilder(0).(*array.Int64Builder)
		bb := b.FieldBuilder(1).(*array.StringBuilder)
		for _, v := range c.Values {
			if v == nil {
				b.AppendNull()
				continue
			}
			b.Append(true)
			m := v.(map[string]any)
			if m["a"] == nil {
				ab.AppendNull()
			} else {
				ab.Append(int64(num(m["a"])))
			}
			if m["b"] == nil {
				bb.AppendNull()
			} else {
				bb.Append(m["b"].(string))
			}
		}
		return seriesFrom(t, c.Name, b.NewArray())
	}
	t.Fatalf("unknown dtype %q", c.DType)
	return nil
}

func seriesFrom(t *testing.T, name string, a arrow.Array) *series.Series {
	t.Helper()
	s, err := series.New(name, a)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
