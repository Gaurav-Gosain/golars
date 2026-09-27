package series_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// jsonStrSeries builds a String series where the value "<null>" marks
// a null row.
func jsonStrSeries(t *testing.T, opt series.Option, vals ...string) *series.Series {
	t.Helper()
	valid := make([]bool, len(vals))
	for i, v := range vals {
		valid[i] = v != "<null>"
	}
	s, err := series.FromString("j", vals, valid, opt)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// jsonRender prints every row of s with arrow's ValueStr, "null" for
// nulls, so nested results compare as one string.
func jsonRender(s *series.Series) string {
	a := s.Chunk(0)
	parts := make([]string, a.Len())
	for i := range parts {
		if a.IsNull(i) {
			parts[i] = "null"
		} else {
			parts[i] = a.ValueStr(i)
		}
	}
	return strings.Join(parts, " | ")
}

func TestJSONDecodeParity(t *testing.T) {
	st := func(fields ...dtype.StructField) dtype.DType { return dtype.Struct(fields...) }
	f := func(name string, dt dtype.DType) dtype.StructField { return dtype.StructField{Name: name, DType: dt} }
	cases := []struct {
		name string
		in   []string
		to   dtype.DType
		want string // jsonRender of the result
	}{
		{"struct_basic", []string{`{"a":1,"b":"x"}`, "<null>", `{"a":2.5}`, `{"b":null,"c":[1,2]}`, `null`},
			st(f("a", dtype.Int64()), f("b", dtype.String())),
			`{"a":1,"b":"x"} | null | {"a":2,"b":null} | {"a":null,"b":null} | null`},
		{"struct_missing_fields", []string{`{"a":1}`, `{}`},
			st(f("a", dtype.Int32()), f("z", dtype.Float64())),
			`{"a":1,"z":null} | {"a":null,"z":null}`},
		{"struct_number_to_string", []string{`{"a":1}`, `{"a":"x"}`, `{"a":2.0}`},
			st(f("a", dtype.String())), `{"a":"1"} | {"a":"x"} | {"a":"2"}`},
		{"struct_truncate", []string{`{"a":1}`, `{"a":1.5}`, `{"a":true}`},
			st(f("a", dtype.Int64())), `{"a":1} | {"a":1} | {"a":1}`},
		{"duplicate_key_first_wins", []string{`{"a":1,"a":2}`}, st(f("a", dtype.Int64())), `{"a":1}`},
		{"float", []string{"1", "2", "true", "1e3"}, dtype.Float64(), "1 | 2 | 1 | 1000"},
		{"int_trunc", []string{"-2.7", "1e3", "  7  "}, dtype.Int64(), "-2 | 1000 | 7"},
		{"int_overflow_null", []string{"12345678901234567890", "300", "-1"}, dtype.Int64(), "null | 300 | -1"},
		{"uint8_overflow", []string{"1", "300", "-1"}, dtype.Uint8(), "1 | null | null"},
		{"uint64_big", []string{"12345678901234567890"}, dtype.Uint64(), "12345678901234567890"},
		{"int32_overflow", []string{"3000000000", "1"}, dtype.Int32(), "null | 1"},
		{"float32", []string{"1.5"}, dtype.Float32(), "1.5"},
		{"string_display", []string{"2.5", "1e20", "-0.0", "1.0", "100", "0.1", "true", "null", `"x"`},
			dtype.String(), "2.5 | 100000000000000000000 | -0 | 1 | 100 | 0.1 | true | null | x"},
		{"string_unicode_escape", []string{`"héllo\né"`, `"😀"`}, dtype.String(), "héllo\né | \U0001F600"},
		{"list_scalar_wraps", []string{`[1]`, `2`, `[]`, "<null>", `[null,3]`}, dtype.List(dtype.Int64()),
			"[1] | [2] | [] | null | [null,3]"},
		{"list_of_list", []string{`[[1],[2,3],null]`}, dtype.List(dtype.List(dtype.Int64())), "[[1],[2,3],null]"},
		{"list_of_string_mixed", []string{`[1, "x"]`}, dtype.List(dtype.String()), `["1","x"]`},
		{"list_of_struct", []string{`[{"a":1},{"a":2,"b":3},null]`}, dtype.List(st(f("a", dtype.Int64()))),
			`[{"a":1},{"a":2},null]`},
		{"nested_struct", []string{`{"a":{"b":{"c":"d"}}}`, `{"a":null}`},
			st(f("a", st(f("b", st(f("c", dtype.String())))))),
			`{"a":{"b":{"c":"d"}}} | {"a":null}`},
		{"blank_is_null", []string{"", "  ", "1"}, dtype.Int64(), "null | null | 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			opt := series.WithAllocator(mem)
			s := jsonStrSeries(t, opt, c.in...)
			defer s.Release()
			out, err := s.Str().JSONDecode(c.to, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if !out.DType().Equal(c.to) || out.Name() != "j" {
				t.Fatalf("got %s %s, want %s", out.Name(), out.DType(), c.to)
			}
			if got := jsonRender(out); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}

func TestJSONDecodeErrors(t *testing.T) {
	st := dtype.Struct(dtype.StructField{Name: "a", DType: dtype.Int64()})
	cases := []struct {
		name string
		in   string
		to   dtype.DType
	}{
		{"garbage", "garbage", dtype.Int64()},
		{"trailing", "1 2", dtype.Int64()},
		{"string_as_int", `"1"`, dtype.Int64()},
		{"string_as_float", `"1.5"`, dtype.Float64()},
		{"number_as_bool", `1`, dtype.Bool()},
		{"string_as_bool", `"true"`, dtype.Bool()},
		{"object_as_list", `{"a":1}`, dtype.List(dtype.Int64())},
		{"array_as_struct", `[1,2]`, st},
		{"object_as_string", `{"x":1}`, dtype.String()},
		{"bad_field_type", `{"a":"x"}`, st},
		{"unterminated", `{"a":1`, st},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			opt := series.WithAllocator(mem)
			s := jsonStrSeries(t, opt, c.in)
			defer s.Release()
			out, err := s.Str().JSONDecode(c.to, opt)
			if err == nil {
				out.Release()
				t.Fatalf("expected error for %q as %s", c.in, c.to)
			}
		})
	}
	s := jsonStrSeries(t, series.WithAllocator(testutil.NewCheckedAllocator(t)), "1")
	defer s.Release()
	if _, err := s.Str().JSONDecode(dtype.Date()); err == nil {
		t.Error("unsupported target dtype should error")
	}
}

func TestJSONPathMatchParity(t *testing.T) {
	docs := []string{
		`{"a":{"b":"x","n":1.50,"i":12,"t":true,"z":null,"l":[1,"two",{"k":3}],"o":{"p":[1, 2]}}}`,
		"<null>",
		`{"a":[{"b":1},{"b":2}]}`,
		`garbage`,
		``,
		`[10,20]`,
		`{"a b":5, "é":"u"}`,
	}
	// Expected values come from polars 1.39.3; "null" means a null row.
	cases := map[string]string{
		"$.a.b":       "x | null | null | null | null | null | null",
		"$.a.n":       "1.5 | null | null | null | null | null | null",
		"$.a.i":       "12 | null | null | null | null | null | null",
		"$.a.t":       "true | null | null | null | null | null | null",
		"$.a.z":       "null | null | null | null | null | null | null",
		"$.a.l":       `[1,"two",{"k":3}] | null | null | null | null | null | null`,
		"$.a.l[0]":    "1 | null | null | null | null | null | null",
		"$.a.l[1]":    "two | null | null | null | null | null | null",
		"$.a.l[2]":    `{"k":3} | null | null | null | null | null | null`,
		"$.a.l[-1]":   `{"k":3} | null | null | null | null | null | null`,
		"$.a.o":       `{"p":[1,2]} | null | null | null | null | null | null`,
		"$.a[0].b":    "null | null | 1 | null | null | null | null",
		"$.a[*].b":    "null | null | 1 | null | null | null | null",
		"$.a.l[*]":    "1 | null | null | null | null | null | null",
		"$['a']['b']": "x | null | null | null | null | null | null",
		"$[1]":        "null | null | null | null | null | 20 | null",
		"$.missing":   "null | null | null | null | null | null | null",
		"$": `{"a":{"b":"x","n":1.5,"i":12,"t":true,"z":null,"l":[1,"two",{"k":3}],"o":{"p":[1,2]}}} | null | ` +
			`{"a":[{"b":1},{"b":2}]} | null | null | [10,20] | {"a b":5,"é":"u"}`,
		"$['a b']":   "null | null | null | null | null | null | 5",
		"$.é":        "null | null | null | null | null | null | u",
		"$..b":       "x | null | 1 | null | null | null | null",
		"$.a.*":      `x | null | {"b":1} | null | null | null | null`,
		"$.a.l[0:2]": "1 | null | null | null | null | null | null",
	}
	for path, want := range cases {
		t.Run(path, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			opt := series.WithAllocator(mem)
			s := jsonStrSeries(t, opt, docs...)
			defer s.Release()
			out, err := s.Str().JSONPathMatch(path, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if !out.DType().IsString() {
				t.Fatalf("dtype = %s", out.DType())
			}
			if got := jsonRender(out); got != want {
				t.Errorf("got  %s\nwant %s", got, want)
			}
		})
	}
	s := jsonStrSeries(t, series.WithAllocator(testutil.NewCheckedAllocator(t)), "{}")
	defer s.Release()
	for _, bad := range []string{"a.b", "$.[", "$x"} {
		if _, err := s.Str().JSONPathMatch(bad); err == nil {
			t.Errorf("path %q should fail to compile", bad)
		}
	}
}

func TestJSONPathMatchNumberFormatting(t *testing.T) {
	docs := []string{`{"a":2.0}`, `{"a":1e20}`, `{"a":1E3}`, `{"a":-0.0}`, `{"a":1.0e-7}`,
		`{"a":100000000000000000000}`, `{"a":[2.0, 1e20, 1.50, 1E3]}`, `{"a":"é\n"}`,
		`{"a":["é\n"]}`, `{"a":-5}`, `{"a":0.10}`, `{"a":12345678901234567890}`, `{"a":{"b":"x\"y"}}`}
	want := []string{"2.0", "1e+20", "1000.0", "-0.0", "1e-7", "1e+20", "[2.0,1e+20,1.5,1000.0]",
		"é\n", `["é\n"]`, "-5", "0.1", "12345678901234567890", `{"b":"x\"y"}`}
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s := jsonStrSeries(t, opt, docs...)
	defer s.Release()
	out, err := s.Str().JSONPathMatch("$.a", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	a := out.Chunk(0).(*array.String)
	for i, w := range want {
		if a.Value(i) != w {
			t.Errorf("row %d (%s) = %q, want %q", i, docs[i], a.Value(i), w)
		}
	}
}

func TestStructJSONEncodeParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	// Build the struct through json_decode so the test stays short.
	f := func(name string, dt dtype.DType) dtype.StructField { return dtype.StructField{Name: name, DType: dt} }
	dt := dtype.Struct(
		f("a", dtype.Int64()), f("b", dtype.String()), f("c", dtype.Float64()), f("d", dtype.Bool()),
		f("e", dtype.List(dtype.Int64())), f("f", dtype.Struct(f("g", dtype.Int64()))),
	)
	src := jsonStrSeries(t, opt,
		`{"a":1,"b":"x","c":1.5,"d":true,"e":[1,2],"f":{"g":null}}`,
		"<null>",
		`{"a":null,"b":"q\"\né<\u0001\t /\\","c":2.0,"d":false,"e":[],"f":null}`,
	)
	defer src.Release()
	st, err := src.Str().JSONDecode(dt, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	out, err := st.Struct().JSONEncode(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	want := []string{
		`{"a":1,"b":"x","c":1.5,"d":true,"e":[1,2],"f":{"g":null}}`,
		`null`,
		`{"a":null,"b":"q\"\né<\u0001\t` + " " + `/\\","c":2.0,"d":false,"e":[],"f":null}`,
	}
	a := out.Chunk(0).(*array.String)
	if a.NullN() != 0 || out.Name() != "j" {
		t.Fatalf("null struct rows must encode as the text null; name %s", out.Name())
	}
	for i, w := range want {
		if a.Value(i) != w {
			t.Errorf("row %d = %s\nwant    %s", i, a.Value(i), w)
		}
	}
}

func TestStructJSONEncodeFloats(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	vals := []float64{2.0, 1e20, 1e15, 1e16, 123456789.123, 1e-5, 1e-6, 1e-7, 0.001, 1.5e300, 5e-324,
		12345678901234567.0, 0.30000000000000004, 3.14e-10, 100.0, 1234567.0, 0, 0}
	want := []string{"2.0", "1e+20", "1000000000000000.0", "1e+16", "123456789.123", "0.00001", "1e-6", "1e-7",
		"0.001", "1.5e+300", "5e-324", "1.2345678901234568e+16", "0.30000000000000004", "3.14e-10", "100.0",
		"1234567.0", "-0.0", "0.0"}
	vals[16] = negZero()
	x, _ := series.FromFloat64("x", vals, nil, opt)
	defer x.Release()
	st := jsonStructOf(t, x)
	defer st.Release()
	out, err := st.Struct().JSONEncode(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	a := out.Chunk(0).(*array.String)
	for i, w := range want {
		if got := a.Value(i); got != `{"x":`+w+`}` {
			t.Errorf("%v: got %s want %s", vals[i], got, w)
		}
	}

	f32, _ := series.FromFloat32("x", []float32{1, 0.1, 1e20, 3.4e38, 1e-7}, nil, opt)
	defer f32.Release()
	st32 := jsonStructOf(t, f32)
	defer st32.Release()
	out32, err := st32.Struct().JSONEncode(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out32.Release()
	want32 := []string{"1.0", "0.1", "1e+20", "3.4e+38", "1e-7"}
	a32 := out32.Chunk(0).(*array.String)
	for i, w := range want32 {
		if got := a32.Value(i); got != `{"x":`+w+`}` {
			t.Errorf("f32 row %d: got %s want %s", i, got, w)
		}
	}
}

func negZero() float64 {
	z := 0.0
	return -z
}

// jsonStructOf wraps a single column into a one-field struct Series.
func jsonStructOf(t *testing.T, s *series.Series) *series.Series {
	t.Helper()
	child := s.Chunk(0)
	field := arrow.Field{Name: s.Name(), Type: child.DataType(), Nullable: true}
	arr, err := array.NewStructArrayWithFields([]arrow.Array{child}, []arrow.Field{field})
	if err != nil {
		t.Fatal(err)
	}
	out, err := series.New("s", arr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
