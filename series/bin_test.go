package series_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// binFixture mirrors the polars probe frame:
// [b"hello", None, b"", "héllo".encode(), b"\x00\xff"].
func binFixture(t *testing.T, mem memory.Allocator) *series.Series {
	t.Helper()
	s, err := series.FromBinary("b",
		[][]byte{[]byte("hello"), nil, {}, []byte("héllo"), {0x00, 0xff}},
		[]bool{true, false, true, true, true},
		series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type binOptBytes struct {
	v     []byte
	valid bool
}

func binB(s string) binOptBytes { return binOptBytes{[]byte(s), true} }

var binNullB = binOptBytes{}

func binCheckBinary(t *testing.T, s *series.Series, want []binOptBytes) {
	t.Helper()
	if s.DType().ID() != arrow.BINARY {
		t.Fatalf("dtype = %s, want binary", s.DType())
	}
	a := s.Chunk(0).(*array.Binary)
	if a.Len() != len(want) {
		t.Fatalf("len = %d, want %d", a.Len(), len(want))
	}
	for i, w := range want {
		if a.IsValid(i) != w.valid {
			t.Errorf("row %d valid = %v, want %v", i, a.IsValid(i), w.valid)
			continue
		}
		if w.valid && !bytes.Equal(a.Value(i), w.v) {
			t.Errorf("row %d = %q, want %q", i, a.Value(i), w.v)
		}
	}
}

type binOptStr struct {
	v     string
	valid bool
}

func binS(s string) binOptStr { return binOptStr{s, true} }

var binNullS = binOptStr{}

func binCheckString(t *testing.T, s *series.Series, want []binOptStr) {
	t.Helper()
	a, ok := s.Chunk(0).(*array.String)
	if !ok {
		t.Fatalf("dtype = %s, want str", s.DType())
	}
	if a.Len() != len(want) {
		t.Fatalf("len = %d, want %d", a.Len(), len(want))
	}
	for i, w := range want {
		if a.IsValid(i) != w.valid {
			t.Errorf("row %d valid = %v, want %v", i, a.IsValid(i), w.valid)
			continue
		}
		if w.valid && a.Value(i) != w.v {
			t.Errorf("row %d = %q, want %q", i, a.Value(i), w.v)
		}
	}
}

func binCheckBools(t *testing.T, s *series.Series, want []any) {
	t.Helper()
	a := s.Chunk(0).(*array.Boolean)
	for i, w := range want {
		if w == nil {
			if a.IsValid(i) {
				t.Errorf("row %d should be null", i)
			}
			continue
		}
		if !a.IsValid(i) || a.Value(i) != w.(bool) {
			t.Errorf("row %d = %v (valid %v), want %v", i, a.Value(i), a.IsValid(i), w)
		}
	}
}

func TestBinPredicatesParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	opt := series.WithAllocator(mem)

	cases := []struct {
		name string
		fn   func() (*series.Series, error)
		want []any
	}{
		{"contains", func() (*series.Series, error) { return s.Bin().Contains([]byte("ll"), opt) }, []any{true, nil, false, true, false}},
		{"contains_empty", func() (*series.Series, error) { return s.Bin().Contains(nil, opt) }, []any{true, nil, true, true, true}},
		{"starts_with", func() (*series.Series, error) { return s.Bin().StartsWith([]byte("he"), opt) }, []any{true, nil, false, false, false}},
		{"ends_with", func() (*series.Series, error) { return s.Bin().EndsWith([]byte("lo"), opt) }, []any{true, nil, false, true, false}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := c.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Name() != "b" || !out.DType().IsBool() {
				t.Fatalf("got %s: %s", out.Name(), out.DType())
			}
			binCheckBools(t, out, c.want)
		})
	}
}

func TestBinEncodeDecodeParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	opt := series.WithAllocator(mem)

	hexOut, err := s.Bin().Encode("hex", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer hexOut.Release()
	binCheckString(t, hexOut, []binOptStr{binS("68656c6c6f"), binNullS, binS(""), binS("68c3a96c6c6f"), binS("00ff")})

	b64, err := s.Bin().Encode("base64", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer b64.Release()
	binCheckString(t, b64, []binOptStr{binS("aGVsbG8="), binNullS, binS(""), binS("aMOpbGxv"), binS("AP8=")})

	// Round trip through bin.decode.
	back, err := b64.Str().Decode("base64", true, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	binCheckBinary(t, back, []binOptBytes{binB("hello"), binNullB, binB(""), binB("héllo"), {[]byte{0, 0xff}, true}})

	if _, err := s.Bin().Encode("rot13", opt); err == nil {
		t.Error("unknown encoding should error")
	}
}

func TestStrEncodingDecodeStrict(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	hexIn, _ := series.FromString("s", []string{"68656c6c6f", "", "", "zz", "6"},
		[]bool{true, false, true, true, true}, opt)
	defer hexIn.Release()
	if _, err := hexIn.Str().Decode("hex", true, opt); err == nil {
		t.Fatal("strict hex decode of invalid input should error")
	}
	out, err := hexIn.Str().Decode("hex", false, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	binCheckBinary(t, out, []binOptBytes{binB("hello"), binNullB, binB(""), binNullB, binNullB})

	b64In, _ := series.FromString("s", []string{"aGVsbG8=", "", "", "!!", "aGVsbG8", "aGVs\nbG8="},
		[]bool{true, false, true, true, true, true}, opt)
	defer b64In.Release()
	if _, err := b64In.Str().Decode("base64", true, opt); err == nil {
		t.Fatal("strict base64 decode of invalid input should error")
	}
	out2, err := b64In.Str().Decode("base64", false, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	binCheckBinary(t, out2, []binOptBytes{binB("hello"), binNullB, binB(""), binNullB, binNullB, binNullB})

	// Binary input to bin.decode behaves the same.
	binIn, _ := series.FromBinary("b", [][]byte{[]byte("6869"), []byte("x")}, nil, opt)
	defer binIn.Release()
	out3, err := binIn.Bin().Decode("hex", false, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out3.Release()
	binCheckBinary(t, out3, []binOptBytes{binB("hi"), binNullB})
}

func TestStrEncodingEncodeParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s, _ := series.FromString("s", []string{"hello", "", "", "héllo"}, []bool{true, false, true, true}, opt)
	defer s.Release()
	h, err := s.Str().Encode("hex", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	binCheckString(t, h, []binOptStr{binS("68656c6c6f"), binNullS, binS(""), binS("68c3a96c6c6f")})
	b, err := s.Str().Encode("base64", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	binCheckString(t, b, []binOptStr{binS("aGVsbG8="), binNullS, binS(""), binS("aMOpbGxv")})

	i, _ := series.FromInt64("i", []int64{1}, nil, opt)
	defer i.Release()
	if _, err := i.Str().Encode("hex", opt); err == nil {
		t.Error("encode on int should error")
	}
}

func TestBinSizeParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	opt := series.WithAllocator(mem)

	b, err := s.Bin().Size("b", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if !b.DType().Equal(dtype.Uint32()) {
		t.Fatalf("dtype = %s", b.DType())
	}
	ua := b.Chunk(0).(*array.Uint32)
	for i, w := range []uint32{5, 0, 0, 6, 2} {
		if i == 1 {
			if ua.IsValid(1) {
				t.Error("row 1 should be null")
			}
			continue
		}
		if ua.Value(i) != w {
			t.Errorf("size[%d] = %d, want %d", i, ua.Value(i), w)
		}
	}
	kb, err := s.Bin().Size("kb", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer kb.Release()
	fa := kb.Chunk(0).(*array.Float64)
	if fa.Value(0) != 0.0048828125 || fa.Value(3) != 0.005859375 || fa.IsValid(1) {
		t.Errorf("kb = %v", fa)
	}
	mb, _ := s.Bin().Size("mb", opt)
	defer mb.Release()
	if v := mb.Chunk(0).(*array.Float64).Value(0); v != 4.76837158203125e-06 {
		t.Errorf("mb = %v", v)
	}
	if _, err := s.Bin().Size("pb", opt); err == nil {
		t.Error("unknown unit should error")
	}
}

func TestBinGetParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	opt := series.WithAllocator(mem)

	if _, err := s.Bin().Get(1, false, opt); err == nil {
		t.Error("get out of bounds should error without null_on_oob")
	}
	check := func(idx int, want []int) {
		t.Helper()
		out, err := s.Bin().Get(idx, true, opt)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		if !out.DType().Equal(dtype.Uint8()) {
			t.Fatalf("dtype = %s", out.DType())
		}
		a := out.Chunk(0).(*array.Uint8)
		for i, w := range want {
			if w < 0 {
				if a.IsValid(i) {
					t.Errorf("get(%d)[%d] should be null", idx, i)
				}
				continue
			}
			if !a.IsValid(i) || int(a.Value(i)) != w {
				t.Errorf("get(%d)[%d] = %d, want %d", idx, i, a.Value(i), w)
			}
		}
	}
	check(1, []int{101, -1, -1, 195, 255})
	check(-1, []int{111, -1, -1, 111, 255})
}

func TestBinSliceParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	opt := series.WithAllocator(mem)

	run := func(name string, fn func() (*series.Series, error), want []binOptBytes) {
		t.Run(name, func(t *testing.T) {
			out, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			binCheckBinary(t, out, want)
		})
	}
	run("head2", func() (*series.Series, error) { return s.Bin().Head(2, opt) },
		[]binOptBytes{binB("he"), binNullB, binB(""), binB("h\xc3"), {[]byte{0, 0xff}, true}})
	run("head-2", func() (*series.Series, error) { return s.Bin().Head(-2, opt) },
		[]binOptBytes{binB("hel"), binNullB, binB(""), binB("h\xc3\xa9l"), binB("")})
	run("tail2", func() (*series.Series, error) { return s.Bin().Tail(2, opt) },
		[]binOptBytes{binB("lo"), binNullB, binB(""), binB("lo"), {[]byte{0, 0xff}, true}})
	run("tail-2", func() (*series.Series, error) { return s.Bin().Tail(-2, opt) },
		[]binOptBytes{binB("llo"), binNullB, binB(""), binB("\xa9llo"), binB("")})
	run("slice1_2", func() (*series.Series, error) { return s.Bin().Slice(1, 2, opt) },
		[]binOptBytes{binB("el"), binNullB, binB(""), binB("\xc3\xa9"), {[]byte{0xff}, true}})
	run("slice-3", func() (*series.Series, error) { return s.Bin().Slice(-3, -1, opt) },
		[]binOptBytes{binB("llo"), binNullB, binB(""), binB("llo"), {[]byte{0, 0xff}, true}})
	run("slice-10_2", func() (*series.Series, error) { return s.Bin().Slice(-10, 2, opt) },
		[]binOptBytes{binB(""), binNullB, binB(""), binB(""), binB("")})
	run("slice10_2", func() (*series.Series, error) { return s.Bin().Slice(10, 2, opt) },
		[]binOptBytes{binB(""), binNullB, binB(""), binB(""), binB("")})
	run("slice1", func() (*series.Series, error) { return s.Bin().Slice(1, -1, opt) },
		[]binOptBytes{binB("ello"), binNullB, binB(""), binB("\xc3\xa9llo"), {[]byte{0xff}, true}})
}

func TestBinReinterpret(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s, _ := series.FromBinary("b", [][]byte{{1, 2}, {1}, nil}, []bool{true, true, false}, opt)
	defer s.Release()
	le, err := s.Bin().Reinterpret(dtype.Uint16(), true, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer le.Release()
	a := le.Chunk(0).(*array.Uint16)
	if a.Value(0) != 513 || a.IsValid(1) || a.IsValid(2) {
		t.Errorf("little endian = %v", a)
	}
	be, _ := s.Bin().Reinterpret(dtype.Uint16(), false, opt)
	defer be.Release()
	if v := be.Chunk(0).(*array.Uint16).Value(0); v != 258 {
		t.Errorf("big endian = %d", v)
	}
}

func TestBinWrongDType(t *testing.T) {
	s, _ := series.FromString("s", []string{"a"}, nil)
	defer s.Release()
	if _, err := s.Bin().Contains([]byte("a")); err == nil {
		t.Error("bin op on string should error")
	}
}

func TestBinEmptySeries(t *testing.T) {
	s := series.Empty("b", dtype.Binary())
	defer s.Release()
	out, err := s.Bin().Encode("hex")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 || !out.DType().IsString() {
		t.Errorf("got %s len %d", out.DType(), out.Len())
	}
}

func TestBinSlicedInput(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := binFixture(t, mem)
	defer s.Release()
	sl, err := s.Slice(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Release()
	out, err := sl.Bin().Encode("hex", series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	binCheckString(t, out, []binOptStr{binS(""), binS("68c3a96c6c6f"), binS("00ff")})
}
