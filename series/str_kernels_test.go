package series

import (
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// strKernelCase is one input shape for the whole-buffer string kernels.
type strKernelCase struct {
	name  string
	vals  []string
	valid []bool
}

func randStrKernelVals(r *rand.Rand, n int, alphabet []string) []string {
	out := make([]string, n)
	for i := range out {
		var b strings.Builder
		for range r.IntN(12) {
			b.WriteString(alphabet[r.IntN(len(alphabet))])
		}
		out[i] = b.String()
	}
	return out
}

func strKernelCases() []strKernelCase {
	r := rand.New(rand.NewPCG(7, 9))
	ascii := []string{"a", "b", "t", "ta", "Z", "_", "0", "@", "[", "`", "{", "z", "A"}
	uni := append([]string{"é", "日本", "ß", "Σ", "İ"}, ascii...)
	allNull := make([]bool, 50)
	mixedValid := func(n int) []bool {
		v := make([]bool, n)
		for i := range v {
			v[i] = r.IntN(4) != 0
		}
		return v
	}
	// Large enough to cross strParallelMinBytes and fan out.
	bigN := 300_000
	bigASCII := randStrKernelVals(r, bigN, ascii)
	bigUni := randStrKernelVals(r, bigN, ascii)
	bigUni[bigN-3] = "tail é non-ascii"
	return []strKernelCase{
		{name: "empty", vals: []string{}},
		{name: "all-null", vals: make([]string, 50), valid: allNull},
		{name: "ascii", vals: randStrKernelVals(r, 1000, ascii)},
		{name: "ascii-nulls", vals: randStrKernelVals(r, 1000, ascii), valid: mixedValid(1000)},
		{name: "unicode", vals: randStrKernelVals(r, 1000, uni)},
		{name: "unicode-nulls", vals: randStrKernelVals(r, 1000, uni), valid: mixedValid(1000)},
		{name: "boundary", vals: []string{"", "t", "a", "xt", "ax", "ta", "", "tta", "at"}},
		{name: "big-ascii", vals: bigASCII, valid: mixedValid(bigN)},
		{name: "big-unicode-tail", vals: bigUni},
	}
}

func checkStrings(t *testing.T, label string, got *Series, want []string, valid []bool) {
	t.Helper()
	arr := got.Chunk(0).(*array.String)
	if arr.Len() != len(want) {
		t.Fatalf("%s: len %d want %d", label, arr.Len(), len(want))
	}
	for i := range want {
		isValid := valid == nil || valid[i]
		if arr.IsValid(i) != isValid {
			t.Fatalf("%s: row %d validity %v want %v", label, i, arr.IsValid(i), isValid)
		}
		if isValid && arr.Value(i) != want[i] {
			t.Fatalf("%s: row %d = %q want %q", label, i, arr.Value(i), want[i])
		}
	}
}

func checkInt64s(t *testing.T, label string, got *Series, want []int64, valid []bool) {
	t.Helper()
	arr := got.Chunk(0).(*array.Int64)
	if arr.Len() != len(want) {
		t.Fatalf("%s: len %d want %d", label, arr.Len(), len(want))
	}
	for i := range want {
		isValid := valid == nil || valid[i]
		if arr.IsValid(i) != isValid {
			t.Fatalf("%s: row %d validity mismatch", label, i)
		}
		if isValid && arr.Value(i) != want[i] {
			t.Fatalf("%s: row %d = %d want %d", label, i, arr.Value(i), want[i])
		}
	}
}

func checkBools(t *testing.T, label string, got *Series, want []bool, valid []bool) {
	t.Helper()
	arr := got.Chunk(0).(*array.Boolean)
	if arr.Len() != len(want) {
		t.Fatalf("%s: len %d want %d", label, arr.Len(), len(want))
	}
	for i := range want {
		isValid := valid == nil || valid[i]
		if arr.IsValid(i) != isValid {
			t.Fatalf("%s: row %d validity mismatch", label, i)
		}
		if arr.Value(i) != (isValid && want[i]) {
			t.Fatalf("%s: row %d = %v want %v", label, i, arr.Value(i), isValid && want[i])
		}
	}
}

func mapWant[T any](vals []string, f func(string) T) []T {
	out := make([]T, len(vals))
	for i, v := range vals {
		out[i] = f(v)
	}
	return out
}

// buildStrSeries builds the series three ways: a plain array, a slice
// of a larger array (non-zero offsets), and two chunks.
func buildStrSeries(t *testing.T, mem memory.Allocator, c strKernelCase) map[string]*Series {
	t.Helper()
	out := map[string]*Series{}
	plain, err := FromString("s", c.vals, c.valid, WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	out["plain"] = plain

	pad := []string{"pre-é", "pre"}
	vals := append(append(append([]string{}, pad...), c.vals...), "post")
	var valid []bool
	if c.valid != nil {
		valid = append(append([]bool{true, false}, c.valid...), true)
	}
	b := array.NewStringBuilder(mem)
	b.AppendValues(vals, valid)
	full := b.NewStringArray()
	b.Release()
	sl := array.NewSlice(full, int64(len(pad)), int64(len(pad)+len(c.vals)))
	full.Release()
	sliced, err := New("s", sl)
	if err != nil {
		t.Fatal(err)
	}
	out["sliced"] = sliced

	if len(c.vals) >= 2 {
		h := len(c.vals) / 2
		var v1, v2 []bool
		if c.valid != nil {
			v1, v2 = c.valid[:h], c.valid[h:]
		}
		a1, _ := FromString("s", c.vals[:h], v1, WithAllocator(mem))
		a2, _ := FromString("s", c.vals[h:], v2, WithAllocator(mem))
		c1, c2 := a1.Chunk(0), a2.Chunk(0)
		c1.Retain()
		c2.Retain()
		a1.Release()
		a2.Release()
		multi, err := New("s", []arrow.Array{c1, c2}...)
		if err != nil {
			t.Fatal(err)
		}
		out["chunked"] = multi
	}
	return out
}

func TestWholeBufferStringKernels(t *testing.T) {
	t.Parallel()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	for _, c := range strKernelCases() {
		for shape, s := range buildStrSeries(t, mem, c) {
			label := c.name + "/" + shape
			ops := s.Str()

			up, err := ops.ToUppercase(WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			checkStrings(t, label+"/upper", up, mapWant(c.vals, strings.ToUpper), c.valid)
			up.Release()

			lo, err := ops.ToLowercase(WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			checkStrings(t, label+"/lower", lo, mapWant(c.vals, strings.ToLower), c.valid)
			lo.Release()

			lb, err := ops.LenBytes(WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			checkInt64s(t, label+"/len_bytes", lb, mapWant(c.vals, func(v string) int64 { return int64(len(v)) }), c.valid)
			lb.Release()

			lc, err := ops.LenChars(WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			checkInt64s(t, label+"/len_chars", lc, mapWant(c.vals, func(v string) int64 { return int64(utf8.RuneCountInString(v)) }), c.valid)
			lc.Release()

			for _, needle := range []string{"t", "ta", "at", "tt", "é", "日本", "Za", "@[", "missing"} {
				got, err := ops.Contains(needle, WithAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				checkBools(t, label+"/contains("+needle+")", got, mapWant(c.vals, func(v string) bool { return strings.Contains(v, needle) }), c.valid)
				got.Release()
			}
			s.Release()
		}
	}
}

// TestContainsNullRowWithBytes covers a null slot whose offsets span
// real bytes: the match must not leak into the value bitmap.
func TestContainsNullRowWithBytes(t *testing.T) {
	t.Parallel()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := memory.NewBufferBytes([]byte("xtaxxta"))
	offs := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 3, 5, 7}))
	validity := memory.NewBufferBytes([]byte{0b101}) // row 1 null
	data := array.NewData(arrow.BinaryTypes.String, 3, []*memory.Buffer{validity, offs, values}, nil, 1, 0)
	arr := array.NewStringData(data)
	data.Release()
	s, err := New("s", arr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()

	got, err := s.Str().Contains("ta", WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	checkBools(t, "null-with-bytes", got, []bool{true, false, true}, []bool{true, false, true})
}

func TestFoldAsciiSWARAllBytes(t *testing.T) {
	t.Parallel()
	src := make([]byte, 0, 128+7)
	for b := range 128 {
		src = append(src, byte(b))
	}
	src = append(src, "tail!zZ"...)
	for _, upper := range []bool{true, false} {
		dst := make([]byte, len(src))
		if !foldAsciiSWAR(src, dst, upper) {
			t.Fatal("ASCII input reported as non-ASCII")
		}
		want := strings.ToLower(string(src))
		if upper {
			want = strings.ToUpper(string(src))
		}
		if string(dst) != want {
			t.Fatalf("upper=%v: fold mismatch", upper)
		}
	}
	if foldAsciiSWAR([]byte("abcdefgh\x80"), make([]byte, 9), true) {
		t.Fatal("non-ASCII tail not detected")
	}
	if foldAsciiSWAR([]byte("abc\xc3\xa9defgh"), make([]byte, 11), true) {
		t.Fatal("non-ASCII word not detected")
	}
}
