package csv_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/schema"
)

func readString(t *testing.T, input string, opts ...iocsv.Option) *dataframe.DataFrame {
	t.Helper()
	mem := testutil.NewCheckedAllocator(t)
	opts = append([]iocsv.Option{iocsv.WithAllocator(mem)}, opts...)
	df, err := iocsv.Read(context.Background(), strings.NewReader(input), opts...)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	t.Cleanup(df.Release)
	return df
}

// cell renders row i of a column as text, "<null>" for nulls.
func cell(t *testing.T, df *dataframe.DataFrame, name string, i int) string {
	t.Helper()
	s, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range s.Chunks() {
		if i >= ch.Len() {
			i -= ch.Len()
			continue
		}
		if ch.IsNull(i) {
			return "<null>"
		}
		return ch.ValueStr(i)
	}
	t.Fatalf("row %d out of range", i)
	return ""
}

func dtypeOf(t *testing.T, df *dataframe.DataFrame, name string) dtype.DType {
	t.Helper()
	s, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	return s.DType()
}

func TestNativeQuotedFields(t *testing.T) {
	t.Parallel()
	input := "id,text,n\n" +
		"1,\"hello, world\",2\n" +
		"2,\"say \"\"hi\"\"\",3\n" +
		"3,\"line1\nline2\",4\n" +
		"4,\"\",5\n" +
		"5,plain,6\n"
	df := readString(t, input)
	if df.Height() != 5 {
		t.Fatalf("height = %d", df.Height())
	}
	want := []string{"hello, world", `say "hi"`, "line1\nline2", "", "plain"}
	for i, w := range want {
		if got := cell(t, df, "text", i); got != w {
			t.Errorf("text[%d] = %q, want %q", i, got, w)
		}
	}
	if got := cell(t, df, "n", 4); got != "6" {
		t.Errorf("n[4] = %q", got)
	}
}

func TestNativeCRLF(t *testing.T) {
	t.Parallel()
	input := "a,b\r\n1,x\r\n2,\"y\r\nz\"\r\n3,w"
	df := readString(t, input)
	if df.Height() != 3 {
		t.Fatalf("height = %d", df.Height())
	}
	if got := cell(t, df, "b", 0); got != "x" {
		t.Errorf("b[0] = %q", got)
	}
	if got := cell(t, df, "b", 1); got != "y\nz" {
		t.Errorf("b[1] = %q, want CRLF folded to LF", got)
	}
	if got := cell(t, df, "a", 2); got != "3" {
		t.Errorf("a[2] = %q", got)
	}
	if !dtypeOf(t, df, "a").Equal(dtype.Int64()) {
		t.Errorf("a dtype = %s", dtypeOf(t, df, "a"))
	}
}

func TestNativeEmptyAndHeaderOnly(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	if _, err := iocsv.Read(context.Background(), strings.NewReader(""), iocsv.WithAllocator(mem)); err == nil {
		t.Error("empty input should error")
	}
	df := readString(t, "a,b,c\n")
	if df.Height() != 0 || df.Width() != 3 {
		t.Fatalf("shape = (%d,%d)", df.Height(), df.Width())
	}
	if !slices.Equal(df.Schema().Names(), []string{"a", "b", "c"}) {
		t.Errorf("names = %v", df.Schema().Names())
	}
	sch, _ := schema.New(schema.Field{Name: "x", DType: dtype.Int64()})
	df2 := readString(t, "", iocsv.WithSchema(sch), iocsv.WithHasHeader(false))
	if df2.Height() != 0 || df2.Width() != 1 {
		t.Errorf("schema empty shape = (%d,%d)", df2.Height(), df2.Width())
	}
}

func TestNativeNulls(t *testing.T) {
	t.Parallel()
	input := "i,f,s,b\n1,1.5,x,true\n,,,\n3,NA,NA,false\n"
	df := readString(t, input, iocsv.WithNullValues("NA"))
	checks := map[string][]string{
		"i": {"1", "<null>", "3"},
		"f": {"1.5", "<null>", "<null>"},
		"s": {"x", "", "<null>"},
		"b": {"true", "<null>", "false"},
	}
	for col, want := range checks {
		for i, w := range want {
			if got := cell(t, df, col, i); got != w {
				t.Errorf("%s[%d] = %q, want %q", col, i, got, w)
			}
		}
	}
	if !dtypeOf(t, df, "f").Equal(dtype.Float64()) || !dtypeOf(t, df, "b").Equal(dtype.Bool()) {
		t.Errorf("dtypes f=%s b=%s", dtypeOf(t, df, "f"), dtypeOf(t, df, "b"))
	}
}

func TestNativeRaggedRows(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	for _, in := range []string{"a,b\n1,2\n3\n", "a,b\n1,2\n3,4,5\n"} {
		_, err := iocsv.Read(context.Background(), strings.NewReader(in), iocsv.WithAllocator(mem))
		if err == nil || !strings.Contains(err.Error(), "line 3") {
			t.Errorf("input %q: err = %v, want a line 3 error", in, err)
		}
	}
	// Ragged row far past the inference sample, inside a parallel chunk.
	var sb strings.Builder
	sb.WriteString("a,b\n")
	for i := range 200_000 {
		fmt.Fprintf(&sb, "%d,%d\n", i, i)
	}
	sb.WriteString("1\n")
	_, err := iocsv.Read(context.Background(), strings.NewReader(sb.String()), iocsv.WithAllocator(mem))
	if err == nil || !strings.Contains(err.Error(), "line 200002") {
		t.Errorf("err = %v, want a line 200002 error", err)
	}
}

func TestNativeWidening(t *testing.T) {
	t.Parallel()
	// The float and the string appear after the 100-row sample.
	var sb strings.Builder
	sb.WriteString("i,s\n")
	for i := range 150_000 {
		switch i {
		case 120_000:
			sb.WriteString("2.5,abc\n")
		default:
			fmt.Fprintf(&sb, "%d,%d\n", i, i)
		}
	}
	df := readString(t, sb.String())
	if !dtypeOf(t, df, "i").Equal(dtype.Float64()) {
		t.Errorf("i dtype = %s, want f64", dtypeOf(t, df, "i"))
	}
	if !dtypeOf(t, df, "s").Equal(dtype.String()) {
		t.Errorf("s dtype = %s, want str", dtypeOf(t, df, "s"))
	}
	if got := cell(t, df, "i", 120_000); got != "2.5" {
		t.Errorf("i[120000] = %q", got)
	}
	if got := cell(t, df, "s", 120_000); got != "abc" {
		t.Errorf("s[120000] = %q", got)
	}
	if got := cell(t, df, "s", 7); got != "7" {
		t.Errorf("s[7] = %q", got)
	}
	// Mixed int and float inside the sample promote to float.
	df2 := readString(t, "x\n1\n2.5\n3\n")
	if !dtypeOf(t, df2, "x").Equal(dtype.Float64()) || cell(t, df2, "x", 0) != "1" {
		t.Errorf("x dtype = %s", dtypeOf(t, df2, "x"))
	}
}

func TestNativeExplicitSchemaParseError(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	sch, _ := schema.New(schema.Field{Name: "x", DType: dtype.Int64()})
	_, err := iocsv.Read(context.Background(), strings.NewReader("x\n1\nfoo\n"),
		iocsv.WithAllocator(mem), iocsv.WithSchema(sch))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("err = %v", err)
	}
}

func TestNativeInferredTypes(t *testing.T) {
	t.Parallel()
	input := "i,f,b,d,ts,s,big\n" +
		"1,1.5,true,2024-01-02,2026-04-19T09:30:00Z,x,99999999999999999999\n" +
		"-2,-3e2,False,1999-12-31,2026-04-19T09:31:00Z,y,1\n"
	df := readString(t, input)
	want := map[string]arrow.DataType{
		"i":   arrow.PrimitiveTypes.Int64,
		"f":   arrow.PrimitiveTypes.Float64,
		"b":   arrow.FixedWidthTypes.Boolean,
		"d":   arrow.FixedWidthTypes.Date32,
		"ts":  &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"},
		"s":   arrow.BinaryTypes.String,
		"big": arrow.PrimitiveTypes.Float64,
	}
	for name, dt := range want {
		got := dtypeOf(t, df, name).Arrow()
		if !arrow.TypeEqual(got, dt) {
			t.Errorf("%s dtype = %s, want %s", name, got, dt)
		}
	}
	if got := cell(t, df, "d", 1); got != "1999-12-31" {
		t.Errorf("d[1] = %q", got)
	}
	if got := cell(t, df, "f", 1); got != "-300" {
		t.Errorf("f[1] = %q", got)
	}
	if got := cell(t, df, "b", 1); got != "false" {
		t.Errorf("b[1] = %q", got)
	}
}

func TestNativeNonASCII(t *testing.T) {
	t.Parallel()
	df := readString(t, "name,city\nJürgen,Zürich\n李,北京\n\"émile, jr\",Montréal\n")
	want := []string{"Jürgen", "李", "émile, jr"}
	for i, w := range want {
		if got := cell(t, df, "name", i); got != w {
			t.Errorf("name[%d] = %q, want %q", i, got, w)
		}
	}
}

func TestNativeInvalidUTF8Errors(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	_, err := iocsv.Read(context.Background(), strings.NewReader("a,b\n1,ok\n2,\xff\xfe\n"), iocsv.WithAllocator(mem))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("err = %v, want a line 3 UTF-8 error", err)
	}
}

func TestNativeColumnsAndNoHeader(t *testing.T) {
	t.Parallel()
	df := readString(t, "a,b,c\n1,x,2.5\n3,y,4.5\n", iocsv.WithColumns("c", "a"))
	if !slices.Equal(df.Schema().Names(), []string{"c", "a"}) {
		t.Fatalf("names = %v", df.Schema().Names())
	}
	if cell(t, df, "c", 1) != "4.5" || cell(t, df, "a", 1) != "3" {
		t.Errorf("values wrong")
	}
	df2 := readString(t, "1,x\n2,y\n", iocsv.WithHasHeader(false))
	if !slices.Equal(df2.Schema().Names(), []string{"f0", "f1"}) || df2.Height() != 2 {
		t.Errorf("no header: names=%v height=%d", df2.Schema().Names(), df2.Height())
	}
	mem := testutil.NewCheckedAllocator(t)
	if _, err := iocsv.Read(context.Background(), strings.NewReader("a\n1\n"),
		iocsv.WithAllocator(mem), iocsv.WithColumns("zz")); err == nil {
		t.Error("unknown column should error")
	}
}

func TestNativeBlankLinesAndBOM(t *testing.T) {
	t.Parallel()
	df := readString(t, "\xef\xbb\xbfa,b\n\n1,2\n\r\n3,4\n\n")
	if df.Height() != 2 || df.Schema().Names()[0] != "a" {
		t.Errorf("height=%d names=%v", df.Height(), df.Schema().Names())
	}
}

// TestNativeLargeMatchesEncodingCSV compares a multi-chunk parallel
// parse against encoding/csv on random data with quotes, embedded
// newlines, nulls and non-ASCII text.
func TestNativeLargeMatchesEncodingCSV(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(5, 6))
	words := []string{"alpha", "beta, gamma", "say \"x\"", "multi\nline", "Zürich", "", "plain"}
	const rows = 120_000
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"id", "v", "f", "s", "flag"})
	for i := range rows {
		v := ""
		if r.IntN(10) != 0 {
			v = strconv.FormatInt(r.Int64N(1<<40)-(1<<39), 10)
		}
		f := strconv.FormatFloat(r.NormFloat64()*1e3, 'g', -1, 64)
		s := words[r.IntN(len(words))] + strconv.Itoa(r.IntN(50))
		_ = w.Write([]string{strconv.Itoa(i), v, f, s, strconv.FormatBool(r.IntN(2) == 0)})
	}
	w.Flush()
	raw := buf.Bytes()

	ref, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	df := readString(t, string(raw))
	if df.Height() != rows {
		t.Fatalf("height = %d", df.Height())
	}
	cols := []string{"id", "v", "f", "s", "flag"}
	get := map[string]func(int) (string, bool){}
	for _, name := range cols {
		s, _ := df.Column(name)
		if s.NumChunks() != 1 {
			t.Errorf("%s has %d chunks, want 1", name, s.NumChunks())
		}
		arr := s.Chunk(0)
		get[name] = func(i int) (string, bool) {
			if arr.IsNull(i) {
				return "", false
			}
			switch a := arr.(type) {
			case *array.Int64:
				return strconv.FormatInt(a.Value(i), 10), true
			case *array.Float64:
				return strconv.FormatFloat(a.Value(i), 'g', -1, 64), true
			case *array.String:
				return a.Value(i), true
			case *array.Boolean:
				return strconv.FormatBool(a.Value(i)), true
			}
			t.Fatalf("unexpected array %T", arr)
			return "", false
		}
	}
	for i := range rows {
		for c, name := range cols {
			want := ref[i+1][c]
			got, ok := get[name](i)
			if !ok {
				if want != "" {
					t.Fatalf("row %d col %s: null, want %q", i, name, want)
				}
				continue
			}
			if got != want {
				t.Fatalf("row %d col %s: %q, want %q", i, name, got, want)
			}
		}
	}
}
