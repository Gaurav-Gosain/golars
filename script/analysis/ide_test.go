package analysis

import (
	"slices"
	"strings"
	"testing"
)

// completeAt analyses src (with | marking the cursor) and returns the
// completion labels.
func completeAt(t *testing.T, src string) []string {
	t.Helper()
	i := strings.Index(src, "|")
	src = src[:i] + src[i+1:]
	line := strings.Count(src[:i], "\n")
	col := i - strings.LastIndex(src[:i], "\n") - 1
	r := Analyze(src, Options{Dir: exampleDir})
	var out []string
	for _, it := range r.Complete(line, col).Items {
		out = append(out, it.Label)
	}
	return out
}

func TestComplete(t *testing.T) {
	const load = "load examples/script/data/salaries.csv\n"
	for _, tc := range []struct {
		src     string
		want    []string
		notWant []string
	}{
		{"fil|", []string{"filter"}, []string{"load"}},
		{load + "filter am|", []string{"amount"}, []string{"name"}},
		{load + "filter |", []string{"amount", "name", "coalesce", "dt", "when"}, nil},
		{load + "with x = name.str.to_up|", []string{"to_uppercase"}, []string{"year"}},
		{load + "with x = name.|", []string{"str", "round", "sum"}, nil},
		{load + "with x = dt.ye|", []string{"year"}, nil},
		{load + "with x = cut(amount, [1], la|", []string{"labels="}, nil},
		{load + "sort amount |", []string{"asc", "desc", "name"}, nil},
		{load + "groupby name amount:s|", []string{"sum", "std"}, []string{"max"}},
		{load + "join x on name |", []string{"inner", "left", "right", "full", "semi", "anti", "cross", "suffix"}, nil},
		{load + "cast amount f|", []string{"f32", "f64"}, nil},
		// Multi-key full join: separate key columns with the suffix.
		{"load examples/script/data/salaries.csv as s\n" + load + "join s on name,amount full suffix _s\nselect |",
			[]string{"name", "amount", "name_s", "amount_s"}, nil},
		// Semi joins keep only the focus columns.
		{"load examples/script/data/salaries.csv as s\nwith x = 1\n" + load + "with y = 2\njoin s on name semi\nselect |",
			[]string{"name", "y"}, []string{"x"}},
		{"load examples/script/data/sal|", []string{"examples/script/data/salaries.csv"}, nil},
		{"load examples/script/data/salaries.csv as s\nuse |", []string{"s"}, nil},
		{load + "with m = amount / 12\nselect m|", []string{"m"}, nil},
	} {
		got := completeAt(t, tc.src)
		for _, w := range tc.want {
			if !slices.Contains(got, w) {
				t.Errorf("%q: missing %q in %v", tc.src, w, trim(got))
			}
		}
		for _, w := range tc.notWant {
			if slices.Contains(got, w) {
				t.Errorf("%q: unexpected %q", tc.src, w)
			}
		}
	}
}

func trim(s []string) []string {
	if len(s) > 12 {
		return append(s[:12:12], "...")
	}
	return s
}

func TestHover(t *testing.T) {
	src := "load examples/script/data/salaries.csv\nwith m = round(amount / 12, 1)\nfilter m > 1\n"
	r := Analyze(src, Options{Dir: exampleDir})
	for _, tc := range []struct {
		line, col int
		want      string
	}{
		{0, 1, "load <path>"},
		{1, 10, "round(x, decimals: int = 0)"},
		{1, 17, "**amount** `i64`"},
		{1, 5, "**m** `f64`"},
		{2, 7, "**m** `f64`"},
		{0, 10, "| amount | i64 |"},
	} {
		if got := r.Hover(tc.line, tc.col); !strings.Contains(got, tc.want) {
			t.Errorf("hover %d:%d = %q, want %q", tc.line, tc.col, got, tc.want)
		}
	}
}

func TestSignatureAt(t *testing.T) {
	src := "load examples/script/data/salaries.csv\nwith m = round(amount, \n"
	r := Analyze(src, Options{Dir: exampleDir})
	sig, found := r.SignatureAt(1, len("with m = round(amount, "))
	if !found || sig.Info.Name != "round" || sig.Active != 1 {
		t.Fatalf("got %+v %v", sig, found)
	}
	src = "load examples/script/data/salaries.csv\nwith m = amount.round(\n"
	r = Analyze(src, Options{Dir: exampleDir})
	sig, found = r.SignatureAt(1, len("with m = amount.round("))
	if !found || sig.Active != 1 {
		t.Fatalf("method form: got %+v %v", sig, found)
	}
}
