package exprparse

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// exprSeeds covers every construct of the grammar.
var exprSeeds = []string{
	`a + b * c - d / e`,
	`x // 2 % 3 ** -1`,
	`-x ** 2`,
	`--1`,
	`-(1)`,
	`a == 1 and b != "x" or not c`,
	`x is_null`,
	`x IS_NOT_NULL and y`,
	`name contains "a" or name like 'b%'`,
	`x in [1, 2, 3]`,
	`x not in ["a", "b",]`,
	`when a > 1 then "hi" when a < 0 then "lo" otherwise "mid"`,
	`when a then b`,
	`dt.year(ts)`,
	`ts.dt.year()`,
	`name.str.upper()`,
	`str.len_chars(name)`,
	`price.sum`,
	`round(x, 2)`,
	`cut(x, [0, 10], labels=["lo", "mid", "hi"], left_closed=true)`,
	`coalesce(a, b).str.trim()`,
	`col("my col") + lit(1.5e3)`,
	`(a + b).abs().over("k")`,
	`sum("x")`,
	`list.eval(l, element() * 2)`,
	`x.rolling_mean_by(ts, "7d", min_periods=1)`,
	`concat_str(a, b, separator="-")`,
	`true and FALSE or null`,
	`"a\"b" + 'c\'d'`,
	`.5 + 1. + 2E+2`,
	`a = 1`,
	`(a + 1`,
	`f(x, k=1, 2)`,
	`x.str`,
	`"unterminated`,
	`héllo + wörld`,
	`a ! b`,
}

// FuzzParse checks that no input panics the parser or the compiler,
// that errors point inside the input, and that compile errors are
// never internal.
func FuzzParse(f *testing.F) {
	for _, s := range exprSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, err := Parse(s)
		if err == nil {
			return
		}
		var pe *Error
		if !errors.As(err, &pe) {
			t.Fatalf("%q: error %v is not an *Error", s, err)
		}
		if pe.Pos < 0 || pe.Pos > len(s) || pe.End < pe.Pos || pe.End > len(s) {
			t.Fatalf("%q: span [%d,%d) outside the input", s, pe.Pos, pe.End)
		}
		if strings.Contains(err.Error(), "internal:") {
			t.Fatalf("%q: %v", s, err)
		}
	})
}

// FuzzFormat checks that formatting is stable: the formatted text
// parses to the same tree, and formatting it again changes nothing.
func FuzzFormat(f *testing.F) {
	for _, s := range exprSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n, err := ParseTree(s)
		if err != nil {
			return
		}
		out := Format(n)
		n2, err := ParseTree(out)
		if err != nil {
			t.Fatalf("%q formatted to %q, which does not parse: %v", s, out, err)
		}
		if a, b := dump(n), dump(n2); a != b {
			t.Fatalf("%q formatted to %q, which parses differently:\n%s\n%s", s, out, a, b)
		}
		if again := Format(n2); again != out {
			t.Fatalf("format is not idempotent: %q then %q", out, again)
		}
	})
}

// dump renders the structure of a tree without positions.
func dump(n *Node) string {
	if n == nil {
		return "nil"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "(%d %q %#v %q %q p%d", n.Kind, n.Name, n.Lit, n.NS, n.Infix, n.Parens)
	if n.Recv != nil {
		b.WriteString(" recv=" + dump(n.Recv))
	}
	for _, a := range n.Args {
		b.WriteString(" " + dump(a))
	}
	for _, k := range n.Kw {
		b.WriteString(" " + k.Name + "=" + dump(k.Val))
	}
	if n.Final != nil {
		b.WriteString(" else=" + dump(n.Final))
	}
	b.WriteByte(')')
	return b.String()
}

func TestFormatCanonical(t *testing.T) {
	for in, want := range map[string]string{
		`a+b*c`:                          `a + b * c`,
		`x  NOT  IN [1,2]`:               `x not in [1, 2]`,
		`WHEN a THEN 1 OTHERWISE 2`:      `when a then 1 otherwise 2`,
		`cut( x ,[0,10],labels = ["a"])`: `cut(x, [0, 10], labels=["a"])`,
		`ts . dt . year ( )`:             `ts.dt.year()`,
		`price.sum`:                      `price.sum()`,
		`- 1`:                            `-1`,
		`(a+b)  .abs()`:                  `(a + b).abs()`,
		`x is_null AND y IS_NOT_NULL`:    `x is_null and y is_not_null`,
		`s contains 'a'`:                 `s contains 'a'`,
		`[1,2,]`:                         `[1, 2]`,
	} {
		got, err := FormatSource(in)
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("FormatSource(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestErrorPositions(t *testing.T) {
	for _, tc := range []struct {
		src, span, hint, fix string
	}{
		{`amount + `, "+", "", ""},
		{`a = 1`, "=", "did you mean '=='?", "=="},
		{`str.to_uppercse(name)`, "to_uppercse", `did you mean "to_uppercase"?`, "to_uppercase"},
		{`x.rond(2)`, "rond", `did you mean "round"?`, "round"},
		{`coalesec(a, b)`, "coalesec", `did you mean "coalesce"?`, "coalesce"},
		{`cut(x, [0], lables=["a"])`, "lables", `did you mean "labels"?`, "labels"},
		{`round(x, "two")`, `"two"`, "", ""},
		{`a adn b`, "adn", `did you mean "and"?`, "and"},
		{`when a thn 1`, "thn", `did you mean "then"?`, "then"},
		{`f(1, 2`, "", "add the missing ')'", ")"},
		{`round(x, 1, 2)`, "round(x, 1, 2)", "usage: round(x, decimals: int = 0)", ""},
	} {
		_, err := Parse(tc.src)
		var pe *Error
		if !errors.As(err, &pe) {
			t.Errorf("%s: want *Error, got %v", tc.src, err)
			continue
		}
		if got := tc.src[pe.Pos:pe.End]; got != tc.span {
			t.Errorf("%s: span %q, want %q (%v)", tc.src, got, tc.span, err)
		}
		if tc.hint != "" && pe.Hint != tc.hint {
			t.Errorf("%s: hint %q, want %q", tc.src, pe.Hint, tc.hint)
		}
		if pe.Fix != tc.fix {
			t.Errorf("%s: fix %q, want %q", tc.src, pe.Fix, tc.fix)
		}
	}
}
