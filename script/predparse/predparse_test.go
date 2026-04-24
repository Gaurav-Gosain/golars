package predparse

import (
	"strings"
	"testing"
)

func TestParseSimple(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"a > 5", `(col("a") > 5)`},
		{"age >= 30", `(col("age") >= 30)`},
		{`name == "alice"`, `(col("name") == "alice")`},
		{"active == true", `(col("active") == true)`},
		{"x is_null", `col("x").is_null()`},
		{"x is_not_null", `col("x").is_not_null()`},
		{"a > 5 and b < 10", `((col("a") > 5) and (col("b") < 10))`},
		{"a > 5 or b < 10", `((col("a") > 5) or (col("b") < 10))`},
		{"a > 5 and b < 10 or c == 1",
			`(((col("a") > 5) and (col("b") < 10)) or (col("c") == 1))`},
		{`name contains "foo"`, `col("name").str.contains(foo)`},
		{`brand like "%COPPER"`, `col("brand").str.like(%COPPER)`},
		{`sku starts_with "A"`, `col("sku").str.starts_with(A)`},
		{`comment not_like "%URGENT%" and qty > 0`,
			`(col("comment").str.not_like(%URGENT%) and (col("qty") > 0))`},
		{"matches_vowel", `col("matches_vowel")`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.String() != tc.want {
				t.Errorf("got %q\nwant %q", got.String(), tc.want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()
	bad := []string{
		"a = 5",
		"a > ",
		"a ? 5",
		`"x > 5"`,
	}
	for _, input := range bad {
		if _, err := Parse(input); err == nil {
			t.Errorf("expected error for %q", input)
		}
	}
}

func TestTokenizeEdgeCases(t *testing.T) {
	t.Parallel()
	toks, err := tokenize(`  foo >= 1.5  and  bar == "hi there"  `)
	if err != nil {
		t.Fatalf("tokenize: %v", err)
	}
	got := make([]string, len(toks))
	for i, tk := range toks {
		got[i] = tk.lex
	}
	want := []string{"foo", ">=", "1.5", "and", "bar", "==", "hi there"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("tokens = %v, want %v", got, want)
	}
}
