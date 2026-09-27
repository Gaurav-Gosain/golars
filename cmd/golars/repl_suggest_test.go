package main

import (
	"strings"
	"testing"
)

func TestREPLSuggest(t *testing.T) {
	s, _ := scriptState(t)
	mustRun(t, s, "with decade = age // 10 * 10")
	for _, tc := range []struct {
		line, ghost, hint string
	}{
		{".fil", "ter", "filter <predicate>"},
		{"filter ag", "e", "i64"},
		{"filter dec", "ade", "i64"},
		{"with x = name.str.to_up", "per", "str.to_upper(x)"},
		{"with x = dt.ye", "ar", "dt.year(x)"},
		{"with r = round(age, ", "decimals=", "round(x, decimals: int = 0)"},
		{"sort age d", "ept", "also: decade, desc"},
	} {
		ghost, hint := s.suggest(tc.line)
		if ghost != tc.ghost || !strings.Contains(hint, tc.hint) {
			t.Errorf("suggest(%q) = %q, %q; want %q, %q", tc.line, ghost, hint, tc.ghost, tc.hint)
		}
	}
}

func TestREPLHighlight(t *testing.T) {
	line := `filter dt.year(ts) == 2024 and name in ["a"]`
	var got []string
	for _, sp := range highlightGlr(line) {
		got = append(got, line[sp.Start:sp.End])
	}
	want := `filter dt year == 2024 and in "a"`
	if strings.Join(got, " ") != want {
		t.Errorf("highlighted %q, want %q", strings.Join(got, " "), want)
	}
}

func TestREPLUnclosed(t *testing.T) {
	for s, want := range map[string]bool{
		"with x = cut(a, [1,": true, "with x = f(a)": false, `filter a == "x`: true,
		`filter a == "(" # (`: false, "select a, (b": true,
	} {
		if got := unclosed(s); got != want {
			t.Errorf("unclosed(%q) = %v", s, got)
		}
	}
}
