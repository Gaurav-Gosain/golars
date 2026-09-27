package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLintFindings(t *testing.T) {
	src := `load a.csv as base
stash unused
use missing
filter name == "a#b"   # quoted hash is not a comment
filter name == "oops
filtet x
stash joined
join joined on id
use base
.melt id x
`
	got := map[string]bool{}
	for _, m := range lintGlr(src) {
		got[fmt.Sprintf("%d: %s", m.line, m.msg)] = true
	}
	want := []string{
		`2: stash "unused" is never used`,
		`3: use "missing" with no earlier stash or load ... as`,
		`5: filter has unbalanced quotes`,
		`6: unknown command "filtet" (did you mean "filter"?)`,
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing warning %q; got %v", w, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d warnings, want %d: %v", len(got), len(want), got)
	}
}

// The bundled examples are the reference scripts; they must lint clean.
func TestExampleScriptsLintClean(t *testing.T) {
	paths, err := filepath.Glob("../../examples/script/*.glr")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no example scripts found: %v", err)
	}
	for _, p := range paths {
		src, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range lintGlr(string(src)) {
			t.Errorf("%s:%d: %s", p, m.line, m.msg)
		}
	}
}
