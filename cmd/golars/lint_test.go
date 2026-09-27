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
	r := lintGlr(src, t.TempDir())
	for _, d := range r.Diags {
		got[fmt.Sprintf("%d: %s", d.Line+1, d.Msg)] = true
	}
	want := []string{
		`1: file "a.csv" does not exist`,
		`2: stash needs a loaded frame; nothing is loaded yet`,
		`3: no frame named "missing"`,
		`5: unterminated string`,
		`6: unknown command "filtet"`,
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing finding %q; got %v", w, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d findings, want %d: %v", len(got), len(want), got)
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
		if r := lintGlr(string(src), filepath.Dir(p)); len(r.Diags) > 0 {
			t.Error(r.Render(p))
		}
	}
}

func TestUnifiedDiff(t *testing.T) {
	got := unifiedDiff("s.glr", "load a\nfilter x>1\nshow\n", "load a\nfilter x > 1\nshow\n")
	want := "--- s.glr\n+++ s.glr (formatted)\n@@ -1,3 +1,3 @@\n load a\n-filter x>1\n+filter x > 1\n show\n"
	if got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}
