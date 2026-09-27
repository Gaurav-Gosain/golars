package syntax

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fuzzSeeds(f *testing.F) {
	paths, _ := filepath.Glob("../../examples/script/*.glr")
	for _, p := range paths {
		if b, err := os.ReadFile(p); err == nil {
			f.Add(string(b))
		}
	}
	for _, s := range []string{
		"load a.csv as t\nuse t\nfilter a > 1 \\\n  and b < 2 # c\n",
		"groupby k, j a:sum:t n = len() (m = x.max())\n",
		"group_by_dynamic ts every 1h period 2h by k closed left a:sum\n",
		"join_asof r on ts by u nearest tolerance 2h\n",
		"select a, b = c * 2, d.str.len()\n",
		"with a = 'x#y', b = \"q\\\"\"\n",
		".SORT a DESC, b\n",
		"filter (a\n",
		"with = 1\n",
		"cast x datetime[ms]\n",
		"\\\n\\\n",
	} {
		f.Add(s)
	}
}

// FuzzStatements checks that no input panics the statement parser,
// that diagnostics point inside the source, and that formatting is
// idempotent and keeps the statements' meaning.
func FuzzStatements(f *testing.F) {
	fuzzSeeds(f)
	f.Fuzz(func(t *testing.T, src string) {
		if strings.ContainsRune(src, '\r') {
			// CRLF is folded by the formatter; compare on LF input.
			src = strings.ReplaceAll(src, "\r", "")
		}
		file := Parse(src)
		for _, st := range file.Stmts {
			for _, d := range st.Diags {
				if d.Line < 0 || d.Line >= len(file.Lines) || d.Col < 0 || d.Col > len(file.Lines[d.Line]) {
					t.Fatalf("%q: diagnostic outside the source: %+v", src, d)
				}
			}
		}
		_ = file.Tokens()
		once := Format(src)
		twice := Format(once)
		if twice != once {
			t.Fatalf("format not idempotent:\n%q\n%q\n%q", src, once, twice)
		}
		if a, b := structure(src), structure(once); a != b {
			t.Fatalf("format changed the statements of %q:\n%s\n---\n%s", src, a, b)
		}
	})
}
