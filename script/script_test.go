package script_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
)

func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"load x", ".load x"},
		{".load x", ".load x"},
		{"  load  x  ", ".load  x"},
		{"# just a comment", ""},
		{"load x # trailing", ".load x"},
		{`with x = 'alpha#beta' # trailing`, `.with x = 'alpha#beta'`},
		{`with x = 'it\'s#valid' # trailing`, `.with x = 'it\'s#valid'`},
		{"", ""},
		{"   ", ""},
		{"\t.sort  col", ".sort  col"},
	}
	for _, c := range cases {
		if got := script.Normalize(c.in); got != c.want {
			t.Fatalf("Normalize(%q) = %q want %q", c.in, got, c.want)
		}
	}
}

func TestRunnerRecordsAllLines(t *testing.T) {
	var seen []string
	r := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
		seen = append(seen, line)
		return nil
	})}
	in := strings.NewReader(`
# header
load data.csv
 filter age > 30 # inline
sort age desc
`)
	if err := r.Run(in, "inline"); err != nil {
		t.Fatal(err)
	}
	want := []string{".load data.csv", ".filter age > 30", ".sort age desc"}
	if !equalStrings(seen, want) {
		t.Fatalf("got %v, want %v", seen, want)
	}
}

func TestRunnerStopsOnFirstErr(t *testing.T) {
	stopErr := errors.New("stop")
	called := 0
	r := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
		called++
		if line == ".bad" {
			return stopErr
		}
		return nil
	})}
	err := r.Run(strings.NewReader("good\nbad\nalso-good\n"), "t")
	if err == nil || !errors.Is(err, stopErr) {
		t.Fatalf("want stopErr, got %v", err)
	}
	if called != 2 {
		t.Fatalf("called=%d want 2 (stop after 'bad')", called)
	}
}

func TestRunnerContinueOnErr(t *testing.T) {
	var errOut strings.Builder
	called := 0
	r := script.Runner{
		Exec: script.ExecutorFunc(func(line string) error {
			called++
			if line == ".bad" {
				return errors.New("oops")
			}
			return nil
		}),
		ContinueOnErr: true,
		ErrOut:        &errOut,
	}
	err := r.Run(strings.NewReader("good\nbad\nalso-good\n"), "t")
	if err != nil {
		t.Fatalf("want nil err (continue), got %v", err)
	}
	if called != 3 {
		t.Fatalf("called=%d want 3", called)
	}
	if !strings.Contains(errOut.String(), "t:2") {
		t.Fatalf("expected t:2 in err out, got %q", errOut.String())
	}
}

func TestRunnerTrace(t *testing.T) {
	var traced []string
	r := script.Runner{
		Exec:  script.ExecutorFunc(func(string) error { return nil }),
		Trace: func(line string) { traced = append(traced, line) },
	}
	r.Run(strings.NewReader("a\n# skip\nb\n"), "t")
	if !equalStrings(traced, []string{".a", ".b"}) {
		t.Fatalf("trace: %v", traced)
	}
}

func TestRunFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.glr")
	if err := os.WriteFile(path, []byte("load a.csv\nsort age\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var lines []string
	r := script.Runner{Exec: script.ExecutorFunc(func(l string) error {
		lines = append(lines, l)
		return nil
	})}
	if err := r.RunFile(path); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(lines, []string{".load a.csv", ".sort age"}) {
		t.Fatalf("RunFile: %v", lines)
	}
}

func TestRunFileMissing(t *testing.T) {
	r := script.Runner{Exec: script.ExecutorFunc(func(string) error { return nil })}
	err := r.RunFile(filepath.Join(t.TempDir(), "nope.glr"))
	if err == nil {
		t.Fatal("want error for missing file")
	}
}

func TestRunnerNilExec(t *testing.T) {
	r := script.Runner{}
	err := r.Run(strings.NewReader("x"), "t")
	if err == nil {
		t.Fatal("want error for nil Executor")
	}
}

// Edge cases: very long lines and blank-heavy input.
func TestLargeScript(t *testing.T) {
	var b strings.Builder
	for i := range 1000 {
		if i%10 == 0 {
			b.WriteString("# comment\n")
		}
		b.WriteString("step\n")
	}
	n := 0
	r := script.Runner{Exec: script.ExecutorFunc(func(string) error {
		n++
		return nil
	})}
	if err := r.Run(strings.NewReader(b.String()), "t"); err != nil {
		t.Fatal(err)
	}
	if n != 1000 {
		t.Fatalf("ran %d, want 1000", n)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestLineContinuation: trailing backslash joins the next physical
// line into the same statement.
func TestLineContinuation(t *testing.T) {
	var seen []string
	r := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
		seen = append(seen, line)
		return nil
	})}
	in := strings.NewReader("filter age > 30 \\\n  and dept == \"eng\"\nshow\n")
	if err := r.Run(in, "t"); err != nil {
		t.Fatal(err)
	}
	want := []string{`.filter age > 30    and dept == "eng"`, ".show"}
	if !equalStrings(seen, want) {
		t.Fatalf("got %q\nwant %q", seen, want)
	}
}

// TestCommentInsideQuotedString: a `#` inside a double-quoted value
// must NOT start a comment.
func TestCommentInsideQuotedString(t *testing.T) {
	got := script.Normalize(`filter name == "alpha#beta"`)
	want := `.filter name == "alpha#beta"`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestSuggestOnTypo: an unknown verb gets a "did you mean?" annotation.
func TestSuggestOnTypo(t *testing.T) {
	want := map[string]string{
		"filtet":   "filter",
		"grouby":   "groupby",
		"descbe":   "describe",
		"srt":      "sort",
		"nothing2": "",
	}
	for input, expected := range want {
		got := script.SuggestCommand(input)
		if got != expected {
			t.Errorf("SuggestCommand(%q) = %q, want %q", input, got, expected)
		}
	}
}
