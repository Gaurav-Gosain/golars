package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
)

func TestEveryCommandHasHandler(t *testing.T) {
	for _, c := range script.Commands {
		if handlers[c.Name] == nil {
			t.Errorf("script command %q has no handler in dispatch.go", c.Name)
		}
	}
	for name := range handlers {
		if spec := script.FindCommand(name); spec == nil || spec.Name != name {
			t.Errorf("handler %q is not a canonical command in script/spec.go", name)
		}
	}
}

const peopleCSV = `name,age,dept
ada,27,eng
brian,34,ops
carl,19,eng
diana,42,ops
ernest,30,eng
`

// scriptState returns a session with peopleCSV loaded and stdout
// silenced for the duration of the test.
func scriptState(t *testing.T) (*state, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "people.csv")
	if err := os.WriteFile(path, []byte(peopleCSV), 0o644); err != nil {
		t.Fatal(err)
	}
	silenceStdout(t)
	s := newState(false)
	t.Cleanup(s.close)
	mustRun(t, s, "load "+path)
	return s, dir
}

func silenceStdout(t *testing.T) {
	t.Helper()
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	real := os.Stdout
	os.Stdout = devNull
	t.Cleanup(func() {
		os.Stdout = real
		devNull.Close()
	})
}

func mustRun(t *testing.T, s *state, lines ...string) {
	t.Helper()
	for _, l := range lines {
		if err := s.handle(l); err != nil {
			t.Fatalf("%s: %v", l, err)
		}
	}
}

func focusShape(t *testing.T, s *state) (int, int) {
	t.Helper()
	df, err := s.materialize()
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	return df.Height(), df.Width()
}

func TestPipelineCommandsStayLazy(t *testing.T) {
	s, _ := scriptState(t)
	mustRun(t, s,
		"filter age > 20",
		"select name, age",
		"sort age desc",
		"reverse",
		"unique",
		"with older = age + 1",
		"limit 3",
	)
	if s.lf == nil {
		t.Fatal("pipeline commands should leave a lazy plan")
	}
	if h, w := focusShape(t, s); h != 3 || w != 3 {
		t.Fatalf("shape = %dx%d, want 3x3", h, w)
	}
}

func TestReshapeCommandsReplaceFocus(t *testing.T) {
	cases := map[string][2]int{
		"sample 2 7":       {2, 3},
		"shuffle 3":        {5, 3},
		"top_k 2 age":      {2, 3},
		"bottom_k 1 age":   {1, 3},
		"unpivot name age": {5, 3},
	}
	for stmt, want := range cases {
		t.Run(stmt, func(t *testing.T) {
			s, _ := scriptState(t)
			mustRun(t, s, stmt)
			if s.lf != nil {
				t.Error("reshape commands should materialise the focus")
			}
			if h, w := focusShape(t, s); h != want[0] || w != want[1] {
				t.Errorf("shape = %dx%d, want %dx%d", h, w, want[0], want[1])
			}
		})
	}
}

func TestNamedFrames(t *testing.T) {
	s, dir := scriptState(t)
	path := filepath.Join(dir, "people.csv")
	mustRun(t, s,
		"load "+path+" as people",
		"filter age < 31", // compute.EqLit rejects string literals today
		"stash eng",
		"use people",
	)
	if s.focused != "people" {
		t.Fatalf("focused = %q", s.focused)
	}
	mustRun(t, s, "join eng on name inner")
	if h, _ := focusShape(t, s); h != 3 {
		t.Fatalf("join height = %d, want 3", h)
	}
	// Dropping the frame the focus came from is safe: the focus is a
	// clone.
	mustRun(t, s, "drop_frame people", "head 1")
	if s.focused != "" {
		t.Errorf("focused should clear after dropping its frame, got %q", s.focused)
	}
	// An anonymous load clears the focus name.
	mustRun(t, s, "use eng", "load "+path)
	if s.focused != "" {
		t.Errorf("anonymous load should clear the focus name, got %q", s.focused)
	}
}

func TestAliasesDispatch(t *testing.T) {
	s, dir := scriptState(t)
	out := filepath.Join(dir, "out.parquet")
	mustRun(t, s, "write "+out, "avg age", "melt name age", "?", "tree")
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("write alias did not save: %v", err)
	}
}

func TestDispatchErrors(t *testing.T) {
	s, _ := scriptState(t)
	cases := map[string]string{
		"filtet age > 1":              `unknown command "filtet" (did you mean "filter"?)`,
		"sort age sideways":           `sort: unknown column "sideways" (want a column, asc or desc)`,
		"sort desc":                   `sort: direction "desc" must follow a column`,
		"head -3":                     `head: invalid row count "-3": want a non-negative integer`,
		"cast age i128":               `cast: unknown dtype "i128"`,
		"pivot name dept age median":  `pivot: unknown aggregation "median"`,
		"join":                        "usage: join <path|NAME> on <key> [inner|left|cross]",
		"load a b c d":                "usage: load <path> [as NAME]",
		"use nope":                    `use: no frame named "nope"`,
		"groupby dept age:frobnicate": `groupby: aggregation "age:frobnicate": unknown op "frobnicate"`,
		"load missing.txt":            `load: unsupported file extension ".txt"`,
		".":                           `unknown command ""`,
	}
	for stmt, want := range cases {
		err := s.handle(stmt)
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("%q: got %v, want prefix %q", stmt, err, want)
		}
		if err != nil && strings.Count(err.Error(), "did you mean") > 1 {
			t.Errorf("%q: suggestion repeated: %v", stmt, err)
		}
	}
}

// Every command must fail cleanly, never panic, when nothing is
// loaded or the arguments are nonsense.
func TestNoPanicOnBadInput(t *testing.T) {
	silenceStdout(t)
	skip := map[string]bool{"ishow": true, "cd": true, "clear": true}
	argSets := []string{"", " x", " x y", " 0", " -1 0", " a as", " x on y z w", " 999999999999999999999"}
	for _, c := range script.Commands {
		if skip[c.Name] {
			continue
		}
		for _, args := range argSets {
			for _, loaded := range []bool{false, true} {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("%s%s (loaded=%v) panicked: %v", c.Name, args, loaded, r)
						}
					}()
					s := newState(false)
					defer s.close()
					if loaded {
						dir := t.TempDir()
						path := filepath.Join(dir, "p.csv")
						if err := os.WriteFile(path, []byte(peopleCSV), 0o644); err != nil {
							t.Fatal(err)
						}
						if err := s.handle("load " + path); err != nil {
							t.Fatal(err)
						}
					}
					_ = s.handle(c.Name + args)
				}()
			}
		}
	}
}

func TestExitStopsScript(t *testing.T) {
	silenceStdout(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "x.glr")
	if err := os.WriteFile(path, []byte("exit\nnot_a_command\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newState(false)
	defer s.close()
	if err := runScript(s, path); err != nil {
		t.Fatalf("exit should end the script cleanly, got %v", err)
	}
	if err := s.handle("quit"); !errors.Is(err, errExit) {
		t.Fatalf("quit = %v, want errExit", err)
	}
}

func TestScanCommands(t *testing.T) {
	s, dir := scriptState(t)
	tsv := filepath.Join(dir, "t.tsv")
	if err := os.WriteFile(tsv, []byte("a\tb\n1\t\n2\t3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustRun(t, s, "scan_csv "+tsv)
	if s.df != nil || s.lf == nil {
		t.Fatal("scan should leave a lazy focus")
	}
	df, err := s.materialize()
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Width() != 2 {
		t.Fatalf("scan_csv on .tsv should keep the tab delimiter, width = %d", df.Width())
	}
	col, _ := df.Column("b")
	if col.NullCount() != 1 {
		t.Errorf("empty field should scan as null, null count = %d", col.NullCount())
	}
	mustRun(t, s, "schema", "scan_auto "+tsv+" as staged")
	if _, found := s.frames["staged"]; !found {
		t.Error("scan_auto ... as NAME should stage a frame")
	}
	if err := s.handle("scan_auto " + filepath.Join(dir, "nope.xyz")); err == nil {
		t.Error("scan_auto on an unknown extension should fail")
	}
}

func TestSplitAssignment(t *testing.T) {
	cases := []struct {
		in, name, expr string
		ok             bool
	}{
		{"x = a + 1", "x", "a + 1", true},
		{"flag = a == 1", "flag", "a == 1", true},
		{`s = "a=b"`, "s", `"a=b"`, true},
		{"x == 1", "", "", false},
		{"= 1", "", "", false},
		{"x =", "", "", false},
		{"", "", "", false},
	}
	for _, c := range cases {
		name, expr, isAssign := splitAssignment(c.in)
		if name != c.name || expr != c.expr || isAssign != c.ok {
			t.Errorf("splitAssignment(%q) = %q, %q, %v", c.in, name, expr, isAssign)
		}
	}
}

func TestColumnList(t *testing.T) {
	got := columnList([]string{"a,b", "c", ",", "d,"})
	want := []string{"a", "b", "c", "d"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("columnList = %v, want %v", got, want)
	}
}

func TestKernelCellEdgeCases(t *testing.T) {
	s := newReviewState(t)
	for _, code := range []string{".", "exit", "", "# only a comment", "  \n\t"} {
		resp := executeCell(s, kernelRequest{Code: code})
		if code == "." {
			if resp.Error == "" {
				t.Errorf("cell %q should report an unknown command", code)
			}
			continue
		}
		if resp.Error != "" {
			t.Errorf("cell %q: unexpected error %q", code, resp.Error)
		}
	}
	if !lastLineIsDisplayCommand("select name\nshow # trailing\n\n") {
		t.Error("show should count as a display command")
	}
	if lastLineIsDisplayCommand("show\nselect name") {
		t.Error("select should not count as a display command")
	}
}
