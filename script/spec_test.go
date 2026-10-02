package script_test

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
)

var validArgKinds = []string{"", "none", "path", "frame", "column", "count"}

func TestSpecTableIsConsistent(t *testing.T) {
	seen := map[string]string{}
	for _, c := range script.Commands {
		if c.Name == "" || c.Name != strings.ToLower(c.Name) {
			t.Errorf("command %q: name must be non-empty lower case", c.Name)
		}
		if !strings.HasPrefix(c.Signature, c.Name) {
			t.Errorf("command %q: signature %q must start with the name", c.Name, c.Signature)
		}
		if c.Summary == "" {
			t.Errorf("command %q: missing summary", c.Name)
		}
		if !slices.Contains(script.Categories, c.Category) {
			t.Errorf("command %q: category %q not in script.Categories", c.Name, c.Category)
		}
		if !slices.Contains(validArgKinds, c.ArgKind) {
			t.Errorf("command %q: unknown ArgKind %q", c.Name, c.ArgKind)
		}
		for _, n := range append([]string{c.Name}, c.Aliases...) {
			if prev, dup := seen[n]; dup {
				t.Errorf("name %q used by both %q and %q", n, prev, c.Name)
			}
			seen[n] = c.Name
		}
	}
}

func TestFindCommandResolvesAliases(t *testing.T) {
	cases := map[string]string{
		"load": "load", ".LOAD": "load", "write": "save", ".browse": "ishow",
		"tree": "explain_tree", "show_graph": "graph", "quit": "exit",
		"q": "exit", "?": "help", "avg": "mean", "melt": "unpivot",
		"ff": "forward_fill", "scan_arrow": "scan_ipc", "null-count": "null_count",
	}
	for in, want := range cases {
		spec := script.FindCommand(in)
		if spec == nil || spec.Name != want {
			t.Errorf("FindCommand(%q) = %v, want %q", in, spec, want)
		}
	}
	if script.FindCommand("definitely_not_a_command") != nil {
		t.Error("unknown command should not resolve")
	}
}

func TestCompleteCommand(t *testing.T) {
	got := script.CompleteCommand(".scan_")
	for _, want := range []string{"scan_csv", "scan_parquet", "scan_arrow", "scan_jsonl"} {
		if !slices.Contains(got, want) {
			t.Errorf("CompleteCommand(.scan_) missing %q: %v", want, got)
		}
	}
	if got := script.CompleteCommand("zzz"); len(got) != 0 {
		t.Errorf("CompleteCommand(zzz) = %v, want none", got)
	}
}

func TestSuggestTranspositionAndCap(t *testing.T) {
	cases := map[string]string{
		"fitler":   "filter", // one transposition
		"slect":    "select",
		"shcema":   "schema",
		"xyzzy":    "",
		"":         "",
		"groupbyy": "groupby",
	}
	for in, want := range cases {
		if got := script.SuggestCommand(in); got != want {
			t.Errorf("SuggestCommand(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUnknownCommandError(t *testing.T) {
	err := script.UnknownCommand(".filtet")
	if !errors.Is(err, script.ErrUnknownCommand) {
		t.Fatalf("error does not wrap ErrUnknownCommand: %v", err)
	}
	if want := `unknown command "filtet" (did you mean "filter"?)`; err.Error() != want {
		t.Errorf("got %q, want %q", err, want)
	}
	if got := script.UnknownCommand("qwertyuiop").Error(); strings.Contains(got, "did you mean") {
		t.Errorf("unexpected suggestion: %q", got)
	}
}

func TestRunnerDoesNotDoubleAnnotate(t *testing.T) {
	r := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
		return script.UnknownCommand(line)
	})}
	err := r.Run(strings.NewReader("filtet x\n"), "t.glr")
	if err == nil {
		t.Fatal("want error")
	}
	if n := strings.Count(err.Error(), "did you mean"); n != 1 {
		t.Errorf("suggestion count = %d in %q, want 1", n, err)
	}
}

// A host error that says "unknown" about something other than the
// command must not get a command suggestion attached.
func TestRunnerLeavesOtherUnknownErrorsAlone(t *testing.T) {
	r := script.Runner{Exec: script.ExecutorFunc(func(string) error {
		return fmt.Errorf("cast: unknown dtype %q", "i128")
	})}
	err := r.Run(strings.NewReader("cast a i128\n"), "t.glr")
	if err == nil || strings.Contains(err.Error(), "did you mean") {
		t.Errorf("got %v, want no suggestion", err)
	}
}

// Third-party executors that return a plain "unknown command" message
// still get a hint.
func TestRunnerAnnotatesPlainUnknownCommand(t *testing.T) {
	r := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
		return fmt.Errorf("unknown command %s", line)
	})}
	err := r.Run(strings.NewReader("filtet x\n"), "t.glr")
	if err == nil || !strings.Contains(err.Error(), `did you mean "filter"`) {
		t.Errorf("got %v, want a filter suggestion", err)
	}
}

func TestMarkdownIncludesAliasesAndExamples(t *testing.T) {
	md := script.FindCommand("save").Markdown()
	for _, want := range []string{"### `save`", "```glr\nsave <path>\n```", "Aliases: `write`"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown missing %q:\n%s", want, md)
		}
	}
	md = script.FindCommand("filter").Markdown()
	if !regexp.MustCompile(`(?s)\*\*Examples\*\*.*filter age > 25`).MatchString(md) {
		t.Errorf("filter markdown missing examples:\n%s", md)
	}
}
