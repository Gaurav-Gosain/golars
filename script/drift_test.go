package script_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
)

// The tree-sitter grammar and docs/scripting.md each keep their own
// copy of the command list. These tests fail when either drifts from
// script.Commands so a new command cannot land half-documented.

func grammarCommands(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("../editors/tree-sitter-golars/grammar.js")
	if err != nil {
		t.Fatalf("read grammar: %v", err)
	}
	block := regexp.MustCompile(`(?s)command: \$ => choice\((.*?)\$\.identifier`).FindSubmatch(src)
	if block == nil {
		t.Fatal("grammar.js: command choice block not found")
	}
	var out []string
	for _, m := range regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(block[1], -1) {
		out = append(out, string(m[1]))
	}
	return out
}

func TestGrammarListsEveryCommand(t *testing.T) {
	got := grammarCommands(t)
	for _, name := range script.Names() {
		if !slices.Contains(got, name) {
			t.Errorf("editors/tree-sitter-golars/grammar.js: command %q missing from the command choice", name)
		}
	}
	for _, name := range got {
		if script.FindCommand(name) == nil {
			t.Errorf("editors/tree-sitter-golars/grammar.js: %q is not a command in script/spec.go", name)
		}
	}
}

// The VS Code TextMate grammar, the Vim syntax file and the JupyterLab
// extension highlight commands by keyword. Names with characters a
// keyword cannot hold (`?`, `null-count`) are exempt.
func TestEditorSyntaxListsEveryCommand(t *testing.T) {
	ident := regexp.MustCompile(`^[a-z_]+$`)
	vscode, err := os.ReadFile("../editors/vscode-golars/syntaxes/glr.tmLanguage.json")
	if err != nil {
		t.Fatal(err)
	}
	vim, err := os.ReadFile("../editors/nvim-golars/syntax/glr.vim")
	if err != nil {
		t.Fatal(err)
	}
	vscodeWords := map[string]bool{}
	if m := regexp.MustCompile(`\\\\b\(([a-z_|]+)\)\\\\b`).FindSubmatch(vscode); m != nil {
		for w := range strings.SplitSeq(string(m[1]), "|") {
			vscodeWords[w] = true
		}
	} else {
		t.Fatal("glr.tmLanguage.json: command pattern not found")
	}
	jlab, err := os.ReadFile("../editors/jupyterlab-golars/src/index.ts")
	if err != nil {
		t.Fatal(err)
	}
	jlabWords := map[string]bool{}
	if m := regexp.MustCompile(`(?s)const COMMANDS = new Set\(\[(.*?)\]\);`).FindSubmatch(jlab); m != nil {
		for _, w := range regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(m[1], -1) {
			jlabWords[string(w[1])] = true
		}
	} else {
		t.Fatal("jupyterlab-golars/src/index.ts: COMMANDS set not found")
	}
	vimWords := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^syn keyword glrCommand\s+(.*)$`).FindAllSubmatch(vim, -1) {
		for w := range strings.FieldsSeq(string(m[1])) {
			vimWords[w] = true
		}
	}
	for _, name := range script.Names() {
		if !ident.MatchString(name) {
			continue
		}
		if !vscodeWords[name] {
			t.Errorf("editors/vscode-golars/syntaxes/glr.tmLanguage.json: command %q missing", name)
		}
		if !vimWords[name] {
			t.Errorf("editors/nvim-golars/syntax/glr.vim: command %q missing", name)
		}
		if !jlabWords[name] {
			t.Errorf("editors/jupyterlab-golars/src/index.ts: command %q missing", name)
		}
	}
}

func TestScriptingDocListsEveryCommand(t *testing.T) {
	src, err := os.ReadFile("../docs/scripting.md")
	if err != nil {
		t.Fatalf("read docs: %v", err)
	}
	doc := string(src)
	start := strings.Index(doc, "## Statement reference")
	if start < 0 {
		t.Fatal("docs/scripting.md: no Statement reference section")
	}
	section := doc[start:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	for _, name := range script.Names() {
		re := regexp.MustCompile("`" + regexp.QuoteMeta(name) + "[ `]")
		if !re.MatchString(section) {
			t.Errorf("docs/scripting.md: command %q missing from the Statement reference", name)
		}
	}
}
