package main

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/analysis"
)

// Every ```glr block in docs/scripting.md runs cleanly from the
// repository root and lints clean, so the documentation cannot drift
// from the language.
func TestScriptingDocExamplesRun(t *testing.T) {
	doc, err := os.ReadFile("../../docs/scripting.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := regexp.MustCompile("(?s)```glr\n(.*?)```").FindAllStringSubmatch(string(doc), -1)
	if len(blocks) < 8 {
		t.Fatalf("found %d glr blocks", len(blocks))
	}
	t.Chdir("../..")
	silenceStdout(t)
	for i, b := range blocks {
		src := b[1]
		if r := analysis.Analyze(src, analysis.Options{Dir: "."}); len(r.Diags) > 0 {
			t.Errorf("docs block %d does not lint clean:\n%s\n%s", i+1, src, r.Render("block"))
		}
		s := newState(false)
		runner := script.Runner{Exec: script.ExecutorFunc(s.handle)}
		if err := runner.Run(strings.NewReader(src), "block"); err != nil && !errors.Is(err, errExit) {
			t.Errorf("docs block %d failed:\n%s\n%v", i+1, src, err)
		}
		s.close()
	}
}
