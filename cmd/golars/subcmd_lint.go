package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script/analysis"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// newLintCmd reports problems in a .glr script without running it:
// syntax errors, unknown commands, columns, functions, frames and
// files, wrong argument counts and types, type errors the engine
// would raise, and unused stashes. Columns and dtypes are tracked
// through the pipeline from the schemas of the loaded files.
func newLintCmd() *cobra.Command {
	var short, asJSON bool
	cmd := &cobra.Command{
		Use:   "lint FILE.glr [FILE.glr...]",
		Short: "report .glr mistakes without running the script",
		Long: "Check scripts statically. Data files are read for their schema only, so\n" +
			"unknown columns, bad function calls and dtype errors are caught before a run.\n" +
			"Relative paths resolve against the script directory and its parents.",
		Example: "golars lint pipeline.glr\ngolars lint --short examples/script/*.glr",
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.Flags().BoolVarP(&short, "short", "s", false, "one line per finding, without source excerpts")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print findings as JSON")
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		findings := 0
		var all []lintJSON
		for _, path := range args {
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			r := lintGlr(string(src), filepath.Dir(path))
			findings += len(r.Diags)
			switch {
			case asJSON:
				for _, d := range r.Diags {
					all = append(all, toLintJSON(path, r.File.Lines, d))
				}
			case short:
				for _, d := range r.Diags {
					fmt.Println(shortDiag(path, r.File.Lines, d))
				}
			default:
				fmt.Print(r.Render(path))
			}
		}
		if asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if all == nil {
				all = []lintJSON{}
			}
			if err := enc.Encode(all); err != nil {
				return err
			}
		}
		if findings > 0 {
			if !asJSON {
				fmt.Fprintf(os.Stderr, "%d finding(s)\n", findings)
			}
			return errSilent
		}
		return nil
	}
	return cmd
}

// lintGlr analyses a script whose relative paths resolve against dir.
func lintGlr(src, dir string) *analysis.Result {
	return analysis.Analyze(src, analysis.Options{Dir: dir})
}

func shortDiag(path string, lines []string, d syntax.Diag) string {
	msg := fmt.Sprintf("%s:%d:%d: %s: %s", path, d.Line+1, column(lines, d.Line, d.Col), d.Severity, d.Msg)
	if d.Hint != "" {
		msg += " (" + d.Hint + ")"
	}
	return msg
}

// column converts a byte column to a 1-based character column.
func column(lines []string, line, col int) int {
	if line < 0 || line >= len(lines) {
		return col + 1
	}
	return utf8.RuneCountInString(lines[line][:min(col, len(lines[line]))]) + 1
}

type lintJSON struct {
	File      string `json:"file"`
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	EndLine   int    `json:"end_line"`
	EndColumn int    `json:"end_column"`
	Severity  string `json:"severity"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Hint      string `json:"hint,omitempty"`
	Fix       string `json:"fix,omitempty"`
}

func toLintJSON(path string, lines []string, d syntax.Diag) lintJSON {
	out := lintJSON{
		File: path, Line: d.Line + 1, Column: column(lines, d.Line, d.Col),
		EndLine: d.EndLine + 1, EndColumn: column(lines, d.EndLine, d.EndCol),
		Severity: d.Severity.String(), Code: d.Code, Message: d.Msg, Hint: d.Hint,
	}
	if d.Fix != nil {
		out.Fix = d.Fix.Text
	}
	return out
}
