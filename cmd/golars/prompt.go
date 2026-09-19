package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/Gaurav-Gosain/golars/repl"
	"github.com/Gaurav-Gosain/golars/script"
)

// newCLIPrompt builds a repl.Prompt wired for golars' REPL. The
// Suggester is closured over the live *state so column names and cwd
// are always current for context-aware completions.
func newCLIPrompt(s *state) *repl.Prompt {
	historyFile := ""
	if home, err := os.UserHomeDir(); err == nil {
		historyFile = filepath.Join(home, ".golars_history")
	}
	return repl.New(repl.Options{
		PromptFunc:  prompt,
		HistoryPath: historyFile,
		Suggester:   repl.SuggesterFunc(s.suggest),
	})
}

// golarsCommands lists every dot-command the REPL accepts. Kept in one
// place so completion, .help, and command dispatch stay in sync.
var golarsCommands = buildCommandList()

func buildCommandList() []string {
	out := make([]string, 0, len(script.Commands)+4)
	for _, spec := range script.Commands {
		out = append(out, "."+spec.Name)
	}
	out = append(out, ".h", ".?", ".q", ".avg", ".ff", ".bf", ".melt", ".null-count", ".scan_arrow", ".scan_jsonl")
	return out
}

// suggest wires the repl.Suggester callback to golars-specific context.
// Returns a ghost to append on Tab/Right-arrow and an optional right-
// side hint annotation.
func (s *state) suggest(line string) (string, string) {
	if line == "" {
		return "", "type .help for commands, tab to accept suggestions"
	}
	parts, trailingSpace := repl.SplitFields(line)
	// No space yet: completing the command itself.
	if !trailingSpace && len(parts) == 1 {
		partial := "." + strings.ToLower(strings.TrimPrefix(parts[0], "."))
		return repl.CompletePrefix(partial, golarsCommands), ""
	}
	if len(parts) == 0 {
		return "", ""
	}
	cmd := "." + strings.ToLower(strings.TrimPrefix(parts[0], "."))
	var current string
	if !trailingSpace && len(parts) > 1 {
		current = parts[len(parts)-1]
	}
	cols := s.currentColumns()
	switch cmd {
	case ".select", ".drop", ".sort", ".describe":
		if g := repl.CompleteFromList(current, cols, ','); g != "" {
			return g, ""
		}
		if current == "" {
			return "", "expects column name"
		}
	case ".filter":
		if !strings.ContainsAny(line, "<>=!") {
			if g := repl.CompleteFromList(current, cols, ','); g != "" {
				return g, "then: op (>, <, ==, etc.) value"
			}
		}
	case ".load", ".save", ".join", ".source":
		cwd, _ := os.Getwd()
		if g := repl.CompletePath(current, cwd); g != "" {
			return g, ""
		}
	case ".head", ".tail", ".limit":
		if current == "" {
			return "", "row count (default 10)"
		}
	}
	return "", ""
}

// currentColumns returns the live schema column names for completion.
func (s *state) currentColumns() []string {
	if s.df == nil && s.lf == nil {
		return nil
	}
	sch, err := s.currentLazy().Schema()
	if err != nil {
		return nil
	}
	return sch.Names()
}
