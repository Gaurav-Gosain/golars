package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/repl"
	"github.com/Gaurav-Gosain/golars/script"
)

// runREPL is the shared entry point for `golars` and `golars repl`.
func runREPL(loadPath, scriptPath, previewPath string, previewRows int, previewFormat string, timing bool) error {
	s := newState(timing)
	defer s.close()
	if loadPath != "" {
		load := &call{spec: script.FindCommand("load"), args: []string{loadPath}}
		if err := cmdLoad(s, load); err != nil {
			return err
		}
	}
	switch {
	case scriptPath != "":
		return runScript(s, scriptPath)
	case previewPath != "":
		return runPreview(s, previewPath, previewRows, previewFormat)
	}
	return s.repl()
}

func (s *state) repl() error {
	p := newCLIPrompt(s)
	defer p.Close()
	banner()

	// A trailing `\` keeps reading the next line into the same
	// statement, matching script files.
	var cont strings.Builder
	for {
		line, err := p.ReadLine()
		if err != nil {
			if errors.Is(err, repl.ErrCanceled) {
				cont.Reset()
				continue
			}
			if errors.Is(err, repl.ErrEOF) {
				fmt.Println(dimStyle.Render("bye"))
				return nil
			}
			return err
		}
		if body, found := strings.CutSuffix(strings.TrimRight(line, " \t"), "\\"); found {
			cont.WriteString(body)
			cont.WriteByte(' ')
			continue
		}
		if cont.Len() > 0 {
			cont.WriteString(line)
			line = cont.String()
			cont.Reset()
		}
		start := time.Now()
		err = s.handle(line)
		if errors.Is(err, errExit) {
			fmt.Println(dimStyle.Render("bye"))
			return nil
		}
		if err != nil {
			printErr(err)
		}
		if s.showTiming && strings.TrimSpace(line) != "" {
			printTiming(time.Since(start))
		}
	}
}

func banner() {
	fmt.Println()
	fmt.Println(logoStyle.Render("  ▓▓▓  golars v" + version))
	fmt.Println(dimStyle.Render("  pure-Go DataFrames with lazy plan and optimizer"))
	fmt.Println(dimStyle.Render("  type ") + cmdStyle.Render(".help") + dimStyle.Render(" to list commands"))
	fmt.Println()
}

func prompt() string { return promptStyle.Render("golars") + dimStyle.Render(" » ") }

func printErr(err error) {
	fmt.Println(errStyle.Render("error:") + " " + errMsgStyle.Render(err.Error()))
}

func printTiming(d time.Duration) {
	style := successStyle
	switch {
	case d > 100*time.Millisecond:
		style = errStyle
	case d > 10*time.Millisecond:
		style = warnStyle
	}
	fmt.Println(style.Render("took " + d.String()))
}

// newCLIPrompt builds the line editor. The suggester closes over the
// live state so column and frame names are always current.
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

// replCommands is every command and alias with its leading dot, for
// prefix completion at the prompt.
var replCommands = func() []string {
	names := script.Names()
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = "." + n
	}
	return out
}()

// suggest returns the ghost text to append on Tab or Right, and an
// optional hint shown to the right of the input. Argument completion
// follows the command's spec ArgKind.
func (s *state) suggest(line string) (string, string) {
	if line == "" {
		return "", "type .help for commands, tab to accept suggestions"
	}
	parts, trailingSpace := repl.SplitFields(line)
	if len(parts) == 0 {
		return "", ""
	}
	if !trailingSpace && len(parts) == 1 {
		partial := "." + strings.ToLower(strings.TrimPrefix(parts[0], "."))
		return repl.CompletePrefix(partial, replCommands), ""
	}
	spec := script.FindCommand(parts[0])
	if spec == nil {
		return "", ""
	}
	var current string
	if !trailingSpace {
		current = parts[len(parts)-1]
	}
	switch spec.ArgKind {
	case "column":
		if spec.Name == "filter" && strings.ContainsAny(line, "<>=!") {
			return "", ""
		}
		if g := repl.CompleteFromList(current, s.currentColumns(), ','); g != "" {
			return g, ""
		}
		if current == "" {
			return "", "column name"
		}
	case "frame":
		if g := repl.CompleteFromList(current, s.frameNames(), ','); g != "" {
			return g, ""
		}
		if spec.Name == "join" {
			cwd, _ := os.Getwd()
			return repl.CompletePath(current, cwd), ""
		}
	case "path":
		cwd, _ := os.Getwd()
		if g := repl.CompletePath(current, cwd); g != "" {
			return g, ""
		}
	case "count":
		if current == "" {
			return "", "row count"
		}
	}
	return "", ""
}

// currentColumns returns the focused pipeline's column names.
func (s *state) currentColumns() []string {
	if !s.hasFocus() {
		return nil
	}
	sch, err := s.schema()
	if err != nil {
		return nil
	}
	return sch.Names()
}

func (s *state) frameNames() []string {
	names := make([]string, 0, len(s.frames))
	for n := range s.frames {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}
