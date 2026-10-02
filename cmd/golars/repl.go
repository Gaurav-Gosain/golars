package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/Gaurav-Gosain/golars/repl"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/analysis"
	"github.com/Gaurav-Gosain/golars/script/syntax"
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
	// statement, matching script files; so does an unclosed bracket
	// or quote.
	cont := &s.cont
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
		if unclosed(cont.String() + line) {
			cont.WriteString(line)
			cont.WriteByte(' ')
			continue
		}
		if cont.Len() > 0 {
			cont.WriteString(strings.TrimLeft(line, " \t"))
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

// contPrompt marks a continuation line; it has the width of prompt.
func contPrompt() string { return dimStyle.Render("     ... ") }

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
		PromptFunc: func() string {
			if s.cont.Len() > 0 {
				return contPrompt()
			}
			return prompt()
		},
		HistoryPath: historyFile,
		Suggester:   repl.SuggesterFunc(s.suggest),
		Highlighter: highlightGlr,
	})
}

// sessionCache holds the session's frames as the analyser sees them.
// Reading a schema can collect zero rows of a lazy scan, so it is
// refreshed once per statement rather than per keystroke.
type sessionCache struct {
	eval int
	opts analysis.Options
	ok   bool
}

// analysisOptions describes the focused and staged frames for
// completion.
func (s *state) analysisOptions() analysis.Options {
	if s.completion.ok && s.completion.eval == s.evalCount {
		return s.completion.opts
	}
	cwd, _ := os.Getwd()
	opts := analysis.Options{Dir: cwd, Frames: map[string]analysis.Frame{}}
	if s.hasFocus() {
		f := analysis.Frame{Rows: -1}
		if sch, err := s.schema(); err == nil {
			f.Schema = sch
		}
		if s.lf == nil && s.df != nil {
			f.Rows, f.Exact = s.df.Height(), true
		}
		opts.Focus = &f
	}
	for name, nf := range s.frames {
		if nf.df != nil {
			opts.Frames[name] = analysis.Frame{Schema: nf.df.Schema(), Rows: nf.df.Height(), Exact: true, Source: nf.path}
		}
	}
	s.completion = sessionCache{eval: s.evalCount, opts: opts, ok: true}
	return opts
}

// suggest returns ghost text for the line being typed (the rest of
// the best completion) and a hint: the item's type or signature and
// the other candidates. It uses the same analysis as golars-lsp, so
// columns come from the live session and functions from the
// expression API.
func (s *state) suggest(line string) (string, string) {
	if line == "" {
		if s.cont.Len() > 0 {
			return "", "continuing the statement; finish it or press ctrl+c"
		}
		return "", "type .help for commands, → or tab to accept a suggestion"
	}
	src := s.cont.String() + line
	r := analysis.Analyze(src, s.analysisOptions())
	lastLine := strings.Count(src, "\n")
	col := len(src) - strings.LastIndex(src, "\n") - 1
	c := r.Complete(lastLine, col)
	typed := src[len(src)-(col-c.From):]
	var ghost, hint string
	var others []string
	firstKind := analysis.ItemKind(0)
	for _, it := range c.Items {
		text := it.Label
		if it.Kind == analysis.ItemParam {
			text = it.Insert
		}
		if !strings.HasPrefix(text, typed) || text == typed {
			continue
		}
		if ghost == "" {
			ghost = text[len(typed):]
			hint = it.Detail
			firstKind = it.Kind
			continue
		}
		if len(others) < 4 {
			others = append(others, text)
		}
	}
	if sig, found := r.SignatureAt(lastLine, col); found && (ghost == "" || hint == "" || firstKind == analysis.ItemParam) {
		hint = sig.Info.Label()
	}
	if len(others) > 0 {
		if hint != "" {
			hint += "  "
		}
		hint += "also: " + strings.Join(others, ", ")
	}
	return ghost, hint
}

// Highlight styles for REPL input.
var tokenStyles = map[syntax.TokenClass]lipgloss.Style{
	syntax.TokKeyword:    lipgloss.NewStyle().Foreground(warningColor),
	syntax.TokFunction:   lipgloss.NewStyle().Foreground(infoColor),
	syntax.TokMethod:     lipgloss.NewStyle().Foreground(infoColor),
	syntax.TokNamespace:  lipgloss.NewStyle().Foreground(primaryColor),
	syntax.TokParameter:  lipgloss.NewStyle().Foreground(primaryColor).Italic(true),
	syntax.TokString:     lipgloss.NewStyle().Foreground(accentColor),
	syntax.TokNumber:     lipgloss.NewStyle().Foreground(headerColor),
	syntax.TokComment:    lipgloss.NewStyle().Foreground(dimColor).Italic(true),
	syntax.TokType:       lipgloss.NewStyle().Foreground(primaryColor),
	syntax.TokClass:      lipgloss.NewStyle().Foreground(primaryColor).Bold(true),
	syntax.TokEnumMember: lipgloss.NewStyle().Foreground(warningColor),
	syntax.TokOperator:   lipgloss.NewStyle().Foreground(dimColor),
}

// highlightGlr colours a REPL line with the tokens golars-lsp sends
// as semantic tokens.
func highlightGlr(line string) []repl.Span {
	f := syntax.Parse(line)
	var out []repl.Span
	for _, t := range f.Tokens() {
		st, found := tokenStyles[t.Class]
		if !found || t.Line != 0 {
			continue
		}
		out = append(out, repl.Span{Start: t.Col, End: t.EndCol, Style: st})
	}
	return out
}

// unclosed reports whether s ends inside an open bracket or quote,
// so the REPL keeps reading the statement on the next line.
func unclosed(s string) bool {
	depth := 0
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '#':
			return depth > 0
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		}
	}
	return depth > 0 || quote != 0
}
