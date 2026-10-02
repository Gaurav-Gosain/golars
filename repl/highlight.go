package repl

import (
	"strings"

	"charm.land/lipgloss/v2"
)

// Span styles the bytes [Start, End) of the input line.
type Span struct {
	Start, End int
	Style      lipgloss.Style
}

// Highlighter returns style spans for an input line. Spans may be in
// any order; bytes outside every span are drawn plain. It runs on
// every keystroke, so it should be cheap.
type Highlighter func(line string) []Span

// cursorStyle draws the cursor cell when the line is highlighted.
var cursorStyle = lipgloss.NewStyle().Reverse(true)

// renderPlain draws value with the spans applied and no cursor.
func renderPlain(value string, spans []Span) string {
	out := renderHighlighted(value, spans, -1, "", lipgloss.NewStyle())
	return out
}

// renderHighlighted draws value with the spans applied, the cursor at
// rune index cursor, and ghost text after the value.
func renderHighlighted(value string, spans []Span, cursor int, ghost string, ghostStyle lipgloss.Style) string {
	styleAt := make([]int, len(value)) // index into spans, -1 for plain
	for i := range styleAt {
		styleAt[i] = -1
	}
	for k, sp := range spans {
		for b := max(sp.Start, 0); b < min(sp.End, len(value)); b++ {
			styleAt[b] = k
		}
	}
	var out, run strings.Builder
	runStyle := -2
	flush := func() {
		if run.Len() == 0 {
			return
		}
		if runStyle >= 0 {
			out.WriteString(spans[runStyle].Style.Render(run.String()))
		} else {
			out.WriteString(run.String())
		}
		run.Reset()
	}
	idx := 0
	for b, r := range value {
		if idx == cursor {
			flush()
			out.WriteString(cursorStyle.Render(string(r)))
			runStyle = -2
			idx++
			continue
		}
		if styleAt[b] != runStyle {
			flush()
			runStyle = styleAt[b]
		}
		run.WriteRune(r)
		idx++
	}
	flush()
	if cursor < 0 {
		return out.String()
	}
	if cursor >= idx {
		g := []rune(ghost)
		if len(g) > 0 {
			out.WriteString(cursorStyle.Render(string(g[0])))
			out.WriteString(ghostStyle.Render(string(g[1:])))
		} else {
			out.WriteString(cursorStyle.Render(" "))
		}
	} else if ghost != "" {
		out.WriteString(ghostStyle.Render(ghost))
	}
	return out.String()
}
