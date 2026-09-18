package main

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/Gaurav-Gosain/golars/lazy"
)

// graphStyleByPrefix colours each line of an ExplainTree output
// based on the node-kind prefix (`SORT`, `AGG`, `FILTER`, `SCAN`,
// `INNER JOIN`, etc.). Produces a styled-but-still-plain-text tree
// suitable for the terminal. Users who want a raster image can pipe
// MermaidGraph output into `mmdc` instead.
var graphStyleByPrefix = map[string]lipgloss.Style{
	"SCAN":         lipgloss.NewStyle().Foreground(lipgloss.Color("2")), // green
	"FILTER":       lipgloss.NewStyle().Foreground(lipgloss.Color("3")), // yellow
	"SORT":         lipgloss.NewStyle().Foreground(lipgloss.Color("4")), // blue
	"AGG":          lipgloss.NewStyle().Foreground(lipgloss.Color("6")), // cyan
	"PROJECT":      lipgloss.NewStyle().Foreground(lipgloss.Color("5")), // magenta
	"WITH_COLUMNS": lipgloss.NewStyle().Foreground(lipgloss.Color("5")),
	"SELECT":       lipgloss.NewStyle().Foreground(lipgloss.Color("5")),
	"INNER JOIN":   lipgloss.NewStyle().Foreground(lipgloss.Color("13")), // bright magenta
	"LEFT JOIN":    lipgloss.NewStyle().Foreground(lipgloss.Color("13")),
	"CROSS JOIN":   lipgloss.NewStyle().Foreground(lipgloss.Color("13")),
	"SLICE":        lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	"LIMIT":        lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	"UNIQUE":       lipgloss.NewStyle().Foreground(lipgloss.Color("10")),
	"DROP":         lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	"RENAME":       lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	"CAST":         lipgloss.NewStyle().Foreground(lipgloss.Color("11")),
	"CACHE":        lipgloss.NewStyle().Foreground(lipgloss.Color("12")),
	"FILL_NULL":    lipgloss.NewStyle().Foreground(lipgloss.Color("11")),
	"DROP_NULLS":   lipgloss.NewStyle().Foreground(lipgloss.Color("11")),
	"WITH_ROW_IDX": lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
}

var connectorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("8")) // dim

// renderStyledTree returns the box-drawn ExplainTree output with
// node labels coloured per kind and connector glyphs dimmed.
func renderStyledTree(n lazy.Node) string {
	raw := lazy.ExplainTree(n)
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(raw, "\n"), "\n") {
		prefix, label := splitTreeLine(line)
		b.WriteString(connectorStyle.Render(prefix))
		b.WriteString(colourForLabel(label).Render(label))
		b.WriteByte('\n')
	}
	return b.String()
}

// splitTreeLine finds the index where the box-drawing prefix ends
// and the node label begins. Any character after the last run of
// " │├└─" is treated as the label.
func splitTreeLine(line string) (prefix, label string) {
	runes := []rune(line)
	split := 0
	for i, r := range runes {
		if r == ' ' || r == '│' || r == '├' || r == '└' || r == '─' {
			split = i + 1
			continue
		}
		break
	}
	return string(runes[:split]), string(runes[split:])
}

func colourForLabel(label string) lipgloss.Style {
	for prefix, style := range graphStyleByPrefix {
		if strings.HasPrefix(label, prefix) {
			return style
		}
	}
	return lipgloss.NewStyle()
}

// cmdShowGraph prints the styled plan tree. Alias: .graph
func (s *state) cmdShowGraph() error {
	lf := s.currentLazy()
	plan := lf.Plan()
	fmt.Println()
	fmt.Print(renderStyledTree(plan))
	fmt.Println()
	return nil
}

// cmdMermaid prints the Mermaid flowchart source for the current
// lazy plan. Pipe into `mmdc` for a PNG/SVG, or paste into any
// Markdown host that speaks Mermaid.
//
// Output is pure Mermaid with no banner so the command composes
// cleanly in shell pipelines:
//
//	golars explain --mermaid pipeline.glr | mmdc -i - -o plan.png
func (s *state) cmdMermaid() error {
	lf := s.currentLazy()
	fmt.Print(lazy.MermaidGraph(lf.Plan()))
	return nil
}
