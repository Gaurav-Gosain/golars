package main

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/Gaurav-Gosain/golars/lazy"
)

// Plan commands print the focused pipeline's logical plan.

func cmdExplain(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	out, err := s.currentLazy().Explain()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(out)
	fmt.Println()
	return nil
}

// cmdExplainTree is explain rendered as a box-drawn tree.
func cmdExplainTree(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	out, err := s.currentLazy().ExplainTree()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(out)
	fmt.Println()
	return nil
}

// cmdGraph prints the plan tree with node labels coloured by kind.
func cmdGraph(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(renderStyledTree(s.currentLazy().Plan()))
	fmt.Println()
	return nil
}

// cmdMermaid prints Mermaid flowchart source with no banner, so it
// composes in pipelines:
//
//	golars explain --mermaid pipeline.glr | mmdc -i - -o plan.png
func cmdMermaid(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	fmt.Print(lazy.MermaidGraph(s.currentLazy().Plan()))
	return nil
}

type graphStyle struct {
	prefix string
	style  lipgloss.Style
}

func fg(c string) lipgloss.Style { return lipgloss.NewStyle().Foreground(lipgloss.Color(c)) }

// graphStyles colours plan nodes by label prefix. Longer prefixes come
// first so DROP_NULLS is not matched as DROP.
var graphStyles = []graphStyle{
	{"WITH_COLUMNS", fg("5")},
	{"WITH_ROW_IDX", fg("8")},
	{"DROP_NULLS", fg("11")},
	{"INNER JOIN", fg("13")},
	{"CROSS JOIN", fg("13")},
	{"LEFT JOIN", fg("13")},
	{"FILL_NULL", fg("11")},
	{"PROJECT", fg("5")},
	{"FILTER", fg("3")},
	{"SELECT", fg("5")},
	{"RENAME", fg("8")},
	{"UNIQUE", fg("10")},
	{"SLICE", fg("8")},
	{"LIMIT", fg("8")},
	{"CACHE", fg("12")},
	{"SCAN", fg("2")},
	{"SORT", fg("4")},
	{"DROP", fg("8")},
	{"CAST", fg("11")},
	{"AGG", fg("6")},
}

var connectorStyle = fg("8")

// renderStyledTree returns the ExplainTree output with node labels
// coloured per kind and the box-drawing connectors dimmed.
func renderStyledTree(n lazy.Node) string {
	var b strings.Builder
	for line := range strings.SplitSeq(strings.TrimRight(lazy.ExplainTree(n), "\n"), "\n") {
		prefix, label := splitTreeLine(line)
		b.WriteString(connectorStyle.Render(prefix))
		b.WriteString(styleForLabel(label).Render(label))
		b.WriteByte('\n')
	}
	return b.String()
}

// splitTreeLine splits a tree line into the box-drawing prefix and the
// node label that follows it.
func splitTreeLine(line string) (prefix, label string) {
	label = strings.TrimLeft(line, " │├└─")
	return line[:len(line)-len(label)], label
}

func styleForLabel(label string) lipgloss.Style {
	for _, g := range graphStyles {
		if strings.HasPrefix(label, g.prefix) {
			return g.style
		}
	}
	return lipgloss.NewStyle()
}
