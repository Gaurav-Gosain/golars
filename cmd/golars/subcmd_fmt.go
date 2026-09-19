package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script"
)

// newFmtCmd rewrites a .glr file into canonical form:
//   - strip leading `.` (scripts use the bare form)
//   - collapse runs of whitespace inside commands
//   - normalize unknown-command detection to a lint warning
func newFmtCmd() *cobra.Command {
	var write, printDiff bool
	cmd := &cobra.Command{
		Use:     "fmt FILE.glr [FILE.glr...]",
		Short:   "canonicalize a .glr script",
		Example: "golars fmt -w script.glr",
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.Flags().BoolVarP(&write, "write", "w", false, "write result back to each file instead of stdout")
	cmd.Flags().BoolVarP(&printDiff, "diff", "d", false, "print a unified diff of the formatting change")
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		failed := false
		for _, p := range args {
			src, err := os.ReadFile(p)
			if err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				failed = true
				continue
			}
			out := formatGlr(string(src))
			switch {
			case write:
				if err := os.WriteFile(p, []byte(out), 0o644); err != nil {
					fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
					failed = true
				}
			case printDiff:
				if string(src) != out {
					fmt.Print(unifiedDiff(p, string(src), out))
				}
			default:
				fmt.Print(out)
			}
		}
		if failed {
			return errSubcommandFailed
		}
		return nil
	}
	return cmd
}

// formatGlr returns the canonical form of a .glr script. Leading dot
// prefixes are stripped from lines that contain a recognised command
// (so `.load x` becomes `load x`: the bare form is the one favoured
// by script files; REPL still accepts both). Comments and blank lines
// are preserved verbatim. Multiple spaces inside an argument list
// collapse to a single space (string literals keep their interior
// intact).
func formatGlr(src string) string {
	var buf strings.Builder
	for raw := range strings.SplitSeq(strings.TrimSuffix(src, "\n"), "\n") {
		line := raw
		trimmed := strings.TrimSpace(line)
		// Empty or comment line: preserve original whitespace.
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			buf.WriteString(line)
			buf.WriteByte('\n')
			continue
		}
		stripped := strings.TrimPrefix(trimmed, ".")
		parts := splitKeepQuoted(stripped)
		if len(parts) == 0 {
			buf.WriteString(line)
			buf.WriteByte('\n')
			continue
		}
		first := strings.ToLower(parts[0])
		if script.FindCommand(first) != nil {
			parts[0] = first
		}
		buf.WriteString(strings.Join(parts, " "))
		buf.WriteByte('\n')
	}
	// Avoid doubling the final newline if the original lacked one.
	out := buf.String()
	if !strings.HasSuffix(src, "\n") {
		out = strings.TrimSuffix(out, "\n")
	}
	return out
}

// splitKeepQuoted splits on whitespace but keeps "quoted strings" as a
// single token. Crude but adequate for .glr's argument grammar.
func splitKeepQuoted(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			cur.WriteString(s[i : i+2])
			i++
			continue
		}
		if c == '#' && inQuote == 0 {
			cur.WriteString(s[i:])
			break
		}
		if c == inQuote {
			inQuote = 0
		} else if inQuote == 0 && (c == '"' || c == '\'') {
			inQuote = c
		}
		if inQuote == 0 && (c == ' ' || c == '\t') {
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			continue
		}
		cur.WriteByte(c)
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// unifiedDiff produces a minimal unified-diff style output for two
// strings. Just enough for `golars fmt -d` to show a user what
// changed; not a full replacement for diff(1).
func unifiedDiff(path, a, b string) string {
	var buf strings.Builder
	aLines := strings.Split(a, "\n")
	bLines := strings.Split(b, "\n")
	fmt.Fprintf(&buf, "--- %s (original)\n+++ %s (formatted)\n", path, path)
	// Line-by-line walk; for equal-length pairs we check line equality.
	n := max(len(bLines), len(aLines))
	for i := range n {
		var av, bv string
		if i < len(aLines) {
			av = aLines[i]
		}
		if i < len(bLines) {
			bv = bLines[i]
		}
		if av == bv {
			continue
		}
		if av != "" {
			fmt.Fprintf(&buf, "-%s\n", av)
		}
		if bv != "" {
			fmt.Fprintf(&buf, "+%s\n", bv)
		}
	}
	return buf.String()
}
