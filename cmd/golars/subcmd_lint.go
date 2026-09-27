package main

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script"
)

// newLintCmd reports likely bugs in a .glr script without running it.
// Detects unknown commands, unused stashes, use-before-load, and
// unbalanced quotes in filter predicates.
func newLintCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "lint FILE.glr [FILE.glr...]",
		Short:   "report common .glr mistakes without running the script",
		Example: "golars lint pipeline.glr",
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		warnings := 0
		for _, path := range args {
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, m := range lintGlr(string(src)) {
				fmt.Printf("%s:%d: %s\n", path, m.line, m.msg)
				warnings++
			}
		}
		if warnings > 0 {
			fmt.Fprintf(os.Stderr, "%d lint warning(s)\n", warnings)
			return errSilent
		}
		return nil
	}
	return cmd
}

type lintMsg struct {
	line int
	msg  string
}

// lintGlr walks the script and returns warnings sorted by line.
// Line numbers are 1-based. Continuation lines (trailing `\`) are not
// joined, so a warning points at the physical line it came from.
func lintGlr(src string) []lintMsg {
	var out []lintMsg
	stashed := map[string]int{}  // name -> line where stashed
	defined := map[string]bool{} // names from load/scan ... as NAME and stash
	used := map[string]bool{}
	for i, raw := range strings.Split(src, "\n") {
		lineNo := i + 1
		stmt := strings.TrimPrefix(script.Normalize(raw), ".")
		parts := splitKeepQuoted(stmt)
		if len(parts) == 0 {
			continue
		}
		spec := script.FindCommand(parts[0])
		if spec == nil {
			out = append(out, lintMsg{line: lineNo, msg: script.UnknownCommand(parts[0]).Error()})
			continue
		}
		switch spec.Name {
		case "stash":
			if len(parts) < 2 {
				out = append(out, lintMsg{line: lineNo, msg: "stash requires a NAME"})
				break
			}
			stashed[parts[1]] = lineNo
			defined[parts[1]] = true
		case "use", "join":
			if len(parts) < 2 {
				out = append(out, lintMsg{line: lineNo, msg: "usage: " + spec.Signature})
				break
			}
			name := parts[1]
			used[name] = true
			// join also accepts a file path, so only `use` must name a
			// staged frame.
			if spec.Name == "use" && !defined[name] {
				out = append(out, lintMsg{
					line: lineNo,
					msg:  fmt.Sprintf("use %q with no earlier stash or load ... as", name),
				})
			}
		case "load", "scan_csv", "scan_parquet", "scan_ipc", "scan_json", "scan_ndjson", "scan_auto":
			if len(parts) >= 4 && strings.EqualFold(parts[2], "as") {
				defined[parts[3]] = true
			}
		case "filter":
			if strings.Count(strings.Join(parts[1:], " "), `"`)%2 != 0 {
				out = append(out, lintMsg{line: lineNo, msg: "filter has unbalanced quotes"})
			}
		}
	}
	for name, line := range stashed {
		if !used[name] {
			out = append(out, lintMsg{line: line, msg: fmt.Sprintf("stash %q is never used", name)})
		}
	}
	slices.SortStableFunc(out, func(a, b lintMsg) int { return a.line - b.line })
	return out
}
