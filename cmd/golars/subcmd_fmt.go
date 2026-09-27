package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// newFmtCmd rewrites a .glr file into canonical form. The rules live
// in script/syntax.Format, shared with the language server.
func newFmtCmd() *cobra.Command {
	var write, printDiff, check bool
	cmd := &cobra.Command{
		Use:   "fmt FILE.glr [FILE.glr...]",
		Short: "canonicalize a .glr script",
		Long: "Print each script in canonical form: lower-case commands without the\n" +
			"leading dot, single spaces, `, ` in lists, formatted expressions, aligned\n" +
			"`with NAME = ...` and `load PATH as NAME` runs and trailing comments.\n" +
			"Lines with syntax errors are kept as written.",
		Example: "golars fmt -w script.glr\ngolars fmt --check examples/*.glr",
		Args:    cobra.MinimumNArgs(1),
	}
	cmd.Flags().BoolVarP(&write, "write", "w", false, "write result back to each file instead of stdout")
	cmd.Flags().BoolVarP(&printDiff, "diff", "d", false, "print a unified diff of the formatting change")
	cmd.Flags().BoolVarP(&check, "check", "c", false, "list files that are not formatted and exit 1 if any")
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
			case check:
				if string(src) != out {
					fmt.Println(p)
					failed = true
				}
			case write:
				if string(src) == out {
					continue
				}
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
			return errSilent
		}
		return nil
	}
	return cmd
}

// formatGlr returns the canonical form of a .glr script.
func formatGlr(src string) string { return syntax.Format(src) }

// unifiedDiff renders the change from a to b as a unified diff with
// three lines of context, computed from a longest common subsequence
// of lines.
func unifiedDiff(path, a, b string) string {
	al := strings.Split(strings.TrimSuffix(a, "\n"), "\n")
	bl := strings.Split(strings.TrimSuffix(b, "\n"), "\n")
	// lcs[i][j] is the LCS length of al[i:] and bl[j:].
	lcs := make([][]int, len(al)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(bl)+1)
	}
	for i := len(al) - 1; i >= 0; i-- {
		for j := len(bl) - 1; j >= 0; j-- {
			if al[i] == bl[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	type op struct {
		kind byte // ' ', '-', '+'
		text string
		ai   int
		bi   int
	}
	var ops []op
	i, j := 0, 0
	for i < len(al) || j < len(bl) {
		switch {
		case i < len(al) && j < len(bl) && al[i] == bl[j]:
			ops = append(ops, op{' ', al[i], i, j})
			i++
			j++
		case i < len(al) && (j == len(bl) || lcs[i+1][j] >= lcs[i][j+1]):
			ops = append(ops, op{'-', al[i], i, j})
			i++
		default:
			ops = append(ops, op{'+', bl[j], i, j})
			j++
		}
	}
	var buf strings.Builder
	fmt.Fprintf(&buf, "--- %s\n+++ %s (formatted)\n", path, path)
	const context = 3
	for k := 0; k < len(ops); {
		if ops[k].kind == ' ' {
			k++
			continue
		}
		start := max(k-context, 0)
		end := k
		for end < len(ops) {
			if ops[end].kind != ' ' {
				end++
				continue
			}
			run := end
			for run < len(ops) && ops[run].kind == ' ' {
				run++
			}
			if run-end > 2*context || run == len(ops) {
				end = min(end+context, len(ops))
				break
			}
			end = run
		}
		aLen, bLen := 0, 0
		for _, o := range ops[start:end] {
			if o.kind != '+' {
				aLen++
			}
			if o.kind != '-' {
				bLen++
			}
		}
		fmt.Fprintf(&buf, "@@ -%d,%d +%d,%d @@\n", ops[start].ai+1, aLen, ops[start].bi+1, bLen)
		for _, o := range ops[start:end] {
			buf.WriteByte(o.kind)
			buf.WriteString(o.text)
			buf.WriteByte('\n')
		}
		k = end
	}
	return buf.String()
}
