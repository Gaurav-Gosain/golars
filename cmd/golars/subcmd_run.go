package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/script"
)

// newRunCmd executes a .glr script end-to-end, tracing each line as
// it runs. Mirrors the REPL's .source command as a one-shot.
func newRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "run SCRIPT.glr",
		Short:   "execute a .glr script",
		Example: "golars run pipeline.glr",
		Args:    cobra.ExactArgs(1),
	}
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		s := newState(false)
		if err := runScript(s, args[0]); err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		return nil
	}
	return cmd
}

// newExplainCmd runs a script up to the point of collection, then
// prints the optimised plan. Optional --profile wraps the collect in
// a Profiler and prints per-node timings; --trace writes a
// chrome-trace JSON for use with chrome://tracing or Perfetto.
func newExplainCmd() *cobra.Command {
	var profile bool
	var tracePath string
	var tree bool
	var mermaid bool
	var graph bool
	cmd := &cobra.Command{
		Use:   "explain SCRIPT.glr",
		Short: "print the lazy plan for a .glr script",
		Example: `golars explain --profile pipeline.glr
  golars explain --mermaid pipeline.glr | mmdc -i - -o plan.png
  golars explain --graph   pipeline.glr`,
		Args: cobra.ExactArgs(1),
	}
	cmd.Flags().BoolVar(&profile, "profile", false, "collect per-node timings and print them")
	cmd.Flags().StringVar(&tracePath, "trace", "", "write a chrome-trace JSON to PATH")
	cmd.Flags().BoolVar(&tree, "tree", false, "render the plan as a box-drawn tree")
	cmd.Flags().BoolVar(&mermaid, "mermaid", false, "emit plain Mermaid flowchart source on stdout")
	cmd.Flags().BoolVar(&graph, "graph", false, "colourised box-drawn tree for the terminal")
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		s := newState(false)
		runner := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
			// Drop the trailing .explain so we capture the pre-collect
			// plan; otherwise the script's own .explain runs first.
			if line == ".explain" || line == "explain" {
				return nil
			}
			return s.handle(line)
		})}
		// Silence per-statement "ok ..." noise while the script
		// builds the pipeline when the caller asked for a clean
		// machine-readable output. Without this mmdc or a
		// screenshot script would have to strip banner lines.
		quiet := mermaid || graph
		realStdout := os.Stdout
		if quiet {
			devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err == nil {
				os.Stdout = devNull
				defer func() {
					os.Stdout = realStdout
					_ = devNull.Close()
				}()
			} else {
				// Fallback: discard writer wrapped as a throwaway file.
				_ = io.Discard
			}
		}
		if err := runner.RunFile(args[0]); err != nil {
			if quiet {
				os.Stdout = realStdout
			}
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		if quiet {
			os.Stdout = realStdout
		}
		explainFn := s.cmdExplain
		switch {
		case mermaid:
			explainFn = s.cmdMermaid
		case graph:
			explainFn = s.cmdShowGraph
		case tree:
			explainFn = s.cmdExplainTree
		}
		if err := explainFn(); err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		if profile || tracePath != "" {
			if err := runExplainProfile(s, profile, tracePath); err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
		}
		return nil
	}
	return cmd
}

// runExplainProfile runs the focused lazy pipeline under a Profiler
// and prints / writes the result.
func runExplainProfile(s *state, profile bool, tracePath string) error {
	if s.lf == nil {
		return fmt.Errorf("no pipeline to profile")
	}
	p := lazy.NewProfiler()
	out, err := s.lf.Collect(s.ctx, lazy.WithProfiler(p))
	if err != nil {
		return err
	}
	out.Release()
	if profile {
		fmt.Println()
		fmt.Print(p.Report())
	}
	if tracePath != "" {
		if err := os.WriteFile(tracePath, []byte(p.ChromeTrace()), 0o644); err != nil {
			return err
		}
		fmt.Printf("%s wrote chrome-trace to %s\n", successStyle.Render("✓"), tracePath)
	}
	return nil
}

// glrFileCompletion returns completion hints restricted to .glr files.
func glrFileCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"glr"}, cobra.ShellCompDirectiveFilterFileExt
}
