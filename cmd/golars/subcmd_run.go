package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

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
		defer s.close()
		return runScript(s, args[0])
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
		defer s.close()
		// Build the pipeline quietly: the only output is the plan.
		// Plan statements inside the script are skipped so they do not
		// print a partial plan first.
		runner := script.Runner{Exec: script.ExecutorFunc(func(line string) error {
			if spec := script.FindCommand(strings.Fields(line)[0]); spec != nil && spec.Category == "plan" {
				return nil
			}
			return s.handle(line)
		})}
		err := withStdoutSilenced(func() error { return runner.RunFile(args[0]) })
		if err != nil && !errors.Is(err, errExit) {
			return err
		}
		render := cmdExplain
		switch {
		case mermaid:
			render = cmdMermaid
		case graph:
			render = cmdGraph
		case tree:
			render = cmdExplainTree
		}
		if err := render(s, nil); err != nil {
			return err
		}
		if profile || tracePath != "" {
			return runExplainProfile(s, profile, tracePath)
		}
		return nil
	}
	return cmd
}

// runExplainProfile runs the focused lazy pipeline under a Profiler
// and prints / writes the result.
func runExplainProfile(s *state, profile bool, tracePath string) error {
	if s.lf == nil {
		return fmt.Errorf("profile: the script leaves no lazy pipeline to run")
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
		ok("wrote chrome trace to %s", tracePath)
	}
	return nil
}

// glrFileCompletion returns completion hints restricted to .glr files.
func glrFileCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"glr"}, cobra.ShellCompDirectiveFilterFileExt
}
