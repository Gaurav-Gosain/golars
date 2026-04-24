package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/script/transpile"
)

// newTranspileCmd wires `golars transpile SCRIPT.glr -o out.go`.
//
// Emits a standalone Go program that reproduces the pipeline using
// the public golars API. Intended for users who prototype in `.glr`
// then want to port the final pipeline into a larger Go codebase.
// The generated output always compiles; commands without a direct
// lazy equivalent become `// TODO(glr):` reminders rather than being
// silently dropped.
func newTranspileCmd() *cobra.Command {
	var out string
	var pkg string
	cmd := &cobra.Command{
		Use:   "transpile SCRIPT.glr",
		Short: "emit a Go program that reproduces the .glr pipeline",
		Example: `golars transpile my-pipeline.glr -o main.go
  golars transpile my-pipeline.glr --package analytics -o pipeline.go`,
		Args: cobra.ExactArgs(1),
	}
	cmd.Flags().StringVarP(&out, "out", "o", "", "write Go source to PATH (default: stdout)")
	cmd.Flags().StringVar(&pkg, "package", "main", "Go package name for the generated file")
	cmd.ValidArgsFunction = glrFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		w := os.Stdout
		if out != "" {
			f, err := os.Create(out)
			if err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			defer f.Close()
			w = f
		}
		if err := transpile.Transpile(args[0], w, pkg); err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		return nil
	}
	return cmd
}
