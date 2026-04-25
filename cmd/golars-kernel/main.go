// Command golars-kernel is a Jupyter kernel for the golars `.glr`
// scripting language. It speaks the Jupyter messaging protocol v5.3
// over ZeroMQ and delegates cell execution to a child `golars
// kernel-host` subprocess so the in-binary state matches the REPL.
//
// Usage:
//
//	golars-kernel <connection-file>     # invoked by Jupyter
//	golars-kernel install               # register kernel.json
//	golars-kernel install --user        # explicit user-scope install
//	golars-kernel install --prefix DIR  # install into a venv prefix
//
// Once installed, JupyterLab / Jupyter Notebook / VSCode pick "golars"
// up automatically. See docs/jupyter.md for the walkthrough.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// version is overridden by goreleaser at build time.
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "golars-kernel <connection-file>",
		Short:         "Jupyter kernel for the golars .glr scripting language",
		Args:          cobra.MaximumNArgs(1),
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			conn, err := loadConnectionFile(args[0])
			if err != nil {
				return fmt.Errorf("connection file: %w", err)
			}
			k, err := newKernel(context.Background(), conn)
			if err != nil {
				return err
			}
			defer k.Close()
			return k.Run()
		},
	}
	root.AddCommand(newInstallCmd(), newVersionCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "golars-kernel:", err)
		os.Exit(1)
	}
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version and exit",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Println(version)
		},
	}
}
