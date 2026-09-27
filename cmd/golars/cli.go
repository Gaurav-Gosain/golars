package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/fang"
	"github.com/spf13/cobra"
)

// errSilent makes the process exit 1 without printing anything more.
// Subcommands return it after they have already reported the outcome
// themselves (diff found differences, lint found warnings).
var errSilent = errors.New("silent failure")

// execCLI builds the cobra tree, pipes it through fang for styled help
// and errors, and returns the process exit code.
func execCLI() int {
	// Color policy runs before cobra so subcommand output honours
	// --no-color / NO_COLOR even when cobra short-circuits on help
	// or completion.
	applyColorPolicy(colorsEnabled(os.Args[1:]))

	root := newRootCmd()
	err := fang.Execute(context.Background(), root,
		fang.WithVersion(version),
		fang.WithErrorHandler(printCLIError))
	if err != nil {
		return 1
	}
	return 0
}

// printCLIError writes a failed command's error as one line, in the
// same "error ..." form the REPL uses. fang's default handler
// title-cases the message, which mangles file paths.
func printCLIError(w io.Writer, _ fang.Styles, err error) {
	if errors.Is(err, errSilent) {
		return
	}
	fmt.Fprintln(w, errStyle.Render("error:")+" "+errMsgStyle.Render(err.Error()))
	if isUsageError(err) {
		fmt.Fprintln(w, dimStyle.Render("Try --help for usage."))
	}
}

// isUsageError recognises cobra's argument and flag parsing errors.
func isUsageError(err error) bool {
	msg := err.Error()
	for _, prefix := range []string{
		"unknown flag", "unknown shorthand flag", "flag needs an argument",
		"invalid argument", "unknown command", "accepts ", "requires at least",
		"requires at most", "requires exactly",
	} {
		if strings.HasPrefix(msg, prefix) {
			return true
		}
	}
	return false
}

// newRootCmd wires the subcommand tree. Each subcommand registers its
// own cobra flags; root collects the REPL-specific flags (--load,
// --run, --preview, ...) so they work both on `golars` and on
// `golars repl`.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "golars",
		Short: "pure-Go DataFrames with lazy plan and optimizer",
		Long: `golars is a pure-Go port of polars built on Apache Arrow.

With no subcommand, runs the interactive REPL. Use a subcommand
(schema, sql, head, browse, ...) for one-shot queries against files.`,
		Example: `  golars                              # start the REPL
  golars sql 'SELECT * FROM t' t.csv  # run SQL against a file
  golars schema t.csv                 # print column names + dtypes
  golars head t.csv 20                # print first 20 rows
  golars browse t.csv                 # interactive TUI viewer`,
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// `--no-color` is parsed ahead of cobra by applyColorPolicy so we
	// only register the flag here to keep cobra's help complete.
	root.PersistentFlags().Bool("no-color", false, "disable ANSI colour output")

	bindReplFlags(root)
	root.RunE = replRunE(root)

	root.AddCommand(
		newRunCmd(),
		newFmtCmd(),
		newLintCmd(),
		newSchemaCmd(),
		newStatsCmd(),
		newHeadCmd(),
		newTailCmd(),
		newDiffCmd(),
		newSqlCmd(),
		newBrowseCmd(),
		newExplainCmd(),
		newDoctorCmd(),
		newPeekCmd(),
		newSampleCmd(),
		newConvertCmd(),
		newCatCmd(),
		newReplCmd(),
		newTranspileCmd(),
		newKernelHostCmd(),
		newVersionCmd(),
	)

	return root
}

// bindReplFlags registers the REPL-mode flags (--load, --run, etc.)
// on a command. Used on both root and the explicit `repl` subcommand
// so invocation is identical either way.
func bindReplFlags(cmd *cobra.Command) {
	cmd.Flags().String("load", "", "load a csv/parquet/arrow file at startup")
	cmd.Flags().String("run", "", "run a .glr script and exit")
	cmd.Flags().String("preview", "", "machine-readable script preview (for editor plugins)")
	cmd.Flags().Int("preview-rows", 10, "row cap for --preview output")
	cmd.Flags().String("preview-format", "table", "preview output format: table (default) or markdown")
	cmd.Flags().Bool("timing", false, "show per-command wall time in the REPL")
}

// replRunE returns a RunE that reads the REPL flags off cmd and
// dispatches to runREPL. Shared by root and the `repl` subcommand.
func replRunE(_ *cobra.Command) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		loadPath, _ := cmd.Flags().GetString("load")
		scriptPath, _ := cmd.Flags().GetString("run")
		previewPath, _ := cmd.Flags().GetString("preview")
		previewRows, _ := cmd.Flags().GetInt("preview-rows")
		previewFormat, _ := cmd.Flags().GetString("preview-format")
		timing, _ := cmd.Flags().GetBool("timing")
		return runREPL(loadPath, scriptPath, previewPath, previewRows, previewFormat, timing)
	}
}

// newReplCmd is the explicit `golars repl` subcommand, identical to
// invoking `golars` with no subcommand. Provided for users who want
// to be unambiguous in shell scripts or documentation.
func newReplCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "repl",
		Short:         "start the interactive REPL",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	bindReplFlags(cmd)
	cmd.RunE = replRunE(cmd)
	return cmd
}

// newVersionCmd mirrors the `--version` flag as a subcommand so both
// `golars version` and `golars --version` work. Handy in Dockerfiles
// and CI smoke-tests that expect positional invocation.
func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "print version and exit",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("golars version %s\n", version)
		},
	}
}
