package main

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/browse"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
)

// newBrowseCmd opens the TUI viewer for a data file. Arrow keys scroll,
// `/` filters, `q` quits. See the browse package for the model.
func newBrowseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "browse FILE",
		Short:   "interactive TUI table viewer",
		Example: "golars browse data.csv",
		Args:    cobra.ExactArgs(1),
	}
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		ctx := context.Background()
		df, err := fileio.Read(ctx, args[0])
		if err != nil {
			return err
		}
		defer df.Release()
		return browse.RunWithContext(ctx, df, args[0])
	}
	return cmd
}
