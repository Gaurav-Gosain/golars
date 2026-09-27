package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
)

// newDiffCmd prints a row-level diff between two data files. Without
// --key, rows are compared positionally. With --key COL, rows are
// joined on that column and compared cell-by-cell.
func newDiffCmd() *cobra.Command {
	var keyCol string
	cmd := &cobra.Command{
		Use:     "diff A B",
		Short:   "row-level diff between two data files",
		Example: "golars diff --key id a.csv b.csv",
		Args:    cobra.ExactArgs(2),
	}
	cmd.Flags().StringVarP(&keyCol, "key", "k", "", "column to match rows on (default: positional compare)")
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		ctx := context.Background()
		a, err := fileio.Read(ctx, args[0])
		if err != nil {
			return err
		}
		defer a.Release()
		b, err := fileio.Read(ctx, args[1])
		if err != nil {
			return err
		}
		defer b.Release()
		if !schemaEqual(a, b) {
			return fmt.Errorf("schemas differ:\n  A: %s\n  B: %s", a.Schema(), b.Schema())
		}
		var changed int
		if keyCol != "" {
			changed, err = diffKeyed(a, b, keyCol)
		} else {
			changed = diffPositional(a, b)
		}
		if err != nil {
			return err
		}
		// Like diff(1), differences exit 1. The report above already
		// said what changed, so no error message follows.
		if changed > 0 {
			return errSilent
		}
		return nil
	}
	return cmd
}

func schemaEqual(a, b *dataframe.DataFrame) bool {
	if a.Width() != b.Width() {
		return false
	}
	an := a.ColumnNames()
	bn := b.ColumnNames()
	for i := range an {
		if an[i] != bn[i] {
			return false
		}
	}
	return true
}

// diffPositional compares rows at the same index. Prints "+" for
// rows present only in B, "-" for only in A, "~" for differing rows.
// Returns the number of differing rows.
func diffPositional(a, b *dataframe.DataFrame) int {
	aRows, _ := a.Rows()
	bRows, _ := b.Rows()
	n := max(len(bRows), len(aRows))
	changed := 0
	for i := range n {
		var ar, br []any
		if i < len(aRows) {
			ar = aRows[i]
		}
		if i < len(bRows) {
			br = bRows[i]
		}
		switch {
		case ar == nil:
			fmt.Printf("%s row %d: %v\n", successStyle.Render("+"), i, br)
			changed++
		case br == nil:
			fmt.Printf("%s row %d: %v\n", errStyle.Render("-"), i, ar)
			changed++
		default:
			if !rowsEqual(ar, br) {
				fmt.Printf("%s row %d: %v → %v\n",
					cmdStyle.Render("~"), i, ar, br)
				changed++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "%d row(s) differ\n", changed)
	return changed
}

// diffKeyed joins on keyCol and reports per-key additions, removals,
// and updates. Returns (changed, error).
func diffKeyed(a, b *dataframe.DataFrame, keyCol string) (int, error) {
	aIdx, err := keyToIndex(a, keyCol)
	if err != nil {
		return 0, err
	}
	bIdx, err := keyToIndex(b, keyCol)
	if err != nil {
		return 0, err
	}
	aRows, _ := a.Rows()
	bRows, _ := b.Rows()
	changed := 0
	for k, ai := range aIdx {
		bi, ok := bIdx[k]
		if !ok {
			fmt.Printf("%s %s: %v\n", errStyle.Render("-"), k, aRows[ai])
			changed++
			continue
		}
		if !rowsEqual(aRows[ai], bRows[bi]) {
			fmt.Printf("%s %s: %v → %v\n", cmdStyle.Render("~"), k, aRows[ai], bRows[bi])
			changed++
		}
	}
	for k, bi := range bIdx {
		if _, ok := aIdx[k]; ok {
			continue
		}
		fmt.Printf("%s %s: %v\n", successStyle.Render("+"), k, bRows[bi])
		changed++
	}
	fmt.Fprintf(os.Stderr, "%d key(s) differ\n", changed)
	return changed, nil
}

func keyToIndex(df *dataframe.DataFrame, col string) (map[string]int, error) {
	c, err := df.Column(col)
	if err != nil {
		return nil, err
	}
	chunk := c.Chunk(0)
	n := chunk.Len()
	out := make(map[string]int, n)
	for i := range n {
		out[cellKeyString(chunk, i)] = i
	}
	return out, nil
}

func cellKeyString(chunk any, i int) string {
	// Null marker for any dtype. Upstream diff logic keys rows by the
	// returned string, so a deterministic token per-null position is
	// enough for set arithmetic.
	if v, ok := chunk.(interface{ IsValid(int) bool }); ok && !v.IsValid(i) {
		return "\x00"
	}
	switch a := chunk.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(i))
	}
	// Catch-all for temporal (Timestamp/Date/Time/Duration) and any
	// other Arrow array that implements ValueStr. Preferred to the
	// old row-index fallback because an actual key value is way more
	// useful in diff output.
	if v, ok := chunk.(interface{ ValueStr(int) string }); ok {
		return v.ValueStr(i)
	}
	return fmt.Sprintf("?%d", i)
}

func rowsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if fmt.Sprint(a[i]) != fmt.Sprint(b[i]) {
			return false
		}
	}
	return true
}
