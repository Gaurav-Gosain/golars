package main

import (
	"fmt"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/browse"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Inspect commands print information about the focus without
// changing it.

// collectAndPrint runs lf and prints the result as a table followed by
// a row-count footer.
func (s *state) collectAndPrint(lf lazy.LazyFrame) error {
	df, err := lf.Collect(s.ctx)
	if err != nil {
		return err
	}
	defer df.Release()
	if !printTable(df) {
		fmt.Println(dimStyle.Render("  " + plural(df.Height(), "row") + " shown"))
	}
	return nil
}

// cmdHead handles `head [N]` and `show [N]`.
func cmdHead(s *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	n, err := optCount(c.args, 0, 10, "row count")
	if err != nil {
		return err
	}
	return s.collectAndPrint(s.currentLazy().Head(n))
}

func cmdTail(s *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	n, err := optCount(c.args, 0, 10, "row count")
	if err != nil {
		return err
	}
	return s.collectAndPrint(s.currentLazy().Tail(n))
}

func cmdSchema(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	sch, err := s.schema()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println(titleStyle.Render("schema"))
	fmt.Println()
	for _, f := range sch.Fields() {
		fmt.Printf("  %s  %s\n",
			headerStyle.Render(padRight(f.Name, 20)),
			infoStyle.Render(f.DType.String()))
	}
	fmt.Println()
	return nil
}

// withFrame materialises the focus, hands it to fn, and releases it.
func (s *state) withFrame(fn func(df *dataframe.DataFrame) error) error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	return fn(df)
}

// printDerived materialises the focus, derives a frame with fn, and
// prints it.
func (s *state) printDerived(fn func(df *dataframe.DataFrame) (*dataframe.DataFrame, error)) error {
	return s.withFrame(func(df *dataframe.DataFrame) error {
		out, err := fn(df)
		if err != nil {
			return err
		}
		defer out.Release()
		printTable(out)
		return nil
	})
}

// cmdDescribe handles `describe [COL...]`: summary statistics for
// every column, or only the listed ones.
func cmdDescribe(s *state, c *call) error {
	cols := columnList(c.args)
	return s.printDerived(func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		if len(cols) == 0 {
			return df.Describe(s.ctx)
		}
		sub, err := df.Select(cols...)
		if err != nil {
			return nil, err
		}
		defer sub.Release()
		return sub.Describe(s.ctx)
	})
}

func cmdNullCount(s *state, _ *call) error {
	return s.printDerived(func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.NullCount(), nil
	})
}

func cmdGlimpse(s *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	n, err := optCount(c.args, 0, 5, "row count")
	if err != nil {
		return err
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	return s.withFrame(func(df *dataframe.DataFrame) error {
		fmt.Println(df.Glimpse(n))
		return nil
	})
}

func cmdSize(s *state, _ *call) error {
	return s.withFrame(func(df *dataframe.DataFrame) error {
		fmt.Printf("%s %s, %s\n", successStyle.Render("estimated size:"),
			humanBytes(df.EstimatedSize()), shape(df))
		return nil
	})
}

// cmdPartitionBy prints how many rows fall into each key combination.
// Printing every partition would be noisy; use DataFrame.PartitionBy
// from Go to get the frames themselves.
func cmdPartitionBy(s *state, c *call) error {
	keys := columnList(c.args)
	if len(keys) == 0 {
		return c.usage()
	}
	return s.withFrame(func(df *dataframe.DataFrame) error {
		parts, err := df.PartitionBy(s.ctx, keys...)
		if err != nil {
			return err
		}
		defer func() {
			for _, p := range parts {
				p.Release()
			}
		}()
		ok("%s by %v", plural(len(parts), "partition"), keys)
		for i, p := range parts {
			fmt.Printf("  %d: %s\n", i, plural(p.Height(), "row"))
		}
		return nil
	})
}

// cmdInteractiveShow opens the focus in the browse TUI on the alt
// screen. Filter, sort and hide inside the TUI are view-only; the
// pipeline is untouched. Export the view with `:export PATH`.
func cmdInteractiveShow(s *state, _ *call) error {
	return s.withFrame(func(df *dataframe.DataFrame) error {
		label := s.focused
		if label == "" {
			label = "<pipeline>"
			if s.path != "" {
				label = filepath.Base(s.path)
			}
		}
		return browse.RunWithContext(s.ctx, df, label)
	})
}

func humanBytes(n int) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
}

// plural formats n with noun, adding "s" unless n is 1.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
