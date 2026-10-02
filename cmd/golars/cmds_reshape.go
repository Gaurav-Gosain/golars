package main

import (
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// Reshape commands have no lazy equivalent. Each one materialises the
// focus, transforms it, and makes the result the new focus.

func cmdSample(s *state, c *call) error {
	if len(c.args) == 0 || len(c.args) > 2 {
		return c.usage()
	}
	n, err := optCount(c.args, 0, 0, "row count")
	if err != nil {
		return err
	}
	seed, err := optSeed(c.args, 1)
	if err != nil {
		return err
	}
	return s.transformFocus("sampled", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Sample(s.ctx, n, false, seed)
	})
}

func cmdShuffle(s *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	seed, err := optSeed(c.args, 0)
	if err != nil {
		return err
	}
	return s.transformFocus("shuffled", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Shuffle(s.ctx, seed)
	})
}

// topKCmd builds top_k (largest first) and bottom_k (smallest first).
func topKCmd(largest bool) handlerFunc {
	return func(s *state, c *call) error {
		if len(c.args) != 2 {
			return c.usage()
		}
		k, err := optCount(c.args, 0, 0, "K")
		if err != nil {
			return err
		}
		col := c.args[1]
		return s.transformFocus("kept "+c.spec.Name, func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			if largest {
				return df.TopK(s.ctx, k, col)
			}
			return df.BottomK(s.ctx, k, col)
		})
	}
}

func cmdTranspose(s *state, c *call) error {
	if len(c.args) > 2 {
		return c.usage()
	}
	header, prefix := "column", "row"
	if len(c.args) >= 1 {
		header = c.args[0]
	}
	if len(c.args) == 2 {
		prefix = c.args[1]
	}
	return s.transformFocus("transposed", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Transpose(s.ctx, header, prefix)
	})
}

func cmdUnpivot(s *state, c *call) error {
	if len(c.args) == 0 || len(c.args) > 2 {
		return c.usage()
	}
	ids := columnList(c.args[:1])
	var vals []string
	if len(c.args) == 2 {
		vals = columnList(c.args[1:])
	}
	return s.transformFocus("unpivoted", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Unpivot(s.ctx, ids, vals)
	})
}

// cmdPivot handles `pivot INDEX_COLS ON VALUES [AGG]`.
func cmdPivot(s *state, c *call) error {
	if len(c.args) < 3 || len(c.args) > 4 {
		return c.usage()
	}
	idx := columnList(c.args[:1])
	on, vals := c.args[1], c.args[2]
	agg := dataframe.PivotFirst
	if len(c.args) == 4 {
		var known bool
		if agg, known = parsePivotAgg(c.args[3]); !known {
			return fmt.Errorf("unknown aggregation %q: want first, sum, mean, min, max or count", c.args[3])
		}
	}
	return s.transformFocus("pivoted", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Pivot(s.ctx, idx, on, vals, agg)
	})
}

func parsePivotAgg(name string) (dataframe.PivotAgg, bool) {
	switch strings.ToLower(name) {
	case "first":
		return dataframe.PivotFirst, true
	case "sum":
		return dataframe.PivotSum, true
	case "mean", "avg":
		return dataframe.PivotMean, true
	case "min":
		return dataframe.PivotMin, true
	case "max":
		return dataframe.PivotMax, true
	case "count":
		return dataframe.PivotCount, true
	}
	return dataframe.PivotFirst, false
}

func cmdUnnest(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	return s.transformFocus("unnested "+c.args[0], func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Unnest(s.ctx, c.args[0])
	})
}

// cmdExplode handles `explode COL[,COL...]`. Several list columns
// explode together and must have equal lengths per row.
func cmdExplode(s *state, c *call) error {
	cols := columnList(c.args)
	if len(cols) == 0 {
		return c.usage()
	}
	return s.transformFocus("exploded "+strings.Join(cols, ", "), func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.ExplodeColumns(s.ctx, cols...)
	})
}

// cmdToDummies handles `to_dummies [COL...] [drop_first]`: one-hot
// encode the listed columns (all when none are given) into u8
// indicator columns named COL_VALUE.
func cmdToDummies(s *state, c *call) error {
	var opts dataframe.ToDummiesOptions
	for _, a := range columnList(c.args) {
		if strings.EqualFold(a, "drop_first") {
			opts.DropFirst = true
			continue
		}
		opts.Columns = append(opts.Columns, a)
	}
	return s.transformFocus("one-hot encoded", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.ToDummies(s.ctx, opts)
	})
}

// cmdUpsample handles `upsample COL EVERY`, e.g. `upsample ts 1d`.
func cmdUpsample(s *state, c *call) error {
	if len(c.args) != 2 {
		return c.usage()
	}
	return s.transformFocus("upsampled", func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return df.Upsample(s.ctx, c.args[0], c.args[1])
	})
}
