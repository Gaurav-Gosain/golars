package main

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// Aggregate commands collect the focus and print a scalar or a
// one-row frame. They never change the focus.

// scalarAggCmd handles `OP COL` for sum, mean, min, max, median, std,
// skew, kurtosis and approx_n_unique.
func scalarAggCmd(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	name, op := c.args[0], c.spec.Name
	return s.withFrame(func(df *dataframe.DataFrame) error {
		col, err := df.Column(name)
		if err != nil {
			return err
		}
		var v any
		switch op {
		case "sum":
			v, err = col.Sum()
		case "mean":
			v, err = col.Mean()
		case "min":
			v, err = col.Min()
		case "max":
			v, err = col.Max()
		case "median":
			v, err = col.Median()
		case "std":
			v, err = col.Std()
		case "skew":
			v, err = col.Skew()
		case "kurtosis":
			v, err = col.Kurtosis()
		case "approx_n_unique":
			v, err = col.ApproxNUnique()
		default:
			return fmt.Errorf("unknown aggregation")
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s(%s) = %v\n", cmdStyle.Render(op), name, v)
		return nil
	})
}

// pairStatCmd handles `corr A B` (Pearson) and `cov A B` (ddof=1).
func pairStatCmd(s *state, c *call) error {
	if len(c.args) != 2 {
		return c.usage()
	}
	op := c.spec.Name
	return s.withFrame(func(df *dataframe.DataFrame) error {
		a, err := df.Column(c.args[0])
		if err != nil {
			return err
		}
		b, err := df.Column(c.args[1])
		if err != nil {
			return err
		}
		var v float64
		if op == "corr" {
			v, err = a.PearsonCorr(b)
		} else {
			v, err = a.Covariance(b, 1)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s(%s, %s) = %v\n", cmdStyle.Render(op), c.args[0], c.args[1], v)
		return nil
	})
}

// frameAggCmd handles the *_all family, printing one row of
// per-column aggregates.
func frameAggCmd(s *state, c *call) error {
	if len(c.args) != 0 {
		return c.usage()
	}
	op := c.spec.Name
	return s.printDerived(func(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		switch op {
		case "sum_all":
			return df.SumAll(s.ctx)
		case "mean_all":
			return df.MeanAll(s.ctx)
		case "min_all":
			return df.MinAll(s.ctx)
		case "max_all":
			return df.MaxAll(s.ctx)
		case "std_all":
			return df.StdAll(s.ctx)
		case "var_all":
			return df.VarAll(s.ctx)
		case "median_all":
			return df.MedianAll(s.ctx)
		case "count_all":
			return df.CountAll(s.ctx)
		case "null_count_all":
			return df.NullCountAll(s.ctx)
		}
		return nil, fmt.Errorf("unknown aggregation")
	})
}
