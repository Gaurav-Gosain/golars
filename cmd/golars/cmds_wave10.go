package main

import (
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

type (
	aliasForDataFrame = dataframe.DataFrame
	dataframePivotAgg = dataframe.PivotAgg
)

const (
	dataframePivotAggFirst = dataframe.PivotFirst
	dataframePivotAggSum   = dataframe.PivotSum
	dataframePivotAggMean  = dataframe.PivotMean
	dataframePivotAggMin   = dataframe.PivotMin
	dataframePivotAggMax   = dataframe.PivotMax
	dataframePivotAggCount = dataframe.PivotCount
)

// cmdTopK / cmdBottomK print the first K rows of the sorted-by-col view.
// Syntax: .top_k K COL  |  .bottom_k K COL
func (s *state) cmdTopK(op string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: .%s K COL", op)
	}
	k, ok := parseNat(args[0])
	if !ok {
		return fmt.Errorf(".%s: invalid K %q", op, args[0])
	}
	col := args[1]
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	var out *aliasForDataFrame
	if op == "top_k" {
		d, err := df.TopK(s.ctx, k, col)
		if err != nil {
			return err
		}
		out = d
	} else {
		d, err := df.BottomK(s.ctx, k, col)
		if err != nil {
			return err
		}
		out = d
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdTranspose prints the transposed frame.
// Syntax: .transpose [HEADER_COL] [PREFIX]
func (s *state) cmdTranspose(args []string) error {
	header, prefix := "column", "row"
	if len(args) >= 1 {
		header = args[0]
	}
	if len(args) >= 2 {
		prefix = args[1]
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Transpose(s.ctx, header, prefix)
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdUnpivot reshapes to long form.
// Syntax: .unpivot ID_COL[,ID_COL...] [VAL_COL[,VAL_COL...]]
func (s *state) cmdUnpivot(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .unpivot ID_COLS [VAL_COLS]")
	}
	idVars := strings.Split(args[0], ",")
	var valVars []string
	if len(args) >= 2 {
		valVars = strings.Split(args[1], ",")
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Unpivot(s.ctx, idVars, valVars)
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdPartitionBy prints a summary of how rows split across the key
// combinations. Printing every partition as a table would be noisy;
// callers who want the data should call df.PartitionBy from code.
func (s *state) cmdPartitionBy(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .partition_by KEYS")
	}
	keys := strings.Split(args[0], ",")
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	parts, err := df.PartitionBy(s.ctx, keys...)
	if err != nil {
		return err
	}
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	fmt.Printf("%s %d partitions by %v:\n", successStyle.Render("✓"), len(parts), keys)
	for i, p := range parts {
		fmt.Printf("  %d: %d rows\n", i, p.Height())
	}
	return nil
}

// cmdScalarStat prints one-liners for Skew/Kurtosis/ApproxNUnique.
func (s *state) cmdScalarStat(op string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .%s COL", op)
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	col, err := df.Column(args[0])
	if err != nil {
		return err
	}
	switch op {
	case "skew":
		v, err := col.Skew()
		if err != nil {
			return err
		}
		fmt.Printf("%s(%s) = %v\n", cmdStyle.Render("skew"), args[0], v)
	case "kurtosis":
		v, err := col.Kurtosis()
		if err != nil {
			return err
		}
		fmt.Printf("%s(%s) = %v\n", cmdStyle.Render("kurtosis"), args[0], v)
	case "approx_n_unique":
		v, err := col.ApproxNUnique()
		if err != nil {
			return err
		}
		fmt.Printf("%s(%s) = %d\n", cmdStyle.Render("approx_n_unique"), args[0], v)
	default:
		return fmt.Errorf("unknown scalar stat %q", op)
	}
	return nil
}

// cmdPivot runs df.Pivot from a simple one-line syntax:
//
//	.pivot INDEX_COLS ON VALUES [AGG]
//
// where INDEX_COLS is a comma-separated list.
func (s *state) cmdPivot(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: .pivot INDEX_COLS ON VALUES [AGG]")
	}
	idx := strings.Split(args[0], ",")
	on := args[1]
	vals := args[2]
	agg := "first"
	if len(args) >= 4 {
		agg = strings.ToLower(args[3])
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Pivot(s.ctx, idx, on, vals, pivotAggFromString(agg))
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

func pivotAggFromString(s string) dataframePivotAgg {
	switch s {
	case "sum":
		return dataframePivotAggSum
	case "mean", "avg":
		return dataframePivotAggMean
	case "min":
		return dataframePivotAggMin
	case "max":
		return dataframePivotAggMax
	case "count":
		return dataframePivotAggCount
	}
	return dataframePivotAggFirst
}

// cmdCorrCov: .corr COL1 COL2 or .cov COL1 COL2 prints one scalar.
func (s *state) cmdCorrCov(op string, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: .%s COL1 COL2", op)
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	a, err := df.Column(args[0])
	if err != nil {
		return err
	}
	b, err := df.Column(args[1])
	if err != nil {
		return err
	}
	var v float64
	switch op {
	case "corr":
		v, err = a.PearsonCorr(b)
	case "cov":
		v, err = a.Covariance(b, 1)
	default:
		return fmt.Errorf("unknown pair stat %q", op)
	}
	if err != nil {
		return err
	}
	fmt.Printf("%s(%s, %s) = %v\n", cmdStyle.Render(op), args[0], args[1], v)
	return nil
}

// cmdUnnest projects the fields of a struct-typed column as top-level
// columns. Syntax: .unnest COL
func (s *state) cmdUnnest(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .unnest COL")
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Unnest(s.ctx, args[0])
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdExplode fans out each element of a list-typed column into its
// own row. Syntax: .explode COL
func (s *state) cmdExplode(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .explode COL")
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Explode(s.ctx, args[0])
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdUpsample interpolates a timestamp column at a regular interval,
// left-joining source rows onto the dense grid. Syntax:
// .upsample COL EVERY (e.g. .upsample ts 1d)
func (s *state) cmdUpsample(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: .upsample COL EVERY (e.g. .upsample ts 1d)")
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	out, err := df.Upsample(s.ctx, args[0], args[1])
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}
