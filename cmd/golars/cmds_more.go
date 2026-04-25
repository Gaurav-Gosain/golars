package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// cmdCast casts a single column to the named dtype.
// Syntax: .cast COL TYPE
func (s *state) cmdCast(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: .cast COL TYPE")
	}
	col, typeName := args[0], strings.ToLower(args[1])
	dt, ok := parseDType(typeName)
	if !ok {
		return fmt.Errorf(".cast: unknown dtype %q (expected: i64, i32, f64, f32, bool, str)", typeName)
	}
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	lf := s.currentLazy().Cast(col, dt)
	s.lf = &lf
	fmt.Printf("%s added CAST %q -> %s to pipeline\n", successStyle.Render("✓"), col, dt)
	return nil
}

// cmdFillNull fills nulls across all compatible columns.
// Syntax: .fill_null VALUE
func (s *state) cmdFillNull(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .fill_null VALUE")
	}
	raw := args[0]
	v := parseScalar(raw)
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	lf := s.currentLazy().FillNull(v)
	s.lf = &lf
	fmt.Printf("%s added FILL_NULL value=%v to pipeline\n", successStyle.Render("✓"), v)
	return nil
}

// cmdDropNull drops rows with any null.
// Syntax: .drop_null [COL...]
func (s *state) cmdDropNull(args []string) error {
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	lf := s.currentLazy().DropNulls(args...)
	s.lf = &lf
	if len(args) == 0 {
		fmt.Printf("%s added DROP_NULLS (all columns) to pipeline\n", successStyle.Render("✓"))
	} else {
		fmt.Printf("%s added DROP_NULLS cols=%v to pipeline\n", successStyle.Render("✓"), args)
	}
	return nil
}

// cmdRename renames a single column.
// Syntax: .rename OLD AS NEW
func (s *state) cmdRename(args []string) error {
	if len(args) != 3 || !strings.EqualFold(args[1], "as") {
		return fmt.Errorf("usage: .rename OLD AS NEW")
	}
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	lf := s.currentLazy().Rename(args[0], args[2])
	s.lf = &lf
	fmt.Printf("%s renamed %q to %q\n", successStyle.Render("✓"), args[0], args[2])
	return nil
}

// cmdScalarAgg handles .sum/.mean/.min/.max/.median/.std COL. Prints
// a single scalar line instead of a table.
func (s *state) cmdScalarAgg(op string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .%s COL", op)
	}
	colName := args[0]
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	col, err := df.Column(colName)
	if err != nil {
		return err
	}
	var (
		val  float64
		aerr error
	)
	switch op {
	case "sum":
		val, aerr = col.Sum()
	case "mean", "avg":
		val, aerr = col.Mean()
	case "min":
		val, aerr = col.Min()
	case "max":
		val, aerr = col.Max()
	case "median":
		val, aerr = col.Median()
	case "std":
		val, aerr = col.Std()
	default:
		return fmt.Errorf("unknown scalar agg %q", op)
	}
	if aerr != nil {
		return aerr
	}
	fmt.Printf("%s(%s) = %v\n", cmdStyle.Render(op), colName, val)
	return nil
}

// cmdWithRowIndex prepends an int64 row-number column.
// Syntax: .with_row_index NAME [OFFSET]
func (s *state) cmdWithRowIndex(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .with_row_index NAME [OFFSET]")
	}
	name := args[0]
	var offset int64
	if len(args) >= 2 {
		v, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf(".with_row_index: invalid offset %q", args[1])
		}
		offset = v
	}
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	lf := s.currentLazy().WithRowIndex(name, offset)
	s.lf = &lf
	fmt.Printf("%s added row index %q starting at %d\n",
		successStyle.Render("✓"), name, offset)
	return nil
}

// cmdPwd prints the current working directory.
func (s *state) cmdPwd() error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	fmt.Println(wd)
	return nil
}

// cmdLs lists the files in PATH (defaults to cwd).
func (s *state) cmdLs(args []string) error {
	dir := "."
	if len(args) >= 1 {
		dir = args[0]
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			name = cmdStyle.Render(name + "/")
		}
		fmt.Println(name)
	}
	return nil
}

// cmdCd changes the REPL's working directory.
func (s *state) cmdCd(args []string) error {
	if len(args) == 0 {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		return os.Chdir(home)
	}
	p := args[0]
	if !filepath.IsAbs(p) {
		cwd, _ := os.Getwd()
		p = filepath.Join(cwd, p)
	}
	return os.Chdir(p)
}

// parseDType maps a short type name to a dtype.DType. Kept narrow:
// only the dtypes the REPL can actually cast between.
func parseDType(name string) (dtype.DType, bool) {
	switch name {
	case "i64", "int64":
		return dtype.Int64(), true
	case "i32", "int32":
		return dtype.Int32(), true
	case "f64", "float64":
		return dtype.Float64(), true
	case "f32", "float32":
		return dtype.Float32(), true
	case "bool":
		return dtype.Bool(), true
	case "str", "string", "utf8":
		return dtype.String(), true
	}
	return dtype.DType{}, false
}

// parseScalar heuristically converts a CLI-style argument to a Go
// value. Numeric if pure digits; float if it has a decimal; string
// (with trimmed surrounding quotes) otherwise.
func parseScalar(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "true" {
		return true
	}
	if raw == "false" {
		return false
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(raw, 64); err == nil {
		return v
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return raw[1 : len(raw)-1]
	}
	return raw
}
