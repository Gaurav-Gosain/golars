package analysis

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
	"github.com/Gaurav-Gosain/golars/series"
)

// columnArgFuncs take column names as string arguments.
var columnArgFuncs = map[string]bool{"col": true, "over": true, "exclude": true}

// legacyAggs accept a quoted column name: `sum("amount")`.
var legacyAggs = []string{"sum", "mean", "min", "max", "count", "first", "last",
	"median", "std", "var", "n_unique"}

// exprColumns calls fn for every column an expression reads, with its
// byte span relative to the expression text.
func exprColumns(n *exprparse.Node, fn func(name string, pos, end int)) {
	n.Walk(func(m *exprparse.Node) bool {
		switch m.Kind {
		case exprparse.KindCol:
			fn(m.Name, m.NamePos, m.NameEnd())
		case exprparse.KindCall:
			strArgs := columnArgFuncs[m.Name] && m.NS == ""
			if !strArgs && m.Recv == nil && m.NS == "" && len(m.Args) == 1 && len(m.Kw) == 0 &&
				slices.Contains(legacyAggs, m.Name) {
				strArgs = true
			}
			if !strArgs {
				return true
			}
			for _, arg := range m.Args {
				elems := []*exprparse.Node{arg}
				if arg.Kind == exprparse.KindList {
					elems = arg.Args
				}
				for _, e := range elems {
					if s, isStr := e.Lit.(string); isStr && e.Kind == exprparse.KindLit {
						// Point inside the quotes.
						fn(s, e.Pos+1, max(e.End-1, e.Pos+1))
					}
				}
			}
		}
		return true
	})
}

// checkExpr resolves an expression part against frame f: unknown
// columns (with did-you-mean fixes), unknown functions and bad
// arguments. It returns the compiled expression when it is usable.
func (a *analyzer) checkExpr(p syntax.Part, f Frame) (expr.Expr, bool) {
	if p.Expr == nil {
		return expr.Expr{}, false
	}
	okCols := true
	exprColumns(p.Expr, func(name string, pos, end int) {
		part := syntax.Part{Kind: syntax.PartColumn, Off: p.Off + pos, End: p.Off + end, Text: name, Value: name}
		if !a.refColumn(part, f, true) {
			okCols = false
		}
	})
	e, err := exprparse.Compile(p.Expr, p.Text)
	if err != nil {
		a.diag(syntax.ExprDiag(a.cur, err, p.Off, "bad-call"))
		return expr.Expr{}, false
	}
	return e, okCols
}

// evalExprs evaluates exprs as a projection over an empty frame of
// schema sch. It returns the output schema or the engine's error.
func evalExprs(sch *schema.Schema, exprs ...expr.Expr) (out *schema.Schema, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	df := probe(Frame{Schema: sch}, true)
	defer df.Release()
	res, err := lazy.FromDataFrame(df).Select(exprs...).Collect(bgCtx)
	if err != nil {
		return nil, err
	}
	defer res.Release()
	return res.Schema(), nil
}

// probe returns a frame with f's schema to run operations on: one
// all-null row when nullRow is set (literals and fills then see a
// value, as they do on real data), otherwise no rows.
func probe(f Frame, nullRow bool) *dataframe.DataFrame {
	if !nullRow {
		return dataframe.Empty(f.Schema)
	}
	cols := make([]*series.Series, 0, f.Schema.Len())
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}
	for _, fl := range f.Schema.Fields() {
		s, err := series.FullNull(fl.Name, fl.DType.Arrow(), 1)
		if err != nil {
			release()
			return dataframe.Empty(f.Schema)
		}
		cols = append(cols, s)
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		release()
		return dataframe.Empty(f.Schema)
	}
	return df
}

// runLazy applies fn to a lazy scan of a one-row null frame of f's
// schema and collects the result.
func runLazy(f Frame, nullRow bool, fn func(lazy.LazyFrame) lazy.LazyFrame) (out *dataframe.DataFrame, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("%v", r)
		}
	}()
	df := probe(f, nullRow)
	defer df.Release()
	return fn(lazy.FromDataFrame(df)).Collect(bgCtx)
}

// runEager applies fn to a frame of f's schema: one null row, or no
// rows when nullRow is false.
func runEager(f Frame, nullRow bool, fn func(*dataframe.DataFrame) (*dataframe.DataFrame, error)) (out *dataframe.DataFrame, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("%v", r)
		}
	}()
	df := probe(f, nullRow)
	defer df.Release()
	return fn(df)
}

// engineError reports an error the engine raised for a statement at
// part p (or the whole statement), naming a column when the message
// is about one.
func (a *analyzer) engineError(p syntax.Part, err error, f Frame) {
	msg := cleanEngineError(err)
	if m := notFoundRE.FindStringSubmatch(msg); m != nil && f.Schema != nil {
		a.unknownColumn(p.Off, p.End, m[1], f)
		return
	}
	d := a.diagAt(p.Off, p.End, syntax.SevError, "type-error", "%s", msg)
	if strings.Contains(msg, "runArithLit") || strings.Contains(msg, "literal type string") ||
		strings.Contains(msg, "str vs ") || strings.Contains(msg, " vs str") {
		d.Hint = "join strings with concat_str(a, b), or cast a column first with cast(x, \"f64\")"
	}
}

var notFoundRE = regexp.MustCompile(`(?:column not found|not found): "([^"]+)"`)

// cleanEngineError strips package prefixes from an engine error.
func cleanEngineError(err error) string {
	var pe *exprparse.Error
	if errors.As(err, &pe) {
		return pe.Msg
	}
	// Engine errors nest as "projection EXPR: compute: dtype
	// mismatch: str vs i64"; keep the part a user can act on.
	var keep []string
	for _, seg := range strings.Split(err.Error(), ": ") {
		switch {
		case seg == "lazy", seg == "eval", seg == "compute", seg == "dataframe", seg == "series",
			seg == "projection", seg == "GroupBy",
			strings.HasPrefix(seg, "projection "), strings.HasPrefix(seg, "agg "),
			strings.HasPrefix(seg, "with_columns "), strings.HasPrefix(seg, "filter "):
			continue
		}
		keep = append(keep, seg)
	}
	if len(keep) == 0 {
		return err.Error()
	}
	return strings.Join(keep, ": ")
}
