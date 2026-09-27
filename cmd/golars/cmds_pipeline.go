package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/predparse"
)

// Pipeline commands add a lazy step to the focused pipeline. Nothing
// runs until a later statement collects (show, head, save, ...).

// cmdSelect handles `select a, b` (plain column names, commas or
// spaces) and `select a, total = x + y, dt.year(ts)` (comma-separated
// expressions, optionally named).
func cmdSelect(s *state, c *call) error {
	items, err := selectItems(c.rest)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	exprs := make([]expr.Expr, len(items))
	labels := make([]string, len(items))
	for i, it := range items {
		exprs[i], labels[i] = it.e, it.label
	}
	s.pushLazy(s.currentLazy().Select(exprs...), "added SELECT %s", strings.Join(labels, ", "))
	return nil
}

type selectItem struct {
	e     expr.Expr
	label string
}

// selectItems parses the select list. Text without parentheses, `=`
// or quotes is a plain column list, so names such as `my-col` keep
// working; anything else is a comma-separated expression list.
func selectItems(rest string) ([]selectItem, error) {
	if !strings.ContainsAny(rest, "(=\"'") {
		cols := columnList(strings.Fields(rest))
		out := make([]selectItem, len(cols))
		for i, name := range cols {
			out[i] = selectItem{expr.Col(name), name}
		}
		return out, nil
	}
	var out []selectItem
	for _, part := range script.SplitTopLevel(rest, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, text, named := splitAssignment(part)
		if !named {
			text = part
		}
		e, err := exprparse.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", part, err)
		}
		if named {
			e = e.Alias(name)
		}
		out = append(out, selectItem{e, part})
	}
	return out, nil
}

func cmdDrop(s *state, c *call) error {
	cols := columnList(c.args)
	if len(cols) == 0 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Drop(cols...), "added DROP %s", strings.Join(cols, ", "))
	return nil
}

func cmdFilter(s *state, c *call) error {
	if c.rest == "" {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	e, err := predparse.Parse(c.rest)
	if err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Filter(e), "added FILTER %s", dimStyle.Render(e.String()))
	return nil
}

// cmdSort handles `sort COL [asc|desc] [COL [asc|desc]]...`. Nulls
// sort first, as in polars.
func cmdSort(s *state, c *call) error {
	keys, desc, err := parseSortKeys(c.args)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	// Check the keys now so a typo such as `sort age sideways` fails at
	// this statement instead of at the next collect.
	if sch, err := s.schema(); err == nil {
		for _, k := range keys {
			if !sch.Contains(k) {
				return fmt.Errorf("unknown column %q (want a column, asc or desc)", k)
			}
		}
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		dir := "asc"
		if desc[i] {
			dir = "desc"
		}
		parts[i] = k + " " + dir
	}
	lf := s.currentLazy()
	if len(keys) == 1 {
		lf = lf.Sort(keys[0], desc[0])
	} else {
		opts := make([]compute.SortOptions, len(keys))
		for i := range keys {
			opts[i] = compute.SortOptions{Descending: desc[i]}
		}
		lf = lf.SortBy(keys, opts)
	}
	s.pushLazy(lf, "added SORT %s", strings.Join(parts, ", "))
	return nil
}

// parseSortKeys reads `col [asc|desc]` pairs. Commas between keys are
// accepted too.
func parseSortKeys(args []string) (keys []string, desc []bool, err error) {
	for _, a := range columnList(args) {
		switch strings.ToLower(a) {
		case "asc", "desc":
			if len(keys) == 0 {
				return nil, nil, fmt.Errorf("direction %q must follow a column", a)
			}
			desc[len(desc)-1] = strings.EqualFold(a, "desc")
		default:
			keys = append(keys, a)
			desc = append(desc, false)
		}
	}
	return keys, desc, nil
}

func cmdLimit(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	n, err := optCount(c.args, 0, 0, "row count")
	if err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Limit(n), "added LIMIT %d", n)
	return nil
}

// cmdGroupBy handles `groupby KEYS AGG...` where each AGG is
// `col:op[:alias]` or `name=EXPR`.
func cmdGroupBy(s *state, c *call) error {
	args := script.SplitTopLevel(c.rest, ' ')
	if len(args) < 2 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	keys := columnList(args[:1])
	aggs, err := parseAggSpecs(args[1:])
	if err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().GroupBy(keys...).Agg(aggs...),
		"added GROUP BY %s with %d aggregations", strings.Join(keys, ", "), len(aggs))
	return nil
}

func parseAggSpecs(specs []string) ([]expr.Expr, error) {
	aggs := make([]expr.Expr, 0, len(specs))
	for _, spec := range specs {
		e, err := parseAggSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("aggregation %q: %w", spec, err)
		}
		aggs = append(aggs, e)
	}
	return aggs, nil
}

// aggOps lists the `col:op` shorthands.
const aggOps = "sum, mean, min, max, count, null_count, first, last, median, std, var or n_unique"

// parseAggSpec turns `col:op[:alias]` or `name=EXPR` into an
// aggregation expression. A shorthand's output column is named alias,
// or col when no alias is given.
func parseAggSpec(spec string) (expr.Expr, error) {
	if name, text, named := splitAssignment(spec); named {
		e, err := exprparse.Parse(text)
		if err != nil {
			return expr.Expr{}, err
		}
		return e.Alias(name), nil
	}
	parts := strings.Split(spec, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return expr.Expr{}, fmt.Errorf("want col:op[:alias] or name=expr")
	}
	col := strings.TrimSpace(parts[0])
	op := strings.ToLower(strings.TrimSpace(parts[1]))
	alias := col
	if len(parts) == 3 {
		alias = strings.TrimSpace(parts[2])
	}
	if col == "" || alias == "" {
		return expr.Expr{}, fmt.Errorf("want col:op[:alias]")
	}
	e := expr.Col(col)
	switch op {
	case "sum":
		e = e.Sum()
	case "mean", "avg":
		e = e.Mean()
	case "min":
		e = e.Min()
	case "max":
		e = e.Max()
	case "count":
		e = e.Count()
	case "null_count":
		e = e.NullCount()
	case "first":
		e = e.First()
	case "last":
		e = e.Last()
	case "median":
		e = e.Median()
	case "std":
		e = e.Std()
	case "var":
		e = e.Var()
	case "n_unique":
		e = e.NUnique()
	default:
		return expr.Expr{}, fmt.Errorf("unknown op %q (want %s)", op, aggOps)
	}
	return e.Alias(alias), nil
}

// cmdGroupByDynamic handles `group_by_dynamic INDEX every DUR [period
// DUR] [offset DUR] [by KEYS] [closed C] [label L] AGG...`.
func cmdGroupByDynamic(s *state, c *call) error {
	args := script.SplitTopLevel(c.rest, ' ')
	if len(args) < 4 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	index := args[0]
	var opts dataframe.DynamicGroupOptions
	rest := args[1:]
options:
	for len(rest) >= 2 {
		v := rest[1]
		switch strings.ToLower(rest[0]) {
		case "every":
			opts.Every = v
		case "period":
			opts.Period = v
		case "offset":
			opts.Offset = v
		case "by":
			opts.GroupBy = columnList([]string{v})
		case "closed":
			opts.Closed = v
		case "label":
			opts.Label = v
		case "start_by":
			opts.StartBy = v
		default:
			break options
		}
		rest = rest[2:]
	}
	if opts.Every == "" {
		return fmt.Errorf("missing `every DURATION`, as in `group_by_dynamic ts every 1h amount:sum`")
	}
	if len(rest) == 0 {
		return c.usage()
	}
	aggList, err := parseAggSpecs(rest)
	if err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().GroupByDynamic(index, opts).Agg(aggList...),
		"added GROUP BY DYNAMIC %s every %s with %d aggregations", index, opts.Every, len(aggList))
	return nil
}

// otherFrame resolves a staged frame name, or reads target as a file.
// The caller owns the returned frame.
func (s *state) otherFrame(target string) (*dataframe.DataFrame, error) {
	if nf, found := s.frames[target]; found {
		return nf.df.Clone(), nil
	}
	df, err := fileio.Read(s.ctx, target)
	if err != nil {
		return nil, fmt.Errorf("%q is not a staged frame and could not be read as a file: %w", target, err)
	}
	return df, nil
}

// cmdJoin joins the focus with a staged frame NAME or a file PATH.
// A staged name wins over a path of the same spelling. The join runs
// eagerly: the result becomes the new focus.
func cmdJoin(s *state, c *call) error {
	if len(c.args) < 3 || len(c.args) > 4 || !strings.EqualFold(c.args[1], "on") {
		return c.usage()
	}
	target, key := c.args[0], c.args[2]
	how := dataframe.InnerJoin
	if len(c.args) == 4 {
		switch strings.ToLower(c.args[3]) {
		case "inner":
		case "left":
			how = dataframe.LeftJoin
		case "cross":
			how = dataframe.CrossJoin
		default:
			return fmt.Errorf("unknown join type %q: want inner, left or cross", c.args[3])
		}
	}
	other, err := s.otherFrame(target)
	if err != nil {
		return err
	}
	defer other.Release()
	cur, err := s.materialize()
	if err != nil {
		return err
	}
	joined, err := cur.Join(s.ctx, other, []string{key}, how)
	cur.Release()
	if err != nil {
		return err
	}
	s.replaceFocus(joined)
	ok("joined (%s) via %s on %s", shape(joined), how, key)
	return nil
}

// cmdJoinAsof handles `join_asof NAME on KEY [by COLS]
// [backward|forward|nearest] [tolerance T]`: each left row takes the
// nearest right row by KEY, within the same BY group. Both sides must
// be sorted by KEY (per group).
func cmdJoinAsof(s *state, c *call) error {
	if len(c.args) < 3 || !strings.EqualFold(c.args[1], "on") {
		return c.usage()
	}
	target := c.args[0]
	opts := dataframe.AsofOptions{On: c.args[2]}
	strategy := "backward"
	rest := c.args[3:]
	for i := 0; i < len(rest); i++ {
		word := strings.ToLower(rest[i])
		switch word {
		case "backward":
			opts.Strategy = dataframe.AsofBackward
		case "forward":
			opts.Strategy = dataframe.AsofForward
		case "nearest":
			opts.Strategy = dataframe.AsofNearest
		case "by", "tolerance":
			if i+1 >= len(rest) {
				return fmt.Errorf("%s needs a value", word)
			}
			i++
			if word == "by" {
				opts.By = columnList(rest[i : i+1])
			} else {
				opts.Tolerance = parseScalar(rest[i])
			}
			continue
		default:
			return fmt.Errorf("unexpected %q: want by, backward, forward, nearest or tolerance", rest[i])
		}
		strategy = word
	}
	other, err := s.otherFrame(target)
	if err != nil {
		return err
	}
	defer other.Release()
	cur, err := s.materialize()
	if err != nil {
		return err
	}
	joined, err := cur.JoinAsof(s.ctx, other, opts)
	cur.Release()
	if err != nil {
		return err
	}
	s.replaceFocus(joined)
	ok("asof joined (%s), %s on %s", shape(joined), strategy, opts.On)
	return nil
}

// cmdWith handles `with NAME = EXPR`, appending a derived column. The
// expression grammar lives in script/exprparse.
func cmdWith(s *state, c *call) error {
	name, exprText, isAssign := splitAssignment(c.rest)
	if !isAssign {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	e, err := exprparse.Parse(exprText)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	s.pushLazy(s.currentLazy().WithColumns(e.Alias(name)),
		"with %s = %s", cmdStyle.Render(name), dimStyle.Render(exprText))
	return nil
}

// splitAssignment splits `NAME = EXPR` at the first `=` that is not
// part of `==` and not inside a quoted string. Both sides are
// trimmed and must be non-empty.
func splitAssignment(rest string) (name, exprText string, isAssign bool) {
	inQuote := byte(0)
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if inQuote != 0 {
			if c == '\\' && i+1 < len(rest) {
				i++
			} else if c == inQuote {
				inQuote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			inQuote = c
		case '=':
			if i+1 < len(rest) && rest[i+1] == '=' {
				i++
				continue
			}
			name = strings.TrimSpace(rest[:i])
			exprText = strings.TrimSpace(rest[i+1:])
			if name == "" || exprText == "" {
				return "", "", false
			}
			return name, exprText, true
		}
	}
	return "", "", false
}

func cmdCollect(s *state, _ *call) error {
	if s.lf == nil {
		if s.df == nil {
			return errNoFrame
		}
		return fmt.Errorf("nothing to collect: the pipeline is empty")
	}
	df, err := s.lf.Collect(s.ctx)
	if err != nil {
		return err
	}
	s.replaceFocus(df)
	ok("collected (%s)", shape(df))
	return nil
}

// cmdReset drops the pending lazy steps. After a lazy scan there is no
// materialised source to fall back to, so reset clears the focus.
func cmdReset(s *state, _ *call) error {
	s.lf = nil
	if s.df == nil {
		s.path, s.focused = "", ""
	}
	ok("pipeline reset")
	return nil
}

func cmdReverse(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Reverse(), "added REVERSE")
	return nil
}

func cmdUnique(s *state, _ *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Unique(), "added UNIQUE")
	return nil
}

func cmdCast(s *state, c *call) error {
	if len(c.args) != 2 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	dt, err := exprparse.ParseDType(c.args[1])
	if err != nil {
		return fmt.Errorf("%w: want one of %s", err, strings.Join(exprparse.DTypeNames(), ", "))
	}
	s.pushLazy(s.currentLazy().Cast(c.args[0], dt), "added CAST %s to %s", c.args[0], dt)
	return nil
}

func cmdFillNull(s *state, c *call) error {
	if c.rest == "" {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	v := parseScalar(c.rest)
	s.pushLazy(s.currentLazy().FillNull(v), "added FILL_NULL %v", v)
	return nil
}

func cmdDropNull(s *state, c *call) error {
	if err := s.requireFocus(); err != nil {
		return err
	}
	cols := columnList(c.args)
	what := "any column"
	if len(cols) > 0 {
		what = strings.Join(cols, ", ")
	}
	s.pushLazy(s.currentLazy().DropNulls(cols...), "added DROP_NULLS on %s", what)
	return nil
}

func cmdFillNan(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	v, err := strconv.ParseFloat(c.args[0], 64)
	if err != nil {
		return fmt.Errorf("invalid number %q", c.args[0])
	}
	s.pushLazy(s.currentLazy().FillNan(v), "added FILL_NAN %v", v)
	return nil
}

// fillCmd builds forward_fill / backward_fill. LIMIT 0 means no limit.
func fillCmd(forward bool) handlerFunc {
	return func(s *state, c *call) error {
		if len(c.args) > 1 {
			return c.usage()
		}
		if err := s.requireFocus(); err != nil {
			return err
		}
		limit, err := optCount(c.args, 0, 0, "limit")
		if err != nil {
			return err
		}
		lf := s.currentLazy()
		if forward {
			lf = lf.ForwardFill(limit)
		} else {
			lf = lf.BackwardFill(limit)
		}
		s.pushLazy(lf, "added %s limit=%d", strings.ToUpper(c.spec.Name), limit)
		return nil
	}
}

func cmdRename(s *state, c *call) error {
	if len(c.args) != 3 || !strings.EqualFold(c.args[1], "as") {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	s.pushLazy(s.currentLazy().Rename(c.args[0], c.args[2]), "added RENAME %s to %s", c.args[0], c.args[2])
	return nil
}

func cmdWithRowIndex(s *state, c *call) error {
	if len(c.args) == 0 || len(c.args) > 2 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	var offset int64
	if len(c.args) == 2 {
		v, err := strconv.ParseInt(c.args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset %q", c.args[1])
		}
		offset = v
	}
	s.pushLazy(s.currentLazy().WithRowIndex(c.args[0], offset),
		"added row index %s starting at %d", c.args[0], offset)
	return nil
}

// horizontalCmd handles the *_horizontal family: `OP OUT [COL...]`
// appends a row-wise reduction named OUT.
func horizontalCmd(s *state, c *call) error {
	if len(c.args) == 0 {
		return c.usage()
	}
	if err := s.requireFocus(); err != nil {
		return err
	}
	out, cols := c.args[0], columnList(c.args[1:])
	lf := s.currentLazy()
	switch c.spec.Name {
	case "sum_horizontal":
		lf = lf.SumHorizontal(out, cols...)
	case "mean_horizontal":
		lf = lf.MeanHorizontal(out, cols...)
	case "min_horizontal":
		lf = lf.MinHorizontal(out, cols...)
	case "max_horizontal":
		lf = lf.MaxHorizontal(out, cols...)
	case "all_horizontal":
		lf = lf.AllHorizontal(out, cols...)
	case "any_horizontal":
		lf = lf.AnyHorizontal(out, cols...)
	default:
		return fmt.Errorf("unknown horizontal op")
	}
	s.pushLazy(lf, "added %s as %s", strings.ToUpper(c.spec.Name), out)
	return nil
}
