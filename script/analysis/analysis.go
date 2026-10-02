// Package analysis checks .glr scripts without running them. It
// replays the pipeline symbolically: data files contribute their
// schema (read from a sample of the file), and every statement is
// applied to an empty frame of the current schema with the real
// golars operations, so the column names and dtypes it reports are
// the ones a run would produce.
//
// The result drives `golars lint`, the language server (diagnostics,
// completion, hover, inlay hints, go to definition, rename) and the
// REPL and Jupyter kernel completion.
package analysis

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// Options configure an analysis.
type Options struct {
	// Dir resolves relative paths; usually the script's directory.
	Dir string
	// Frames are staged frames that exist before the script runs (a
	// REPL or kernel session).
	Frames map[string]Frame
	// Focus is the focused frame before the script runs, if any.
	Focus *Frame
	// Name labels the script in rendered diagnostics.
	Name string
}

// Frame is what the analyser knows about one frame.
type Frame struct {
	// Schema is nil when the columns cannot be known statically (after
	// pivot or to_dummies, or from an unreadable file).
	Schema *schema.Schema
	// Rows is the row count, or -1 when unknown. When Exact is false
	// it is an upper bound.
	Rows  int
	Exact bool
	// Origins maps a column to where it was defined.
	Origins map[string]Loc
	// Source is the path or frame name the frame came from.
	Source string
}

// Loc is a span on one line: 0-based line, byte columns.
type Loc struct {
	Line, Col, EndCol int
}

// unknownFrame has no schema and no row count.
func unknownFrame() Frame { return Frame{Rows: -1} }

// Known reports whether the frame's columns are known.
func (f Frame) Known() bool { return f.Schema != nil }

// Columns returns the column names, nil when unknown.
func (f Frame) Columns() []string {
	if f.Schema == nil {
		return nil
	}
	return f.Schema.Names()
}

// DType returns the dtype of column name as text, "" when unknown.
func (f Frame) DType(name string) string {
	if f.Schema == nil {
		return ""
	}
	if fl, found := f.Schema.FieldByName(name); found {
		return fl.DType.String()
	}
	return ""
}

// Shape renders the frame's shape as `rows × cols`, with `?` for
// unknown parts and `≤` for an upper bound.
func (f Frame) Shape() string {
	rows := "?"
	if f.Rows >= 0 {
		rows = strconv.Itoa(f.Rows)
		if !f.Exact {
			rows = "≤" + rows
		}
	}
	cols := "?"
	if f.Schema != nil {
		cols = strconv.Itoa(f.Schema.Len())
	}
	return rows + " × " + cols
}

// Step is the state around one statement.
type Step struct {
	Stmt *syntax.Stmt
	// Before and After are the focused frame around the statement;
	// HasFocus reports whether a frame was focused before it.
	Before, After Frame
	HasFocus      bool
	// Staged lists the staged frame names before the statement.
	Staged []string
	// Frames and FramesAfter are the staged frames around the
	// statement.
	Frames, FramesAfter map[string]Frame
	// Changes is true for statements that change the focus shape.
	Changes bool
	// Items maps a named expression's output column to its dtype, for
	// hover on `with NAME = ...` targets.
	Items map[string]string
}

// SymKind distinguishes frame and column symbols.
type SymKind uint8

// Symbol kinds.
const (
	SymFrame SymKind = iota + 1
	SymColumn
)

// Symbol is a definition or reference of a frame or column name.
type Symbol struct {
	Kind SymKind
	Name string
	Loc
	Def bool
	// Target is the definition a reference resolves to.
	Target *Loc
}

// Result is the outcome of an analysis.
type Result struct {
	File    *syntax.File
	Steps   []Step
	Diags   []syntax.Diag
	Symbols []Symbol
	opts    Options
}

// Analyze parses and checks src.
func Analyze(src string, opts Options) *Result {
	f := syntax.Parse(src)
	r := &Result{File: f, opts: opts}
	a := &analyzer{r: r, ctx: context.Background(), staged: map[string]*staged{}}
	for name, fr := range opts.Frames {
		a.staged[name] = &staged{frame: fr, used: true}
	}
	if opts.Focus != nil {
		a.focus, a.base, a.hasFocus = *opts.Focus, *opts.Focus, true
	}
	for _, st := range f.Stmts {
		r.Diags = append(r.Diags, st.Diags...)
		a.step(st)
	}
	a.finish()
	slices.SortStableFunc(r.Diags, func(x, y syntax.Diag) int {
		if x.Line != y.Line {
			return x.Line - y.Line
		}
		return x.Col - y.Col
	})
	return r
}

type staged struct {
	frame Frame
	def   *Loc
	used  bool
	stash bool
}

type analyzer struct {
	r        *Result
	ctx      context.Context
	focus    Frame
	base     Frame // the focus as of the last materialisation, for reset
	hasFocus bool
	staged   map[string]*staged
	depth    int // source nesting
	quiet    bool
	// emptyProbe runs the next lazy step on a zero-row frame instead
	// of a null row (window operations reject null keys).
	emptyProbe bool
	cur        *syntax.Stmt
	step0      *Step
}

func (a *analyzer) diag(d syntax.Diag) {
	if !a.quiet {
		a.r.Diags = append(a.r.Diags, d)
	}
}

func (a *analyzer) errAt(p syntax.Part, code, format string, args ...any) *syntax.Diag {
	return a.diagAt(p.Off, p.End, syntax.SevError, code, format, args...)
}

func (a *analyzer) diagAt(off, end int, sev syntax.Severity, code, format string, args ...any) *syntax.Diag {
	if a.quiet {
		return &syntax.Diag{}
	}
	a.r.Diags = append(a.r.Diags, a.cur.DiagAt(off, end, sev, code, format, args...))
	return &a.r.Diags[len(a.r.Diags)-1]
}

func (a *analyzer) loc(p syntax.Part) Loc {
	line, col := a.cur.Position(p.Off)
	_, end := a.cur.Position(p.End)
	return Loc{Line: line, Col: col, EndCol: end}
}

func (a *analyzer) symbol(s Symbol) {
	if !a.quiet {
		a.r.Symbols = append(a.r.Symbols, s)
	}
}

func (a *analyzer) snapshot() map[string]Frame {
	frames := make(map[string]Frame, len(a.staged))
	for n, s := range a.staged {
		frames[n] = s.frame
	}
	return frames
}

func (a *analyzer) stagedNames() []string {
	out := make([]string, 0, len(a.staged))
	for n := range a.staged {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

func (a *analyzer) step(st *syntax.Stmt) {
	a.cur = st
	step := Step{Stmt: st, Before: a.focus, HasFocus: a.hasFocus, Staged: a.stagedNames(), Frames: a.snapshot()}
	a.step0 = &step
	if st.Spec != nil && len(st.Diags) == 0 {
		a.apply(st)
	} else if st.Spec != nil {
		// Still record references so navigation works on a line with
		// a typo elsewhere.
		a.applyBroken(st)
	}
	step.After = a.focus
	step.FramesAfter = a.snapshot()
	step.Changes = shapeChanged(step.Before, step.After) || (st.Spec != nil && isSource(st.Spec.Name))
	if !a.quiet {
		a.r.Steps = append(a.r.Steps, step)
	}
}

func shapeChanged(b, a Frame) bool {
	if b.Rows != a.Rows || b.Exact != a.Exact || b.Known() != a.Known() {
		return true
	}
	return b.Known() && b.Schema.Len() != a.Schema.Len()
}

func isSource(name string) bool {
	return name == "load" || strings.HasPrefix(name, "scan_") || name == "use"
}

// needsFocus lists the categories whose commands work on the focus.
func needsFocus(spec *script.CommandSpec) bool {
	switch spec.Category {
	case "pipeline", "reshape", "inspect", "aggregate", "plan":
		return spec.Name != "reset"
	}
	return spec.Name == "save" || spec.Name == "stash"
}

// applyBroken handles a statement with syntax errors: frame and
// column parts still resolve, and the focus keeps what can still be
// known so one typo does not hide every later finding.
func (a *analyzer) applyBroken(st *syntax.Stmt) {
	var added []string
	for _, p := range st.Parts {
		switch p.Kind {
		case syntax.PartFrame:
			a.refFrame(p, false)
		case syntax.PartColumn:
			a.refColumn(p, a.focus, false)
		case syntax.PartNewColumn:
			added = append(added, p.Value)
		}
	}
	switch st.Spec.Name {
	case "filter", "sort", "limit", "unique", "reverse", "fill_null", "fill_nan", "forward_fill",
		"backward_fill", "drop_null", "sample", "shuffle", "top_k", "bottom_k":
		// The schema is unchanged.
	case "with":
		if a.focus.Known() {
			for _, name := range added {
				if !a.focus.Schema.Contains(name) {
					a.focus.Schema = a.focus.Schema.WithField(nullField(name))
				}
			}
		}
	default:
		if st.Spec.Category == "pipeline" || st.Spec.Category == "reshape" || isSource(st.Spec.Name) {
			a.focus = unknownFrame()
			a.hasFocus = true
		}
	}
}

// requireFocus reports a statement that needs a frame when none is
// loaded, with a fix that inserts a load.
func (a *analyzer) requireFocus(st *syntax.Stmt) bool {
	if a.hasFocus {
		return true
	}
	d := a.diagAt(st.Cmd.Off, st.Cmd.End, syntax.SevError, "no-frame", "%s needs a loaded frame; nothing is loaded yet", st.Spec.Name)
	d.Hint = "add a load statement first"
	if path := a.guessDataFile(); path != "" {
		d.Fix = &syntax.Fix{Title: "Add `load " + path + "`", Line: st.Line, Insert: true, Text: "load " + path}
	} else {
		d.Fix = &syntax.Fix{Title: "Add a load statement", Line: st.Line, Insert: true, Text: "load data.csv"}
	}
	a.focus = unknownFrame()
	a.hasFocus = true
	return false
}

// guessDataFile returns a data file near the script for the
// missing-load fix, relative to the script directory.
func (a *analyzer) guessDataFile() string {
	dir := a.r.opts.Dir
	if dir == "" {
		return ""
	}
	for _, sub := range []string{".", "data"} {
		entries, err := os.ReadDir(filepath.Join(dir, sub))
		if err != nil {
			continue
		}
		for _, e := range entries {
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".csv", ".tsv", ".parquet", ".pq", ".arrow", ".ipc", ".json", ".ndjson", ".jsonl":
				return filepath.ToSlash(filepath.Join(sub, e.Name()))
			}
		}
	}
	return ""
}

// refFrame records a reference to a staged frame and reports it when
// unknown (unless it may be a path).
func (a *analyzer) refFrame(p syntax.Part, orPath bool) (*staged, bool) {
	s, found := a.staged[p.Value]
	if found {
		s.used = true
		a.symbol(Symbol{Kind: SymFrame, Name: p.Value, Loc: a.loc(p), Target: s.def})
		return s, true
	}
	if orPath {
		return nil, false
	}
	d := a.errAt(p, "unknown-frame", "no frame named %q", p.Value)
	if best := script.Closest(p.Value, a.stagedNames()); best != "" {
		d.Hint = "did you mean \"" + best + "\"?"
		d.Fix = &syntax.Fix{Title: "Change to " + best, Text: best}
	} else {
		d.Hint = "stage one first with load PATH as " + p.Value + " or stash " + p.Value
	}
	a.symbol(Symbol{Kind: SymFrame, Name: p.Value, Loc: a.loc(p)})
	return nil, false
}

// refColumn checks that column p exists in f and records the
// reference. It returns false when the column is unknown.
func (a *analyzer) refColumn(p syntax.Part, f Frame, report bool) bool {
	name := p.Value
	var target *Loc
	if o, found := f.Origins[name]; found {
		target = &o
	}
	a.symbol(Symbol{Kind: SymColumn, Name: name, Loc: a.loc(p), Target: target})
	if !report || f.Schema == nil || f.Schema.Contains(name) {
		return true
	}
	a.unknownColumn(p.Off, p.End, name, f)
	return false
}

func (a *analyzer) unknownColumn(off, end int, name string, f Frame) {
	d := a.diagAt(off, end, syntax.SevError, "unknown-column", "unknown column %q", name)
	if best := script.Closest(name, f.Schema.Names()); best != "" {
		d.Hint = "did you mean \"" + best + "\"?"
		d.Fix = &syntax.Fix{Title: "Change to " + best, Text: best}
		return
	}
	names := f.Schema.Names()
	if len(names) > 8 {
		names = append(names[:8:8], "...")
	}
	d.Hint = "columns: " + strings.Join(names, ", ")
}

// defColumn records the definition of a new column.
func (a *analyzer) defColumn(p syntax.Part) Loc {
	l := a.loc(p)
	a.symbol(Symbol{Kind: SymColumn, Name: p.Value, Loc: l, Def: true})
	return l
}

// withOrigins returns a copy of f.Origins with the given additions.
func withOrigins(f Frame, add map[string]Loc) map[string]Loc {
	out := make(map[string]Loc, len(f.Origins)+len(add))
	for k, v := range f.Origins {
		out[k] = v
	}
	for k, v := range add {
		out[k] = v
	}
	return out
}

// sourceFrame builds the frame of a data file read at p.
func (a *analyzer) sourceFrame(p syntax.Part) Frame {
	abs := Resolve(p.Value, a.r.opts.Dir)
	fi := statFile(abs)
	if fi.err != nil {
		if os.IsNotExist(fi.err) {
			d := a.errAt(p, "missing-file", "file %q does not exist", p.Value)
			if best := a.closestFile(p.Value); best != "" {
				d.Hint = "did you mean \"" + best + "\"?"
				d.Fix = &syntax.Fix{Title: "Change to " + best, Text: best}
			}
		} else {
			a.diagAt(p.Off, p.End, syntax.SevWarning, "unreadable-file", "cannot read %q: %v", p.Value, fi.err)
		}
		return unknownFrame()
	}
	f := Frame{Schema: fi.sch, Rows: fi.rows, Exact: fi.rows >= 0, Source: p.Value}
	if f.Schema != nil {
		l := a.loc(p)
		f.Origins = make(map[string]Loc, f.Schema.Len())
		for _, n := range f.Schema.Names() {
			f.Origins[n] = l
		}
	}
	return f
}

// closestFile suggests a sibling file for a path that does not
// exist, looking in the first candidate directory that does.
func (a *analyzer) closestFile(path string) string {
	dir := a.r.opts.Dir
	if parent := filepath.Dir(path); parent != "." {
		dir = Resolve(parent, a.r.opts.Dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	best := script.Closest(filepath.Base(path), names)
	if best == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Join(filepath.Dir(path), best))
}

// setFocus replaces the focus with out (a result on an empty frame),
// keeping rows and adding origins.
func (a *analyzer) setFocus(out *dataframe.DataFrame, rows int, exact bool, add map[string]Loc) {
	f := Frame{Rows: rows, Exact: exact, Source: a.focus.Source}
	if out != nil {
		f.Schema = out.Schema()
		out.Release()
	}
	f.Origins = withOrigins(a.focus, add)
	a.focus = f
}

func (a *analyzer) finish() {
	for name, s := range a.staged {
		if s.stash && !s.used && s.def != nil {
			d := syntax.Diag{Line: s.def.Line, Col: s.def.Col, EndLine: s.def.Line, EndCol: s.def.EndCol,
				Severity: syntax.SevWarning, Code: "unused-stash",
				Msg: fmt.Sprintf("stash %q is never used", name), Hint: "use it with use, join or join_asof, or remove the stash"}
			a.r.Diags = append(a.r.Diags, d)
		}
	}
}

// StepAt returns the step of the statement covering line, or nil.
func (r *Result) StepAt(line int) *Step {
	for i := range r.Steps {
		st := r.Steps[i].Stmt
		if line >= st.Line && line <= st.EndLine {
			return &r.Steps[i]
		}
	}
	return nil
}

// StateAt returns the focused frame and staged frames in effect at
// the start of line: after every statement that ends before it.
func (r *Result) StateAt(line int) (focus Frame, hasFocus bool, frames map[string]Frame) {
	if s := r.StepAt(line); s != nil {
		return s.Before, s.HasFocus, s.Frames
	}
	focus = unknownFrame()
	if r.opts.Focus != nil {
		focus, hasFocus = *r.opts.Focus, true
	}
	frames = r.opts.Frames
	for i := range r.Steps {
		s := &r.Steps[i]
		if s.Stmt.EndLine >= line {
			break
		}
		focus, hasFocus = s.After, true
		frames = s.FramesAfter
	}
	return focus, hasFocus, frames
}

// Render formats every diagnostic with a caret line.
func (r *Result) Render(name string) string {
	var b strings.Builder
	for _, d := range r.Diags {
		b.WriteString(syntax.Render(name, r.File.Lines, d))
	}
	return b.String()
}
