// Package transpile converts a .glr script into a self-contained Go
// program that reproduces the pipeline using the golars library API.
//
// The output targets readers who want to drop a `.glr` scratch into
// a real Go module once the exploration is done. Structural commands
// (load/select/drop/sort/limit/head/groupby/with/filter/...) map to
// direct lazy.LazyFrame calls. Commands that have no lazy equivalent
// (browse, explain, info, etc.) are skipped silently and listed in a
// header comment so nothing is invisibly dropped.
//
// Runs of pipeline ops between materialisation points are emitted as
// a single method chain, mirroring how a hand-written program would
// read. The runtime optimiser (lazy.DefaultOptimizer) still runs on
// each Collect, so predicate pushdown, slice pushdown, CSE, etc.
// apply transparently to the generated code.
package transpile

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/predparse"
)

// Transpile reads the script at path and writes equivalent Go source
// to w. The package name defaults to "main" when pkg is empty; the
// generated program is a complete binary ready for `go run`.
func Transpile(path string, w io.Writer, pkg string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if pkg == "" {
		pkg = "main"
	}
	t := &trans{
		pkg:     pkg,
		src:     string(raw),
		imports: map[string]struct{}{},
		frames:  map[string]string{},
		varSeqs: map[string]int{},
	}
	t.imports["github.com/Gaurav-Gosain/golars/lazy"] = struct{}{}
	t.imports["github.com/Gaurav-Gosain/golars/expr"] = struct{}{}
	t.imports["github.com/Gaurav-Gosain/golars"] = struct{}{}

	if err := t.walk(); err != nil {
		return err
	}
	return t.render(w)
}

// trans holds the running transpilation state. The "chain" model:
// each pipeline op (Filter, Sort, ...) appends to `pending`. The next
// materialisation (`show`/`save`/`use`/`stash`) calls `flush()`, which
// emits one chained Go statement that binds `focus` to
// `chainOrigin.Op1(...).Op2(...)...`.
type trans struct {
	pkg     string
	src     string
	imports map[string]struct{}
	stmts   []string

	focus       string   // Go var name for the focused lazy frame
	chainOrigin string   // Go expression the next chain extends from
	pending     []string // ".Method(args)" fragments to flush

	frames  map[string]string // glr frame name -> Go var
	varSeqs map[string]int    // per-prefix counter for fresh names

	materialised bool
	needsFmt     bool
	needsCtx     bool
	needsDisplay bool
	skipped      []string // REPL-only commands silently dropped
}

func (t *trans) walk() error {
	for line := range splitLines(t.src) {
		raw := line
		stmt := script.Normalize(raw)
		if stmt == "" {
			continue
		}
		if err := t.handle(stmt); err != nil {
			return fmt.Errorf("line %q: %w", raw, err)
		}
	}
	// Implicit display when the script never materialised: flush any
	// pending chain into focus, then print.
	if !t.materialised && (t.focus != "" || len(t.pending) > 0) {
		t.flush()
		if t.focus != "" {
			t.emitComment("Implicit display: no show/head/collect/save in script.")
			t.emitDisplay(t.focus)
		}
	} else {
		t.flush()
	}
	return nil
}

func (t *trans) handle(stmt string) error {
	parts := strings.Fields(stmt)
	cmd := strings.TrimPrefix(parts[0], ".")
	args := parts[1:]
	rest := strings.TrimSpace(strings.TrimPrefix(stmt, parts[0]))

	switch cmd {
	case "load":
		return t.load(args)
	case "select":
		return t.selectCols(args)
	case "drop":
		return t.dropCols(args)
	case "sort":
		return t.sort(args)
	case "limit":
		return t.limit(args)
	case "head":
		return t.head(args)
	case "tail":
		return t.tail(args)
	case "filter":
		return t.filter(rest)
	case "with":
		return t.with(rest)
	case "groupby":
		return t.groupby(args)
	case "rename":
		return t.rename(args)
	case "show":
		return t.show()
	case "use":
		return t.use(args)
	case "stash":
		return t.stash(args)
	case "join":
		return t.join(args)
	case "save", "write":
		return t.save(args)
	case "collect":
		return t.collect()
	case "reset":
		// Drop the lazy pipeline; keep nothing focused. Subsequent
		// `load` will start a fresh chain.
		t.abandonFocus()
		t.focus = ""
		t.chainOrigin = ""
		t.pending = nil
		t.materialised = false
		return nil
	// REPL-only commands - meaningless in a compiled program. Track
	// for the header note instead of polluting the body with TODOs.
	case "frames", "drop_frame", "schema", "describe", "ishow", "browse",
		"explain", "explain_tree", "tree", "graph", "show_graph", "mermaid",
		"timing", "info", "clear", "help", "exit", "quit", "source",
		"null_count", "null_count_all", "size", "glimpse",
		"sum_all", "mean_all", "min_all", "max_all", "std_all", "var_all", "median_all":
		t.noteSkipped(cmd)
		return nil
	default:
		// Genuine unknown - leave a TODO breadcrumb. The walker keeps
		// going so the rest of the script still translates.
		t.flush()
		t.emitComment("TODO(glr): command %q has no direct lazy API; port manually", cmd)
		return nil
	}
}

// --- handlers ---------------------------------------------------

func (t *trans) load(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("load requires a path")
	}
	path := args[0]
	stage := ""
	if len(args) >= 3 && strings.EqualFold(args[1], "as") {
		stage = args[2]
	}
	reader := readerForExt(path)
	if reader == "" {
		return fmt.Errorf("load: unknown file extension for %q", path)
	}
	opts := ""
	if reader == "ReadCSV" {
		t.imports["github.com/Gaurav-Gosain/golars/io/csv"] = struct{}{}
		opts = `, csv.WithNullValues("")`
	}
	df := t.freshVar("df")
	t.emit("%s, err := golars.%s(%q%s)", df, reader, path, opts)
	t.emit("if err != nil { log.Fatal(err) }")
	t.emit("defer %s.Release()", df)

	if stage == "" {
		// Default frame: focus, but defer the lf binding to the next
		// flush so the chain can fold through.
		t.abandonFocus()
		lf := t.freshVar("lf")
		t.focus = lf
		t.chainOrigin = fmt.Sprintf("lazy.FromDataFrame(%s)", df)
		t.pending = nil
	} else {
		// Staged frame: bind eagerly under the stage var so `use NAME`
		// can derive from it later.
		stageVar := goIdent(stage)
		t.emit("%s := lazy.FromDataFrame(%s)", stageVar, df)
		t.frames[stage] = stageVar
	}
	return nil
}

func (t *trans) selectCols(args []string) error {
	cols, err := parseCommaList(args)
	if err != nil {
		return err
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = fmt.Sprintf("expr.Col(%q)", c)
	}
	t.pipe("Select", strings.Join(exprs, ", "))
	return nil
}

func (t *trans) dropCols(args []string) error {
	cols, err := parseCommaList(args)
	if err != nil {
		return err
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = strconv.Quote(c)
	}
	t.pipe("Drop", strings.Join(quoted, ", "))
	return nil
}

func (t *trans) sort(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("sort requires a column")
	}
	col := args[0]
	desc := false
	if len(args) >= 2 && strings.EqualFold(args[1], "desc") {
		desc = true
	}
	t.pipe("Sort", fmt.Sprintf("%q, %t", col, desc))
	return nil
}

func (t *trans) limit(args []string) error {
	n := parseN(args, 10)
	t.pipe("Limit", strconv.Itoa(n))
	return nil
}

// head N is display-only - prints first N rows without mutating the
// pipeline, matching REPL behaviour. `limit N` is the mutating version.
func (t *trans) head(args []string) error {
	n := parseN(args, 10)
	t.flush()
	if t.focus == "" {
		return fmt.Errorf("head requires a loaded frame")
	}
	t.emitDisplay(fmt.Sprintf("%s.Limit(%d)", t.focus, n))
	return nil
}

func (t *trans) tail(args []string) error {
	n := parseN(args, 10)
	t.flush()
	if t.focus == "" {
		return fmt.Errorf("tail requires a loaded frame")
	}
	t.needsFmt = true
	t.needsCtx = true
	t.materialised = true
	t.emitComment("tail materialises; no direct lazy API. Falling back to Collect+Tail.")
	t.emit("{")
	t.emit("\tout, err := %s.Collect(ctx)", t.focus)
	t.emit("\tif err != nil { log.Fatal(err) }")
	t.emit("\tdefer out.Release()")
	t.emit("\ttailed := out.Tail(%d)", n)
	t.emit("\tdefer tailed.Release()")
	t.emit("\tfmt.Println(tailed)")
	t.emit("}")
	return nil
}

func (t *trans) filter(rest string) error {
	if rest == "" {
		return fmt.Errorf("filter requires a predicate")
	}
	e, err := predparse.Parse(rest)
	if err != nil {
		e, err = exprparse.Parse(rest)
	}
	if err != nil {
		return fmt.Errorf("filter: parse %q: %w", rest, err)
	}
	t.pipe("Filter", renderExpr(e.Node()))
	return nil
}

func (t *trans) with(rest string) error {
	name, exprText, err := splitAssign(rest)
	if err != nil {
		return err
	}
	e, err := exprparse.Parse(exprText)
	if err != nil {
		return fmt.Errorf("with %q: parse: %w", name, err)
	}
	goExpr := renderExpr(e.Node())
	t.pipe("WithColumns", fmt.Sprintf("%s.Alias(%q)", goExpr, name))
	return nil
}

func (t *trans) groupby(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("groupby requires at least one key")
	}
	keys := strings.Split(args[0], ",")
	quotedKeys := make([]string, len(keys))
	for i, k := range keys {
		quotedKeys[i] = strconv.Quote(strings.TrimSpace(k))
	}
	var aggs []string
	for _, spec := range args[1:] {
		parts := strings.Split(spec, ":")
		if len(parts) < 2 {
			t.emitComment("skipping invalid agg spec %q", spec)
			continue
		}
		col := parts[0]
		op := parts[1]
		alias := ""
		if len(parts) >= 3 {
			alias = parts[2]
		}
		a := fmt.Sprintf("expr.Col(%q).%s()", col, aggMethod(op))
		if alias != "" {
			a = fmt.Sprintf("%s.Alias(%q)", a, alias)
		}
		aggs = append(aggs, a)
	}
	// GroupBy.Agg is two methods chained together, but it's still a
	// single dot-chain link from the focus var's perspective.
	t.pendingChain(fmt.Sprintf("GroupBy(%s).Agg(%s)",
		strings.Join(quotedKeys, ", "), strings.Join(aggs, ", ")))
	return nil
}

func (t *trans) use(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("use requires a frame name")
	}
	name := args[0]
	target, ok := t.frames[name]
	if !ok {
		return fmt.Errorf("use: unknown frame %q (stage with `load ... as %s` or `stash %s`)", name, name, name)
	}
	t.abandonFocus()
	// Allocate a fresh focus var derived from the staged frame so
	// later mutations don't trash the snapshot.
	lf := t.freshVar("lf")
	t.focus = lf
	t.chainOrigin = target
	t.pending = nil
	return nil
}

func (t *trans) stash(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("stash requires a name")
	}
	name := args[0]
	if t.focus == "" && len(t.pending) == 0 {
		return fmt.Errorf("stash: no focused frame to snapshot")
	}
	t.flush()
	stageVar := goIdent(name)
	t.emit("%s := %s", stageVar, t.focus)
	t.frames[name] = stageVar
	return nil
}

func (t *trans) join(args []string) error {
	if len(args) < 3 || !strings.EqualFold(args[1], "on") {
		return fmt.Errorf("join: expected NAME on KEY [how]")
	}
	name := args[0]
	key := args[2]
	how := "InnerJoin"
	if len(args) >= 4 {
		switch strings.ToLower(args[3]) {
		case "inner":
			how = "InnerJoin"
		case "left":
			how = "LeftJoin"
		case "cross":
			how = "CrossJoin"
		default:
			return fmt.Errorf("join: unknown join type %q", args[3])
		}
	}
	rhs, ok := t.frames[name]
	if !ok {
		return fmt.Errorf("join: unknown frame %q", name)
	}
	t.imports["github.com/Gaurav-Gosain/golars/dataframe"] = struct{}{}
	t.pipe("Join", fmt.Sprintf("%s, []string{%q}, dataframe.%s", rhs, key, how))
	return nil
}

func (t *trans) rename(args []string) error {
	if len(args) < 3 || !strings.EqualFold(args[1], "as") {
		return fmt.Errorf("rename: expected OLD as NEW")
	}
	t.pipe("Rename", fmt.Sprintf("%q, %q", args[0], args[2]))
	return nil
}

func (t *trans) show() error {
	t.flush()
	if t.focus == "" {
		return fmt.Errorf("show requires a loaded frame")
	}
	t.emitDisplay(fmt.Sprintf("%s.Limit(10)", t.focus))
	return nil
}

func (t *trans) collect() error {
	t.flush()
	if t.focus == "" {
		return fmt.Errorf("collect requires a focused frame")
	}
	t.needsCtx = true
	t.materialised = true
	t.emit("{")
	t.emit("\tout, err := %s.Collect(ctx)", t.focus)
	t.emit("\tif err != nil { log.Fatal(err) }")
	t.emit("\tdefer out.Release()")
	t.emit("\t_ = out")
	t.emit("}")
	return nil
}

func (t *trans) save(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("save requires a path")
	}
	path := args[0]
	writer := writerForExt(path)
	if writer == "" {
		return fmt.Errorf("save: unknown file extension for %q", path)
	}
	t.flush()
	if t.focus == "" {
		return fmt.Errorf("save requires a focused frame")
	}
	t.needsCtx = true
	t.materialised = true
	t.emit("{")
	t.emit("\tout, err := %s.Collect(ctx)", t.focus)
	t.emit("\tif err != nil { log.Fatal(err) }")
	t.emit("\tdefer out.Release()")
	t.emit("\tif err := golars.%s(out, %q); err != nil { log.Fatal(err) }", writer, path)
	t.emit("}")
	return nil
}

// --- chain plumbing ---------------------------------------------

// pipe queues a method call onto the pending chain. The fragment is
// stored without a leading dot so renderChain can choose the right
// separator (".") and place a line break after it for multi-link
// chains.
func (t *trans) pipe(method, args string) {
	t.pendingChain(fmt.Sprintf("%s(%s)", method, args))
}

// pendingChain queues an arbitrary chain fragment (e.g.
// "GroupBy(...).Agg(...)") onto the pending chain.
func (t *trans) pendingChain(fragment string) {
	if t.focus == "" {
		// Pipeline op before any load - flag and skip rather than
		// emitting unbound code.
		t.emitComment("skipped %s: no loaded frame", strings.SplitN(fragment, "(", 2)[0])
		return
	}
	t.pending = append(t.pending, fragment)
}

// flush binds the focus var to chainOrigin + pending. Used by
// materialisation handlers (show/head/collect/save/stash) that need
// the focus var defined in the surrounding scope.
//
// Two or more chain links wrap across lines for readability; gofmt
// applies the standard continuation indent.
func (t *trans) flush() {
	if t.focus == "" {
		return
	}
	chain := renderChain(t.pending)
	t.pending = nil
	bound := t.chainOrigin == t.focus
	if bound && chain == "" {
		return
	}
	if bound {
		t.emit("%s = %s%s", t.focus, t.focus, chain)
	} else {
		t.emit("%s := %s%s", t.focus, t.chainOrigin, chain)
		t.chainOrigin = t.focus
	}
}

// renderChain joins pending "Method(args)" fragments into a dot-chain
// suffix. Single link stays on one line; two or more wrap with
// dot-at-end-of-line so gofmt applies the standard continuation
// indent and parsing succeeds (Go forbids leading-dot statements).
// The first link of a wrapped chain also moves onto its own line so
// every method aligns at the same column.
func renderChain(pending []string) string {
	switch len(pending) {
	case 0:
		return ""
	case 1:
		return "." + pending[0]
	}
	return ".\n\t\t" + strings.Join(pending, ".\n\t\t")
}

// groupby is the one place a single chain link wraps two methods
// (GroupBy().Agg()). Stored without leading dot; renderChain prefixes.

// abandonFocus discards the current focus + pending chain ahead of a
// focus switch (load / use / reset). Per glr semantics, `use NAME`
// returns to a clone of the named frame and any unstashed work on the
// previous focus is lost - mirror that by emitting nothing. To keep
// in-progress work, the user must `stash` first (which calls flush()
// to bind the chain under a name).
func (t *trans) abandonFocus() {
	t.pending = nil
}

// emitDisplay prints "display(ctx, <expr>)" and marks dependencies.
func (t *trans) emitDisplay(expr string) {
	t.needsFmt = true
	t.needsCtx = true
	t.needsDisplay = true
	t.materialised = true
	t.emit("display(ctx, %s)", expr)
}

func (t *trans) emit(format string, a ...any) {
	t.stmts = append(t.stmts, fmt.Sprintf(format, a...))
}

func (t *trans) emitComment(format string, a ...any) {
	t.stmts = append(t.stmts, "// "+fmt.Sprintf(format, a...))
}

func (t *trans) noteSkipped(cmd string) {
	if slices.Contains(t.skipped, cmd) {
		return
	}
	t.skipped = append(t.skipped, cmd)
}

func (t *trans) freshVar(prefix string) string {
	t.varSeqs[prefix]++
	return fmt.Sprintf("%s%d", prefix, t.varSeqs[prefix])
}

// --- emission ---------------------------------------------------

// render assembles the full Go source: header, imports (stdlib +
// external groups), main(), optional display() helper. The result is
// gofmt'd and unused imports pruned via go/ast.
func (t *trans) render(w io.Writer) error {
	var buf strings.Builder
	fmt.Fprintln(&buf, "// Code generated by `golars transpile`. DO NOT EDIT by hand.")
	if len(t.skipped) > 0 {
		sort.Strings(t.skipped)
		fmt.Fprintf(&buf, "// REPL-only commands skipped: %s.\n", strings.Join(t.skipped, ", "))
	}
	fmt.Fprintln(&buf, "// Runtime applies lazy.DefaultOptimizer (predicate/slice/projection")
	fmt.Fprintln(&buf, "// pushdown, CSE, simplify) on each Collect.")
	fmt.Fprintf(&buf, "package %s\n\n", t.pkg)

	// Always pull in the wrappers main needs. Pruning later removes
	// any that ended up unused.
	if t.needsCtx {
		t.imports["context"] = struct{}{}
		t.imports["log"] = struct{}{}
	}
	if t.needsFmt {
		t.imports["fmt"] = struct{}{}
		t.imports["log"] = struct{}{}
	}

	stdlib, external := splitImports(t.imports)
	sort.Strings(stdlib)
	sort.Strings(external)
	fmt.Fprintln(&buf, "import (")
	for _, imp := range stdlib {
		fmt.Fprintf(&buf, "\t%q\n", imp)
	}
	if len(stdlib) > 0 && len(external) > 0 {
		fmt.Fprintln(&buf)
	}
	for _, imp := range external {
		fmt.Fprintf(&buf, "\t%q\n", imp)
	}
	fmt.Fprintln(&buf, ")")
	fmt.Fprintln(&buf)

	fmt.Fprintln(&buf, "func main() {")
	if t.needsCtx {
		fmt.Fprintln(&buf, "\tctx := context.Background()")
	}
	for _, s := range t.stmts {
		fmt.Fprintf(&buf, "\t%s\n", s)
	}
	fmt.Fprintln(&buf, "}")

	if t.needsDisplay {
		fmt.Fprintln(&buf)
		fmt.Fprintln(&buf, "// display collects and prints lf, releasing the result on return.")
		fmt.Fprintln(&buf, "func display(ctx context.Context, lf lazy.LazyFrame) {")
		fmt.Fprintln(&buf, "\tout, err := lf.Collect(ctx)")
		fmt.Fprintln(&buf, "\tif err != nil { log.Fatal(err) }")
		fmt.Fprintln(&buf, "\tdefer out.Release()")
		fmt.Fprintln(&buf, "\tfmt.Println(out)")
		fmt.Fprintln(&buf, "}")
	}

	cleaned, err := pruneAndFormat(buf.String())
	if err != nil {
		_, werr := io.WriteString(w, buf.String())
		if werr != nil {
			return werr
		}
		return fmt.Errorf("gofmt: %w", err)
	}
	_, err = w.Write(cleaned)
	return err
}

// pruneAndFormat parses the generated source, drops imports whose
// package name never appears in the body, preserves the stdlib /
// external grouping, and returns gofmt'd bytes.
func pruneAndFormat(src string) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "transpile.go", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	used := make(map[string]struct{})
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		used[ident.Name] = struct{}{}
		return true
	})
	var kept []ast.Spec
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		name := path
		if i := strings.LastIndex(path, "/"); i >= 0 {
			name = path[i+1:]
		}
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if _, ok := used[name]; ok {
			kept = append(kept, spec)
		}
	}
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.IMPORT {
			continue
		}
		gd.Specs = kept
		break
	}
	var out strings.Builder
	if err := printer.Fprint(&out, fset, file); err != nil {
		return nil, err
	}
	return format.Source([]byte(out.String()))
}

// splitImports separates stdlib from external paths. Stdlib paths
// have no dot in their first segment.
func splitImports(in map[string]struct{}) (stdlib, external []string) {
	for path := range in {
		first, _, _ := strings.Cut(path, "/")
		if strings.Contains(first, ".") {
			external = append(external, path)
		} else {
			stdlib = append(stdlib, path)
		}
	}
	return
}

// --- helpers ----------------------------------------------------

func readerForExt(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv", ".tsv":
		return "ReadCSV"
	case ".parquet", ".pq":
		return "ReadParquet"
	case ".arrow", ".ipc":
		return "ReadIPC"
	case ".json":
		return "ReadJSON"
	case ".ndjson", ".jsonl":
		return "ReadNDJSON"
	}
	return ""
}

func writerForExt(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv", ".tsv":
		return "WriteCSV"
	case ".parquet", ".pq":
		return "WriteParquet"
	case ".arrow", ".ipc":
		return "WriteIPC"
	case ".json":
		return "WriteJSON"
	case ".ndjson", ".jsonl":
		return "WriteNDJSON"
	}
	return ""
}

func parseCommaList(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("expected a column list")
	}
	joined := strings.Join(args, " ")
	cols := strings.Split(joined, ",")
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func parseN(args []string, def int) int {
	if len(args) == 0 {
		return def
	}
	if v, err := strconv.Atoi(args[0]); err == nil {
		return v
	}
	return def
}

func splitAssign(rest string) (name, expr string, err error) {
	rest = strings.TrimSpace(rest)
	inQuote := byte(0)
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if inQuote != 0 {
			if c == '\\' && i+1 < len(rest) {
				i++
				continue
			}
			if c == inQuote {
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
			expr = strings.TrimSpace(rest[i+1:])
			if name == "" || expr == "" {
				return "", "", fmt.Errorf("with: expected NAME = EXPR")
			}
			return name, expr, nil
		}
	}
	return "", "", fmt.Errorf("with: missing `=`")
}

func aggMethod(s string) string {
	switch s {
	case "sum":
		return "Sum"
	case "mean", "avg":
		return "Mean"
	case "min":
		return "Min"
	case "max":
		return "Max"
	case "count":
		return "Count"
	case "null_count":
		return "NullCount"
	case "first":
		return "First"
	case "last":
		return "Last"
	case "median":
		return "Median"
	case "std":
		return "Std"
	case "var":
		return "Var"
	}
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// goIdent converts a glr frame name (snake_case) into a Go-friendly
// camelCase identifier. Falls back to the original string if it
// contains characters Go won't accept.
func goIdent(s string) string {
	if s == "" {
		return "_"
	}
	var b strings.Builder
	upper := false
	for i, r := range s {
		switch {
		case r == '_':
			upper = true
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9'):
			if upper {
				if r >= 'a' && r <= 'z' {
					r = r - 'a' + 'A'
				}
				upper = false
			}
			b.WriteRune(r)
		default:
			b.WriteRune('_')
			upper = false
		}
	}
	out := b.String()
	if out == "" {
		return "_"
	}
	// Ensure it starts with a letter.
	if c := out[0]; !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
		out = "f_" + out
	}
	return out
}

func splitLines(s string) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		start := 0
		for i := 0; i < len(s); i++ {
			if s[i] == '\n' {
				if !yield(s[start:i]) {
					return
				}
				start = i + 1
			}
		}
		if start < len(s) {
			yield(s[start:])
		}
	}
}
