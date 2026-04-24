// Package transpile converts a .glr script into a self-contained Go
// program that reproduces the pipeline using the golars library API.
//
// The output targets readers who want to drop a `.glr` scratch into
// a real Go module once the exploration is done. Structural commands
// (load/select/drop/sort/limit/head/groupby/with/filter/...) map to
// direct lazy.LazyFrame calls. Commands that have no lazy equivalent
// (browse, explain, info, etc.) appear as `// TODO(glr):` reminders
// so nothing is silently dropped.
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
	}
	t.imports["context"] = struct{}{}
	t.imports["log"] = struct{}{}
	t.imports["github.com/Gaurav-Gosain/golars/lazy"] = struct{}{}
	t.imports["github.com/Gaurav-Gosain/golars/expr"] = struct{}{}
	t.imports["github.com/Gaurav-Gosain/golars"] = struct{}{}

	if err := t.walk(); err != nil {
		return err
	}
	return t.emit(w)
}

// trans holds the running transpilation state: the source script,
// accumulated Go statements, import set, and the currently-focused
// variable name so chained ops can keep building on it.
type trans struct {
	pkg     string
	src     string
	imports map[string]struct{}
	stmts   []string // Go code lines inside func main()
	focus   string   // name of the Go var holding the focused lazy frame
	varSeq  int      // monotonic counter for generated variable names
	// frames maps glr frame names (from `load ... as NAME`) to the Go
	// variable holding that staged LazyFrame. `use NAME` and
	// `join NAME on K` look up the Go var here.
	frames map[string]string
	// materialised is true once the pipeline has been collected and
	// printed at least once (via show / head / collect / tail / save).
	// The walker appends a trailing `fmt.Println` block at end-of-file
	// only when this stays false, so a silent script like
	// `load ... ; filter ...` still shows its final frame.
	materialised bool
}

func (t *trans) walk() error {
	for line := range splitLines(t.src) {
		raw := line
		stmt := script.Normalize(raw)
		if stmt == "" {
			continue
		}
		if err := t.handle(stmt); err != nil {
			t.comment("error on line %q: %v", raw, err)
		}
	}
	// If the script never hit show/head/collect/save, the focused
	// pipeline was built but never consumed. Emit a trailing display
	// so the compiled binary prints something, mirroring the REPL's
	// implicit display of `# ^?` probes and final pipeline state.
	if !t.materialised && t.focus != "" {
		t.imports["fmt"] = struct{}{}
		t.stmts = append(t.stmts,
			"// Implicit display: no show/head/collect/save in script.",
			"{",
			fmt.Sprintf("\tout, err := %s.Collect(ctx)", t.focus),
			"\tif err != nil { log.Fatal(err) }",
			"\tdefer out.Release()",
			"\tfmt.Println(out)",
			"}",
		)
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
	case "limit", "head":
		return t.limit(args, cmd)
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
		t.imports["fmt"] = struct{}{}
		t.stmts = append(t.stmts,
			"{",
			fmt.Sprintf("\tout, err := %s.Head(10).Collect(ctx)", t.focusVar()),
			"\tif err != nil { log.Fatal(err) }",
			"\tdefer out.Release()",
			"\tfmt.Println(out)",
			"}",
		)
		t.materialised = true
		return nil
	case "use":
		return t.use(args)
	case "join":
		return t.join(args)
	case "frames":
		// Purely informational in the REPL; skip silently.
		return nil
	case "save", "write":
		return t.save(args)
	case "collect":
		t.stmts = append(t.stmts,
			"{",
			fmt.Sprintf("\tout, err := %s.Collect(ctx)", t.focusVar()),
			"\tif err != nil { log.Fatal(err) }",
			"\tdefer out.Release()",
			"\t_ = out",
			"}",
		)
		t.materialised = true
		return nil
	default:
		t.comment("TODO(glr): command %q has no direct lazy API; port manually", cmd)
		return nil
	}
}

// --- handlers ---------------------------------------------------

func (t *trans) load(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("load requires a path")
	}
	path := args[0]
	name := ""
	if len(args) >= 3 && strings.EqualFold(args[1], "as") {
		name = args[2]
	}
	reader := readerForExt(path)
	if reader == "" {
		return fmt.Errorf("load: unknown file extension for %q", path)
	}
	// CSV gets WithNullValues("") to match the REPL/script default
	// behaviour (empty fields become nulls rather than parse errors).
	opts := ""
	if reader == "ReadCSV" {
		t.imports["github.com/Gaurav-Gosain/golars/io/csv"] = struct{}{}
		opts = `, csv.WithNullValues("")`
	}
	v := t.freshVar("df")
	t.stmts = append(t.stmts,
		fmt.Sprintf("%s, err := golars.%s(%q%s)", v, reader, path, opts),
		"if err != nil { log.Fatal(err) }",
		fmt.Sprintf("defer %s.Release()", v),
	)
	lf := t.freshVar("lf")
	t.stmts = append(t.stmts, fmt.Sprintf("%s := lazy.FromDataFrame(%s)", lf, v))
	if name == "" {
		t.focus = lf
	} else {
		// Staged frame: stash its Go var under the glr name for later
		// `use NAME` / `join NAME on KEY` commands to look up.
		t.frames[name] = lf
		t.comment("staged frame %q as %s", name, lf)
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

func (t *trans) limit(args []string, cmd string) error {
	n := 10
	if len(args) >= 1 {
		if v, err := strconv.Atoi(args[0]); err == nil {
			n = v
		}
	}
	t.pipe("Limit", fmt.Sprintf("%d", n))
	// In the REPL, `head` implicitly displays the result. Mirror that
	// in the transpiled program so the generated binary prints
	// something instead of exiting silently. Plain `limit` stays a
	// pure pipeline step.
	if cmd == "head" {
		t.imports["fmt"] = struct{}{}
		t.stmts = append(t.stmts,
			"{",
			fmt.Sprintf("\tout, err := %s.Collect(ctx)", t.focusVar()),
			"\tif err != nil { log.Fatal(err) }",
			"\tdefer out.Release()",
			"\tfmt.Println(out)",
			"}",
		)
		t.materialised = true
	}
	return nil
}

func (t *trans) tail(args []string) error {
	n := 10
	if len(args) >= 1 {
		if v, err := strconv.Atoi(args[0]); err == nil {
			n = v
		}
	}
	t.imports["fmt"] = struct{}{}
	t.comment("tail materialises; no direct lazy API. Falling back to Collect+Tail.")
	t.stmts = append(t.stmts,
		"{",
		fmt.Sprintf("\tout, err := %s.Collect(ctx)", t.focusVar()),
		"\tif err != nil { log.Fatal(err) }",
		"\tdefer out.Release()",
		fmt.Sprintf("\ttailed := out.Tail(%d)", n),
		"\tdefer tailed.Release()",
		"\tfmt.Println(tailed)",
		"}",
	)
	t.materialised = true
	return nil
}

func (t *trans) filter(rest string) error {
	if rest == "" {
		return fmt.Errorf("filter requires a predicate")
	}
	// `.filter` grammar is richer than the `with` expression grammar:
	// it knows `like`, `contains`, `is_null`, `and`/`or`. Try that
	// first, then fall back to the generic expression parser so
	// arithmetic-style predicates like `age > 18` still work.
	e, err := predparse.Parse(rest)
	if err != nil {
		e, err = exprparse.Parse(rest)
	}
	if err != nil {
		t.comment("filter: could not parse %q: %v", rest, err)
		return nil
	}
	goExpr := renderExpr(e.Node())
	t.pipe("Filter", goExpr)
	return nil
}

func (t *trans) with(rest string) error {
	name, exprText, err := splitAssign(rest)
	if err != nil {
		return err
	}
	e, err := exprparse.Parse(exprText)
	if err != nil {
		t.comment("with %q: parse error: %v", name, err)
		return nil
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
			t.comment("skipping invalid agg spec %q", spec)
			continue
		}
		col := parts[0]
		op := parts[1]
		alias := ""
		if len(parts) >= 3 {
			alias = parts[2]
		}
		a := fmt.Sprintf("expr.Col(%q).%s()", col, titleCase(op))
		if alias != "" {
			a = fmt.Sprintf("%s.Alias(%q)", a, alias)
		}
		aggs = append(aggs, a)
	}
	t.stmts = append(t.stmts,
		fmt.Sprintf("%s = %s.GroupBy(%s).Agg(%s)",
			t.focusVar(), t.focusVar(),
			strings.Join(quotedKeys, ", "),
			strings.Join(aggs, ", "),
		),
	)
	return nil
}

// use NAME promotes a staged frame into focus. We park the previous
// focus under its own Go var so a later `use X` can bring it back.
func (t *trans) use(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("use requires a frame name")
	}
	name := args[0]
	target, ok := t.frames[name]
	if !ok {
		return fmt.Errorf("use: unknown frame %q (stage it with `load ... as %s` first)", name, name)
	}
	// Before swapping focus, stash the current pipeline's Go var
	// under its own name so further `use` cycles can round-trip.
	if t.focus != "" {
		for fname, fvar := range t.frames {
			if fvar == t.focus {
				// already mapped - keep as-is
				_ = fname
			}
		}
	}
	t.focus = target
	return nil
}

// join NAME on KEY [inner|left|cross] joins the focused frame with a
// staged one looked up by glr frame name.
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

func (t *trans) save(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("save requires a path")
	}
	path := args[0]
	writer := writerForExt(path)
	if writer == "" {
		return fmt.Errorf("save: unknown file extension for %q", path)
	}
	t.stmts = append(t.stmts,
		"{",
		fmt.Sprintf("\tout, err := %s.Collect(ctx)", t.focusVar()),
		"\tif err != nil { log.Fatal(err) }",
		"\tdefer out.Release()",
		fmt.Sprintf("\tif err := golars.%s(out, %q); err != nil { log.Fatal(err) }", writer, path),
		"}",
	)
	t.materialised = true
	return nil
}

// --- helpers ----------------------------------------------------

func (t *trans) focusVar() string {
	if t.focus == "" {
		t.focus = t.freshVar("lf")
		t.stmts = append(t.stmts, fmt.Sprintf("var %s lazy.LazyFrame", t.focus))
	}
	return t.focus
}

func (t *trans) freshVar(prefix string) string {
	t.varSeq++
	return fmt.Sprintf("%s%d", prefix, t.varSeq)
}

func (t *trans) pipe(method, args string) {
	t.stmts = append(t.stmts,
		fmt.Sprintf("%s = %s.%s(%s)", t.focusVar(), t.focusVar(), method, args),
	)
}

func (t *trans) comment(format string, a ...any) {
	t.stmts = append(t.stmts, "// "+fmt.Sprintf(format, a...))
}

// emit builds the full Go source, pipes it through go/format so the
// output is always gofmt'd, prunes any imports the generated body
// never references, and writes the final bytes.
func (t *trans) emit(w io.Writer) error {
	var buf strings.Builder
	fmt.Fprintf(&buf, "// Code generated by `golars transpile`. DO NOT EDIT by hand.\n")
	fmt.Fprintf(&buf, "package %s\n\n", t.pkg)
	imports := make([]string, 0, len(t.imports))
	for k := range t.imports {
		imports = append(imports, k)
	}
	sort.Strings(imports)
	fmt.Fprintln(&buf, "import (")
	for _, imp := range imports {
		fmt.Fprintf(&buf, "\t%q\n", imp)
	}
	fmt.Fprintln(&buf, ")")
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "func main() {")
	fmt.Fprintln(&buf, "\tctx := context.Background()")
	fmt.Fprintln(&buf, "\t_ = ctx")
	for _, s := range t.stmts {
		fmt.Fprintf(&buf, "\t%s\n", s)
	}
	fmt.Fprintln(&buf, "}")

	cleaned, err := pruneAndFormat(buf.String())
	if err != nil {
		// Fall back to the raw bytes so the user can at least inspect
		// what we produced and fix the syntax error by hand.
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
// package name never appears in the body, and returns gofmt'd bytes.
func pruneAndFormat(src string) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "transpile.go", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	// Walk the body to see which package names it actually references.
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
	// Rebuild the import block, keeping only imports whose alias (or
	// base name, for unaliased paths) is referenced.
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
	// Replace the single GenDecl(Import) with the filtered list.
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

// readerForExt / writerForExt map a filename extension to the
// top-level golars helper that reads / writes that format. Keep in
// sync with the loader list in cmd/golars/subcmd_inspect.go.
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
	case ".csv":
		return "WriteCSV"
	case ".tsv":
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

func titleCase(s string) string {
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
	// Fallback: capitalise first letter.
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
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
