package analysis

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// ItemKind classifies a completion item.
type ItemKind uint8

// Completion item kinds.
const (
	ItemCommand ItemKind = iota + 1
	ItemKeyword
	ItemColumn
	ItemFrame
	ItemFile
	ItemFolder
	ItemFunction
	ItemMethod
	ItemNamespace
	ItemParam
	ItemValue
)

// Item is one completion candidate.
type Item struct {
	Label string
	Kind  ItemKind
	// Detail is a one-line description (a dtype, a signature).
	Detail string
	// Doc is markdown documentation.
	Doc string
	// Insert is the text to insert; Snippet marks LSP snippet syntax.
	Insert  string
	Snippet bool
}

// Completion is the answer to a completion request.
type Completion struct {
	Items []Item
	// From is the byte column on the cursor line where the replaced
	// word starts; the cursor column is its end.
	From int
}

// cursorMark stands in for the word being typed, so the statement
// parser tells us what role the cursor position has.
const cursorMark = "zzglrcursorzz"

// Complete returns the completions at a 0-based line and byte column.
func (r *Result) Complete(line, col int) Completion {
	text := ""
	if line >= 0 && line < len(r.File.Lines) {
		text = r.File.Lines[line]
	}
	col = min(max(col, 0), len(text))
	before := text[:col]
	if _, cc := splitCommentIdx(before); cc >= 0 {
		return Completion{From: col}
	}
	word := typedWord(before)
	from := col - len(word)

	// Rebuild the statement text up to the cursor, including earlier
	// continuation lines.
	prefix := before
	start := line
	for start > 0 {
		prev := strings.TrimRight(r.File.Lines[start-1], " \t")
		body, _ := splitCommentIdx(prev)
		body = strings.TrimRight(body, " \t")
		if !strings.HasSuffix(body, "\\") {
			break
		}
		prefix = strings.TrimSuffix(body, "\\") + " " + prefix
		start--
	}
	trimmed := strings.TrimLeft(prefix, " \t")
	trimmed = strings.TrimPrefix(trimmed, ".")
	if !strings.ContainsAny(trimmed, " \t") {
		return Completion{Items: commandItems(word), From: from}
	}
	probeText := prefix[:len(prefix)-len(word)] + word + cursorMark + closers(prefix)
	f := syntax.Parse(probeText)
	if len(f.Stmts) == 0 {
		return Completion{From: from}
	}
	st := f.Stmts[0]
	if st.Spec == nil {
		return Completion{From: from}
	}
	focus, _, frames := r.StateAt(start)
	c := &completer{r: r, focus: focus, frames: frames, word: word, st: st}
	off := strings.Index(st.Text, cursorMark)
	part := st.PartAt(off)
	for i := range st.Parts {
		if p := &st.Parts[i]; p.Kind == syntax.PartAgg && off >= p.Off && off <= p.End {
			part = p
		}
	}
	c.items(part, off)
	return Completion{Items: c.out, From: from}
}

func splitCommentIdx(s string) (string, int) {
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			i++
		case c == inQuote:
			inQuote = 0
		case (c == '"' || c == '\'') && inQuote == 0:
			inQuote = c
		case c == '#' && inQuote == 0:
			return s[:i], i
		}
	}
	return s, -1
}

// typedWord returns the identifier-like word that ends at the end of
// s (letters, digits, _, and path characters).
func typedWord(s string) string {
	i := len(s)
	for i > 0 {
		c := s[i-1]
		if c == '_' || c == '-' || c == '/' || c == '~' || c >= 0x80 ||
			(c >= '0' && c <= '9') || (c|0x20 >= 'a' && c|0x20 <= 'z') {
			i--
			continue
		}
		// Keep dots inside paths (data/x.cs) but not method dots.
		if c == '.' && i > 1 && strings.ContainsAny(s[:i], "/") {
			i--
			continue
		}
		break
	}
	return s[i:]
}

// closers returns the brackets that close every one still open in s,
// so a statement truncated at the cursor still parses.
func closers(s string) string {
	var stack []byte
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '(':
			stack = append(stack, ')')
		case c == '[':
			stack = append(stack, ']')
		case (c == ')' || c == ']') && len(stack) > 0:
			stack = stack[:len(stack)-1]
		}
	}
	out := make([]byte, 0, len(stack)+1)
	if quote != 0 {
		out = append(out, quote)
	}
	for i := len(stack) - 1; i >= 0; i-- {
		out = append(out, stack[i])
	}
	return string(out)
}

type completer struct {
	r      *Result
	focus  Frame
	frames map[string]Frame
	word   string
	st     *syntax.Stmt
	out    []Item
	seen   map[string]bool
}

func (c *completer) add(it Item) {
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[it.Label] || !strings.HasPrefix(strings.ToLower(it.Label), strings.ToLower(c.word)) {
		return
	}
	c.seen[it.Label] = true
	c.out = append(c.out, it)
}

func (c *completer) items(p *syntax.Part, off int) {
	if p == nil {
		c.words()
		return
	}
	switch p.Kind {
	case syntax.PartPath:
		c.paths()
	case syntax.PartFrame:
		c.frameNames()
	case syntax.PartTarget:
		c.frameNames()
		c.paths()
	case syntax.PartColumn:
		c.columns()
		if c.st.Spec.Grammar == "@sort" || c.st.Spec.Name == "to_dummies" {
			c.words()
		}
	case syntax.PartList:
		c.columns()
		c.words()
	case syntax.PartOption, syntax.PartKeyword, syntax.PartText, syntax.PartWord:
		c.words()
		if p.Kind == syntax.PartText && (c.st.Spec.Grammar == "@groupby" || c.st.Spec.Grammar == "@dynamic") {
			c.columns()
		}
	case syntax.PartAgg:
		if len(p.Items) > 1 && strings.Contains(p.Items[1].Text, cursorMark) {
			for _, op := range syntax.AggOps {
				c.add(Item{Label: op, Kind: ItemKeyword, Detail: "aggregation"})
			}
			return
		}
		if strings.Contains(p.Items[0].Text, cursorMark) {
			c.columns()
		}
	case syntax.PartDType:
		for _, n := range exprparse.DTypeNames() {
			c.add(Item{Label: n, Kind: ItemValue, Detail: "dtype"})
		}
	case syntax.PartDuration:
		for _, d := range []string{"1s", "1m", "5m", "15m", "30m", "1h", "1d", "1w", "1mo", "1q", "1y"} {
			c.add(Item{Label: d, Kind: ItemValue, Detail: "duration"})
		}
	case syntax.PartExpr:
		c.expr(p, off)
	}
}

func (c *completer) words() {
	for _, w := range syntax.GrammarWords(c.st.Spec) {
		c.add(Item{Label: w, Kind: ItemKeyword})
	}
}

func (c *completer) columns() {
	if !c.focus.Known() {
		return
	}
	for _, name := range c.focus.Columns() {
		insert := name
		if !isPlainName(name) {
			insert = `col("` + name + `")`
		}
		c.add(Item{Label: name, Kind: ItemColumn, Detail: c.focus.DType(name), Insert: insert})
	}
}

func isPlainName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !(ch == '_' || (ch >= '0' && ch <= '9' && i > 0) || (ch|0x20 >= 'a' && ch|0x20 <= 'z') || ch >= 0x80) {
			return false
		}
	}
	return !slices.Contains(exprparse.Keywords(), strings.ToLower(s))
}

func (c *completer) frameNames() {
	names := make([]string, 0, len(c.frames))
	for n := range c.frames {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c.add(Item{Label: n, Kind: ItemFrame, Detail: "frame " + c.frames[n].Shape()})
	}
}

// dataExt lists the extensions load and join read.
var dataExt = []string{".csv", ".tsv", ".parquet", ".pq", ".arrow", ".ipc", ".json", ".ndjson", ".jsonl"}

func (c *completer) paths() {
	dirPart, _ := filepath.Split(c.word)
	base := c.r.opts.Dir
	dir := Resolve(dirPart, base)
	if dirPart == "" {
		dir = base
		if dir == "" {
			dir = "."
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	wantGlr := c.st.Spec.Name == "source"
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if e.IsDir() {
			c.add(Item{Label: dirPart + name + "/", Kind: ItemFolder})
			continue
		}
		ext := strings.ToLower(filepath.Ext(name))
		if wantGlr && ext != ".glr" || !wantGlr && !slices.Contains(dataExt, ext) &&
			c.st.Spec.Name != "save" && c.st.Spec.Name != "ls" && c.st.Spec.Name != "cd" {
			continue
		}
		c.add(Item{Label: dirPart + name, Kind: ItemFile})
	}
}

// expr completes inside an expression part.
func (c *completer) expr(p *syntax.Part, off int) {
	if p.Expr == nil {
		c.columns()
		c.freeFunctions()
		return
	}
	rel := off - p.Off
	var target, parent *exprparse.Node
	var argOf *exprparse.Node
	var walk func(n, par *exprparse.Node)
	walk = func(n, par *exprparse.Node) {
		if n == nil {
			return
		}
		if (n.Kind == exprparse.KindCol || n.Kind == exprparse.KindCall) && rel >= n.NamePos && rel <= n.NameEnd() &&
			strings.Contains(n.Name, cursorMark) {
			target, parent = n, par
		}
		walk(n.Recv, n)
		for _, a := range n.Args {
			walk(a, n)
		}
		for _, k := range n.Kw {
			walk(k.Val, n)
		}
		walk(n.Final, n)
	}
	walk(p.Expr, nil)
	if target == nil {
		return
	}
	if parent != nil && parent.Kind == exprparse.KindCall && slices.Contains(parent.Args, target) && target.Recv == nil {
		argOf = parent
	}
	switch {
	case target.Kind == exprparse.KindCall && target.Recv != nil:
		// x.<method> or x.ns.<method>
		if target.NS != "" {
			c.methods(target.NS, false)
			return
		}
		if target.Recv.Kind == exprparse.KindCol && exprparse.IsNamespace(target.Recv.Name) && target.Recv.Parens == 0 {
			// dt.<fn>: a namespace function, or a method of a column
			// named dt.
			c.methods(target.Recv.Name, true)
		}
		c.methods("", false)
		for _, ns := range exprparse.Namespaces() {
			c.add(Item{Label: ns, Kind: ItemNamespace, Detail: ns + " namespace"})
		}
	case target.Kind == exprparse.KindCall && target.NS != "":
		c.methods(target.NS, true)
	default:
		if argOf != nil {
			c.keywordArgs(argOf)
		}
		c.columns()
		c.freeFunctions()
		for _, kw := range []string{"when", "then", "otherwise", "and", "or", "not", "in", "true", "false", "null"} {
			c.add(Item{Label: kw, Kind: ItemKeyword})
		}
	}
}

func (c *completer) freeFunctions() {
	for _, ns := range exprparse.Namespaces() {
		c.add(Item{Label: ns, Kind: ItemNamespace, Detail: ns + " namespace"})
	}
	fns := exprparse.Functions()
	for _, group := range []string{"free", ""} {
		for _, name := range fns[group] {
			info, found := exprparse.Lookup("", name)
			if !found {
				continue
			}
			c.add(funcItem(info, true))
		}
	}
}

// methods offers the functions of namespace ns. asFunction selects
// the `ns.fn(x, ...)` form (the receiver is the first argument).
func (c *completer) methods(ns string, asFunction bool) {
	for _, name := range exprparse.Functions()[ns] {
		info, found := exprparse.Lookup(ns, name)
		if !found {
			continue
		}
		c.add(funcItem(info, asFunction))
	}
}

// funcItem builds a completion for a function with a snippet for its
// required arguments.
func funcItem(f exprparse.FuncInfo, asFunction bool) Item {
	var args []string
	n := 0
	if asFunction && !f.Free {
		n++
		args = append(args, "${1:x}")
	}
	for _, p := range f.Params {
		if p.Default != "" || p.Variadic && len(args) > 0 {
			break
		}
		n++
		args = append(args, "${"+itoa(n)+":"+p.Name+"}")
	}
	kind := ItemFunction
	if !asFunction {
		kind = ItemMethod
	}
	return Item{
		Label: f.Name, Kind: kind, Detail: f.Label(), Doc: f.Doc,
		Insert: f.Name + "(" + strings.Join(args, ", ") + ")$0", Snippet: true,
	}
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + string(rune('0'+n%10))
}

// keywordArgs offers `name=` for the parameters and options of call.
func (c *completer) keywordArgs(call *exprparse.Node) {
	ns, name := call.NS, call.Name
	info, found := exprparse.Lookup(ns, name)
	if !found {
		return
	}
	used := map[string]bool{}
	for _, k := range call.Kw {
		used[k.Name] = true
	}
	names := append([]string(nil), f2names(info.Params)...)
	names = append(names, info.Keywords...)
	for _, n := range names {
		if used[n] {
			continue
		}
		c.add(Item{Label: n + "=", Kind: ItemParam, Detail: "keyword argument of " + info.Name, Insert: n + "="})
	}
}

func f2names(ps []exprparse.Param) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		if !strings.HasPrefix(p.Name, "arg") {
			out = append(out, p.Name)
		}
	}
	return out
}

func commandItems(word string) []Item {
	var out []Item
	for _, spec := range script.Commands {
		names := append([]string{spec.Name}, spec.Aliases...)
		for _, n := range names {
			if !strings.HasPrefix(n, strings.ToLower(word)) {
				continue
			}
			snippet := syntax.Snippet(&spec)
			if n != spec.Name {
				snippet = n + strings.TrimPrefix(snippet, spec.Name)
			}
			out = append(out, Item{
				Label: n, Kind: ItemCommand, Detail: spec.Signature, Doc: spec.Markdown(),
				Insert: snippet, Snippet: strings.Contains(snippet, "$"),
			})
		}
	}
	return out
}
