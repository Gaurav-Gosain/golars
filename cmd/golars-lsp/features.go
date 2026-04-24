package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/script"
)

// --------------------------------------------------------------------
// Position / range types + document slicing helpers
// --------------------------------------------------------------------

type position struct {
	Line      uint32 `json:"line"`
	Character uint32 `json:"character"`
}

type lspRange struct {
	Start position `json:"start"`
	End   position `json:"end"`
}

// lineAt returns the document's i-th logical line (UTF-8 bytes), or
// "" if out of range. Callers should normalise the position's
// Character via byteCol first.
func (d *document) lineAt(i int) string {
	if i < 0 || i >= len(d.lines) {
		return ""
	}
	return d.lines[i]
}

// byteCol converts a UTF-16-code-unit column (LSP's default) to a
// UTF-8 byte column within `line`. For ASCII content these are
// identical; we do the conversion anyway because script identifiers
// may include UTF-8 (e.g. column names set by the host).
func byteCol(line string, utf16Col int) int {
	if utf16Col <= 0 {
		return 0
	}
	col := 0
	for i, r := range line {
		if col >= utf16Col {
			return i
		}
		if r <= 0xFFFF {
			col++
		} else {
			col += 2 // surrogate pair
		}
	}
	return len(line)
}

// --------------------------------------------------------------------
// Document analysis: one pass over the lines
// --------------------------------------------------------------------

type analysis struct {
	// frames lists every NAME declared via `load PATH as NAME` up to
	// (and including) the current line. Populated in lexical order.
	frames []string
	// diagnostics holds unknown commands / missing-arg findings.
	diagnostics []diagnostic
}

func analyze(d *document) analysis {
	out := analysis{}
	for li, raw := range d.lines {
		stmt := script.Normalize(raw)
		if stmt == "" {
			continue
		}
		parts := strings.Fields(stmt)
		cmdTok := parts[0] // has leading '.'
		cmdName := strings.TrimPrefix(cmdTok, ".")
		spec := script.FindCommand(cmdName)
		if spec == nil {
			// Unknown command. Point the diagnostic at the command token.
			startCol := indexOfToken(raw, cmdName)
			out.diagnostics = append(out.diagnostics, diagnostic{
				Range: lspRange{
					Start: position{Line: uint32(li), Character: uint32(startCol)},
					End:   position{Line: uint32(li), Character: uint32(startCol + len(cmdName))},
				},
				Severity: diagnosticSeverityError,
				Source:   "golars",
				Message:  "unknown command: " + cmdName,
			})
			continue
		}
		// Track `load PATH as NAME` and try to resolve the path; if
		// it can't be found the user gets a hint in the sign column.
		if spec.Name == "load" && len(parts) >= 2 {
			if len(parts) >= 4 && strings.EqualFold(parts[2], "as") {
				out.frames = append(out.frames, parts[3])
			}
			path := parts[1]
			if !filepath.IsAbs(path) {
				resolved := resolvePath(path, docDir(d))
				if _, err := os.Stat(resolved); err != nil {
					startCol := indexOfToken(raw, path)
					out.diagnostics = append(out.diagnostics, diagnostic{
						Range: lspRange{
							Start: position{Line: uint32(li), Character: uint32(startCol)},
							End:   position{Line: uint32(li), Character: uint32(startCol + len(path))},
						},
						Severity: diagnosticSeverityWarning,
						Source:   "golars",
						Message:  "file not found relative to script: " + path,
					})
				}
			}
		}
		// Very light arity check: commands whose signature has a <>
		// required arg must have at least one positional argument.
		if hasRequiredArg(spec.Signature) && len(parts) < 2 {
			startCol := indexOfToken(raw, cmdName)
			out.diagnostics = append(out.diagnostics, diagnostic{
				Range: lspRange{
					Start: position{Line: uint32(li), Character: uint32(startCol)},
					End:   position{Line: uint32(li), Character: uint32(startCol + len(cmdName))},
				},
				Severity: diagnosticSeverityWarning,
				Source:   "golars",
				Message:  "missing argument for " + spec.Name + ": " + spec.Signature,
			})
		}
	}
	return out
}

// indexOfToken finds the start of the first occurrence of `tok` in
// `line`, or 0 if not found. Used to anchor diagnostics on the
// command token rather than the leading whitespace.
func indexOfToken(line, tok string) int {
	if tok == "" {
		return 0
	}
	if i := strings.Index(line, tok); i >= 0 {
		return i
	}
	return 0
}

// hasRequiredArg returns true if the signature contains a <required>
// placeholder past the command name.
func hasRequiredArg(sig string) bool {
	rest := sig
	if sp := strings.IndexByte(rest, ' '); sp >= 0 {
		rest = rest[sp+1:]
	} else {
		return false
	}
	return strings.ContainsRune(rest, '<')
}

// --------------------------------------------------------------------
// publishDiagnostics
// --------------------------------------------------------------------

const (
	diagnosticSeverityError   = 1
	diagnosticSeverityWarning = 2
)

type diagnostic struct {
	Range    lspRange `json:"range"`
	Severity int      `json:"severity"`
	Source   string   `json:"source,omitempty"`
	Message  string   `json:"message"`
}

type publishDiagnosticsParams struct {
	URI         string       `json:"uri"`
	Diagnostics []diagnostic `json:"diagnostics"`
}

func (s *server) publishDiagnostics(d *document) {
	a := analyze(d)
	if a.diagnostics == nil {
		a.diagnostics = []diagnostic{}
	}
	s.notify("textDocument/publishDiagnostics", publishDiagnosticsParams{
		URI:         d.uri,
		Diagnostics: a.diagnostics,
	})
}

// --------------------------------------------------------------------
// textDocument/completion
// --------------------------------------------------------------------

type completionParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Position     position               `json:"position"`
}

type completionItem struct {
	Label         string  `json:"label"`
	Kind          int     `json:"kind,omitempty"` // LSP CompletionItemKind
	Detail        string  `json:"detail,omitempty"`
	Documentation *markup `json:"documentation,omitempty"`
	InsertText    string  `json:"insertText,omitempty"`
	SortText      string  `json:"sortText,omitempty"`
	FilterText    string  `json:"filterText,omitempty"`
}

type markup struct {
	Kind  string `json:"kind"` // "markdown" or "plaintext"
	Value string `json:"value"`
}

// LSP CompletionItemKind values we reference. Keyword is the kind
// clients use for language keywords: Neovim treats it as plain-text
// insertion, whereas Function triggers paren auto-insertion. Variable
// is the right choice for column names and staged frame names.
const (
	ciKindText     = 1
	ciKindVariable = 6
	ciKindKeyword  = 14
	ciKindFile     = 17
	ciKindFolder   = 19
)

func (s *server) handleCompletion(msg *rawMessage) {
	var p completionParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.reply(msg, []completionItem{})
		return
	}
	doc := s.docs.get(p.TextDocument.URI)
	if doc == nil {
		s.reply(msg, []completionItem{})
		return
	}

	line := doc.lineAt(int(p.Position.Line))
	col := byteCol(line, int(p.Position.Character))
	prefix := line[:col]

	items := s.completionsFor(doc, prefix, int(p.Position.Line))
	s.reply(msg, items)
}

// completionsFor picks the right completion set based on what's
// already typed on the current line before the caret. cursorLine is
// the zero-based line index, used to resolve the focused frame at
// that cursor position (so `use NAME` statements above the cursor
// affect which columns we suggest).
func (s *server) completionsFor(doc *document, prefix string, cursorLine int) []completionItem {
	trimmed := strings.TrimLeft(prefix, " \t")
	// Cursor at the start of a command (no space yet).
	if !strings.ContainsAny(trimmed, " \t") {
		return commandCompletions(trimmed)
	}
	parts := strings.Fields(trimmed)
	cmd := strings.TrimPrefix(parts[0], ".")
	spec := script.FindCommand(cmd)
	if spec == nil {
		return nil
	}
	endsInSpace := strings.HasSuffix(prefix, " ") || strings.HasSuffix(prefix, "\t")
	var current string
	if !endsInSpace {
		current = parts[len(parts)-1]
	}
	// argIdx is the 1-based index of the argument the cursor is on.
	// parts[0] is the command itself; parts[1..] are args. When the
	// line ends in whitespace we're starting a fresh arg after the
	// last token.
	argIdx := len(parts) - 1
	if endsInSpace {
		argIdx++
	}
	return argCompletionsAt(spec, cmd, parts, argIdx, current, doc, cursorLine)
}

// argCompletionsAt dispatches on (command, argIdx) so that each
// positional slot gets exactly the right completion kind. Treating
// every arg uniformly via ArgKind is too coarse for commands like
// `join NAME on KEY TYPE` or `load PATH as NAME` where different
// positions expect different vocabularies.
func argCompletionsAt(spec *script.CommandSpec, cmd string, _ []string, argIdx int, current string, doc *document, cursorLine int) []completionItem {
	switch cmd {
	case "load":
		// load PATH [as NAME]
		switch argIdx {
		case 1:
			return pathCompletions(current, doc)
		case 2:
			return []completionItem{{Label: "as", Kind: ciKindKeyword, Detail: "stage frame under a name"}}
		}
	case "save", "source":
		if argIdx == 1 {
			return pathCompletions(current, doc)
		}
	case "use", "drop_frame":
		if argIdx == 1 {
			return frameCompletions(current, doc)
		}
	case "sort":
		// sort <col> [asc|desc]
		switch argIdx {
		case 1:
			return columnCompletions(doc, cmd, current, cursorLine)
		case 2:
			return []completionItem{
				{Label: "asc", Kind: ciKindKeyword, Detail: "ascending (default)"},
				{Label: "desc", Kind: ciKindKeyword, Detail: "descending"},
			}
		}
	case "join":
		// join <path|NAME> on <key> [inner|left|cross]
		switch argIdx {
		case 1:
			// Target frame first. Include staged frames and file paths
			// so both forms of join work.
			out := frameCompletions(current, doc)
			out = append(out, pathCompletions(current, doc)...)
			return out
		case 2:
			return []completionItem{{Label: "on", Kind: ciKindKeyword, Detail: "on <key>"}}
		case 3:
			// The key is usually a column name common to both sides.
			// Offering the focused frame's columns is the best we can
			// do without running the join.
			return columnCompletions(doc, cmd, current, cursorLine)
		case 4:
			return []completionItem{
				{Label: "inner", Kind: ciKindKeyword, Detail: "inner join (default)"},
				{Label: "left", Kind: ciKindKeyword, Detail: "left outer join"},
				{Label: "cross", Kind: ciKindKeyword, Detail: "cartesian cross join"},
			}
		}
	case "select", "drop":
		// Comma-separated column list; always offer columns.
		return columnCompletions(doc, cmd, current, cursorLine)
	case "filter":
		// filter <col> <op> <value> [and|or ...]
		// We can't reliably tell "col" from "value" position without a
		// real parser; offering columns everywhere is fine: the user
		// mostly types values freely, and the column list is still a
		// handy reference.
		return columnCompletions(doc, cmd, current, cursorLine)
	case "groupby":
		// groupby <keys> <col:op[:alias]>...
		return columnCompletions(doc, cmd, current, cursorLine)
	case "limit", "head", "tail":
		// numeric arg: no useful suggestions
		return nil
	}
	// Fall back to the spec's ArgKind for commands we haven't
	// special-cased above.
	switch spec.ArgKind {
	case "path":
		return pathCompletions(current, doc)
	case "frame":
		return frameCompletions(current, doc)
	case "column":
		return columnCompletions(doc, cmd, current, cursorLine)
	}
	return nil
}

func commandCompletions(partial string) []completionItem {
	// Accept `.xxx` or bare `xxx`; strip the dot to match spec names.
	hasDot := strings.HasPrefix(partial, ".")
	p := strings.TrimPrefix(partial, ".")
	out := make([]completionItem, 0, len(script.Commands))
	for _, c := range script.Commands {
		if p != "" && !strings.HasPrefix(c.Name, p) {
			continue
		}
		label := c.Name
		if hasDot {
			label = "." + c.Name
		}
		doc := c.Summary
		if c.LongDoc != "" {
			doc = c.Summary + "\n\n" + c.LongDoc
		}
		// Kind=Keyword (not Function) so clients don't auto-insert
		// parens or treat the label as a callable identifier.
		out = append(out, completionItem{
			Label:         label,
			Kind:          ciKindKeyword,
			Detail:        c.Signature,
			Documentation: &markup{Kind: "markdown", Value: doc},
			InsertText:    c.Name,
			SortText:      c.Category + c.Name,
		})
	}
	return out
}

func pathCompletions(current string, doc *document) []completionItem {
	dir, base := filepath.Split(current)
	root := dir
	if root == "" {
		// Resolve relative paths against the directory of the open
		// document. This matches how the REPL's `.source` and `.load`
		// commands behave when invoked via `-run`.
		if docDir := docDir(doc); docDir != "" {
			root = docDir
		} else {
			root = "."
		}
	} else if !filepath.IsAbs(root) {
		if docDir := docDir(doc); docDir != "" {
			root = filepath.Join(docDir, root)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	out := make([]completionItem, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if base != "" && !strings.HasPrefix(name, base) {
			continue
		}
		kind := ciKindFile
		label := name
		if e.IsDir() {
			kind = ciKindFolder
			label = name + "/"
		}
		out = append(out, completionItem{Label: label, Kind: kind, InsertText: label})
	}
	return out
}

// columnCompletions offers real column names drawn from the focused
// frame's source file at the cursor position. For `groupby`, each
// column is surfaced both as a bare name and as a `col:op` starter
// so the user can finish the agg spec without retyping the column.
func columnCompletions(doc *document, cmd, current string, cursorLine int) []completionItem {
	state := framesAtLine(doc, cursorLine)
	// Strip comma prefix so `a,b,c` + typing completes the last item.
	partial := current
	if i := strings.LastIndexAny(partial, ", \t"); i >= 0 {
		partial = partial[i+1:]
	}

	out := make([]completionItem, 0, len(state.focus.cols))
	for _, col := range state.focus.cols {
		if partial != "" && !strings.HasPrefix(col, partial) {
			continue
		}
		detail := "column"
		if state.focusName != "" {
			detail = "column of `" + state.focusName + "`"
		}
		out = append(out, completionItem{
			Label:      col,
			Kind:       ciKindVariable,
			Detail:     detail,
			InsertText: col,
		})
	}

	// For groupby, also expose `col:sum`, `col:mean`, `col:count`
	// starters per column so the aggregation spec rolls off the
	// tongue. The user still needs to finish `:alias` by hand.
	if cmd == "groupby" && len(state.focus.cols) > 0 {
		ops := []string{"sum", "mean", "min", "max", "count", "null_count", "first", "last"}
		for _, col := range state.focus.cols {
			if partial != "" && !strings.HasPrefix(col, partial) {
				continue
			}
			for _, op := range ops {
				out = append(out, completionItem{
					Label:      col + ":" + op,
					Kind:       ciKindText,
					Detail:     "aggregation",
					InsertText: col + ":" + op,
					SortText:   "z" + col + op, // after bare column entries
				})
			}
		}
		// No columns detected → fall back to a format hint so the user
		// at least sees the col:op:alias shape.
		if len(state.focus.cols) == 0 {
			out = append(out, completionItem{
				Label:  "col:op:alias",
				Kind:   ciKindText,
				Detail: "aggregation spec",
				Documentation: &markup{Kind: "markdown",
					Value: "`col:op[:alias]`: ops: `sum`, `mean`, `min`, `max`, `count`, `null_count`, `first`, `last`."},
			})
		}
	}
	return out
}

// frameCompletions returns names declared via `load PATH as NAME` in
// the same document up to the current cursor. We don't cross-reference
// `use NAME` statements since they consume from the registry.
func frameCompletions(current string, doc *document) []completionItem {
	a := analyze(doc)
	out := make([]completionItem, 0, len(a.frames))
	seen := make(map[string]bool, len(a.frames))
	for _, n := range a.frames {
		if seen[n] {
			continue
		}
		seen[n] = true
		if current != "" && !strings.HasPrefix(n, current) {
			continue
		}
		out = append(out, completionItem{
			Label:      n,
			Kind:       ciKindVariable,
			Detail:     "staged frame",
			InsertText: n,
		})
	}
	return out
}

// docDir parses a `file://…` URI into a local directory path; returns
// "" if the URI is non-file or unparseable. Windows file URIs follow
// the `file:///C:/...` shape where url.URL.Path keeps the leading
// slash; strip it so filepath.Dir returns a native drive-prefixed
// path rather than `/C:/...`.
func docDir(d *document) string {
	u, err := url.Parse(d.uri)
	if err != nil || u.Scheme != "file" {
		return ""
	}
	p := u.Path
	if runtime.GOOS == "windows" && len(p) >= 3 && p[0] == '/' && p[2] == ':' {
		p = p[1:]
	}
	return filepath.Dir(p)
}

// --------------------------------------------------------------------
// textDocument/inlayHint
//
// We surface the "shape" of each frame the script brings into
// existence: row × col counts at end of `load` lines, and a column
// count at end of `use` lines. Row counts are capped at 8 MiB of
// source so a script referencing a 2 GiB CSV doesn't stall the LSP.
// --------------------------------------------------------------------

type inlayHintParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Range        lspRange               `json:"range"`
}

type inlayHint struct {
	Position     position        `json:"position"`
	Label        string          `json:"label"`
	Kind         int             `json:"kind,omitempty"` // 1 = Type, 2 = Parameter
	PaddingLeft  bool            `json:"paddingLeft,omitempty"`
	PaddingRight bool            `json:"paddingRight,omitempty"`
	Tooltip      *markupContent  `json:"tooltip,omitempty"`
}

type markupContent struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

const inlayHintKindType = 1

func (s *server) handleInlayHint(msg *rawMessage) {
	var p inlayHintParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.reply(msg, []inlayHint{})
		return
	}
	doc := s.docs.get(p.TextDocument.URI)
	if doc == nil {
		s.reply(msg, []inlayHint{})
		return
	}
	hints := collectInlayHints(doc, p.Range)
	if hints == nil {
		hints = []inlayHint{}
	}
	s.reply(msg, hints)
}

// collectInlayHints walks the document and emits one hint per
// shape-transforming statement within the requested range. Hints are
// anchored at the end-of-line column so they render as trailing
// annotations. Side-effect commands (show / schema / describe /
// save / frames / ...) are skipped because they don't change the
// focused frame's shape.
//
// Additionally, a comment line ending in `^?` (Twoslash-style) emits
// a richer probe hint listing the focused frame's columns in full -
// a cheap way for the user to peek at the schema without running
// `.schema` in the REPL.
func collectInlayHints(d *document, rng lspRange) []inlayHint {
	lo := int(rng.Start.Line)
	hi := int(rng.End.Line)
	if lo < 0 {
		lo = 0
	}
	if hi >= len(d.lines) {
		hi = len(d.lines) - 1
	}
	dir := docDir(d)

	// Walk from the top so state is correct even when the range
	// starts mid-document. State-updating is decoupled from hint
	// emission: we only emit for lines inside [lo, hi].
	state := frameState{staged: make(map[string]frameShape)}
	hints := make([]inlayHint, 0, hi-lo+1)

	for li := 0; li <= hi && li < len(d.lines); li++ {
		raw := d.lines[li]
		stmt := script.Normalize(raw)
		if stmt == "" {
			// Blank or comment-only: check for the ^? probe.
			if li >= lo && isProbeDirective(raw) {
				if label := probeLabel(&state); label != "" {
					col := strings.Index(raw, "^?")
					if col < 0 {
						col = len(raw)
					}
					hint := inlayHint{
						Position:    position{Line: uint32(li), Character: uint32(col + len("^?"))},
						Label:       label,
						Kind:        inlayHintKindType,
						PaddingLeft: true,
					}
					// Spawn `golars --preview` on the prefix so the
					// hover tooltip shows the actual table. Clients
					// that render inlay tooltips (Zed, VS Code) get
					// the full preview without a dedicated probe
					// plugin. Skip silently when the binary is absent
					// or the preview fails: the single-line label
					// remains useful on its own.
					if tip := renderProbeTooltip(d, li); tip != "" {
						hint.Tooltip = &markupContent{Kind: "markdown", Value: tip}
					}
					hints = append(hints, hint)
				}
			}
			continue
		}

		// Apply the statement to the running state machine.
		applyStmt(&state, dir, stmt)

		parts := strings.Fields(stmt)
		cmd := strings.TrimPrefix(parts[0], ".")
		if !isShapeStatement(cmd) {
			continue
		}
		// For `load PATH as NAME` the focus is unchanged: the
		// relevant shape is the newly-staged frame. Surface that
		// instead so the hint describes what the statement actually
		// produced.
		shape := state.focus
		if cmd == "load" && len(parts) >= 4 && strings.EqualFold(parts[2], "as") {
			if staged, ok := state.staged[parts[3]]; ok {
				shape = staged
			}
		}
		if len(shape.cols) == 0 && shape.rows == rowsUnknown {
			continue
		}
		if li < lo {
			continue
		}
		hints = append(hints, inlayHint{
			Position:    position{Line: uint32(li), Character: uint32(len(raw))},
			Label:       formatShape(shape.rows, len(shape.cols)),
			Kind:        inlayHintKindType,
			PaddingLeft: true,
		})
	}
	return hints
}

// isShapeStatement returns true for commands that produce or
// transform the focused frame's shape.
func isShapeStatement(cmd string) bool {
	switch cmd {
	case "load", "use", "filter", "sort", "limit", "head", "tail",
		"select", "drop", "groupby", "join":
		return true
	}
	return false
}

// isProbeDirective recognises a comment-only line ending in `^?`
// (trailing whitespace tolerated). Matches the Twoslash / Quokka
// convention that turns `// ^?` into "reveal the thing above".
func isProbeDirective(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "#") {
		return false
	}
	trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
	trimmed = strings.TrimRightFunc(trimmed, func(r rune) bool {
		return r == ' ' || r == '\t'
	})
	return strings.HasSuffix(trimmed, "^?")
}

// probeLabel returns a verbose schema summary for the currently-
// focused frame. Rendered when the user places a `# ^?` comment -
// shows the full column list and row estimate inline so they get a
// schema peek without switching to the REPL.
func probeLabel(st *frameState) string {
	if len(st.focus.cols) == 0 {
		return ""
	}
	cols := strings.Join(st.focus.cols, ", ")
	rows := formatShapeRows(st.focus.rows)
	return "cols(" + cols + ") " + rows
}

// formatShapeRows returns just the "N rows" portion used by probe
// labels. Pulled out so the regular shape hint and the probe hint
// share the same row-count vocabulary.
func formatShapeRows(rows int) string {
	if rows < 0 {
		return "· ? rows"
	}
	if rows == 1 {
		return "· 1 row"
	}
	return "· " + fmtInt(rows) + " rows"
}

// formatShape renders "→ N rows × M cols" (or "→ ? rows × M cols"
// when the row count is unknown). Kept short so the hint doesn't
// crowd the editor gutter.
func formatShape(rows, cols int) string {
	var rowStr string
	if rows < 0 {
		rowStr = "? rows"
	} else if rows == 1 {
		rowStr = "1 row"
	} else {
		rowStr = fmtInt(rows) + " rows"
	}
	return "→ " + rowStr + " × " + fmtInt(cols) + " cols"
}

// fmtInt is a minimal base-10 formatter. Sits in the inlay-hint hot
// path so we stay off fmt.Sprintf (not a measurable win for this
// server, but keeps allocations predictable).
func fmtInt(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// --------------------------------------------------------------------
// textDocument/hover
// --------------------------------------------------------------------

type hoverParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Position     position               `json:"position"`
}

type hoverResult struct {
	Contents markup    `json:"contents"`
	Range    *lspRange `json:"range,omitempty"`
}

func (s *server) handleHover(msg *rawMessage) {
	var p hoverParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.reply(msg, nil)
		return
	}
	doc := s.docs.get(p.TextDocument.URI)
	if doc == nil {
		s.reply(msg, nil)
		return
	}
	line := doc.lineAt(int(p.Position.Line))
	if line == "" {
		s.reply(msg, nil)
		return
	}
	col := byteCol(line, int(p.Position.Character))
	tok, start, end := tokenAt(line, col)
	if tok == "" {
		s.reply(msg, nil)
		return
	}
	spec := script.FindCommand(tok)
	if spec == nil {
		// Not a command: maybe a column name with known schema.
		if info := columnHoverInfo(doc, tok, int(p.Position.Line)); info != "" {
			s.reply(msg, hoverResult{
				Contents: markup{Kind: "markdown", Value: info},
				Range: &lspRange{
					Start: position{Line: p.Position.Line, Character: uint32(start)},
					End:   position{Line: p.Position.Line, Character: uint32(end)},
				},
			})
			return
		}
		s.reply(msg, nil)
		return
	}
	body := "**" + spec.Signature + "**\n\n" + spec.Summary
	if spec.LongDoc != "" {
		body += "\n\n" + spec.LongDoc
	}
	s.reply(msg, hoverResult{
		Contents: markup{Kind: "markdown", Value: body},
		Range: &lspRange{
			Start: position{Line: p.Position.Line, Character: uint32(start)},
			End:   position{Line: p.Position.Line, Character: uint32(end)},
		},
	})
}

// tokenAt returns the whitespace-delimited token containing byte
// index col, plus its start and end byte offsets in the line.
func tokenAt(line string, col int) (tok string, start, end int) {
	if col > len(line) {
		col = len(line)
	}
	start = col
	for start > 0 && !isSpace(line[start-1]) {
		start--
	}
	end = col
	for end < len(line) && !isSpace(line[end]) {
		end++
	}
	tok = line[start:end]
	// Strip leading '.' so hover works whether the user typed it or not.
	return strings.TrimPrefix(tok, "."), start, end
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' }

// renderProbeTooltip returns a markdown-formatted table preview for
// a `# ^?` probe at line `probeLine`. Writes the script prefix
// (lines 0..probeLine-1) to a tempfile, invokes the golars CLI in
// --preview mode, and wraps the captured table in a fenced code
// block so inlay-hint tooltips render it with a monospace font.
//
// Returns "" when golars is not on PATH, the invocation fails, or
// the output is empty. Callers treat that as "no tooltip" so the
// inline label remains the only signal.
func renderProbeTooltip(d *document, probeLine int) string {
	bin := findGolarsBin(d)
	if bin == "" {
		return ""
	}
	prefix := strings.Join(d.lines[:probeLine], "\n")
	if prefix == "" {
		return ""
	}
	f, err := os.CreateTemp("", "golars-probe-*.glr")
	if err != nil {
		return ""
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(prefix); err != nil {
		f.Close()
		return ""
	}
	f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--preview", f.Name())
	out, err := cmd.CombinedOutput()
	if err != nil || len(out) == 0 {
		return ""
	}
	return "```\n" + strings.TrimRight(string(out), "\n") + "\n```"
}

// findGolarsBin resolves the golars CLI used to render probe
// tooltips. Checks the workspace's PATH first (what the user would
// hit from a terminal); falls back to the directory the LSP was
// launched from so a bundled release is discoverable even when
// PATH is minimal.
func findGolarsBin(d *document) string {
	if p, err := exec.LookPath("golars"); err == nil {
		return p
	}
	// Derive from the LSP binary's own directory.
	if self, err := os.Executable(); err == nil {
		guess := filepath.Join(filepath.Dir(self), "golars")
		if runtime.GOOS == "windows" {
			guess += ".exe"
		}
		if _, err := os.Stat(guess); err == nil {
			return guess
		}
	}
	_ = d
	return ""
}
