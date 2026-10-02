package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/script/analysis"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// -----------------------------------------------------------------
// completion
// -----------------------------------------------------------------

type completionItem struct {
	Label            string         `json:"label"`
	Kind             int            `json:"kind,omitempty"`
	Detail           string         `json:"detail,omitempty"`
	Documentation    *markupContent `json:"documentation,omitempty"`
	TextEdit         *textEdit      `json:"textEdit,omitempty"`
	InsertTextFormat int            `json:"insertTextFormat,omitempty"`
	SortText         string         `json:"sortText,omitempty"`
	FilterText       string         `json:"filterText,omitempty"`
}

type markupContent struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type textEdit struct {
	Range   lspRange `json:"range"`
	NewText string   `json:"newText"`
}

// LSP CompletionItemKind values.
var itemKinds = map[analysis.ItemKind]int{
	analysis.ItemCommand:   14, // Keyword
	analysis.ItemKeyword:   14,
	analysis.ItemColumn:    5, // Field
	analysis.ItemFrame:     7, // Class
	analysis.ItemFile:      17,
	analysis.ItemFolder:    19,
	analysis.ItemFunction:  3,
	analysis.ItemMethod:    2,
	analysis.ItemNamespace: 9, // Module
	analysis.ItemParam:     6, // Variable
	analysis.ItemValue:     12,
}

// sortRank orders completion kinds: columns first inside expressions.
var sortRank = map[analysis.ItemKind]string{
	analysis.ItemColumn: "0", analysis.ItemParam: "0", analysis.ItemFrame: "1", analysis.ItemCommand: "1",
	analysis.ItemKeyword: "2", analysis.ItemFolder: "1", analysis.ItemFile: "1", analysis.ItemValue: "2",
	analysis.ItemMethod: "3", analysis.ItemFunction: "3", analysis.ItemNamespace: "4",
}

func (s *server) completion(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	c := d.analysis().Complete(line, col)
	items := make([]completionItem, 0, len(c.Items))
	rg := d.rng(line, c.From, line, col)
	for i, it := range c.Items {
		ci := completionItem{
			Label: it.Label, Kind: itemKinds[it.Kind], Detail: it.Detail,
			SortText: sortRank[it.Kind] + fmt.Sprintf("%04d", i),
		}
		if it.Doc != "" {
			ci.Documentation = &markupContent{Kind: "markdown", Value: it.Doc}
		}
		insert := it.Insert
		if insert == "" {
			insert = it.Label
		}
		ci.TextEdit = &textEdit{Range: rg, NewText: insert}
		if it.Snippet {
			ci.InsertTextFormat = 2
		}
		items = append(items, ci)
	}
	return map[string]any{"isIncomplete": false, "items": items}, nil
}

// -----------------------------------------------------------------
// hover and signature help
// -----------------------------------------------------------------

func (s *server) hover(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	r := d.analysis()
	md := r.Hover(line, col)
	if md == "" {
		if c := r.File.CommentAt(line); c != nil && isProbe(c.Text) {
			md = probeText(r, line)
		}
	}
	if md == "" {
		return nil, nil
	}
	return map[string]any{"contents": markupContent{Kind: "markdown", Value: md}}, nil
}

func (s *server) signatureHelp(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	sig, found := d.analysis().SignatureAt(line, col)
	if !found {
		return nil, nil
	}
	label := sig.Info.Label()
	var ps []map[string]any
	pos := strings.IndexByte(label, '(') + 1
	for _, p := range sig.Params {
		i := strings.Index(label[pos:], p)
		if i < 0 {
			continue
		}
		start := pos + i
		ps = append(ps, map[string]any{"label": []int{utf16Len(label[:start]), utf16Len(label[:start+len(p)])}})
		pos = start + len(p)
	}
	info := map[string]any{"label": label, "parameters": ps}
	if sig.Info.Doc != "" {
		info["documentation"] = markupContent{Kind: "markdown", Value: sig.Info.Doc}
	}
	out := map[string]any{"signatures": []any{info}, "activeSignature": 0}
	if sig.Active >= 0 {
		out["activeParameter"] = sig.Active
	}
	return out, nil
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r >= 0x10000 {
			n++
		}
	}
	return n
}

// -----------------------------------------------------------------
// navigation: definition, references, highlights, rename
// -----------------------------------------------------------------

// symbolAt returns the symbol under the position.
func symbolAt(r *analysis.Result, line, col int) *analysis.Symbol {
	for i := range r.Symbols {
		s := &r.Symbols[i]
		if s.Line == line && col >= s.Col && col <= s.EndCol {
			return s
		}
	}
	return nil
}

// defOf returns the definition location a symbol belongs to.
func defOf(s *analysis.Symbol) *analysis.Loc {
	if s.Def {
		return &s.Loc
	}
	return s.Target
}

// related returns every symbol that shares sym's definition.
func related(r *analysis.Result, sym *analysis.Symbol) []analysis.Symbol {
	def := defOf(sym)
	var out []analysis.Symbol
	for _, s := range r.Symbols {
		if s.Kind != sym.Kind || s.Name != sym.Name {
			continue
		}
		if d := defOf(&s); (def == nil && d == nil) || (def != nil && d != nil && *d == *def) {
			out = append(out, s)
		}
	}
	return out
}

func (s *server) definition(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	sym := symbolAt(d.analysis(), line, col)
	if sym == nil || defOf(sym) == nil {
		return nil, nil
	}
	l := defOf(sym)
	return location{URI: d.uri, Range: d.rng(l.Line, l.Col, l.Line, l.EndCol)}, nil
}

func (s *server) references(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	var p struct {
		Context struct {
			IncludeDeclaration bool `json:"includeDeclaration"`
		} `json:"context"`
	}
	_ = json.Unmarshal(params, &p)
	r := d.analysis()
	sym := symbolAt(r, line, col)
	if sym == nil {
		return []location{}, nil
	}
	out := []location{}
	for _, rs := range related(r, sym) {
		if rs.Def && !p.Context.IncludeDeclaration {
			continue
		}
		out = append(out, location{URI: d.uri, Range: d.rng(rs.Line, rs.Col, rs.Line, rs.EndCol)})
	}
	return out, nil
}

func (s *server) documentHighlight(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	r := d.analysis()
	sym := symbolAt(r, line, col)
	if sym == nil {
		return []any{}, nil
	}
	var out []map[string]any
	for _, rs := range related(r, sym) {
		kind := 2 // Read
		if rs.Def {
			kind = 3 // Write
		}
		out = append(out, map[string]any{"range": d.rng(rs.Line, rs.Col, rs.Line, rs.EndCol), "kind": kind})
	}
	return out, nil
}

// renamable reports whether sym can be renamed: frames always, and
// columns the script defines (not columns read from a file).
func renamable(r *analysis.Result, sym *analysis.Symbol) bool {
	if sym.Kind == analysis.SymFrame {
		return defOf(sym) != nil
	}
	def := defOf(sym)
	if def == nil {
		return false
	}
	for _, s := range r.Symbols {
		if s.Def && s.Loc == *def && s.Name == sym.Name {
			return true
		}
	}
	return false
}

func (s *server) prepareRename(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	r := d.analysis()
	sym := symbolAt(r, line, col)
	if sym == nil || !renamable(r, sym) {
		return nil, errors.New("only frames and columns defined in this script can be renamed")
	}
	return map[string]any{"range": d.rng(sym.Line, sym.Col, sym.Line, sym.EndCol), "placeholder": sym.Name}, nil
}

func (s *server) rename(params json.RawMessage) (any, error) {
	d, line, col, err := s.at(params)
	if err != nil {
		return nil, err
	}
	var p struct {
		NewName string `json:"newName"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	if !validName(p.NewName) {
		return nil, fmt.Errorf("%q is not a valid name: use letters, digits and _", p.NewName)
	}
	r := d.analysis()
	sym := symbolAt(r, line, col)
	if sym == nil || !renamable(r, sym) {
		return nil, errors.New("only frames and columns defined in this script can be renamed")
	}
	var edits []textEdit
	for _, rs := range related(r, sym) {
		edits = append(edits, textEdit{Range: d.rng(rs.Line, rs.Col, rs.Line, rs.EndCol), NewText: p.NewName})
	}
	return map[string]any{"changes": map[string][]textEdit{d.uri: edits}}, nil
}

func validName(s string) bool {
	if s == "" || s[0] >= '0' && s[0] <= '9' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '_' && !(c >= '0' && c <= '9') && !(c|0x20 >= 'a' && c|0x20 <= 'z') && c < 0x80 {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------
// document symbols and folding
// -----------------------------------------------------------------

type documentSymbol struct {
	Name           string           `json:"name"`
	Detail         string           `json:"detail,omitempty"`
	Kind           int              `json:"kind"`
	Range          lspRange         `json:"range"`
	SelectionRange lspRange         `json:"selectionRange"`
	Children       []documentSymbol `json:"children,omitempty"`
}

func (s *server) documentSymbol(params json.RawMessage) (any, error) {
	d, err := s.doc(params)
	if err != nil {
		return nil, err
	}
	r := d.analysis()
	out := []documentSymbol{}
	var section *documentSymbol
	flush := func() {
		if section != nil {
			out = append(out, *section)
			section = nil
		}
	}
	for _, step := range r.Steps {
		st := step.Stmt
		full := d.rng(st.Line, 0, st.EndLine, len(d.line(st.EndLine)))
		name := st.Name()
		startsSection := (name == "load" || strings.HasPrefix(name, "scan_")) && stagedTarget(st) == "" || name == "use"
		if startsSection {
			flush()
			title := strings.TrimSpace(st.Text)
			section = &documentSymbol{Name: title, Detail: step.After.Shape(), Kind: 2, Range: full,
				SelectionRange: d.rng(st.Line, st.Cmd.Off, st.Line, st.Cmd.End)}
			continue
		}
		for _, p := range st.Parts {
			var sym documentSymbol
			switch p.Kind {
			case syntax.PartNewFrame:
				sym = documentSymbol{Name: p.Value, Detail: name, Kind: 5}
			case syntax.PartNewColumn:
				sym = documentSymbol{Name: p.Value, Detail: step.After.DType(p.Value), Kind: 8}
			default:
				continue
			}
			l, c := st.Position(p.Off)
			_, ec := st.Position(p.End)
			sym.Range = full
			sym.SelectionRange = d.rng(l, c, l, ec)
			if p.Kind == syntax.PartNewFrame {
				flush()
				out = append(out, sym)
				continue
			}
			if section != nil {
				section.Range.End = full.End
				section.Children = append(section.Children, sym)
			} else {
				out = append(out, sym)
			}
		}
		if section != nil {
			section.Range.End = full.End
		}
	}
	flush()
	return out, nil
}

func stagedTarget(st *syntax.Stmt) string {
	for _, p := range st.Parts {
		if p.Kind == syntax.PartNewFrame {
			return p.Value
		}
	}
	return ""
}

func (s *server) foldingRange(params json.RawMessage) (any, error) {
	d, err := s.doc(params)
	if err != nil {
		return nil, err
	}
	r := d.analysis()
	out := []map[string]any{}
	add := func(start, end int, kind string) {
		if end > start {
			m := map[string]any{"startLine": start, "endLine": end}
			if kind != "" {
				m["kind"] = kind
			}
			out = append(out, m)
		}
	}
	// Runs of whole-line comments.
	for i := 0; i < len(r.File.Comments); {
		j := i
		for j+1 < len(r.File.Comments) && r.File.Comments[j+1].Own && r.File.Comments[j+1].Line == r.File.Comments[j].Line+1 {
			j++
		}
		if r.File.Comments[i].Own {
			add(r.File.Comments[i].Line, r.File.Comments[j].Line, "comment")
		}
		i = j + 1
	}
	// Continued statements, and sections from each load or use to
	// the statement before the next one.
	start := -1
	last := -1
	for _, st := range r.File.Stmts {
		add(st.Line, st.EndLine, "")
		name := st.Name()
		if (name == "load" || strings.HasPrefix(name, "scan_")) && stagedTarget(st) == "" || name == "use" {
			if start >= 0 {
				add(start, last, "region")
			}
			start = st.Line
		}
		last = st.EndLine
	}
	if start >= 0 {
		add(start, last, "region")
	}
	return out, nil
}

// -----------------------------------------------------------------
// formatting and code actions
// -----------------------------------------------------------------

func (s *server) formatting(params json.RawMessage) (any, error) {
	d, err := s.doc(params)
	if err != nil {
		return nil, err
	}
	out := syntax.Format(d.content)
	if out == d.content {
		return []textEdit{}, nil
	}
	end := len(d.lines) - 1
	return []textEdit{{Range: d.rng(0, 0, end, len(d.line(end))), NewText: strings.TrimSuffix(out, "\n") + trailingNL(d.content)}}, nil
}

// trailingNL keeps the document's final newline state for the edit,
// which replaces everything up to the end of the last line.
func trailingNL(content string) string {
	if strings.HasSuffix(content, "\n") {
		return "\n"
	}
	return ""
}

func (s *server) codeAction(params json.RawMessage) (any, error) {
	var p struct {
		TextDocument textDocumentIdentifier `json:"textDocument"`
		Range        lspRange               `json:"range"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	d := s.docs.get(p.TextDocument.URI)
	if d == nil {
		return nil, errNoDocument
	}
	out := []map[string]any{}
	for _, dg := range d.analysis().Diags {
		if dg.Fix == nil || dg.EndLine < p.Range.Start.Line || dg.Line > p.Range.End.Line {
			continue
		}
		var edit textEdit
		if dg.Fix.Insert {
			edit = textEdit{Range: d.rng(dg.Fix.Line, 0, dg.Fix.Line, 0), NewText: dg.Fix.Text + "\n"}
		} else {
			edit = textEdit{Range: d.rng(dg.Line, dg.Col, dg.EndLine, dg.EndCol), NewText: dg.Fix.Text}
		}
		out = append(out, map[string]any{
			"title":       dg.Fix.Title,
			"kind":        "quickfix",
			"diagnostics": []diagnostic{toDiagnostic(d, dg)},
			"isPreferred": true,
			"edit":        map[string]any{"changes": map[string][]textEdit{d.uri: {edit}}},
		})
	}
	return out, nil
}

// -----------------------------------------------------------------
// inlay hints
// -----------------------------------------------------------------

type inlayHint struct {
	Position    position       `json:"position"`
	Label       string         `json:"label"`
	Kind        int            `json:"kind,omitempty"`
	PaddingLeft bool           `json:"paddingLeft,omitempty"`
	Tooltip     *markupContent `json:"tooltip,omitempty"`
}

func (s *server) inlayHint(params json.RawMessage) (any, error) {
	var p struct {
		TextDocument textDocumentIdentifier `json:"textDocument"`
		Range        lspRange               `json:"range"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	d := s.docs.get(p.TextDocument.URI)
	if d == nil {
		return nil, errNoDocument
	}
	return collectInlayHints(d, p.Range), nil
}

func collectInlayHints(d *document, rg lspRange) []inlayHint {
	out := []inlayHint{}
	if rg.End.Line < rg.Start.Line {
		return out
	}
	r := d.analysis()
	inRange := func(line int) bool { return line >= rg.Start.Line && line <= rg.End.Line && line < len(d.lines) }
	for _, step := range r.Steps {
		st := step.Stmt
		if !step.Changes || st.Spec == nil || !inRange(st.EndLine) || len(st.Diags) > 0 {
			continue
		}
		if st.Spec.Category == "inspect" || st.Spec.Category == "aggregate" || stagedTarget(st) != "" {
			continue
		}
		end, _ := syntaxLineEnd(d, st.EndLine)
		h := inlayHint{Position: d.pos(st.EndLine, end), Label: shapeLabel(step.After), Kind: 1, PaddingLeft: true}
		if step.After.Known() {
			h.Tooltip = &markupContent{Kind: "markdown", Value: frameTable(step.After)}
		}
		out = append(out, h)
	}
	for _, c := range r.File.Comments {
		if !isProbe(c.Text) || !inRange(c.Line) {
			continue
		}
		label := probeLabel(r, c.Line)
		out = append(out, inlayHint{Position: d.pos(c.Line, len(d.line(c.Line))), Label: label, PaddingLeft: true,
			Tooltip: &markupContent{Kind: "plaintext", Value: label}})
	}
	return out
}

// syntaxLineEnd returns the byte column where code ends on a line
// (before a trailing comment and whitespace).
func syntaxLineEnd(d *document, line int) (int, bool) {
	s := d.line(line)
	code := s
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
			code = s[:i]
			i = len(s)
		}
	}
	return len(strings.TrimRight(code, " \t\\")), len(code) != len(s)
}

func isProbe(comment string) bool {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "#")) == "^?"
}

// shapeLabel renders "→ 5 rows × 3 cols", with "≤" for upper bounds
// and "?" for unknown parts.
func shapeLabel(f analysis.Frame) string {
	rows := "? rows"
	switch {
	case f.Rows == 1 && f.Exact:
		rows = "1 row"
	case f.Rows >= 0:
		rows = fmtInt(f.Rows) + " rows"
		if !f.Exact {
			rows = "≤" + rows
		}
	}
	cols := "? cols"
	if f.Known() {
		cols = fmtInt(f.Schema.Len()) + " cols"
	}
	return "→ " + rows + " × " + cols
}

func probeLabel(r *analysis.Result, line int) string {
	f, has, _ := r.StateAt(line)
	if !has {
		return "no frame loaded"
	}
	cols := "?"
	if f.Known() {
		var parts []string
		for _, fl := range f.Schema.Fields() {
			parts = append(parts, fl.Name+": "+fl.DType.String())
		}
		cols = strings.Join(parts, ", ")
	}
	return "cols(" + cols + ") " + strings.TrimPrefix(shapeLabel(f), "→ ")
}

func probeText(r *analysis.Result, line int) string {
	f, has, _ := r.StateAt(line)
	if !has {
		return "No frame is loaded here."
	}
	return "**Frame here**: " + strings.TrimPrefix(shapeLabel(f), "→ ") + "\n\n" + frameTable(f)
}

func frameTable(f analysis.Frame) string {
	if !f.Known() {
		return "columns unknown until it runs"
	}
	var b strings.Builder
	b.WriteString("| column | dtype |\n|---|---|\n")
	for _, fl := range f.Schema.Fields() {
		fmt.Fprintf(&b, "| %s | %s |\n", fl.Name, fl.DType)
	}
	return strings.TrimRight(b.String(), "\n")
}

// fmtInt formats n with thousands separators.
func fmtInt(n int) string {
	s := strconv.Itoa(n)
	if n < 1000 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
