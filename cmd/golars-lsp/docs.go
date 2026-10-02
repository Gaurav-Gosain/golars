package main

import (
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Gaurav-Gosain/golars/script/analysis"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// document is an open text document and its cached analysis.
type document struct {
	uri     string
	content string
	lines   []string
	version int32
	// prev is the notebook cell before this one, nil outside a
	// notebook. Cells share one kernel session, so a cell's analysis
	// starts from the state prev ends in.
	prev *document

	once   sync.Once
	result *analysis.Result
}

// analysis returns the document's analysis, computed on first use.
func (d *document) analysis() *analysis.Result {
	d.once.Do(func() {
		opts := analysis.Options{Dir: docDir(d.uri)}
		if d.prev != nil {
			focus, ok, frames := d.prev.analysis().StateAt(math.MaxInt)
			opts.Frames = make(map[string]analysis.Frame, len(frames))
			for name, f := range frames {
				opts.Frames[name] = withoutOrigins(f)
			}
			if ok {
				focus = withoutOrigins(focus)
				opts.Focus = &focus
			}
		}
		d.result = analysis.Analyze(d.content, opts)
	})
	return d.result
}

// withoutOrigins drops column origins, which are line numbers in the
// cell that defined them and would point into the wrong cell.
func withoutOrigins(f analysis.Frame) analysis.Frame {
	f.Origins = nil
	return f
}

// docDir is the directory of a file:// URI or of a notebook cell's
// notebook, "" otherwise.
func docDir(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || (u.Scheme != "file" && u.Scheme != notebookCellScheme) {
		return ""
	}
	return filepath.Dir(filepath.FromSlash(u.Path))
}

// notebookCellScheme is the URI scheme VS Code gives notebook cells:
// vscode-notebook-cell:/path/to/nb.ipynb#CELLID.
const notebookCellScheme = "vscode-notebook-cell"

// notebookOf returns the notebook a cell URI belongs to, or "" for a
// URI that is not a notebook cell.
func notebookOf(uri string) string {
	if !strings.HasPrefix(uri, notebookCellScheme+":") {
		return ""
	}
	nb, _, _ := strings.Cut(uri, "#")
	return nb
}

type docStore struct {
	mu sync.RWMutex
	m  map[string]*document
	// cells lists the open cells of each notebook in didOpen order,
	// which is the notebook order when the notebook first opens.
	cells map[string][]string
}

func newDocStore() *docStore {
	return &docStore{m: make(map[string]*document), cells: make(map[string][]string)}
}

func (s *docStore) set(uri, content string, version int32) *document {
	doc := &document{uri: uri, content: content, lines: strings.Split(content, "\n"), version: version}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[uri] = doc
	nb := notebookOf(uri)
	if nb == "" {
		return doc
	}
	cells := s.cells[nb]
	i := slices.Index(cells, uri)
	if i < 0 {
		cells = append(cells, uri)
		s.cells[nb] = cells
		i = len(cells) - 1
	}
	if i > 0 {
		doc.prev = s.m[cells[i-1]]
	}
	s.relink(cells[i+1:], doc)
	return doc
}

// relink replaces each cell in uris with a fresh copy chained after
// prev, so cached analyses that depend on a changed cell recompute.
// The caller holds s.mu.
func (s *docStore) relink(uris []string, prev *document) {
	for _, u := range uris {
		old := s.m[u]
		next := &document{uri: old.uri, content: old.content, lines: old.lines, version: old.version, prev: prev}
		s.m[u] = next
		prev = next
	}
}

// following returns the notebook cells after uri, whose analyses
// depend on it. It is empty outside a notebook.
func (s *docStore) following(uri string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cells := s.cells[notebookOf(uri)]
	i := slices.Index(cells, uri)
	if i < 0 {
		return nil
	}
	return slices.Clone(cells[i+1:])
}

func (s *docStore) get(uri string) *document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[uri]
}

func (s *docStore) drop(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, uri)
	nb := notebookOf(uri)
	cells := s.cells[nb]
	i := slices.Index(cells, uri)
	if i < 0 {
		return
	}
	cells = slices.Delete(cells, i, i+1)
	if len(cells) == 0 {
		delete(s.cells, nb)
		return
	}
	s.cells[nb] = cells
	var prev *document
	if i > 0 {
		prev = s.m[cells[i-1]]
	}
	s.relink(cells[i:], prev)
}

// -----------------------------------------------------------------
// Positions. LSP columns count UTF-16 code units; the analysis uses
// byte offsets into the line.
// -----------------------------------------------------------------

type position struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

type lspRange struct {
	Start position `json:"start"`
	End   position `json:"end"`
}

type location struct {
	URI   string   `json:"uri"`
	Range lspRange `json:"range"`
}

func (d *document) line(i int) string {
	if i < 0 || i >= len(d.lines) {
		return ""
	}
	return strings.TrimSuffix(d.lines[i], "\r")
}

// byteCol converts a UTF-16 column on line i to a byte offset.
func (d *document) byteCol(i, utf16Col int) int {
	s := d.line(i)
	units := 0
	for off, r := range s {
		if units >= utf16Col {
			return off
		}
		units++
		if r >= 0x10000 {
			units++
		}
	}
	return len(s)
}

// utf16Col converts a byte offset on line i to a UTF-16 column.
func (d *document) utf16Col(i, byteCol int) int {
	s := d.line(i)
	byteCol = min(max(byteCol, 0), len(s))
	units := 0
	for _, r := range s[:byteCol] {
		units++
		if r >= 0x10000 {
			units++
		}
	}
	return units
}

func (d *document) pos(line, byteCol int) position {
	return position{Line: line, Character: d.utf16Col(line, byteCol)}
}

func (d *document) rng(line, col, endLine, endCol int) lspRange {
	return lspRange{Start: d.pos(line, col), End: d.pos(endLine, endCol)}
}

// -----------------------------------------------------------------
// Params shared by the position requests.
// -----------------------------------------------------------------

type textDocumentIdentifier struct {
	URI string `json:"uri"`
}

type positionParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
	Position     position               `json:"position"`
}

var errNoDocument = errors.New("document is not open")

// at decodes position params and returns the document with the byte
// position.
func (s *server) at(params json.RawMessage) (*document, int, int, error) {
	var p positionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, 0, 0, err
	}
	d := s.docs.get(p.TextDocument.URI)
	if d == nil {
		return nil, 0, 0, errNoDocument
	}
	line := p.Position.Line
	return d, line, d.byteCol(line, p.Position.Character), nil
}

func (s *server) doc(params json.RawMessage) (*document, error) {
	var p struct {
		TextDocument textDocumentIdentifier `json:"textDocument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	d := s.docs.get(p.TextDocument.URI)
	if d == nil {
		return nil, errNoDocument
	}
	return d, nil
}

// -----------------------------------------------------------------
// Lifecycle and diagnostics.
// -----------------------------------------------------------------

func (s *server) handleDidOpen(msg *rawMessage) {
	var p struct {
		TextDocument struct {
			URI     string `json:"uri"`
			Version int32  `json:"version"`
			Text    string `json:"text"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.logf("didOpen: %v", err)
		return
	}
	d := s.docs.set(p.TextDocument.URI, p.TextDocument.Text, p.TextDocument.Version)
	s.publishDiagnostics(d)
}

func (s *server) handleDidChange(msg *rawMessage) {
	var p struct {
		TextDocument struct {
			URI     string `json:"uri"`
			Version int32  `json:"version"`
		} `json:"textDocument"`
		ContentChanges []struct {
			Text string `json:"text"`
		} `json:"contentChanges"`
	}
	if err := json.Unmarshal(msg.Params, &p); err != nil || len(p.ContentChanges) == 0 {
		return
	}
	d := s.docs.set(p.TextDocument.URI, p.ContentChanges[len(p.ContentChanges)-1].Text, p.TextDocument.Version)
	s.schedule(d.uri)
	for _, u := range s.docs.following(d.uri) {
		s.schedule(u)
	}
}

func (s *server) handleDidSave(msg *rawMessage) {
	d, err := s.doc(msg.Params)
	if err != nil {
		return
	}
	// Data files next to the script may have changed; drop the cached
	// analysis and publish at once.
	d = s.docs.set(d.uri, d.content, d.version)
	s.cancel(d.uri)
	s.publishDiagnostics(d)
}

func (s *server) handleDidClose(msg *rawMessage) {
	d, err := s.doc(msg.Params)
	if err != nil {
		return
	}
	s.cancel(d.uri)
	following := s.docs.following(d.uri)
	s.docs.drop(d.uri)
	s.notify("textDocument/publishDiagnostics", map[string]any{"uri": d.uri, "diagnostics": []any{}})
	for _, u := range following {
		s.schedule(u)
	}
}

// schedule publishes diagnostics for uri after the debounce pause,
// restarting the pause on every change.
func (s *server) schedule(uri string) {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	if t := s.timers[uri]; t != nil {
		t.Stop()
	}
	if s.debounce == 0 {
		delete(s.timers, uri)
		if d := s.docs.get(uri); d != nil {
			go s.publishDiagnostics(d)
		}
		return
	}
	s.timers[uri] = time.AfterFunc(s.debounce, func() {
		if d := s.docs.get(uri); d != nil {
			s.publishDiagnostics(d)
		}
	})
}

func (s *server) cancel(uri string) {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	if t := s.timers[uri]; t != nil {
		t.Stop()
		delete(s.timers, uri)
	}
}

type diagnostic struct {
	Range    lspRange `json:"range"`
	Severity int      `json:"severity"`
	Code     string   `json:"code,omitempty"`
	Source   string   `json:"source"`
	Message  string   `json:"message"`
}

func (s *server) publishDiagnostics(d *document) {
	r := d.analysis()
	cell := notebookOf(d.uri) != ""
	diags := make([]diagnostic, 0, len(r.Diags))
	for _, dg := range r.Diags {
		// A later cell may use a stash, so a cell cannot know it is
		// unused.
		if cell && dg.Code == "unused-stash" {
			continue
		}
		diags = append(diags, toDiagnostic(d, dg))
	}
	s.notify("textDocument/publishDiagnostics", map[string]any{
		"uri": d.uri, "version": d.version, "diagnostics": diags,
	})
}

func toDiagnostic(d *document, dg syntax.Diag) diagnostic {
	msg := dg.Msg
	if dg.Hint != "" {
		msg += " (" + dg.Hint + ")"
	}
	rg := d.rng(dg.Line, dg.Col, dg.EndLine, dg.EndCol)
	if rg.Start == rg.End {
		// Give zero-width spans one character so editors show them.
		if rg.End.Character < utf8.RuneCountInString(d.line(dg.Line)) {
			rg.End.Character++
		} else if rg.Start.Character > 0 {
			rg.Start.Character--
		}
	}
	return diagnostic{Range: rg, Severity: int(dg.Severity), Code: dg.Code, Source: "golars", Message: msg}
}
