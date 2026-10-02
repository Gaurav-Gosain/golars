package main

import (
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
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

	once   sync.Once
	result *analysis.Result
}

// analysis returns the document's analysis, computed on first use.
func (d *document) analysis() *analysis.Result {
	d.once.Do(func() {
		d.result = analysis.Analyze(d.content, analysis.Options{Dir: docDir(d.uri)})
	})
	return d.result
}

// docDir is the directory of a file:// URI, "" otherwise.
func docDir(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "file" {
		return ""
	}
	return filepath.Dir(filepath.FromSlash(u.Path))
}

type docStore struct {
	mu sync.RWMutex
	m  map[string]*document
}

func newDocStore() *docStore { return &docStore{m: make(map[string]*document)} }

func (s *docStore) set(uri, content string, version int32) *document {
	doc := &document{uri: uri, content: content, lines: strings.Split(content, "\n"), version: version}
	s.mu.Lock()
	s.m[uri] = doc
	s.mu.Unlock()
	return doc
}

func (s *docStore) get(uri string) *document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[uri]
}

func (s *docStore) drop(uri string) {
	s.mu.Lock()
	delete(s.m, uri)
	s.mu.Unlock()
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
	s.docs.drop(d.uri)
	s.notify("textDocument/publishDiagnostics", map[string]any{"uri": d.uri, "diagnostics": []any{}})
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
	diags := make([]diagnostic, 0, len(r.Diags))
	for _, dg := range r.Diags {
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
