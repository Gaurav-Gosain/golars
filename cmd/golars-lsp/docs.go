package main

import (
	"encoding/json"
	"strings"
	"sync"
)

// docStore is an in-memory registry of open documents keyed by LSP
// URI. Content is kept verbatim (full text sync) so we don't have to
// deal with incremental-edit bookkeeping for this tiny language.
//
// notebooks tracks cell ordering for VSCode-style notebook URIs
// (`vscode-notebook-cell:/path/to/foo.ipynb#cellID`). Each notebook
// path maps to its cell URIs in didOpen order so inlay-hint and
// schema inference can replay prior cells before the current one.
type docStore struct {
	mu        sync.RWMutex
	m         map[string]*document
	notebooks map[string][]string
}

type document struct {
	uri     string
	content string
	lines   []string // split once, used by every feature
	version int32
}

func newDocStore() *docStore {
	return &docStore{
		m:         make(map[string]*document),
		notebooks: make(map[string][]string),
	}
}

func (s *docStore) set(uri, content string, version int32) *document {
	doc := &document{
		uri:     uri,
		content: content,
		lines:   strings.Split(content, "\n"),
		version: version,
	}
	s.mu.Lock()
	if _, existed := s.m[uri]; !existed {
		if nb := notebookOf(uri); nb != "" {
			s.notebooks[nb] = append(s.notebooks[nb], uri)
		}
	}
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
	if nb := notebookOf(uri); nb != "" {
		cells := s.notebooks[nb]
		for i, c := range cells {
			if c == uri {
				s.notebooks[nb] = append(cells[:i], cells[i+1:]...)
				break
			}
		}
		if len(s.notebooks[nb]) == 0 {
			delete(s.notebooks, nb)
		}
	}
	s.mu.Unlock()
}

// priorCells returns the contents of every cell in the same notebook
// that was opened before uri, in didOpen order. Empty when uri is not
// a notebook cell or when uri is the first cell.
func (s *docStore) priorCells(uri string) []string {
	nb := notebookOf(uri)
	if nb == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.notebooks[nb]))
	for _, cellURI := range s.notebooks[nb] {
		if cellURI == uri {
			break
		}
		if d := s.m[cellURI]; d != nil {
			out = append(out, d.content)
		}
	}
	return out
}

// notebookOf returns the notebook path for a `vscode-notebook-cell:`
// URI, or "" for plain file URIs. The fragment (cell id) is stripped
// so all cells of the same notebook share a key.
func notebookOf(uri string) string {
	if !strings.HasPrefix(uri, "vscode-notebook-cell:") {
		return ""
	}
	base, _, _ := strings.Cut(uri, "#")
	return base
}

// --------------------------------------------------------------------
// didOpen / didChange / didClose handlers
// --------------------------------------------------------------------

type textDocumentItem struct {
	URI        string `json:"uri"`
	LanguageID string `json:"languageId"`
	Version    int32  `json:"version"`
	Text       string `json:"text"`
}

type didOpenParams struct {
	TextDocument textDocumentItem `json:"textDocument"`
}

func (s *server) handleDidOpen(msg *rawMessage) {
	var p didOpenParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.logf("didOpen unmarshal: %v", err)
		return
	}
	doc := s.docs.set(p.TextDocument.URI, p.TextDocument.Text, p.TextDocument.Version)
	s.publishDiagnostics(doc)
}

type versionedDocumentID struct {
	URI     string `json:"uri"`
	Version int32  `json:"version"`
}

type contentChange struct {
	Text string `json:"text"` // full-sync: the entire new document
}

type didChangeParams struct {
	TextDocument   versionedDocumentID `json:"textDocument"`
	ContentChanges []contentChange     `json:"contentChanges"`
}

func (s *server) handleDidChange(msg *rawMessage) {
	var p didChangeParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.logf("didChange unmarshal: %v", err)
		return
	}
	if len(p.ContentChanges) == 0 {
		return
	}
	// Full-sync: last change holds the full new text.
	doc := s.docs.set(p.TextDocument.URI, p.ContentChanges[len(p.ContentChanges)-1].Text, p.TextDocument.Version)
	s.publishDiagnostics(doc)
}

type textDocumentIdentifier struct {
	URI string `json:"uri"`
}

type didCloseParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
}

func (s *server) handleDidClose(msg *rawMessage) {
	var p didCloseParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		return
	}
	s.docs.drop(p.TextDocument.URI)
	// Clear diagnostics on close so stale markers don't linger.
	s.notify("textDocument/publishDiagnostics", struct {
		URI         string `json:"uri"`
		Diagnostics []any  `json:"diagnostics"`
	}{p.TextDocument.URI, []any{}})
}
