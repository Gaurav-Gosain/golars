package main

import (
	"encoding/json"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/script/analysis"
)

// ideReply answers a kernel-host `complete` or `inspect` request. The
// kernel forwards Jupyter's complete_request and inspect_request here
// so notebooks get the same completions and docs as golars-lsp, with
// columns from the live session.
type ideReply struct {
	// Matches are the completion texts; Start and End are the byte
	// offsets in the cell they replace.
	Matches []string `json:"matches,omitempty"`
	Start   int      `json:"start"`
	End     int      `json:"end"`
	// Types are Jupyter completion types, parallel to Matches.
	Types []ideType `json:"types,omitempty"`
	// Markdown is the inspect text, "" when nothing is under the
	// cursor.
	Markdown string `json:"markdown,omitempty"`
}

type ideType struct {
	Text      string `json:"text"`
	Type      string `json:"type"`
	Signature string `json:"signature,omitempty"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
}

// cellPosition converts a byte offset in a cell to a line and column.
func cellPosition(code string, cursor int) (line, col int) {
	cursor = min(max(cursor, 0), len(code))
	line = strings.Count(code[:cursor], "\n")
	col = cursor - (strings.LastIndex(code[:cursor], "\n") + 1)
	return line, col
}

var jupyterTypes = map[analysis.ItemKind]string{
	analysis.ItemCommand: "keyword", analysis.ItemKeyword: "keyword", analysis.ItemColumn: "property",
	analysis.ItemFrame: "class", analysis.ItemFile: "path", analysis.ItemFolder: "path",
	analysis.ItemFunction: "function", analysis.ItemMethod: "function", analysis.ItemNamespace: "module",
	analysis.ItemParam: "param", analysis.ItemValue: "text",
}

// ideComplete completes code at the byte offset cursor against the
// session state.
func (s *state) ideComplete(code string, cursor int) ideReply {
	line, col := cellPosition(code, cursor)
	r := analysis.Analyze(code, s.analysisOptions())
	c := r.Complete(line, col)
	start := cursor - (col - c.From)
	out := ideReply{Start: start, End: cursor, Matches: []string{}}
	for _, it := range c.Items {
		text := it.Label
		if it.Kind == analysis.ItemParam {
			text = it.Insert
		}
		out.Matches = append(out.Matches, text)
		out.Types = append(out.Types, ideType{Text: text, Type: jupyterTypes[it.Kind], Signature: it.Detail, Start: start, End: cursor})
	}
	return out
}

// ideInspect documents the word at the byte offset cursor.
func (s *state) ideInspect(code string, cursor int) ideReply {
	line, col := cellPosition(code, cursor)
	r := analysis.Analyze(code, s.analysisOptions())
	md := r.Hover(line, col)
	if md == "" && col > 0 {
		md = r.Hover(line, col-1)
	}
	if md == "" {
		if sig, found := r.SignatureAt(line, col); found {
			md = sig.Info.Markdown()
		}
	}
	return ideReply{Markdown: md}
}

// focusTable returns the first rows of the focused frame with every
// column as the structured table JSON. Shape is the shape of the rows
// returned; a height equal to the limit means there may be more.
func focusTable(s *state, req kernelRequest) kernelResponse {
	resp := kernelResponse{ID: req.ID}
	if !s.hasFocus() {
		resp.Error = errNoFrame.Error()
		return resp
	}
	n := req.MaxRows
	if n <= 0 {
		n = 50
	}
	df, err := s.currentLazy().Limit(n).Collect(s.ctx)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	defer df.Release()
	b := df.MimeBundleWith(dataframe.FormatOptions{MaxRows: n, MaxCols: 10000, MaxCellRune: 256})
	resp.Table = json.RawMessage(b[dataframe.TableMIME])
	h, w := df.Shape()
	resp.Shape = &[2]int{h, w}
	return resp
}
