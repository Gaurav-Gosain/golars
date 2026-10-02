package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/Gaurav-Gosain/golars/script/analysis"
)

// ideReply mirrors cmd/golars' reply to complete and inspect requests.
type ideReply struct {
	Matches  []string  `json:"matches"`
	Start    int       `json:"start"`
	End      int       `json:"end"`
	Types    []ideType `json:"types"`
	Markdown string    `json:"markdown"`
}

type ideType struct {
	Text      string `json:"text"`
	Type      string `json:"type"`
	Signature string `json:"signature,omitempty"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
}

// IDE sends a complete or inspect request for code at byte offset
// cursor to the host, which answers from the live session.
func (h *kernelHost) IDE(op, code string, cursor int) (*ideReply, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd == nil {
		return nil, fmt.Errorf("kernel-host not started")
	}
	line, err := json.Marshal(map[string]any{"id": uuid.NewString(), "op": op, "code": code, "cursor": cursor})
	if err != nil {
		return nil, err
	}
	if _, err := h.in.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	respLine, err := h.out.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var resp struct {
		IDE   *ideReply `json:"ide"`
		Error string    `json:"error"`
	}
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return nil, err
	}
	if resp.IDE == nil {
		return nil, fmt.Errorf("host: %s", resp.Error)
	}
	return resp.IDE, nil
}

// ide answers op for code at byte offset cursor: from the host when it
// runs, otherwise from a static analysis of the cell alone.
func (k *kernel) ide(op, code string, cursor int) ideReply {
	if k.host != nil {
		if r, err := k.host.IDE(op, code, cursor); err == nil {
			return *r
		}
	}
	return staticIDE(op, code, cursor)
}

func staticIDE(op, code string, cursor int) ideReply {
	cursor = min(max(cursor, 0), len(code))
	line := strings.Count(code[:cursor], "\n")
	col := cursor - (strings.LastIndex(code[:cursor], "\n") + 1)
	r := analysis.Analyze(code, analysis.Options{})
	if op == "inspect" {
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
	c := r.Complete(line, col)
	start := cursor - (col - c.From)
	out := ideReply{Start: start, End: cursor, Matches: []string{}}
	for _, it := range c.Items {
		text := it.Label
		if it.Kind == analysis.ItemParam {
			text = it.Insert
		}
		out.Matches = append(out.Matches, text)
		out.Types = append(out.Types, ideType{Text: text, Type: "text", Signature: it.Detail, Start: start, End: cursor})
	}
	return out
}

// runeToByte converts a Jupyter cursor_pos (Unicode code points) to a
// byte offset in s.
func runeToByte(s string, pos int) int {
	pos = max(pos, 0)
	for i := range s {
		if pos == 0 {
			return i
		}
		pos--
	}
	return len(s)
}

// byteToRune converts a byte offset in s to code points.
func byteToRune(s string, off int) int {
	return utf8.RuneCountInString(s[:min(max(off, 0), len(s))])
}
