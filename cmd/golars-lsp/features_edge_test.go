package main

import (
	"encoding/json"
	"testing"
)

func TestLSPCompletionWithCRLF(t *testing.T) {
	d := newDocStore().set("untitled:test", "select alpha,beta\r\nselect al\r\n", 1)
	s := newServer(nil, nil, nil)
	var cols []string
	for _, it := range s.completionsFor(d, "select al", 1) {
		if it.Kind == ciKindVariable {
			cols = append(cols, it.Label)
		}
	}
	if len(cols) != 1 || cols[0] != "alpha" {
		t.Fatalf("column completion = %v", cols)
	}
}

func TestLSPCompletionInvalidPosition(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("textDocument/completion", 1, json.RawMessage(`{"position":{"line":-1,"character":0}}`))
	runServer(p)
	reply := p.readFrame(t)
	items, ok := reply["result"].([]any)
	if !ok || len(items) != 0 {
		t.Fatalf("want empty completion result, got %v", reply)
	}
}

// Inside expressions the LSP offers namespace functions, methods and
// top-level functions.
func TestLSPExpressionCompletion(t *testing.T) {
	d := newDocStore().set("untitled:test", "select ts\r\n", 1)
	s := newServer(nil, nil, nil)
	labels := func(prefix string) map[string]bool {
		out := map[string]bool{}
		for _, it := range s.completionsFor(d, prefix, 1) {
			out[it.Label] = true
		}
		return out
	}
	if got := labels("with y = dt.tr"); !got["truncate"] || got["year"] {
		t.Errorf("dt.tr: %v", got)
	}
	if got := labels("filter ts.dt.ye"); !got["year"] {
		t.Errorf("ts.dt.ye: %v", got)
	}
	if got := labels("with y = x.rolling_m"); !got["rolling_mean"] || !got["rolling_mean_by"] {
		t.Errorf("x.rolling_m: %v", got)
	}
	if got := labels("with y = x.s"); !got["str"] || !got["struct"] || !got["shift"] {
		t.Errorf("x.s: %v", got)
	}
	if got := labels("with y = coal"); !got["coalesce"] {
		t.Errorf("coal: %v", got)
	}
}
