package main

import (
	"encoding/json"
	"testing"
)

func TestLSPCompletionWithCRLF(t *testing.T) {
	d := newDocStore().set("untitled:test", "select alpha,beta\r\nselect al\r\n", 1)
	s := newServer(nil, nil, nil)
	items := s.completionsFor(d, "select al", 1)
	if len(items) != 1 || items[0].Label != "alpha" {
		t.Fatalf("completion = %v", items)
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
