package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// complete and inspect requests answer from the live session, with the
// same analysis golars-lsp uses.
func TestKernelHostCompleteInspect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.csv")
	if err := os.WriteFile(path, []byte(peopleCSV), 0o644); err != nil {
		t.Fatal(err)
	}
	cell := "filter ag"
	reqs := []string{
		mustJSON(t, kernelRequest{ID: "1", Code: "load " + path + "\nwith decade = age // 10"}),
		mustJSON(t, kernelRequest{ID: "2", Op: "complete", Code: cell, Cursor: len(cell)}),
		mustJSON(t, kernelRequest{ID: "3", Op: "complete", Code: "with x = dec", Cursor: len("with x = dec")}),
		mustJSON(t, kernelRequest{ID: "4", Op: "inspect", Code: "with x = round(age, 1)", Cursor: 11}),
		mustJSON(t, kernelRequest{ID: "5", Op: "inspect", Code: "filter age > 1", Cursor: 8}),
	}
	var out bytes.Buffer
	if err := runKernelHost(&out, strings.NewReader(strings.Join(reqs, "\n")+"\n")); err != nil {
		t.Fatal(err)
	}
	var resps []kernelResponse
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var r kernelResponse
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		resps = append(resps, r)
	}
	if len(resps) != 5 {
		t.Fatalf("got %d responses", len(resps))
	}
	if c := resps[1].IDE; c == nil || !slices.Contains(c.Matches, "age") || c.Start != 7 || c.End != 9 {
		t.Errorf("complete filter ag: %+v", c)
	}
	if c := resps[2].IDE; c == nil || !slices.Contains(c.Matches, "decade") {
		t.Errorf("complete derived column: %+v", c)
	}
	if md := resps[3].IDE.Markdown; !strings.Contains(md, "round(x, decimals: int = 0)") {
		t.Errorf("inspect round: %q", md)
	}
	if md := resps[4].IDE.Markdown; !strings.Contains(md, "**age** `i64`") {
		t.Errorf("inspect age: %q", md)
	}
}
