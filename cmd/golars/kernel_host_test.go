package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The kernel-host speaks NDJSON: one {id, code} request per line in,
// one response per line out, with state carried across requests.
func TestKernelHostProtocol(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.csv")
	if err := os.WriteFile(path, []byte(peopleCSV), 0o644); err != nil {
		t.Fatal(err)
	}
	reqs := []string{
		mustJSON(t, kernelRequest{ID: "1", Code: "load " + path}),
		"",
		"{not json",
		mustJSON(t, kernelRequest{ID: "2", Code: "filter age > 30"}),
		mustJSON(t, kernelRequest{ID: "3", Code: "head 1"}),
		mustJSON(t, kernelRequest{ID: "4", Code: "nope"}),
		mustJSON(t, kernelRequest{ID: "5", Code: "exit"}),
	}
	var out bytes.Buffer
	if err := runKernelHost(&out, strings.NewReader(strings.Join(reqs, "\n")+"\n")); err != nil {
		t.Fatal(err)
	}
	var resps []kernelResponse
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var r kernelResponse
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("bad response line %q: %v", line, err)
		}
		resps = append(resps, r)
	}
	if len(resps) != 6 {
		t.Fatalf("got %d responses, want 6 (blank lines are skipped): %+v", len(resps), resps)
	}
	if r := resps[0]; r.ID != "1" || r.Error != "" || r.Shape == nil || r.Shape[0] != 5 || r.HTML == "" {
		t.Errorf("load: %+v", r)
	}
	if r := resps[1]; r.ID != "" || !strings.HasPrefix(r.Error, "invalid request") {
		t.Errorf("bad json: %+v", r)
	}
	if r := resps[2]; r.Error != "" || r.Shape == nil || r.Shape[0] != 2 {
		t.Errorf("filter should carry the loaded frame forward: %+v", r)
	}
	if r := resps[3]; r.HTML != "" || !strings.Contains(r.Text, "1 row shown") {
		t.Errorf("head prints a table and skips the HTML display: %+v", r)
	}
	if r := resps[4]; !strings.Contains(r.Error, `unknown command "nope"`) {
		t.Errorf("unknown command: %+v", r)
	}
	if r := resps[5]; r.Error != "" {
		t.Errorf("exit is a no-op in a notebook: %+v", r)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
