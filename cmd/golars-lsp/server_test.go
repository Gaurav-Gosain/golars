package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fileURI turns a local filesystem path into a `file:///` URI that
// the LSP's url.Parse understands on every OS. Windows paths use
// backslashes and need `file:///C:/...` form; POSIX paths are
// already absolute and produce `file:///tmp/...`. Keeps per-test
// setup free of platform branching.
func fileURI(p string) string {
	slashed := filepath.ToSlash(p)
	if strings.HasPrefix(slashed, "/") {
		return "file://" + slashed
	}
	return "file:///" + slashed
}

// framedPipe is a duplex io.Reader/Writer used to drive the server
// end-to-end in-process. Requests written by the test land on the
// server's stdin; responses written by the server land in a buffer
// the test reads back with readFrame.
type framedPipe struct {
	toServer   *bytes.Buffer
	fromServer *bytes.Buffer
	mu         sync.Mutex
}

func newFramedPipe() *framedPipe {
	return &framedPipe{
		toServer:   &bytes.Buffer{},
		fromServer: &bytes.Buffer{},
	}
}

// writeFrame feeds one LSP message into the server's stdin.
func (p *framedPipe) writeFrame(method string, id any, params any) {
	body := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
	}
	if id != nil {
		body["id"] = id
	}
	if params != nil {
		body["params"] = params
	}
	raw, _ := json.Marshal(body)
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.toServer, "Content-Length: %d\r\n\r\n", len(raw))
	p.toServer.Write(raw)
}

// readFrame decodes the next Content-Length framed message.
// Returns (nil, io.EOF) when the server output is drained.
func (p *framedPipe) readFrame(t *testing.T) map[string]any {
	t.Helper()
	r := p.fromServer
	// Parse headers
	var line string
	var contentLen int
	for {
		b, err := r.ReadBytes('\n')
		if err != nil {
			return nil
		}
		line = strings.TrimRight(string(b), "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			if strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
				fmt.Sscanf(strings.TrimSpace(v), "%d", &contentLen)
			}
		}
	}
	buf := make([]byte, contentLen)
	io.ReadFull(r, buf)
	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		t.Fatalf("unmarshal frame: %v; raw=%q", err, string(buf))
	}
	return out
}

// runServer drives the server against the pipe. Stops when the pipe
// reader returns EOF (i.e. all queued requests consumed).
func runServer(p *framedPipe) {
	srv := newServer(p.toServer, p.fromServer, io.Discard)
	srv.Run() // #nosec: errors surface as test failures via readFrame
}

func TestLSPInitialize(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	runServer(p)

	reply := p.readFrame(t)
	if reply["id"] != float64(1) {
		t.Fatalf("id=%v, want 1", reply["id"])
	}
	res, ok := reply["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result: %v", reply)
	}
	caps := res["capabilities"].(map[string]any)
	if caps["hoverProvider"] != true {
		t.Fatal("hoverProvider should be true")
	}
	if caps["textDocumentSync"] != float64(1) {
		t.Fatalf("textDocumentSync=%v, want 1", caps["textDocumentSync"])
	}
}

func TestLSPCompletionAtLineStart(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri":        "file:///tmp/test.glr",
			"languageId": "glr",
			"version":    1,
			"text":       "lo",
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/test.glr"},
		"position":     map[string]any{"line": 0, "character": 2},
	})
	runServer(p)

	// Drain initialize reply + didOpen diagnostics notification.
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	sawLoad := false
	for _, it := range items {
		m := it.(map[string]any)
		if m["label"] == "load" {
			sawLoad = true
			break
		}
	}
	if !sawLoad {
		t.Fatalf("completion missing `load` entry: %v", items)
	}
}

func TestLSPHoverOnCommand(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri":        "file:///tmp/t.glr",
			"languageId": "glr",
			"version":    1,
			"text":       "groupby region amount:sum\n",
		},
	})
	p.writeFrame("textDocument/hover", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/t.glr"},
		"position":     map[string]any{"line": 0, "character": 3},
	})
	runServer(p)
	for range 2 { // init reply + diagnostics notif
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	res, ok := reply["result"].(map[string]any)
	if !ok {
		t.Fatalf("hover result missing: %v", reply)
	}
	contents := res["contents"].(map[string]any)
	val := contents["value"].(string)
	if !strings.Contains(val, "groupby") {
		t.Fatalf("hover body missing command name: %q", val)
	}
}

func TestLSPDefinitionOnFrameName(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri":        "file:///tmp/t.glr",
			"languageId": "glr",
			"version":    1,
			"text":       "load foo.csv as trades\nuse trades\n",
		},
	})
	// Position cursor on `trades` in the `use` line (line 1, col 4).
	p.writeFrame("textDocument/definition", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/t.glr"},
		"position":     map[string]any{"line": 1, "character": 5},
	})
	runServer(p)
	for range 2 { // init reply + diagnostics notif
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	arr, ok := reply["result"].([]any)
	if !ok || len(arr) == 0 {
		t.Fatalf("definition result missing: %v", reply)
	}
	first := arr[0].(map[string]any)
	rng := first["range"].(map[string]any)
	startLine := int(rng["start"].(map[string]any)["line"].(float64))
	if startLine != 0 {
		t.Errorf("definition line = %d, want 0", startLine)
	}
}

func TestLSPDiagnosticsUnknownCommand(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri":        "file:///tmp/bad.glr",
			"languageId": "glr",
			"version":    1,
			"text":       "pizza toppings\n",
		},
	})
	runServer(p)
	// init reply
	p.readFrame(t)
	// diagnostics notification
	n := p.readFrame(t)
	params := n["params"].(map[string]any)
	diags := params["diagnostics"].([]any)
	if len(diags) != 1 {
		t.Fatalf("want 1 diagnostic, got %d", len(diags))
	}
	d := diags[0].(map[string]any)
	if !strings.Contains(d["message"].(string), "unknown command") {
		t.Fatalf("diagnostic message: %v", d)
	}
}

// CSV column headers from a loaded file flow through to filter /
// select / sort completion. We drop a tiny CSV on disk, reference
// it via `load`, and ask for completions on a subsequent `filter`.
func TestLSPColumnCompletionFromCSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "people.csv")
	if err := os.WriteFile(csvPath, []byte("name,age,region\nada,27,EU\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(dir, "x.glr"))
	text := "load people.csv\nfilter "

	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"position":     map[string]any{"line": 1, "character": 7},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["label"].(string)] = true
	}
	for _, want := range []string{"name", "age", "region"} {
		if !seen[want] {
			t.Fatalf("column %q missing from completion: %v", want, items)
		}
	}
}

// Walk-up path resolution: a script in a subdirectory referencing a
// file by a repo-root-relative path (not script-dir-relative) still
// resolves. This is the multisource.glr pattern.
func TestLSPColumnCompletionResolvesViaAncestor(t *testing.T) {
	root := t.TempDir()
	scriptDir := root + "/examples/script"
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(scriptDir+"/data.csv", []byte("alpha,beta\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(scriptDir, "foo.glr"))
	text := "load examples/script/data.csv\nfilter "

	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"position":     map[string]any{"line": 1, "character": 7},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["label"].(string)] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("walk-up resolve failed: %v", items)
	}
}

// Post-`use NAME` focus swap: columns offered on subsequent lines
// come from the promoted frame, not whatever was focused before.
func TestLSPColumnCompletionFollowsUseFocus(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/a.csv", []byte("alpha,beta\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/b.csv", []byte("gamma,delta\n3,4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(dir, "m.glr"))
	text := "load a.csv as a\nload b.csv as b\nuse b\nfilter "

	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"position":     map[string]any{"line": 3, "character": 7},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["label"].(string)] = true
	}
	if !seen["gamma"] || !seen["delta"] {
		t.Fatalf("columns of `b` missing: %v", items)
	}
	if seen["alpha"] || seen["beta"] {
		t.Fatalf("columns of `a` should NOT appear after `use b`: %v", items)
	}
}

// Command completion kind is Keyword: prevents Neovim from
// auto-inserting parens. Regression test for the "filter becomes a
// function call" report.
func TestLSPCommandCompletionKindIsKeyword(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/k.glr", "languageId": "glr", "version": 1, "text": "fi",
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/k.glr"},
		"position":     map[string]any{"line": 0, "character": 2},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	const kindKeyword = float64(14) // LSP CompletionItemKind.Keyword
	for _, it := range items {
		m := it.(map[string]any)
		if m["label"] == "filter" {
			if m["kind"] != kindKeyword {
				t.Fatalf("filter kind = %v, want Keyword (%v)", m["kind"], kindKeyword)
			}
			return
		}
	}
	t.Fatal("completion list missing `filter`")
}

// Full-pipeline shape tracking: every shape-transforming statement
// gets a hint; limit/head/tail bound the row count symbolically while
// filter / inner-join / groupby mark rows unknown.
func TestLSPInlayHintsThroughFullPipeline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/p.csv", []byte("name,age\nada,27\nben,31\ncam,42\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/s.csv", []byte("name,amount\nada,10\nben,20\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(dir, "x.glr"))
	text := `load p.csv as people
load s.csv as salaries
use people
filter age > 25
sort age desc
join salaries on name
limit 2
`
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/inlayHint", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"range": map[string]any{
			"start": map[string]any{"line": 0, "character": 0},
			"end":   map[string]any{"line": 20, "character": 0},
		},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	hints := reply["result"].([]any)
	byLine := make(map[int]string, len(hints))
	for _, h := range hints {
		m := h.(map[string]any)
		ln := int(m["position"].(map[string]any)["line"].(float64))
		byLine[ln] = m["label"].(string)
	}
	// 0: load people as     → 3 rows × 2 cols (staged)
	// 1: load salaries as   → 2 rows × 2 cols (staged)
	// 2: use people         → 3 rows × 2 cols
	// 3: filter             → ? rows × 2 cols
	// 4: sort               → ? rows × 2 cols
	// 5: join on name       → ? rows × 3 cols (2+2−key)
	// 6: limit 2            → 2 rows × 3 cols
	want := map[int][]string{
		0: {"3 rows", "2 cols"},
		1: {"2 rows", "2 cols"},
		2: {"3 rows", "2 cols"},
		3: {"? rows", "2 cols"},
		4: {"? rows", "2 cols"},
		5: {"? rows", "3 cols"},
		6: {"2 rows", "3 cols"},
	}
	for ln, needles := range want {
		got, ok := byLine[ln]
		if !ok {
			t.Fatalf("no hint for line %d; got %v", ln, byLine)
		}
		for _, needle := range needles {
			if !strings.Contains(got, needle) {
				t.Fatalf("line %d label %q missing %q", ln, got, needle)
			}
		}
	}
}

// Probe directive: a `# ^?` comment emits a dense schema peek.
func TestLSPInlayHintProbe(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/p.csv", []byte("name,age,city\nada,1,NYC\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(dir, "x.glr"))
	text := "load p.csv\n# ^?\n"

	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/inlayHint", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"range": map[string]any{
			"start": map[string]any{"line": 0, "character": 0},
			"end":   map[string]any{"line": 5, "character": 0},
		},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	hints := reply["result"].([]any)
	var probe string
	for _, h := range hints {
		m := h.(map[string]any)
		ln := int(m["position"].(map[string]any)["line"].(float64))
		if ln == 1 {
			probe = m["label"].(string)
		}
	}
	if probe == "" {
		t.Fatalf("probe hint missing on line 1: %v", hints)
	}
	for _, col := range []string{"name", "age", "city"} {
		if !strings.Contains(probe, col) {
			t.Fatalf("probe missing column %q: %q", col, probe)
		}
	}
}

// Inlay hints: a `load` line gets a trailing annotation showing the
// source file's shape (rows × cols) read from disk.
func TestLSPInlayHintsForLoad(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "d.csv")
	if err := os.WriteFile(csvPath, []byte("a,b,c\n1,2,3\n4,5,6\n7,8,9\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docURI := fileURI(filepath.Join(dir, "x.glr"))
	text := "load d.csv\n"

	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": docURI, "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/inlayHint", 2, map[string]any{
		"textDocument": map[string]any{"uri": docURI},
		"range": map[string]any{
			"start": map[string]any{"line": 0, "character": 0},
			"end":   map[string]any{"line": 5, "character": 0},
		},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	hints := reply["result"].([]any)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d: %v", len(hints), hints)
	}
	h := hints[0].(map[string]any)
	label := h["label"].(string)
	// 3 data rows × 3 cols.
	if !strings.Contains(label, "3 rows") || !strings.Contains(label, "3 cols") {
		t.Fatalf("hint label = %q, expected it to mention rows and cols", label)
	}
	pos := h["position"].(map[string]any)
	if pos["line"].(float64) != 0 {
		t.Fatalf("hint line = %v, want 0", pos["line"])
	}
}

// Position-aware arg completion for `join`: after the key, suggest
// inner/left/cross. Regression for the user report that
// "join X on Y " wasn't offering join-type keywords.
func TestLSPJoinKeywordsAppearAfterKey(t *testing.T) {
	p := newFramedPipe()
	// Two staged frames so `join people on name ` is well-formed up
	// to the cursor and we're asking for the 4th argument.
	text := "load a.csv as people\nload b.csv as salaries\njoin people on name "
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/j.glr", "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/j.glr"},
		"position":     map[string]any{"line": 2, "character": len("join people on name ")},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["label"].(string)] = true
	}
	for _, want := range []string{"inner", "left", "cross"} {
		if !seen[want] {
			t.Fatalf("join-type keyword %q missing: %v", want, items)
		}
	}
	// And must NOT contain column names or frame names at this position.
	if seen["people"] || seen["salaries"] {
		t.Fatal("frame names should not appear at arg index 4 of join")
	}
}

// Position-aware arg completion for `load`: after PATH, suggest `as`.
func TestLSPLoadKeywordAsAfterPath(t *testing.T) {
	p := newFramedPipe()
	text := "load data/x.csv "
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/l.glr", "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/l.glr"},
		"position":     map[string]any{"line": 0, "character": len(text)},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected single `as` completion, got %v", items)
	}
	if label := items[0].(map[string]any)["label"]; label != "as" {
		t.Fatalf("arg 2 of load: got %v, want `as`", label)
	}
}

func TestLSPFrameCompletionFromEarlierLoad(t *testing.T) {
	p := newFramedPipe()
	text := "load data/trades.csv as trades\nload data/users.csv as users\nuse "
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/m.glr", "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/m.glr"},
		"position":     map[string]any{"line": 2, "character": 4},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	want := map[string]bool{"trades": false, "users": false}
	for _, it := range items {
		m := it.(map[string]any)
		lbl := m["label"].(string)
		if _, ok := want[lbl]; ok {
			want[lbl] = true
		}
	}
	for n, ok := range want {
		if !ok {
			t.Fatalf("frame completion missing %q: %v", n, items)
		}
	}
}

// TestLSPNewCommandsInCompletion pins that every command added to
// script/spec.go is reachable from the LSP completion list at line
// start. Prevents silent drift when a new command lands in the spec
// without wiring into cmd/golars-lsp.
func TestLSPNewCommandsInCompletion(t *testing.T) {
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/new.glr", "languageId": "glr", "version": 1, "text": "",
		},
	})
	p.writeFrame("textDocument/completion", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/new.glr"},
		"position":     map[string]any{"line": 0, "character": 0},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	items := reply["result"].([]any)
	// Every command in this list must be completable. Add new
	// commands here when script/spec.go grows.
	want := map[string]bool{
		"explain_tree": false, "tree": false,
		"scan_csv": false, "scan_parquet": false, "scan_ipc": false,
		"scan_json": false, "scan_ndjson": false, "scan_auto": false,
		"unnest": false, "explode": false, "upsample": false,
	}
	for _, it := range items {
		m := it.(map[string]any)
		if _, ok := want[m["label"].(string)]; ok {
			want[m["label"].(string)] = true
		}
	}
	for n, ok := range want {
		if !ok {
			t.Errorf("completion missing command %q", n)
		}
	}
}

// TestLSPInlayHintsAfterScan covers the shape-tracking case for
// scan_csv. The LSP should carry schema from the scan into downstream
// filter/select statements, matching behaviour for eager load.
func TestLSPInlayHintsAfterScan(t *testing.T) {
	dir := t.TempDir()
	csv := filepath.Join(dir, "s.csv")
	if err := os.WriteFile(csv, []byte("id,name,amount\n1,a,10\n2,b,20\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	text := "scan_csv " + csv + "\nfilter amount > 10\n"
	p := newFramedPipe()
	p.writeFrame("initialize", 1, map[string]any{})
	p.writeFrame("textDocument/didOpen", nil, map[string]any{
		"textDocument": map[string]any{
			"uri": "file:///tmp/scan.glr", "languageId": "glr", "version": 1, "text": text,
		},
	})
	p.writeFrame("textDocument/inlayHint", 2, map[string]any{
		"textDocument": map[string]any{"uri": "file:///tmp/scan.glr"},
		"range": map[string]any{
			"start": map[string]any{"line": 0, "character": 0},
			"end":   map[string]any{"line": 2, "character": 0},
		},
	})
	runServer(p)
	for range 2 {
		p.readFrame(t)
	}
	reply := p.readFrame(t)
	result, ok := reply["result"].([]any)
	if !ok || len(result) == 0 {
		t.Fatalf("no inlay hints returned for scan_csv pipeline: %v", reply)
	}
	// At least one hint should report the 3-column shape produced
	// by the CSV scan so the user can see the schema propagated.
	found := false
	for _, h := range result {
		m := h.(map[string]any)
		if lbl, ok := m["label"].(string); ok && strings.Contains(lbl, "cols") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a col-count hint after scan_csv, got: %v", result)
	}
}
