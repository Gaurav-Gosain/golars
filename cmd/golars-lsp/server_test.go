package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// client drives a server over real LSP framing on in-memory pipes.
type client struct {
	t      *testing.T
	w      io.WriteCloser
	nextID int
	mu     sync.Mutex
	resp   map[int]chan json.RawMessage
	notes  chan map[string]any
	done   chan error
}

func newClient(t *testing.T, debounceMs int) *client {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &client{t: t, w: inW, resp: map[int]chan json.RawMessage{}, notes: make(chan map[string]any, 64), done: make(chan error, 1)}
	srv := newServer(inR, outW, io.Discard)
	go func() {
		err := srv.Run()
		outW.Close()
		c.done <- err
	}()
	go c.readLoop(bufio.NewReader(outR))
	t.Cleanup(func() {
		c.w.Close()
		select {
		case <-c.done:
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
	})
	c.request("initialize", map[string]any{"initializationOptions": map[string]any{"debounceMs": debounceMs}})
	c.notify("initialized", map[string]any{})
	return c
}

func (c *client) readLoop(r *bufio.Reader) {
	for {
		n := 0
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				close(c.notes)
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				break
			}
			if v, found := strings.CutPrefix(line, "Content-Length: "); found {
				n, _ = strconv.Atoi(v)
			}
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			close(c.notes)
			return
		}
		var msg struct {
			ID     *int            `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  *rpcError       `json:"error"`
			Params map[string]any  `json:"params"`
		}
		if err := json.Unmarshal(body, &msg); err != nil {
			continue
		}
		if msg.ID != nil {
			c.mu.Lock()
			ch := c.resp[*msg.ID]
			c.mu.Unlock()
			if msg.Error != nil {
				ch <- json.RawMessage(`{"__error":` + strconv.Quote(msg.Error.Message) + `}`)
			} else {
				ch <- msg.Result
			}
			continue
		}
		c.notes <- map[string]any{"method": msg.Method, "params": msg.Params}
	}
}

func (c *client) send(v any) {
	body, _ := json.Marshal(v)
	fmt.Fprintf(c.w, "Content-Length: %d\r\n\r\n%s", len(body), body)
}

func (c *client) notify(method string, params any) {
	c.send(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

// request sends a request and decodes its result into a generic value.
func (c *client) request(method string, params any) any {
	c.t.Helper()
	raw := c.requestRaw(method, params)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

func (c *client) requestRaw(method string, params any) json.RawMessage {
	c.t.Helper()
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan json.RawMessage, 1)
	c.resp[id] = ch
	c.mu.Unlock()
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		c.t.Fatalf("%s: no response", method)
		return nil
	}
}

// diagnostics waits for the next publishDiagnostics for uri.
func (c *client) diagnostics(uri string) []any {
	c.t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case n, open := <-c.notes:
			if !open {
				c.t.Fatal("server closed")
			}
			if n["method"] != "textDocument/publishDiagnostics" {
				continue
			}
			p := n["params"].(map[string]any)
			if p["uri"] == uri {
				return p["diagnostics"].([]any)
			}
		case <-timeout:
			c.t.Fatal("no diagnostics published")
			return nil
		}
	}
}

func fileURI(p string) string {
	slashed := filepath.ToSlash(p)
	if strings.HasPrefix(slashed, "/") {
		return "file://" + slashed
	}
	return "file:///" + slashed
}

// openDoc writes src to a temp dir with the example data and opens it.
func (c *client) open(src string) string {
	c.t.Helper()
	dir := c.t.TempDir()
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		c.t.Fatal(err)
	}
	for _, name := range []string{"salaries.csv", "people.csv", "events.csv"} {
		b, err := os.ReadFile(filepath.Join("../../examples/script/data", name))
		if err != nil {
			c.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(data, name), b, 0o644); err != nil {
			c.t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "s.glr")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		c.t.Fatal(err)
	}
	uri := fileURI(path)
	c.notify("textDocument/didOpen", map[string]any{"textDocument": map[string]any{
		"uri": uri, "languageId": "glr", "version": 1, "text": src}})
	return uri
}

func pos(uri string, line, char int) map[string]any {
	return map[string]any{"textDocument": map[string]any{"uri": uri}, "position": map[string]any{"line": line, "character": char}}
}

func labels(v any) []string {
	var out []string
	m, _ := v.(map[string]any)
	items, _ := m["items"].([]any)
	for _, it := range items {
		out = append(out, it.(map[string]any)["label"].(string))
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

const script = `load data/salaries.csv
with monthly = round(amount / 12, 1)
filter monthly > 5000 and name.str.starts_with("a")
stash rich
use rich
select name, monthly
# ^?
`

func TestLSPInitializeCapabilities(t *testing.T) {
	c := newClient(t, 0)
	res := c.request("initialize", map[string]any{}).(map[string]any)
	caps := res["capabilities"].(map[string]any)
	for _, k := range []string{"completionProvider", "hoverProvider", "signatureHelpProvider", "definitionProvider",
		"referencesProvider", "renameProvider", "documentSymbolProvider", "foldingRangeProvider",
		"semanticTokensProvider", "documentFormattingProvider", "codeActionProvider", "inlayHintProvider"} {
		if caps[k] == nil {
			t.Errorf("missing capability %s", k)
		}
	}
}

func TestLSPDiagnosticsOnOpenAndChange(t *testing.T) {
	c := newClient(t, 30)
	uri := c.open(script)
	if d := c.diagnostics(uri); len(d) != 0 {
		t.Fatalf("clean script has diagnostics: %v", d)
	}
	// Two quick changes: only the last one is analysed (debounce).
	for v, text := range []string{"load data/salaries.csv\nfilter amout > 1\n", "load data/salaries.csv\nfilter amout > 1\nfiltet x\n"} {
		c.notify("textDocument/didChange", map[string]any{
			"textDocument":   map[string]any{"uri": uri, "version": v + 2},
			"contentChanges": []any{map[string]any{"text": text}},
		})
	}
	d := c.diagnostics(uri)
	if len(d) != 2 {
		t.Fatalf("want 2 diagnostics, got %v", d)
	}
	first := d[0].(map[string]any)
	if !strings.Contains(first["message"].(string), `unknown column "amout" (did you mean "amount"?)`) {
		t.Errorf("message %q", first["message"])
	}
	rg := first["range"].(map[string]any)
	start := rg["start"].(map[string]any)
	end := rg["end"].(map[string]any)
	if start["line"] != 1.0 || start["character"] != 7.0 || end["character"] != 12.0 {
		t.Errorf("range %v", rg)
	}
}

func TestLSPCompletion(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open(script + "filter mo\nwith x = name.str.to_up\nwith y = cut(amount, [1], la\nload data/sal\n")
	c.diagnostics(uri)
	for _, tc := range []struct {
		line, char int
		want       string
	}{
		{0, 2, "load"},
		{7, 9, "monthly"},
		{8, 23, "to_uppercase"},
		{9, 28, "labels="},
		{10, 13, "data/salaries.csv"},
		{4, 4, "rich"},
	} {
		got := labels(c.request("textDocument/completion", pos(uri, tc.line, tc.char)))
		if !contains(got, tc.want) {
			t.Errorf("%d:%d: %q not in %v", tc.line, tc.char, tc.want, got)
		}
	}
	// Function items carry snippets.
	res := c.request("textDocument/completion", pos(uri, 8, 23)).(map[string]any)
	for _, it := range res["items"].([]any) {
		m := it.(map[string]any)
		if m["label"] == "to_uppercase" {
			if m["insertTextFormat"] != 2.0 || !strings.Contains(m["textEdit"].(map[string]any)["newText"].(string), "to_uppercase(") {
				t.Errorf("item %v", m)
			}
		}
	}
}

func TestLSPHoverAndSignature(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open(script + "with z = round(amount, \n")
	c.diagnostics(uri)
	hover := func(line, char int) string {
		res := c.request("textDocument/hover", pos(uri, line, char))
		if res == nil {
			return ""
		}
		return res.(map[string]any)["contents"].(map[string]any)["value"].(string)
	}
	for _, tc := range []struct {
		line, char int
		want       string
	}{
		{0, 1, "load <path>"},
		{1, 16, "round(x, decimals: int = 0)"},
		{1, 22, "**amount** `i64`"},
		{2, 8, "**monthly** `f64`"},
		{2, 40, "starts_with"},
		{4, 5, "frame rich"},
		{6, 2, "| monthly | f64 |"},
	} {
		if got := hover(tc.line, tc.char); !strings.Contains(got, tc.want) {
			t.Errorf("hover %d:%d = %q, want %q", tc.line, tc.char, got, tc.want)
		}
	}
	sig := c.request("textDocument/signatureHelp", pos(uri, 7, 23)).(map[string]any)
	s0 := sig["signatures"].([]any)[0].(map[string]any)
	if s0["label"] != "round(x, decimals: int = 0)" || sig["activeParameter"] != 1.0 {
		t.Errorf("signature %v", sig)
	}
}

func TestLSPNavigation(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open(script)
	c.diagnostics(uri)
	def := c.request("textDocument/definition", pos(uri, 2, 9)).(map[string]any)
	if r := def["range"].(map[string]any)["start"].(map[string]any); r["line"] != 1.0 || r["character"] != 5.0 {
		t.Errorf("definition of monthly: %v", def)
	}
	def = c.request("textDocument/definition", pos(uri, 4, 5)).(map[string]any)
	if r := def["range"].(map[string]any)["start"].(map[string]any); r["line"] != 3.0 {
		t.Errorf("definition of rich: %v", def)
	}
	refs := c.request("textDocument/references", map[string]any{
		"textDocument": map[string]any{"uri": uri}, "position": map[string]any{"line": 1, "character": 6},
		"context": map[string]any{"includeDeclaration": true}}).([]any)
	if len(refs) != 3 {
		t.Errorf("references of monthly: %v", refs)
	}
	edit := c.request("textDocument/rename", map[string]any{
		"textDocument": map[string]any{"uri": uri}, "position": map[string]any{"line": 1, "character": 6},
		"newName": "per_month"}).(map[string]any)
	if n := len(edit["changes"].(map[string]any)[uri].([]any)); n != 3 {
		t.Errorf("rename edits: %v", edit)
	}
	// Columns read from a file cannot be renamed.
	raw := c.requestRaw("textDocument/prepareRename", pos(uri, 1, 22))
	if !strings.Contains(string(raw), "__error") {
		t.Errorf("prepareRename on a file column: %s", raw)
	}
}

func TestLSPSymbolsFoldingTokens(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open("# header\n# more\n" + script)
	c.diagnostics(uri)
	doc := map[string]any{"textDocument": map[string]any{"uri": uri}}
	syms := c.request("textDocument/documentSymbol", doc).([]any)
	var names []string
	var walk func([]any)
	walk = func(list []any) {
		for _, s := range list {
			m := s.(map[string]any)
			names = append(names, m["name"].(string))
			if ch, ok := m["children"].([]any); ok {
				walk(ch)
			}
		}
	}
	walk(syms)
	for _, want := range []string{"monthly", "rich", "use rich"} {
		if !contains(names, want) {
			t.Errorf("symbols %v missing %q", names, want)
		}
	}
	folds := c.request("textDocument/foldingRange", doc).([]any)
	if len(folds) < 2 {
		t.Errorf("folding ranges %v", folds)
	}
	toks := c.request("textDocument/semanticTokens/full", doc).(map[string]any)["data"].([]any)
	if len(toks) == 0 || len(toks)%5 != 0 {
		t.Fatalf("semantic tokens %v", toks)
	}
	// Decode and look for the round function on line 3 (0-based).
	line, col := 0, 0
	found := false
	for i := 0; i < len(toks); i += 5 {
		dl, dc := int(toks[i].(float64)), int(toks[i+1].(float64))
		if dl > 0 {
			line, col = line+dl, dc
		} else {
			col += dc
		}
		if line == 3 && col == 15 && semanticTypes[int(toks[i+3].(float64))] == "function" {
			found = true
		}
	}
	if !found {
		t.Error("round is not a function token")
	}
}

func TestLSPFormattingAndCodeActions(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open("LOAD data/salaries.csv\nfilter amout>1\n")
	c.diagnostics(uri)
	doc := map[string]any{"textDocument": map[string]any{"uri": uri}, "options": map[string]any{"tabSize": 2, "insertSpaces": true}}
	edits := c.request("textDocument/formatting", doc).([]any)
	if len(edits) != 1 || edits[0].(map[string]any)["newText"] != "load data/salaries.csv\nfilter amout > 1\n" {
		t.Errorf("formatting: %v", edits)
	}
	acts := c.request("textDocument/codeAction", map[string]any{
		"textDocument": map[string]any{"uri": uri},
		"range":        map[string]any{"start": map[string]any{"line": 1, "character": 0}, "end": map[string]any{"line": 1, "character": 0}},
		"context":      map[string]any{"diagnostics": []any{}},
	}).([]any)
	if len(acts) != 1 || acts[0].(map[string]any)["title"] != "Change to amount" {
		t.Fatalf("code actions: %v", acts)
	}
	uri2 := c.open("filter x > 1\n")
	c.diagnostics(uri2)
	acts = c.request("textDocument/codeAction", map[string]any{
		"textDocument": map[string]any{"uri": uri2},
		"range":        map[string]any{"start": map[string]any{"line": 0, "character": 0}, "end": map[string]any{"line": 0, "character": 0}},
	}).([]any)
	if len(acts) != 1 || !strings.HasPrefix(acts[0].(map[string]any)["title"].(string), "Add `load data/") {
		t.Fatalf("missing-load action: %v", acts)
	}
}

func TestLSPInlayHints(t *testing.T) {
	c := newClient(t, 0)
	uri := c.open(script)
	c.diagnostics(uri)
	hints := c.request("textDocument/inlayHint", map[string]any{"textDocument": map[string]any{"uri": uri},
		"range": map[string]any{"start": map[string]any{"line": 0, "character": 0}, "end": map[string]any{"line": 7, "character": 0}}}).([]any)
	var got []string
	for _, h := range hints {
		m := h.(map[string]any)
		got = append(got, fmt.Sprintf("%v:%s", m["position"].(map[string]any)["line"], m["label"]))
	}
	want := []string{"0:→ 5 rows × 2 cols", "1:→ 5 rows × 3 cols", "2:→ ≤5 rows × 3 cols", "4:→ ≤5 rows × 3 cols",
		"5:→ ≤5 rows × 2 cols", "6:cols(name: str, monthly: f64) ≤5 rows × 2 cols"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("hints\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestLSPNotebookCells checks that a VS Code notebook cell sees the
// frames of the cells before it, and that editing or closing a cell
// updates the cells after it.
func TestLSPNotebookCells(t *testing.T) {
	c := newClient(t, 0)
	dir, err := filepath.Abs("../../examples/script")
	if err != nil {
		t.Fatal(err)
	}
	nb := "vscode-notebook-cell:" + strings.TrimPrefix(fileURI(filepath.Join(dir, "nb.ipynb")), "file://")
	cellA, cellB := nb+"#A", nb+"#B"
	openCell := func(uri, src string) {
		c.notify("textDocument/didOpen", map[string]any{"textDocument": map[string]any{
			"uri": uri, "languageId": "golars", "version": 1, "text": src}})
	}
	// published waits for diagnostics on every uri. Cells publish
	// from separate goroutines, so they can arrive in any order.
	published := func(uris ...string) map[string][]string {
		out := map[string][]string{}
		timeout := time.After(5 * time.Second)
		for len(out) < len(uris) {
			select {
			case n := <-c.notes:
				if n["method"] != "textDocument/publishDiagnostics" {
					continue
				}
				p := n["params"].(map[string]any)
				uri := p["uri"].(string)
				if !slices.Contains(uris, uri) {
					continue
				}
				msgs := []string{}
				for _, d := range p["diagnostics"].([]any) {
					msgs = append(msgs, d.(map[string]any)["message"].(string))
				}
				out[uri] = msgs
			case <-timeout:
				t.Fatalf("diagnostics for %v, want %v", out, uris)
			}
		}
		return out
	}
	messages := func(uri string) []string { return published(uri)[uri] }

	// The stash is used by a later cell, so cell A has no unused-stash
	// warning, and cell B resolves both the frame and its columns.
	openCell(cellA, "load data/salaries.csv\nstash base\n")
	if m := messages(cellA); len(m) != 0 {
		t.Fatalf("cell A diagnostics: %v", m)
	}
	openCell(cellB, "use base\nselect name, amount\n# ^?\n")
	if m := messages(cellB); len(m) != 0 {
		t.Fatalf("cell B diagnostics: %v", m)
	}
	hints := c.request("textDocument/inlayHint", map[string]any{"textDocument": map[string]any{"uri": cellB},
		"range": map[string]any{"start": map[string]any{"line": 0, "character": 0}, "end": map[string]any{"line": 3, "character": 0}}}).([]any)
	var probe string
	for _, h := range hints {
		if l := h.(map[string]any)["label"].(string); strings.HasPrefix(l, "cols(") {
			probe = l
		}
	}
	if !strings.HasPrefix(probe, "cols(name: str, amount: i64)") {
		t.Errorf("probe %q", probe)
	}

	// Cell A now loads a frame without amount: cell B is analysed again.
	c.notify("textDocument/didChange", map[string]any{
		"textDocument":   map[string]any{"uri": cellA, "version": 2},
		"contentChanges": []any{map[string]any{"text": "load data/people.csv\nstash base\n"}},
	})
	got := published(cellA, cellB)
	if m := got[cellA]; len(m) != 0 {
		t.Fatalf("cell A diagnostics after change: %v", m)
	}
	if m := got[cellB]; len(m) != 1 || !strings.Contains(m[0], `unknown column "amount"`) {
		t.Fatalf("cell B diagnostics after change: %v", m)
	}

	// Closing cell A leaves cell B with no frame named base.
	c.notify("textDocument/didClose", map[string]any{"textDocument": map[string]any{"uri": cellA}})
	got = published(cellA, cellB)
	if m := got[cellA]; len(m) != 0 {
		t.Fatalf("closed cell A diagnostics: %v", m)
	}
	if m := got[cellB]; len(m) == 0 || !strings.Contains(m[0], `no frame named "base"`) {
		t.Fatalf("cell B diagnostics after close: %v", m)
	}
}

func TestLSPUnknownMethodAndFraming(t *testing.T) {
	c := newClient(t, 0)
	raw := c.requestRaw("workspace/unknownThing", map[string]any{})
	if !strings.Contains(string(raw), "Method not found") {
		t.Errorf("got %s", raw)
	}
	for _, input := range []string{
		"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
		fmt.Sprintf("Content-Length: %d\r\n\r\n", maxMessageBytes+1),
		strings.Repeat("X", maxHeaderBytes+1),
		"Content-Length: 2\r\n\r\n",
	} {
		s := newServer(strings.NewReader(input), io.Discard, io.Discard)
		if err := s.Run(); err == nil {
			t.Errorf("expected framing error for %.30q", input)
		}
	}
	s := newServer(strings.NewReader("Content-Length: 58\r\n\r\n"+`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`), failedWriter{}, io.Discard)
	if err := s.Run(); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("got %v, want closed pipe", err)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// Completion and diagnostics on a 500-line script stay well inside
// an interactive budget. Measured through the protocol.
func TestLSPLatency500Lines(t *testing.T) {
	var b strings.Builder
	b.WriteString("load data/salaries.csv\n")
	for i := 1; i < 500; i++ {
		switch i % 4 {
		case 0:
			fmt.Fprintf(&b, "with c%d = amount * %d + 1\n", i, i)
		case 1:
			fmt.Fprintf(&b, "filter amount > %d and name.str.len_chars() > 0\n", i)
		case 2:
			fmt.Fprintf(&b, "with s%d = str.to_uppercase(name)\n", i)
		default:
			b.WriteString("# comment\n")
		}
	}
	src := b.String()
	c := newClient(t, 0)
	uri := c.open(src)
	c.diagnostics(uri)
	var diags, comps []time.Duration
	for v := 2; v < 22; v++ {
		text := src + fmt.Sprintf("filter c%d > 0\n", 4*v)
		start := time.Now()
		c.notify("textDocument/didChange", map[string]any{"textDocument": map[string]any{"uri": uri, "version": v},
			"contentChanges": []any{map[string]any{"text": text}}})
		if d := c.diagnostics(uri); len(d) != 0 {
			t.Fatalf("diagnostics: %v", d[0])
		}
		diags = append(diags, time.Since(start))
		start = time.Now()
		c.request("textDocument/completion", pos(uri, 500, 8))
		comps = append(comps, time.Since(start))
	}
	slices.Sort(diags)
	slices.Sort(comps)
	medDiag, medComp := diags[len(diags)/2], comps[len(comps)/2]
	t.Logf("500 lines: diagnostics median %s (min %s), completion median %s (min %s)", medDiag, diags[0], medComp, comps[0])
	// The machine may be loaded; judge the median against a generous
	// bound and leave the precise budget to the log.
	if !testing.Short() && !raceEnabled && (medDiag > 150*time.Millisecond || medComp > 150*time.Millisecond) {
		t.Errorf("too slow: diagnostics %s, completion %s", medDiag, medComp)
	}
}
