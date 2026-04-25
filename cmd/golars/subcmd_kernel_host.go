package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/jupyter/render"
)

// ansiCSI matches CSI sequences (colour, cursor moves) so the
// trailing-table strip can decide indent on the visible glyphs, not
// the ANSI envelope. Lipgloss prepends \x1b[<sgr>m to styled cells;
// without this the "starts with 2 spaces" check fails for coloured
// table rows and the strip bails too early.
var ansiCSI = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// kernel-host is a hidden subcommand that turns golars into an NDJSON
// request/response server. The custom Jupyter kernel (cmd/golars-kernel)
// spawns this as a long-running child so cell execution reuses the
// existing dispatcher (state.handle) and frame state persists across
// cells without needing to refactor the dispatcher into a shared pkg.
//
// Wire protocol (newline-delimited JSON, one message per line):
//
//	request:  {"id":"abc","code":"load x.csv\nhead 5"}
//	response: {"id":"abc","text":"...stdout...","stderr":"","html":"...","error":null,"shape":[h,w]}
//
// Errors during execution land in `error` and the run continues. A
// fatal protocol error closes the stream.
//
// Two streams are used internally:
//   - the FD 1 the parent inherited (kept aside) → all NDJSON replies
//   - a temporary pipe swapped into os.Stdout / os.Stderr → captures
//     everything the dispatcher prints during a cell.
type kernelRequest struct {
	ID   string `json:"id"`
	Code string `json:"code"`
}

// lastLineIsDisplayCommand returns true when the cell's final
// non-empty non-comment statement is one that prints a table.
// Used to decide whether to strip the dispatcher's ASCII table out
// of stdout, since the auto-display HTML covers the same data.
func lastLineIsDisplayCommand(code string) bool {
	for _, raw := range reverse(strings.Split(code, "\n")) {
		l := strings.TrimSpace(raw)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		l = strings.TrimPrefix(l, ".")
		first, _, _ := strings.Cut(l, " ")
		switch first {
		case "show", "head", "tail", "collect", "describe":
			return true
		}
		return false
	}
	return false
}

func reverse(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

// stripTrailingTable removes the trailing block of indented lines
// from text. The dispatcher's printTable output is indented with two
// leading spaces and ends with "  N rows shown"; commentary lines
// (`✓ ...`) start at column 0. Walking from the end and dropping
// indented + blank lines until we hit a column-0 line strips exactly
// the table block. ANSI codes are stripped before the indent check
// so a coloured `  name` row (`\x1b[...m  name\x1b[m`) still counts
// as indented.
func stripTrailingTable(text string) string {
	lines := strings.Split(text, "\n")
	end := len(lines)
	for end > 0 {
		l := ansiCSI.ReplaceAllString(lines[end-1], "")
		if l == "" || strings.HasPrefix(l, " ") || strings.HasPrefix(l, "\t") {
			end--
			continue
		}
		break
	}
	if end == len(lines) {
		return text
	}
	out := strings.Join(lines[:end], "\n")
	if !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	return out
}

type kernelResponse struct {
	ID     string `json:"id"`
	Text   string `json:"text"`
	Stderr string `json:"stderr"`
	HTML   string `json:"html"`
	Error  string `json:"error,omitempty"`
	Shape  *[2]int `json:"shape,omitempty"`
}

func newKernelHostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "kernel-host",
		Short:  "(internal) NDJSON server for the Jupyter kernel.",
		Long:   "Reads {id, code} requests on stdin, runs each through the same dispatcher used by `golars run`, replies on stdout. State persists across requests.",
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runKernelHost(cmd.OutOrStdout(), cmd.InOrStdin())
		},
	}
	return cmd
}

func runKernelHost(realOut io.Writer, in io.Reader) error {
	s := newState(false)
	enc := json.NewEncoder(realOut)
	enc.SetEscapeHTML(false)

	// Save the stdio we inherited; the dispatcher writes to os.Stdout /
	// os.Stderr directly so we redirect those for capture, then restore.
	origStdout := os.Stdout
	origStderr := os.Stderr
	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1<<16), 1<<24) // up to 16 MiB per cell
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req kernelRequest
		if err := json.Unmarshal(line, &req); err != nil {
			_ = enc.Encode(kernelResponse{Error: fmt.Sprintf("invalid request: %v", err)})
			continue
		}
		resp := executeCell(s, req)
		if err := enc.Encode(resp); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// executeCell runs req.Code through state.handle while capturing stdout
// + stderr, and returns the multi-mimetype response. Multi-line cells
// are split on '\n' and dispatched one statement at a time so an error
// on line 3 leaves lines 1-2 effectful and 4+ skipped (matching the
// `golars run` behaviour).
func executeCell(s *state, req kernelRequest) kernelResponse {
	resp := kernelResponse{ID: req.ID}

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		resp.Error = fmt.Sprintf("pipe: %v", err)
		return resp
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutW.Close()
		stdoutR.Close()
		resp.Error = fmt.Sprintf("pipe: %v", err)
		return resp
	}

	origStdout := os.Stdout
	origStderr := os.Stderr
	os.Stdout = stdoutW
	os.Stderr = stderrW

	// Drain pipes concurrently so the dispatcher never blocks on a
	// full pipe buffer.
	var stdoutBuf, stderrBuf strings.Builder
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(&stdoutBuf, stdoutR) }()
	go func() { defer wg.Done(); _, _ = io.Copy(&stderrBuf, stderrR) }()

	// Track whether the cell touched the focused pipeline. Cells that
	// only stage extra frames (`load X as A`) leave the focus
	// untouched, in which case auto-displaying the lf would show stale
	// state from a previous cell.
	startDF := s.df
	startLF := s.lf
	var execErr error
	for _, raw := range strings.Split(req.Code, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if err := s.handle(line); err != nil {
			execErr = err
			break
		}
	}
	focusChanged := s.df != startDF || s.lf != startLF

	stdoutW.Close()
	stderrW.Close()
	wg.Wait()
	stdoutR.Close()
	stderrR.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr

	resp.Text = stdoutBuf.String()
	resp.Stderr = stderrBuf.String()
	if execErr != nil {
		resp.Error = execErr.Error()
	}

	// Materialise the focused pipeline (lf wins over df) for the HTML
	// auto-display. Limit(200) caps the work so a 10M-row frame doesn't
	// pay full Collect() cost just for a preview. Skip entirely when
	// the cell didn't touch the focus (pure `load X as NAME` cells, or
	// REPL-only commands).
	if focusChanged {
		display := s.df
		var collected *dataframe.DataFrame
		if s.lf != nil {
			preview := s.lf.Limit(200)
			out, err := preview.Collect(s.ctx)
			if err == nil {
				collected = out
				display = out
			}
		}
		if display != nil {
			resp.HTML = render.HTML(display)
			h, w := display.Shape()
			shape := [2]int{h, w}
			resp.Shape = &shape
		}
		if collected != nil {
			collected.Release()
		}
	}

	// When the cell ends with a display command (show/head/tail/...),
	// the dispatcher already printed an ASCII table to stdout. The
	// HTML auto-display would otherwise render the same data twice.
	// Strip the trailing table block so the user sees one display.
	if resp.HTML != "" && lastLineIsDisplayCommand(req.Code) {
		resp.Text = stripTrailingTable(resp.Text)
	}
	return resp
}
