package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/jupyter/render"
	"github.com/Gaurav-Gosain/golars/script"
)

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
// A request with "structured":true gets tables as data instead of ASCII
// text. The reply then also carries "outputs", the cell's stdout text and
// tables in the order they were produced, and "table", the auto-displayed
// frame. Tables are dataframe.TableMIME JSON objects, next to their HTML:
//
//	{"type":"stdout","text":"..."}
//	{"type":"table","table":{"columns":[...],"dtypes":[...],"rows":[[...]],"shape":[h,w]},"html":"..."}
//
// All of these are optional fields, so clients and hosts of either age
// keep working with each other.
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
	// Structured asks for tables as data (see the protocol above).
	Structured bool `json:"structured,omitempty"`
}

// hostOutput is one entry of a structured reply's outputs.
type hostOutput struct {
	Type  string          `json:"type"` // "stdout" or "table"
	Text  string          `json:"text,omitempty"`
	Table json.RawMessage `json:"table,omitempty"`
	HTML  string          `json:"html,omitempty"`
}

// tableMarker stands in stdout for a table captured by tableHook, so the
// reply can put text and tables back in order. The record separators keep
// it from colliding with anything a command prints.
const tableMarker = "\x1egolars-table:%d\x1e\n"

var tableMarkerRe = regexp.MustCompile("\x1egolars-table:([0-9]+)\x1e\n")

// hookedTableFormat bounds tables printed by commands (head 50 shows 50
// rows) while keeping a runaway `head 1000000` from flooding the reply.
var hookedTableFormat = dataframe.FormatOptions{MaxRows: 500, MaxCols: 8, MaxCellRune: 64}

// splitOutputs turns captured stdout with table markers into ordered
// outputs.
func splitOutputs(text string, tables []hostOutput) []hostOutput {
	var out []hostOutput
	addText := func(s string) {
		if s != "" {
			out = append(out, hostOutput{Type: "stdout", Text: s})
		}
	}
	last := 0
	for _, m := range tableMarkerRe.FindAllStringSubmatchIndex(text, -1) {
		addText(text[last:m[0]])
		last = m[1]
		i, err := strconv.Atoi(text[m[2]:m[3]])
		if err == nil && i < len(tables) {
			out = append(out, tables[i])
		}
	}
	addText(text[last:])
	return out
}

// lastLineIsDisplayCommand reports whether the cell's last statement
// prints a table itself (show, head, tail, describe). The kernel then
// skips the HTML auto-display so the rows are not shown twice.
func lastLineIsDisplayCommand(code string) bool {
	lines := strings.Split(code, "\n")
	for _, line := range slices.Backward(lines) {
		fields := strings.Fields(script.Normalize(line))
		if len(fields) == 0 {
			continue
		}
		spec := script.FindCommand(fields[0])
		if spec == nil {
			return false
		}
		switch spec.Name {
		case "show", "head", "tail", "describe":
			return true
		}
		return false
	}
	return false
}

type kernelResponse struct {
	ID     string  `json:"id"`
	Text   string  `json:"text"`
	Stderr string  `json:"stderr"`
	HTML   string  `json:"html"`
	Error  string  `json:"error,omitempty"`
	Shape  *[2]int `json:"shape,omitempty"`
	// Table is the auto-displayed frame as dataframe.TableMIME JSON
	// (structured requests only).
	Table json.RawMessage `json:"table,omitempty"`
	// Outputs is the ordered stdout text and tables of a structured
	// request.
	Outputs []hostOutput `json:"outputs,omitempty"`
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
	defer s.close()
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
	var tables []hostOutput
	if req.Structured {
		tableHook = func(df *dataframe.DataFrame) {
			b := df.MimeBundleWith(hookedTableFormat)
			fmt.Fprintf(os.Stdout, tableMarker, len(tables))
			tables = append(tables, hostOutput{Type: "table", Table: json.RawMessage(b[dataframe.TableMIME]), HTML: b["text/html"]})
		}
		defer func() { tableHook = nil }()
	}
	runner := script.Runner{Exec: script.ExecutorFunc(s.handle)}
	execErr := runner.Run(strings.NewReader(req.Code), "<cell>")
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
	if req.Structured {
		resp.Outputs = splitOutputs(resp.Text, tables)
		resp.Text = tableMarkerRe.ReplaceAllString(resp.Text, "")
	}
	// `exit` makes no sense inside a notebook; treat it as a no-op.
	if errors.Is(execErr, errExit) {
		execErr = nil
	}
	if execErr != nil {
		resp.Error = execErr.Error()
	}

	// Materialise the focused pipeline (lf wins over df) for the HTML
	// auto-display. Limit(200) caps the work so a 10M-row frame doesn't
	// pay full Collect() cost just for a preview. Skip entirely when
	// the cell didn't touch the focus (pure `load X as NAME` cells, or
	// REPL-only commands).
	if focusChanged && execErr == nil && !lastLineIsDisplayCommand(req.Code) {
		display := s.df
		var collected *dataframe.DataFrame
		if s.lf != nil {
			preview := s.lf.Limit(200)
			out, err := preview.Collect(s.ctx)
			if err != nil {
				resp.Error = err.Error()
				return resp
			}
			collected = out
			display = out
		}
		if display != nil {
			resp.HTML = render.HTML(display)
			if req.Structured {
				resp.Table = json.RawMessage(display.MimeBundle()[dataframe.TableMIME])
			}
			h, w := display.Shape()
			resp.Shape = &[2]int{h, w}
		}
		if collected != nil {
			collected.Release()
		}
	}
	return resp
}
