package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/jupyter/render"
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
	if s.df != nil {
		resp.HTML = render.HTML(s.df)
		h, w := s.df.Shape()
		shape := [2]int{h, w}
		resp.Shape = &shape
	}
	return resp
}
