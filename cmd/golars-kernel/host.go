package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// kernelHost wraps the long-lived `golars kernel-host` subprocess
// that owns the actual REPL state. Each Run() call sends one NDJSON
// request and reads one response. Run is serialised - shell loop is
// single-threaded so there's no real concurrency to worry about, but
// we mu-guard anyway in case a control-loop interrupt restarts mid-Run.
type kernelHost struct {
	ctx context.Context
	mu  sync.Mutex

	cmd  *exec.Cmd
	in   io.WriteCloser
	out  *bufio.Reader
	stop chan struct{}
}

// kernelHostResp matches cmd/golars/subcmd_kernel_host.go's response.
type kernelHostResp struct {
	ID     string  `json:"id"`
	Text   string  `json:"text"`
	Stderr string  `json:"stderr"`
	HTML   string  `json:"html"`
	Error  string  `json:"error,omitempty"`
	Shape  *[2]int `json:"shape,omitempty"`
}

// startKernelHost finds the golars binary and spawns the host. The
// binary is resolved via $GOLARS_BIN, then the kernel binary's own
// directory (release tarball ships them side-by-side), then $PATH.
func startKernelHost(ctx context.Context) (*kernelHost, error) {
	bin, err := findGolarsBin()
	if err != nil {
		return nil, err
	}
	h := &kernelHost{ctx: ctx, stop: make(chan struct{})}
	if err := h.spawn(bin); err != nil {
		return nil, err
	}
	return h, nil
}

func findGolarsBin() (string, error) {
	if env := os.Getenv("GOLARS_BIN"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env, nil
		}
	}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		cand := filepath.Join(dir, "golars")
		if _, err := os.Stat(cand); err == nil {
			return cand, nil
		}
	}
	return exec.LookPath("golars")
}

func (h *kernelHost) spawn(bin string) error {
	cmd := exec.CommandContext(h.ctx, bin, "kernel-host")
	cmd.Stderr = os.Stderr // surface host crashes for debugging
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		return fmt.Errorf("start %s: %w", bin, err)
	}
	h.cmd = cmd
	h.in = stdin
	h.out = bufio.NewReaderSize(stdout, 1<<20)
	return nil
}

// Run ships one cell to the host and blocks for the response. Empty
// code returns an empty response without bothering the host.
func (h *kernelHost) Run(code string) (*kernelHostResp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd == nil {
		return nil, fmt.Errorf("kernel-host not started")
	}

	req := map[string]string{
		"id":   uuid.NewString(),
		"code": code,
	}
	line, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := h.in.Write(append(line, '\n')); err != nil {
		return nil, fmt.Errorf("write to host: %w", err)
	}

	respLine, err := h.out.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read host: %w", err)
	}
	var resp kernelHostResp
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return nil, fmt.Errorf("decode host: %w", err)
	}
	return &resp, nil
}

// Restart kills the in-flight host and starts a fresh one. Used by
// interrupt_request - lazy frames are immutable views so restarts are
// cheap, but loaded sources are lost.
func (h *kernelHost) Restart() {
	h.mu.Lock()
	defer h.mu.Unlock()
	bin, err := findGolarsBin()
	if err != nil {
		return
	}
	if h.cmd != nil && h.cmd.Process != nil {
		_ = h.cmd.Process.Kill()
		_ = h.cmd.Wait()
	}
	_ = h.spawn(bin)
}

func (h *kernelHost) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.in != nil {
		_ = h.in.Close()
	}
	if h.cmd != nil && h.cmd.Process != nil {
		_, _ = h.cmd.Process.Wait()
	}
	close(h.stop)
}
