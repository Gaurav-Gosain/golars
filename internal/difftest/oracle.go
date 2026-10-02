package difftest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Oracle is a long-lived polars runner process.
type Oracle struct {
	mu  sync.Mutex
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
}

// OracleResult is what polars produced for a case.
type OracleResult struct {
	OK     bool       `json:"ok"`
	EType  string     `json:"etype"`
	Error  string     `json:"error"`
	Trace  string     `json:"trace"`
	Schema [][]string `json:"schema"`
	// Cols holds the decoded result columns when OK.
	Cols []ResultCol `json:"-"`
}

// ResultCol is one decoded result column.
type ResultCol struct {
	Name   string
	DType  string
	Values []any
}

// DefaultPython locates the polars virtualenv interpreter. The
// DIFFTEST_PYTHON environment variable overrides it.
func DefaultPython(repoRoot string) string {
	if p := os.Getenv("DIFFTEST_PYTHON"); p != "" {
		return p
	}
	return filepath.Join(repoRoot, "bench", "polars-compare", ".venv", "bin", "python")
}

// StartOracle launches runner.py in serve mode.
func StartOracle(python, runner string) (*Oracle, error) {
	cmd := exec.Command(python, runner, "--serve")
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Oracle{cmd: cmd, in: in, out: bufio.NewReader(out)}, nil
}

// Close stops the process.
func (o *Oracle) Close() error {
	o.in.Close()
	return o.cmd.Wait()
}

// Run asks polars to execute the case stored in dir.
func (o *Oracle) Run(dir string) (*OracleResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, err := fmt.Fprintln(o.in, dir); err != nil {
		return nil, err
	}
	line, err := o.out.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("difftest: oracle died: %w", err)
	}
	if strings.TrimSpace(line) != "done" {
		return nil, fmt.Errorf("difftest: oracle said %q", line)
	}
	return ReadOracleResult(dir)
}

// ReadOracleResult loads result.json and result.arrows from dir.
func ReadOracleResult(dir string) (*OracleResult, error) {
	b, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return nil, err
	}
	var r OracleResult
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if !r.OK {
		return &r, nil
	}
	f, err := os.Open(filepath.Join(dir, "result.arrows"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r.Cols, err = ReadIPCColumns(f)
	return &r, err
}

// ReadIPCColumns decodes an Arrow IPC stream into canonical columns
// without going through golars.
func ReadIPCColumns(rd io.Reader) ([]ResultCol, error) {
	reader, err := ipc.NewReader(rd, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	sch := reader.Schema()
	cols := make([]ResultCol, sch.NumFields())
	for i, f := range sch.Fields() {
		cols[i] = ResultCol{Name: f.Name, DType: ArrowName(f.Type), Values: []any{}}
	}
	for reader.Next() {
		rec := reader.RecordBatch()
		for i := range cols {
			cols[i].Values = append(cols[i].Values, ArrayValues(rec.Column(i))...)
		}
	}
	if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return cols, nil
}

// ArrayValues decodes an arrow array into the canonical Go values used
// by Column.
func ArrayValues(arr arrow.Array) []any {
	out := make([]any, arr.Len())
	for i := range out {
		out[i] = valueAt(arr, i)
	}
	return out
}
