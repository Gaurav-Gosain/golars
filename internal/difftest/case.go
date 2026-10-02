package difftest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/ipc"
)

// Case is one generated test case.
type Case struct {
	Plan  *Plan
	Left  *Frame
	Right *Frame
}

// Clone deep-copies the case.
func (c *Case) Clone() *Case {
	out := &Case{Plan: ClonePlan(c.Plan), Left: c.Left.Clone()}
	if c.Right != nil {
		out.Right = c.Right.Clone()
	}
	return out
}

// WriteDir writes plan.json, left.arrows and right.arrows into dir. The
// frames are encoded by golars' IPC writer; identity plans (no ops)
// double as a check of that writer against polars' reader.
func (c *Case) WriteDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, f := range []string{"result.json", "result.arrows"} {
		os.Remove(filepath.Join(dir, f))
	}
	c.Plan.HasRight = c.Right != nil
	b, err := json.MarshalIndent(c.Plan, "", " ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "plan.json"), b, 0o644); err != nil {
		return err
	}
	if err := writeFrame(filepath.Join(dir, "left.arrows"), c.Left); err != nil {
		return err
	}
	if c.Right != nil {
		return writeFrame(filepath.Join(dir, "right.arrows"), c.Right)
	}
	return nil
}

func writeFrame(path string, f *Frame) error {
	df, err := f.ToDataFrame(memory.DefaultAllocator)
	if err != nil {
		return err
	}
	defer df.Release()
	return ipc.WriteFile(context.Background(), path, df)
}

// LoadCaseDir reads a case written by WriteDir. Chunk boundaries are
// not preserved (the IPC files hold one batch).
func LoadCaseDir(dir string) (*Case, error) {
	b, err := os.ReadFile(filepath.Join(dir, "plan.json"))
	if err != nil {
		return nil, err
	}
	c := &Case{Plan: &Plan{}}
	if err := UnmarshalPlan(b, c.Plan); err != nil {
		return nil, err
	}
	if c.Left, err = readFrame(filepath.Join(dir, "left.arrows")); err != nil {
		return nil, err
	}
	if c.Plan.HasRight {
		if c.Right, err = readFrame(filepath.Join(dir, "right.arrows")); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func readFrame(path string) (*Frame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cols, err := ReadIPCColumns(f)
	if err != nil {
		return nil, err
	}
	out := &Frame{}
	for _, c := range cols {
		t, err := ParseType(c.DType)
		if err != nil {
			return nil, err
		}
		out.Cols = append(out.Cols, Column{Name: c.Name, T: t, Values: c.Values})
	}
	return out, nil
}

// Outcome is everything known about one executed case.
type Outcome struct {
	Verdict Verdict
	Polars  *OracleResult
	Golars  []ResultCol
	GoErr   error
}

// GolarsTimeout bounds a single golars run.
var GolarsTimeout = 60 * time.Second

// Check runs the case on both engines and compares.
func Check(c *Case, dir string, o *Oracle) Outcome {
	if err := c.WriteDir(dir); err != nil {
		return Outcome{Verdict: Verdict{Kind: MHarness, Detail: err.Error()}}
	}
	pr, err := o.Run(dir)
	if err != nil {
		return Outcome{Verdict: Verdict{Kind: MHarness, Detail: err.Error()}}
	}
	got, gerr := runGolarsFrames(c)
	out := Outcome{Polars: pr, Golars: got, GoErr: gerr}
	out.Verdict = classify(c, pr, got, gerr)
	return out
}

// CheckWithOracleResult compares golars against an already computed
// polars result (used by the regression tests, which need no Python).
func CheckWithOracleResult(c *Case, pr *OracleResult) Outcome {
	got, gerr := runGolarsFrames(c)
	return Outcome{Polars: pr, Golars: got, GoErr: gerr, Verdict: classify(c, pr, got, gerr)}
}

func runGolarsFrames(c *Case) ([]ResultCol, error) {
	left, err := c.Left.ToDataFrame(memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("harness: build left: %w", err)
	}
	defer left.Release()
	var right *dataframe.DataFrame
	if c.Right != nil {
		right, err = c.Right.ToDataFrame(memory.DefaultAllocator)
		if err != nil {
			return nil, fmt.Errorf("harness: build right: %w", err)
		}
		defer right.Release()
	}
	type res struct {
		cols []ResultCol
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- res{err: &PanicError{Value: fmt.Sprintf("reading golars result: %v", r), Stack: string(debug.Stack())}}
			}
		}()
		df, err := RunGolars(context.Background(), c.Plan, left, right)
		if err != nil {
			ch <- res{err: err}
			return
		}
		cols := Canonical(df)
		df.Release()
		ch <- res{cols: cols}
	}()
	select {
	case r := <-ch:
		return r.cols, r.err
	case <-time.After(GolarsTimeout):
		return nil, errTimeout
	}
}

var errTimeout = errors.New("golars timeout")

func classify(c *Case, pr *OracleResult, got []ResultCol, gerr error) Verdict {
	var pe *PanicError
	isPanic := errors.As(gerr, &pe)
	if strings.HasPrefix(fmt.Sprint(gerr), "harness:") {
		return Verdict{Kind: MHarness, Detail: gerr.Error()}
	}
	if !pr.OK {
		if strings.Contains(pr.EType, "Panic") {
			return Verdict{Kind: MOraclePanic, Detail: pr.EType + ": " + pr.Error}
		}
		switch {
		case isPanic:
			return Verdict{Kind: MPanic, Detail: fmt.Sprintf("polars raised %s: %s\ngolars panicked: %v\n%s", pr.EType, firstLine(pr.Error), pe.Value, pe.Stack)}
		case errors.Is(gerr, errTimeout):
			return Verdict{Kind: MTimeout}
		case gerr != nil:
			return Verdict{Kind: MBothErrorOkay, Detail: pr.EType + ": " + firstLine(pr.Error) + " | golars: " + gerr.Error()}
		}
		return Verdict{Kind: MMissingError, Detail: fmt.Sprintf("polars raised %s: %s", pr.EType, firstLine(pr.Error))}
	}
	switch {
	case isPanic:
		return Verdict{Kind: MPanic, Detail: fmt.Sprintf("golars panicked: %v\n%s", pe.Value, pe.Stack)}
	case errors.Is(gerr, errTimeout):
		return Verdict{Kind: MTimeout}
	case errors.Is(gerr, ErrUnsupported):
		return Verdict{Kind: MUnsupported, Detail: gerr.Error()}
	case gerr != nil:
		return Verdict{Kind: MGolarsError, Detail: gerr.Error()}
	}
	return Compare(got, pr.Cols, c.Plan.Order)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
