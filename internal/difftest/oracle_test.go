package difftest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// repoRoot finds the module root from this test file.
func repoRoot(t testing.TB) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// startOracle skips the test unless DIFFTEST=1 and the polars venv
// exists. Plain `go test` never needs Python.
func startOracle(t testing.TB) *Oracle {
	if os.Getenv("DIFFTEST") != "1" {
		t.Skip("set DIFFTEST=1 to run tests against the polars oracle")
	}
	root := repoRoot(t)
	py := DefaultPython(root)
	if _, err := os.Stat(py); err != nil {
		t.Skipf("no polars venv at %s", py)
	}
	o, err := StartOracle(py, filepath.Join(root, "bench", "polars-compare", "difftest", "runner.py"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	return o
}

func TestOracleSmoke(t *testing.T) {
	o := startOracle(t)
	g := NewGen(1)
	left := &Frame{Cols: []Column{
		{Name: "k", T: TStr, Values: []any{"a", "b", "a", nil}},
		{Name: "x", T: TI64, Values: []any{int64(1), nil, int64(3), int64(4)}},
		{Name: "f", T: TF64, Values: []any{1.5, 2.5, nil, 0.25}},
	}}
	_ = g
	plans := []*Plan{
		{Order: Order{Mode: OrderExact}},
		{Ops: []Op{{Op: "with_columns", Exprs: []*Expr{Alias(Bin("+", Col("x"), LitV(int64(1))), "y")}}}, Order: Order{Mode: OrderExact}},
		{Ops: []Op{{Op: "group_by", Keys: []string{"k"}, Exprs: []*Expr{Call(Col("x"), "sum"), Alias(Call(Col("f"), "mean"), "m")}}}, Order: Order{Mode: OrderUnordered}},
		{Ops: []Op{{Op: "filter", Pred: Call(Col("x"), "is_null")}}, Order: Order{Mode: OrderExact}},
	}
	dir := t.TempDir()
	for i, p := range plans {
		c := &Case{Plan: p, Left: left}
		out := Check(c, filepath.Join(dir, "c"), o)
		if !out.Verdict.OK() {
			t.Errorf("plan %d: %s: %s", i, out.Verdict.Kind, out.Verdict.Detail)
		}
	}
}
