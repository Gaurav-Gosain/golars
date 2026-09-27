package difftest

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestRegressions replays every shrunk case under testdata/regress
// against the polars result baked into the file. Cases with status
// "known" document open bugs and are skipped with the reason.
func TestRegressions(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "regress", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no regression cases found")
	}
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".json")
		t.Run(name, func(t *testing.T) {
			rc, err := LoadRegressCase(f)
			if err != nil {
				t.Fatal(err)
			}
			if rc.Status == "known" {
				t.Skip("known bug: " + rc.Note)
			}
			c, pr, err := rc.Case()
			if err != nil {
				t.Fatal(err)
			}
			c.Plan.Order = DeriveOrder(c.Plan.Ops)
			out := CheckWithOracleResult(c, pr)
			if !out.Verdict.OK() {
				t.Fatalf("%s: %s\nplan:\n%s", out.Verdict.Kind, out.Verdict.Detail, c.Plan.Describe())
			}
		})
	}
}
