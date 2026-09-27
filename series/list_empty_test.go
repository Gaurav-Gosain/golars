package series_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// List kernels on an empty list column used to panic in takeArrow on
// the nil child value buffer. Found by internal/difftest.
func TestListOpsEmptyColumn(t *testing.T) {
	s := series.Empty("l", dtype.List(dtype.Int64()))
	defer s.Release()
	ops := map[string]func() (*series.Series, error){
		"slice":      func() (*series.Series, error) { return s.List().Slice(0, 1) },
		"head":       func() (*series.Series, error) { return s.List().Head(1) },
		"tail":       func() (*series.Series, error) { return s.List().Tail(0) },
		"reverse":    func() (*series.Series, error) { return s.List().Reverse() },
		"drop_nulls": func() (*series.Series, error) { return s.List().DropNulls() },
	}
	for name, op := range ops {
		out, err := op()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if out.Len() != 0 {
			t.Fatalf("%s: len %d", name, out.Len())
		}
		out.Release()
	}
}
