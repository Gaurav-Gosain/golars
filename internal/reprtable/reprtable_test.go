package reprtable

import (
	"strings"
	"testing"
)

func TestPick(t *testing.T) {
	idx, gap := Pick(3, 10)
	if len(idx) != 3 || gap != -1 {
		t.Fatalf("%v %d", idx, gap)
	}
	idx, gap = Pick(20, 5)
	if gap != 2 || len(idx) != 5 || idx[0] != 0 || idx[2] != 17 || idx[4] != 19 {
		t.Fatalf("%v %d", idx, gap)
	}
	if idx, gap = Pick(4, -1); len(idx) != 4 || gap != -1 {
		t.Fatalf("%v %d", idx, gap)
	}
}

func TestHTML(t *testing.T) {
	v := "<b>"
	tbl := Table{Columns: []string{"a", "b"}, Dtypes: []string{"str", "i64"}, Shape: [2]int{9, 3},
		Rows: [][]*string{{&v, nil}, {&v, nil}}}
	tbl.SetGaps(1, 1)
	out := tbl.HTML()
	for _, want := range []string{"shape: (9, 3)", "&lt;b&gt;", "null", "font-style:italic", "…", "showing 2 of 3 columns"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %s", want, out)
		}
	}
	if strings.Count(out, "<tr>") != 5 { // header, dtypes, row, gap, row
		t.Errorf("rows: %s", out)
	}
	empty := Table{Columns: []string{"a"}, Dtypes: []string{"i64"}, Shape: [2]int{0, 1}}
	if !strings.Contains(empty.HTML(), "empty") {
		t.Error("empty frame")
	}
}
