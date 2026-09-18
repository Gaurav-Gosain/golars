package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCSVRecordCounts(t *testing.T) {
	for _, tc := range []struct {
		body string
		want int
	}{
		{"a,b\n1,2", 1},
		{"a,b\n\"one\ntwo\",2\n", 1},
		{"a,b\n\n1,2\n", 1},
		{"a,b\n1", -1},
		{"", 0},
	} {
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := countCSVRows(path); got != tc.want {
			t.Fatalf("%q: got %d, want %d", tc.body, got, tc.want)
		}
	}
}

func TestFileStatsRejectDirectory(t *testing.T) {
	if got := readFileStats(t.TempDir()); got.rows != rowsUnknown || len(got.cols) != 0 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

// TestStashAndUseBranching verifies that `stash NAME` copies the focus
// into the registry without consuming it, and `use NAME` clones the
// staged entry so later `.use NAME` still works. This is the symbolic
// shape-tracker equivalent of the runtime behaviour in
// cmd/golars/frames.go.
func TestStashAndUseBranching(t *testing.T) {
	st := &frameState{staged: map[string]frameShape{}}
	apply := func(line string) { applyStmt(st, "", line) }

	st.focus = frameShape{cols: []string{"name", "amount"}, rows: 5}
	apply("filter amount >= 60000")
	apply("stash base")

	if _, ok := st.staged["base"]; !ok {
		t.Fatalf("stash base: expected registry entry")
	}
	if got := st.focus.cols; len(got) != 2 {
		t.Fatalf("stash base: focus cols changed unexpectedly: %v", got)
	}

	apply("sort amount desc")
	apply("limit 2")
	if st.focus.rows != 2 {
		t.Fatalf("limit 2: expected rows=2, got %d", st.focus.rows)
	}
	apply("stash top")
	if _, ok := st.staged["top"]; !ok {
		t.Fatalf("stash top: expected registry entry")
	}

	apply("use base")
	if _, ok := st.staged["base"]; !ok {
		t.Fatalf("use base: staged entry must remain (non-consuming)")
	}
	if st.focusName != "base" {
		t.Fatalf("use base: focusName=%q, want %q", st.focusName, "base")
	}
	if st.focus.rows == 2 {
		t.Fatalf("use base: focus should be a clone of stashed base (rows unknown after filter), not the previous limit-2 focus")
	}

	apply("use top")
	if _, ok := st.staged["top"]; !ok {
		t.Fatalf("use top: staged entry must remain after clone-on-promote")
	}
	if st.focus.rows != 2 {
		t.Fatalf("use top: expected rows=2 from snapshot, got %d", st.focus.rows)
	}
}
