package main

import (
	"slices"
	"strings"
	"testing"
)

func completionLabels(items []completionItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Label
	}
	return out
}

func TestCommandCompletionsIncludeAliases(t *testing.T) {
	got := completionLabels(commandCompletions(".scan_"))
	for _, want := range []string{".scan_csv", ".scan_arrow", ".scan_jsonl"} {
		if !slices.Contains(got, want) {
			t.Errorf("completions for .scan_ missing %q: %v", want, got)
		}
	}
	for _, it := range commandCompletions("wri") {
		if it.Label == "write" && !strings.HasPrefix(it.Detail, "alias of save") {
			t.Errorf("write detail = %q, want alias of save", it.Detail)
		}
	}
}

// head, tail and show print rows but leave the focus unchanged, so
// later statements must still see the full row count.
func TestPrintCommandsKeepShape(t *testing.T) {
	st := &frameState{staged: map[string]frameShape{}}
	st.focus = frameShape{cols: []string{"a", "b"}, rows: 50}
	for _, stmt := range []string{"head 3", "tail 2", "show", ".browse", "describe"} {
		applyStmt(st, "", stmt)
	}
	if st.focus.rows != 50 {
		t.Fatalf("rows = %d, want 50", st.focus.rows)
	}
}

func TestShapeTrackingForReshapeCommands(t *testing.T) {
	st := &frameState{staged: map[string]frameShape{}}
	st.focus = frameShape{cols: []string{"id", "x", "y"}, rows: 100}
	apply := func(stmt string) { applyStmt(st, "", stmt) }

	apply("rename x as x2")
	apply("with_row_index idx")
	apply("sum_horizontal total x2 y")
	apply("with y = y * 2") // replaces, does not append
	apply("with z=y+1")
	want := []string{"idx", "id", "x2", "y", "total", "z"}
	if !slices.Equal(st.focus.cols, want) {
		t.Fatalf("cols = %v, want %v", st.focus.cols, want)
	}
	apply("sample 10")
	if st.focus.rows != 10 {
		t.Fatalf("sample 10: rows = %d", st.focus.rows)
	}
	apply("melt id x2,y") // alias of unpivot
	if want := []string{"id", "variable", "value"}; !slices.Equal(st.focus.cols, want) {
		t.Fatalf("unpivot cols = %v, want %v", st.focus.cols, want)
	}
}
