package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKernelHostFrameOps(t *testing.T) {
	dir := t.TempDir()
	csv := filepath.Join(dir, "p.csv")
	if err := os.WriteFile(csv, []byte(peopleCSV), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newState(false)
	defer s.close()
	var g frameGens

	list := func() map[string]hostFrame {
		t.Helper()
		r := hostFrameOp(s, &g, kernelRequest{Op: "frames"})
		if r.Error != "" {
			t.Fatal(r.Error)
		}
		m := map[string]hostFrame{}
		for _, f := range r.Frames {
			if f.Focus {
				m["*"] = f
			} else {
				m[f.Name] = f
			}
		}
		return m
	}
	run := func(code string) {
		t.Helper()
		if r := executeCell(s, kernelRequest{Code: code}); r.Error != "" {
			t.Fatalf("%s: %s", code, r.Error)
		}
	}

	if len(list()) != 0 {
		t.Fatal("frames before any load")
	}
	run("load " + csv + " as people")
	p := list()["people"]
	if p.Gen == 0 || p.Cols == 0 || p.Rows == 0 {
		t.Fatalf("people: %+v", p)
	}
	if again := list(); again["people"].Gen != p.Gen {
		t.Fatal("generation changed without a change")
	}
	run("use people\nfilter age > 30\nstash old")
	m := list()
	if m["people"].Gen != p.Gen || m["old"].Gen == 0 || m["*"].Gen == 0 {
		t.Fatalf("after stash: %+v", m)
	}

	// Export a named frame and the focus, then import one under a new name.
	out := filepath.Join(dir, "old.arrow")
	if r := hostFrameOp(s, &g, kernelRequest{Op: "export", Name: "old", Path: out}); r.Error != "" || r.Shape == nil {
		t.Fatalf("export: %+v", r)
	}
	if r := hostFrameOp(s, &g, kernelRequest{Op: "export", Path: filepath.Join(dir, "focus.arrow")}); r.Error != "" {
		t.Fatalf("export focus: %s", r.Error)
	}
	focusGen := list()["*"].Gen
	r := hostFrameOp(s, &g, kernelRequest{Op: "import", Name: "copy", Path: out})
	if r.Error != "" || r.Gen == 0 {
		t.Fatalf("import: %+v", r)
	}
	m = list()
	if m["copy"].Gen != r.Gen || m["copy"].Rows != m["old"].Rows {
		t.Fatalf("after import: %+v (import gen %d)", m, r.Gen)
	}
	if m["*"].Gen != focusGen {
		t.Fatal("import moved the focus")
	}
	if r := hostFrameOp(s, &g, kernelRequest{Op: "export", Name: "nope", Path: out}); r.Error == "" {
		t.Fatal("export of a missing frame: no error")
	}
	run("drop_frame copy")
	if _, ok := list()["copy"]; ok {
		t.Fatal("dropped frame still listed")
	}
}
