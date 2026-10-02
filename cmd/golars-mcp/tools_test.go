package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCSV writes content to a tempfile and returns the path. The
// file is removed when the test exits.
func writeCSV(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustJSON(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// structured unwraps the structuredContent payload that every tool
// wraps around its response body.
func structured(t *testing.T, res any) map[string]any {
	t.Helper()
	m := res.(map[string]any)
	inner, ok := m["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("no structuredContent in response: %v", m)
	}
	return inner
}

// TestRunSchema verifies that the schema tool returns the column
// names and dtypes for a CSV input.
func TestRunSchema(t *testing.T) {
	path := writeCSV(t, "id,name,amount\n1,a,10.5\n2,b,11.0\n")
	raw := mustJSON(t, map[string]any{"path": path})

	res, err := runSchema(raw)
	if err != nil {
		t.Fatalf("runSchema: %v", err)
	}
	inner := structured(t, res)
	cols, _ := inner["columns"].([]any)
	if len(cols) != 3 {
		t.Fatalf("columns: got %d want 3", len(cols))
	}
}

// TestRunHead checks the head tool returns rows and respects n.
func TestRunHead(t *testing.T) {
	path := writeCSV(t, "id,name\n1,a\n2,b\n3,c\n4,d\n")
	raw := mustJSON(t, map[string]any{"path": path, "n": 2})

	res, err := runHead(raw)
	if err != nil {
		t.Fatalf("runHead: %v", err)
	}
	inner := structured(t, res)
	rows := inner["rows"].([][]any)
	if len(rows) != 2 {
		t.Fatalf("rows: got %d want 2", len(rows))
	}
}

// TestRunRowCount confirms cheap row/col probe.
func TestRunRowCount(t *testing.T) {
	path := writeCSV(t, "a,b,c\n1,2,3\n4,5,6\n7,8,9\n")
	raw := mustJSON(t, map[string]any{"path": path})

	res, err := runRowCount(raw)
	if err != nil {
		t.Fatalf("runRowCount: %v", err)
	}
	inner := structured(t, res)
	if inner["rows"] != 3 || inner["columns"] != 3 {
		t.Fatalf("got %v", inner)
	}
}

// TestRunNullCounts exercises per-column null reporting.
func TestRunNullCounts(t *testing.T) {
	// All present; nullability reporting defaults to zeros.
	path := writeCSV(t, "a,b\n1,2\n3,4\n")
	raw := mustJSON(t, map[string]any{"path": path})

	res, err := runNullCounts(raw)
	if err != nil {
		t.Fatalf("runNullCounts: %v", err)
	}
	inner := structured(t, res)
	if _, ok := inner["counts"]; !ok {
		t.Fatalf("null_counts missing counts key: %v", inner)
	}
}

// TestRunSQL validates that files register as tables by filename
// stem and a SELECT returns rows.
func TestRunSQL(t *testing.T) {
	path := writeCSV(t, "id,amount\n1,10\n2,20\n")
	raw := mustJSON(t, map[string]any{
		"query": "SELECT id FROM data WHERE amount > 15",
		"files": []string{path},
	})

	res, err := runSQL(raw)
	if err != nil {
		t.Fatalf("runSQL: %v", err)
	}
	inner := structured(t, res)
	rows := inner["rows"].([][]any)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d want 1", len(rows))
	}
}

// TestRunMissingPath fails fast on missing required argument rather
// than panicking or returning an empty result.
func TestRunMissingPath(t *testing.T) {
	raw := mustJSON(t, map[string]any{})
	if _, err := runSchema(raw); err == nil {
		t.Fatal("expected error when path missing")
	}
}

// TestLoadByExtUnknown rejects unsupported extensions with a clear
// error, keeping the MCP host's surface readable.
func TestLoadByExtUnknown(t *testing.T) {
	path := writeCSV(t, "x,y\n1,2\n")
	renamed := path + ".bogus"
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	raw := mustJSON(t, map[string]any{"path": renamed})
	_, err := runSchema(raw)
	if err == nil || !strings.Contains(err.Error(), "unsupported file extension") {
		t.Fatalf("expected unsupported-extension error, got %v", err)
	}
}

func TestLoadByExtDirectory(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "data.csv")
	if err := os.Mkdir(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := mustJSON(t, map[string]any{"path": nested})
	_, err := runSchema(raw)
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular-file error, got %v", err)
	}
}

func TestRunHeadNegativeN(t *testing.T) {
	path := writeCSV(t, "x\n1\n2\n")
	raw := mustJSON(t, map[string]any{"path": path, "n": -3})
	if _, err := runHead(raw); err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("expected non-negative error, got %v", err)
	}
}

func TestRunHeadHugeN(t *testing.T) {
	path := writeCSV(t, "x\n1\n")
	raw := mustJSON(t, map[string]any{"path": path, "n": "99999999999999999999"})
	if _, err := runHead(raw); err == nil || !strings.Contains(err.Error(), "in range") {
		t.Fatalf("expected out-of-range error, got %v", err)
	}
}

func TestRunSQLDuplicateStems(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "same.csv")
	b := filepath.Join(dir, "same.ndjson")
	if err := os.WriteFile(a, []byte("v\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("{\"v\":9}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	raw := mustJSON(t, map[string]any{
		"query": "SELECT v FROM same",
		"files": []string{a, b},
	})
	if _, err := runSQL(raw); err == nil || !strings.Contains(err.Error(), "duplicate table name") {
		t.Fatalf("expected duplicate-table error, got %v", err)
	}
}
