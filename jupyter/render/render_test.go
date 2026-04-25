package render

import (
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars"
)

// buildDF: tiny helper since most tests need a DF and we don't want
// to thread a CSV file through every test.
func buildDF(t *testing.T) *golars.DataFrame {
	t.Helper()
	df, err := golars.FromMap(map[string]any{
		"name":   []string{"alice", "bob", "carol"},
		"score":  []int64{95, 78, 88},
		"active": []bool{true, false, true},
	}, []string{"name", "score", "active"})
	if err != nil {
		t.Fatalf("buildDF: %v", err)
	}
	return df
}

func TestHTMLRoundtrip(t *testing.T) {
	df := buildDF(t)
	defer df.Release()

	out := HTML(df)
	if !strings.Contains(out, "<table") || !strings.Contains(out, "</table>") {
		t.Fatalf("HTML missing table tags: %s", out)
	}
	for _, want := range []string{"name", "score", "active", "alice", "bob", "carol", "95", "true"} {
		if !strings.Contains(out, want) {
			t.Errorf("HTML missing %q\n%s", want, out)
		}
	}
	// Inline style means notebooks pick up styling without external CSS.
	if !strings.Contains(out, "border-collapse") {
		t.Errorf("HTML missing inline styling")
	}
}

func TestMarkdownRoundtrip(t *testing.T) {
	df := buildDF(t)
	defer df.Release()

	out := Markdown(df)
	if !strings.HasPrefix(out, "shape:") {
		t.Errorf("markdown missing shape header: %q", out)
	}
	for _, want := range []string{"| name (str) |", "| alice |", "| 95 |"} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown missing %q\n%s", want, out)
		}
	}
}

func TestEmptyDataFrame(t *testing.T) {
	df, err := golars.FromMap(map[string]any{}, nil)
	if err != nil {
		t.Skipf("empty DataFrameFromMap not supported: %v", err)
	}
	defer df.Release()

	if !strings.Contains(HTML(df), "empty") {
		t.Errorf("expected empty marker in HTML")
	}
	if !strings.Contains(Markdown(df), "empty") {
		t.Errorf("expected empty marker in markdown")
	}
}

func TestMimeBundle(t *testing.T) {
	df := buildDF(t)
	defer df.Release()

	bundle := MimeBundle(df)
	for _, mime := range []string{"text/plain", "text/html", "text/markdown"} {
		if bundle[mime] == "" {
			t.Errorf("MimeBundle missing %s", mime)
		}
	}
	// text/plain should be the existing ASCII repr - includes box-drawing.
	if !strings.Contains(bundle["text/plain"], "shape:") {
		t.Errorf("text/plain missing shape marker")
	}
}

func TestNilDataFrame(t *testing.T) {
	if !strings.Contains(HTML(nil), "nil dataframe") {
		t.Errorf("HTML(nil) should be friendly")
	}
	if !strings.Contains(Markdown(nil), "nil dataframe") {
		t.Errorf("Markdown(nil) should be friendly")
	}
	if Text(nil) != "<nil dataframe>" {
		t.Errorf("Text(nil) wrong: %q", Text(nil))
	}
}

func TestHTMLEscaping(t *testing.T) {
	df, err := golars.FromMap(map[string]any{
		"raw": []string{"<script>alert(1)</script>"},
	}, []string{"raw"})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	out := HTML(df)
	if strings.Contains(out, "<script>alert") {
		t.Errorf("HTML did not escape user content:\n%s", out)
	}
	if !strings.Contains(out, "&lt;script&gt;") {
		t.Errorf("expected escaped <script> in HTML")
	}
}
