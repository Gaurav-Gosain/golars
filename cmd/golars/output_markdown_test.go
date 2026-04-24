package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Values containing `|` would break the table without escaping; a
// pipe-split row would suddenly have too many cells.
func TestWriteMarkdownEscapesPipes(t *testing.T) {
	name, _ := series.FromString("name", []string{"a|b", "plain", "back\\slash"}, nil)
	amt, _ := series.FromInt64("amount", []int64{1, 2, 3}, nil)
	df, _ := dataframe.New(name, amt)
	defer df.Release()

	var buf bytes.Buffer
	if err := writeMarkdown(&buf, df); err != nil {
		t.Fatal(err)
	}
	got := buf.String()

	// Each data row must have exactly (width+1) pipes when rendered.
	// With two columns, every row starts + ends with `|` and has one
	// separator in the middle: 3 pipes total per row.
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	for i, ln := range lines {
		if c := strings.Count(ln, "|") - strings.Count(ln, `\|`); c != 3 {
			t.Errorf("line %d unescaped-pipe count = %d, want 3: %q", i, c, ln)
		}
	}

	// Backslash must survive as `\\`.
	if !strings.Contains(got, `back\\slash`) {
		t.Errorf("backslash not escaped: %q", got)
	}

	// Pipe must render as `\|`.
	if !strings.Contains(got, `a\|b`) {
		t.Errorf("pipe not escaped: %q", got)
	}
}

// Newlines inside strings become <br> so the row doesn't split.
func TestWriteMarkdownEscapesNewlines(t *testing.T) {
	s, _ := series.FromString("note", []string{"line1\nline2"}, nil)
	df, _ := dataframe.New(s)
	defer df.Release()

	var buf bytes.Buffer
	if err := writeMarkdown(&buf, df); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "line1\nline2") {
		t.Errorf("raw newline leaked into cell: %q", out)
	}
	if !strings.Contains(out, "line1<br>line2") {
		t.Errorf("newline not replaced with <br>: %q", out)
	}
}
