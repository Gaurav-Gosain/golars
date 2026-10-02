package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func structuredState(t *testing.T) *state {
	t.Helper()
	col, err := series.FromString("name", []string{"alpha", "other"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(col)
	if err != nil {
		col.Release()
		t.Fatal(err)
	}
	s := newState(false)
	s.df = df
	t.Cleanup(s.close)
	return s
}

func TestKernelStructuredOutputs(t *testing.T) {
	s := structuredState(t)
	resp := executeCell(s, kernelRequest{Code: "head 1\nselect name\ntail 1", Structured: true})
	if resp.Error != "" {
		t.Fatal(resp.Error)
	}
	var tables []map[string]any
	for _, o := range resp.Outputs {
		if o.Type == "table" {
			var tbl map[string]any
			if err := json.Unmarshal(o.Table, &tbl); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(o.HTML, "<table") {
				t.Fatalf("table output without html: %+v", o)
			}
			tables = append(tables, tbl)
		}
	}
	if len(tables) != 2 {
		t.Fatalf("want 2 tables in order, got %+v", resp.Outputs)
	}
	if last := tables[1]["rows"].([]any)[0].([]any)[0]; last != "other" {
		t.Fatalf("tail row = %v", last)
	}
	if strings.Contains(resp.Text, "golars-table") || strings.Contains(resp.Text, "shown") {
		t.Fatalf("markers or row counts leaked into text: %q", resp.Text)
	}
}

func TestKernelStructuredAutoDisplay(t *testing.T) {
	s := structuredState(t)
	resp := executeCell(s, kernelRequest{Code: "select name", Structured: true})
	if resp.Error != "" || resp.HTML == "" || len(resp.Table) == 0 {
		t.Fatalf("error=%q html=%d table=%s", resp.Error, len(resp.HTML), resp.Table)
	}
	var tbl struct {
		Columns []string `json:"columns"`
		Shape   [2]int   `json:"shape"`
	}
	if err := json.Unmarshal(resp.Table, &tbl); err != nil || tbl.Shape != [2]int{2, 1} || tbl.Columns[0] != "name" {
		t.Fatalf("table %s (%v)", resp.Table, err)
	}
	// Plain requests are unchanged.
	plain := executeCell(s, kernelRequest{Code: "select name"})
	if len(plain.Table) != 0 || plain.Outputs != nil {
		t.Fatalf("plain request got structured fields: %+v", plain)
	}
}

func TestSplitOutputs(t *testing.T) {
	tables := []hostOutput{{Type: "table", HTML: "a"}, {Type: "table", HTML: "b"}}
	text := "x\n\x1egolars-table:0\x1e\ny\n\x1egolars-table:1\x1e\n"
	out := splitOutputs(text, tables)
	if len(out) != 4 || out[0].Text != "x\n" || out[1].HTML != "a" || out[2].Text != "y\n" || out[3].HTML != "b" {
		t.Fatalf("%+v", out)
	}
}
