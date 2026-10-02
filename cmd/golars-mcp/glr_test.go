package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func callTool(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(args)
	tool := findTool(name)
	if tool == nil {
		t.Fatalf("no tool %s", name)
	}
	res, err := tool.Run(raw)
	if err != nil {
		return map[string]any{"err": err.Error()}
	}
	b, _ := json.Marshal(res)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func text(res map[string]any) string {
	c, _ := res["content"].([]any)
	if len(c) == 0 {
		return ""
	}
	return c[0].(map[string]any)["text"].(string)
}

const exampleData = "../../examples/script"

func TestCheckGlr(t *testing.T) {
	res := callTool(t, "check_glr", map[string]any{
		"script": "load data/salaries.csv\nwith m = amount / 12\nfilter amout > 1\n", "dir": exampleData})
	sc := res["structuredContent"].(map[string]any)
	fs := sc["findings"].([]any)
	if sc["ok"] != false || len(fs) != 1 {
		t.Fatalf("findings %v", sc)
	}
	f := fs[0].(map[string]any)
	if f["line"] != 3.0 || f["column"] != 8.0 || f["fix"] != "amount" {
		t.Errorf("finding %v", f)
	}
	steps := sc["steps"].([]any)
	if !strings.Contains(strings.Join(toStrings(steps[1].(map[string]any)["columns"]), ","), "m: f64") {
		t.Errorf("steps %v", steps)
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestFormatAndReference(t *testing.T) {
	res := callTool(t, "format_glr", map[string]any{"script": "FILTER a>1\n"})
	if text(res) != "filter a > 1" {
		t.Errorf("format %q", text(res))
	}
	if got := text(callTool(t, "glr_reference", map[string]any{"name": "str.to_date"})); !strings.Contains(got, "str.to_date(x, format: str") {
		t.Errorf("reference %q", got)
	}
	if got := text(callTool(t, "glr_reference", map[string]any{"name": "groupby"})); !strings.Contains(got, "groupby <k1") {
		t.Errorf("reference %q", got)
	}
	if got := callTool(t, "glr_reference", map[string]any{"name": "grupby"}); !strings.Contains(got["err"].(string), `did you mean "groupby"`) {
		t.Errorf("reference typo %v", got)
	}
	if got := text(callTool(t, "glr_reference", map[string]any{"namespace": "dt"})); !strings.Contains(got, "dt.year(x)") {
		t.Errorf("namespace listing %q", got)
	}
}

func TestRunGlr(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the golars binary")
	}
	bin := filepath.Join(t.TempDir(), "golars")
	if out, err := exec.Command("go", "build", "-o", bin, "../golars").CombinedOutput(); err != nil {
		t.Fatalf("build golars: %v\n%s", err, out)
	}
	t.Setenv("GOLARS_BIN", bin)
	res := callTool(t, "run_glr", map[string]any{
		"script": "load data/salaries.csv\nwith big = amount > 90000\nsort amount desc\n", "dir": exampleData, "max_rows": 2})
	sc, _ := res["structuredContent"].(map[string]any)
	if sc == nil || sc["ok"] != true {
		t.Fatalf("run: %v", res)
	}
	table := sc["table"].(map[string]any)
	if strings.Join(toStrings(table["columns"]), ",") != "name,amount,big" || len(table["rows"].([]any)) != 2 || sc["truncated"] != true {
		t.Errorf("table %v", sc)
	}
	if !strings.Contains(text(res), "brian,150000,true") {
		t.Errorf("text %q", text(res))
	}
	res = callTool(t, "run_glr", map[string]any{"script": "load data/salaries.csv\nfilter amout > 1\n", "dir": exampleData})
	if res["isError"] != true || !strings.Contains(text(res), `did you mean "amount"`) {
		t.Errorf("static error %v", res)
	}
	res = callTool(t, "run_glr", map[string]any{"script": "load data/salaries.csv\nsave /nonexistent/dir/x.csv\n", "dir": exampleData})
	if res["isError"] != true {
		t.Errorf("runtime error %v", res)
	}
	_ = os.Remove(bin)
}
