package analysis

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const exampleDir = "../../examples/script"

func examples(t testing.TB) map[string]string {
	t.Helper()
	paths, _ := filepath.Glob(filepath.Join(exampleDir, "*.glr"))
	if len(paths) == 0 {
		t.Fatal("no examples")
	}
	out := map[string]string{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		out[p] = string(b)
	}
	return out
}

// The example scripts run clean, so the analyser must not report
// anything in them. They reference data by repo-relative paths, which
// Resolve finds by walking up from the example directory.
func TestExamplesClean(t *testing.T) {
	for path, src := range examples(t) {
		r := Analyze(src, Options{Dir: exampleDir})
		if len(r.Diags) > 0 {
			t.Errorf("%s:\n%s", path, r.Render(path))
		}
	}
}

func TestSchemaTracking(t *testing.T) {
	src := `load examples/script/data/salaries.csv
with monthly = amount / 12, big = amount > 100000
with upper = name.str.to_uppercase()
select name, monthly, big, upper
filter big
limit 2
`
	r := Analyze(src, Options{Dir: exampleDir})
	if len(r.Diags) > 0 {
		t.Fatal(r.Render("t.glr"))
	}
	last := r.Steps[len(r.Steps)-1].After
	got := fmt.Sprintf("%v %s", last.Columns(), last.Shape())
	if want := "[name monthly big upper] ≤2 × 4"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	for col, dt := range map[string]string{"monthly": "f64", "big": "bool", "upper": "str"} {
		if got := last.DType(col); got != dt {
			t.Errorf("%s: dtype %q, want %q", col, got, dt)
		}
	}
}

func TestLintFindings(t *testing.T) {
	src := `filter x > 1
load examples/script/data/salaries.csv as base
stash unused
use missing
use base
filter amout > 10
with y = str.to_uppercse(name)
with z = round(amount, 1, 2)
sort salary desc
with n = name + 1
filter amount
load examples/script/data/salaris.csv
cast amount i128
join base on nme
groupby name amount:summ
`
	r := Analyze(src, Options{Dir: exampleDir})
	var got []string
	for _, d := range r.Diags {
		line := r.File.Lines[d.Line]
		fix := ""
		if d.Fix != nil {
			fix = " fix=" + d.Fix.Text
		}
		got = append(got, fmt.Sprintf("%d %s %q%s", d.Line+1, d.Code, line[d.Col:min(d.EndCol, len(line))], fix))
	}
	want := []string{
		`1 no-frame "filter" fix=load data/events.csv`,
		`3 unused-stash "unused"`,
		`4 unknown-frame "missing"`,
		`6 unknown-column "amout" fix=amount`,
		`7 bad-call "to_uppercse" fix=to_uppercase`,
		`8 bad-call "round(amount, 1, 2)"`,
		`9 unknown-column "salary"`,
		`10 type-error "name + 1"`,
		`11 not-boolean "amount"`,
		`12 missing-file "examples/script/data/salaris.csv" fix=examples/script/data/salaries.csv`,
		`13 unknown-dtype "i128" fix=i8`,
		`14 unknown-column "nme" fix=name`,
		`15 unknown-option "summ" fix=sum`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s\n\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"), r.Render("t.glr"))
	}
}

func TestSymbols(t *testing.T) {
	src := `load examples/script/data/salaries.csv as s
use s
with m = amount / 12
filter m > 1
stash out
use out
`
	r := Analyze(src, Options{Dir: exampleDir})
	var defs, refs []string
	for _, s := range r.Symbols {
		entry := fmt.Sprintf("%s@%d:%d", s.Name, s.Line+1, s.Col)
		if s.Def {
			defs = append(defs, entry)
		} else {
			target := "?"
			if s.Target != nil {
				target = fmt.Sprintf("%d:%d", s.Target.Line+1, s.Target.Col)
			}
			refs = append(refs, entry+"->"+target)
		}
	}
	if got, want := strings.Join(defs, " "), "s@1:42 m@3:5 out@5:6"; got != want {
		t.Errorf("defs %s, want %s", got, want)
	}
	if got, want := strings.Join(refs, " "), "s@2:4->1:42 amount@3:9->1:5 m@4:7->3:5 out@6:4->5:6"; got != want {
		t.Errorf("refs %s, want %s", got, want)
	}
}

// A 500-line script analyses well within the editor latency budget.
func TestLargeScriptLatency(t *testing.T) {
	src := largeScript(500)
	Analyze(src, Options{Dir: exampleDir}) // warm the file cache
	start := time.Now()
	r := Analyze(src, Options{Dir: exampleDir})
	elapsed := time.Since(start)
	if len(r.Diags) > 0 {
		t.Fatal(r.Render("big.glr"))
	}
	t.Logf("analyzed %d lines in %s", strings.Count(src, "\n"), elapsed)
	if elapsed > 200*time.Millisecond && !testing.Short() {
		t.Errorf("analysis took %s", elapsed)
	}
}

func largeScript(lines int) string {
	var b strings.Builder
	b.WriteString("load examples/script/data/salaries.csv\n")
	for i := 1; b.Len() == 0 || strings.Count(b.String(), "\n") < lines; i++ {
		switch i % 5 {
		case 0:
			fmt.Fprintf(&b, "with c%d = amount * %d + 1\n", i, i)
		case 1:
			fmt.Fprintf(&b, "filter amount > %d and name.str.len_chars() > 0\n", i)
		case 2:
			fmt.Fprintf(&b, "with s%d = str.to_uppercase(name)\n", i)
		case 3:
			b.WriteString("# a comment line\n")
		case 4:
			fmt.Fprintf(&b, "with w%d = when amount > %d then \"hi\" otherwise \"lo\"\n", i, i)
		}
	}
	return b.String()
}

func BenchmarkAnalyze500(b *testing.B) {
	src := largeScript(500)
	Analyze(src, Options{Dir: exampleDir})
	b.ResetTimer()
	for range b.N {
		Analyze(src, Options{Dir: exampleDir})
	}
}
