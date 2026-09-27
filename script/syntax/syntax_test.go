package syntax

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script"
)

func exampleScripts(t testing.TB) map[string]string {
	t.Helper()
	paths, err := filepath.Glob("../../examples/script/*.glr")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no example scripts: %v", err)
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

// docSnippets returns the ```glr blocks of docs/scripting.md.
func docSnippets(t testing.TB) []string {
	t.Helper()
	b, err := os.ReadFile("../../docs/scripting.md")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range regexp.MustCompile("(?s)```glr\n(.*?)```").FindAllStringSubmatch(string(b), -1) {
		out = append(out, m[1])
	}
	return out
}

func TestExamplesParseClean(t *testing.T) {
	for path, src := range exampleScripts(t) {
		f := Parse(src)
		for _, st := range f.Stmts {
			for _, d := range st.Diags {
				t.Error(Render(path, f.Lines, d))
			}
		}
	}
	for i, src := range docSnippets(t) {
		f := Parse(src)
		for _, st := range f.Stmts {
			for _, d := range st.Diags {
				t.Error(Render(fmt.Sprintf("docs snippet %d", i), f.Lines, d))
			}
		}
	}
}

// Every example in script.Commands parses against its own grammar.
func TestSpecExamplesParse(t *testing.T) {
	for _, spec := range script.Commands {
		for _, ex := range spec.Examples {
			f := Parse(ex)
			for _, st := range f.Stmts {
				for _, d := range st.Diags {
					t.Errorf("%s example %q: %s", spec.Name, ex, d.Msg)
				}
			}
		}
	}
}

func TestStatementParts(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want string
	}{
		{`load data/x.csv as trades`, `cmd:load path:data/x.csv kw:as newframe:trades`},
		{`.LOAD "my file.csv"`, `cmd:load path:my file.csv`},
		{`join other on id left`, `cmd:join target:other kw:on col:id opt:left`},
		{`drop a, b c`, `cmd:drop list:[a b] list:[c]`},
		{`select a, b`, `cmd:select list:[a b]`},
		{`select my-col other`, `cmd:select list:[my-col] list:[other]`},
		{`select a, total = x + y`, `cmd:select expr:a punct:, newcol:total punct:= expr:x + y`},
		{`select a * 2`, `cmd:select expr:a * 2`},
		{`with a = x + 1, b = dt.year(ts)`, `cmd:with newcol:a punct:= expr:x + 1 newcol:b punct:= expr:dt.year(ts) punct:,`},
		{`filter a > 1 and b in ["x"]`, `cmd:filter expr:a > 1 and b in ["x"]`},
		{`sort a desc, b`, `cmd:sort col:a kw:desc col:b`},
		{`groupby dept, region amount:sum:total n = len() (m = x.max())`,
			`cmd:groupby list:[dept region] agg:[col:amount opt:sum newcol:total] newcol:n punct:= expr:len() newcol:m punct:= expr:x.max()`},
		{`group_by_dynamic ts every 1h by user amount:sum`,
			`cmd:group_by_dynamic col:ts kw:every dur:1h kw:by list:[user] agg:[col:amount opt:sum]`},
		{`join_asof rates on ts by user nearest tolerance 2h`,
			`cmd:join_asof target:rates kw:on col:ts kw:by list:[user] opt:nearest kw:tolerance value:2h`},
		{`to_dummies drop_first`, `cmd:to_dummies kw:drop_first`},
		{`to_dummies a, b drop_first`, `cmd:to_dummies list:[a b] kw:drop_first`},
		{`fill_null "n/a"`, `cmd:fill_null value:n/a`},
		{`cast x datetime[ms]`, `cmd:cast col:x dtype:datetime[ms]`},
		{`rename a as b`, `cmd:rename col:a kw:as newcol:b`},
		{`pivot k on v sum`, `cmd:pivot list:[k] col:on col:v opt:sum`},
		{"filter a > 1 \\\n  and b < 2", `cmd:filter expr:a > 1    and b < 2`},
	} {
		f := Parse(tc.src)
		if len(f.Stmts) != 1 {
			t.Fatalf("%q: %d statements", tc.src, len(f.Stmts))
		}
		st := f.Stmts[0]
		for _, d := range st.Diags {
			t.Errorf("%q: unexpected diagnostic %s", tc.src, d.Msg)
		}
		if got := describeParts(st); got != tc.want {
			t.Errorf("%q\n got %s\nwant %s", tc.src, got, tc.want)
		}
	}
}

func describeParts(st *Stmt) string {
	names := map[PartKind]string{PartText: "text", PartCommand: "cmd", PartKeyword: "kw", PartOption: "opt",
		PartPath: "path", PartFrame: "frame", PartTarget: "target", PartNewFrame: "newframe", PartColumn: "col",
		PartNewColumn: "newcol", PartList: "list", PartAgg: "agg", PartCount: "count", PartNumber: "num",
		PartValue: "value", PartDType: "dtype", PartDuration: "dur", PartWord: "word", PartExpr: "expr", PartPunct: "punct"}
	one := func(p Part) string {
		if len(p.Items) > 0 {
			var items []string
			for _, it := range p.Items {
				if p.Kind == PartAgg {
					items = append(items, names[it.Kind]+":"+it.Value)
				} else {
					items = append(items, it.Value)
				}
			}
			return names[p.Kind] + ":[" + strings.Join(items, " ") + "]"
		}
		return names[p.Kind] + ":" + p.Value
	}
	out := []string{one(st.Cmd)}
	for _, p := range st.Parts {
		out = append(out, one(p))
	}
	return strings.Join(out, " ")
}

func TestStatementDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		src, span, msg, fix string
	}{
		{`filtet x > 1`, "filtet", `unknown command "filtet"`, "filter"},
		{`load`, "", "load needs a file path", ""},
		{`limit ten`, "ten", `expected a non-negative integer, got "ten"`, ""},
		{`join x on id outer`, "outer", `unknown join option "outer"`, ""},
		{`join x on id lefft`, "lefft", `unknown join option "lefft"`, "left"},
		{`join x id`, "id", "expected `on`", ""},
		{`cast x i128`, "i128", `unknown dtype "i128"`, "i8"},
		{`with x amount + 1`, "x amount + 1", "expected NAME = EXPR", ""},
		{`with x = amount +`, "+", "missing operand after +", ""},
		{`filter a = 1`, "=", "unexpected '='", "=="},
		{`groupby k amount:summ`, "summ", `unknown aggregation "summ"`, "sum"},
		{`groupby k amount`, "amount", "expected col:op[:alias]", ""},
		{`group_by_dynamic ts evry 1h a:sum`, "", "group_by_dynamic needs `every DURATION`", ""},
		{`group_by_dynamic ts every 1x a:sum`, "1x", `invalid duration "1x"`, ""},
		{`upsample ts 5m minutes`, "minutes", `unexpected argument "minutes"`, ""},
		{`join_asof r on ts neerest`, "neerest", `unexpected "neerest"`, "nearest"},
		{`sort desc`, "desc", "must follow a column", ""},
		{`schema now`, "now", `unexpected argument "now"`, ""},
	} {
		f := Parse(tc.src)
		st := f.Stmts[0]
		if len(st.Diags) == 0 {
			t.Errorf("%q: no diagnostic", tc.src)
			continue
		}
		d := st.Diags[0]
		line := f.Lines[d.Line]
		if got := line[d.Col:min(d.EndCol, len(line))]; got != tc.span {
			t.Errorf("%q: span %q, want %q (%s)", tc.src, got, tc.span, d.Msg)
		}
		if !strings.Contains(d.Msg, tc.msg) {
			t.Errorf("%q: message %q does not contain %q", tc.src, d.Msg, tc.msg)
		}
		gotFix := ""
		if d.Fix != nil {
			gotFix = d.Fix.Text
		}
		if gotFix != tc.fix {
			t.Errorf("%q: fix %q, want %q", tc.src, gotFix, tc.fix)
		}
	}
}

func TestRenderCaret(t *testing.T) {
	src := "load x.csv\nfilter salry > 100\n"
	f := Parse(src)
	st := f.Stmts[1]
	d := st.DiagAt(7, 12, SevError, "unknown-column", `unknown column "salry"`)
	d.Hint = `did you mean "salary"?`
	want := "s.glr:2:8: error: unknown column \"salry\"\n" +
		"  filter salry > 100\n" +
		"         ^^^^^\n" +
		"  hint: did you mean \"salary\"?\n"
	if got := Render("s.glr", f.Lines, d); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

func TestContinuationPositions(t *testing.T) {
	src := "filter a > 1 \\\n  and bb < 2\n"
	f := Parse(src)
	st := f.Stmts[0]
	off := strings.Index(st.Text, "bb")
	line, col := st.Position(off)
	if line != 1 || col != 6 {
		t.Fatalf("bb at %d:%d, want 1:6", line, col)
	}
	if got := st.Offset(1, 6); got != off {
		t.Fatalf("Offset(1, 6) = %d, want %d", got, off)
	}
}

func TestFormat(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{".LOAD  data/x.csv   as  t\n", "load data/x.csv as t\n"},
		{"filter a>1 AND b=='x'   # keep\n", "filter a > 1 and b == 'x' # keep\n"},
		{"with total=price*qty\nwith n = len()\n", "with total = price * qty\nwith n     = len()\n"},
		{"load a.csv as a\nload bigger.csv as b\n", "load a.csv      as a\nload bigger.csv as b\n"},
		{"select a,b ,c\n", "select a, b, c\n"},
		{"groupby k,j amount:SUM:t n = len() total = x * 2\n", "groupby k, j amount:sum:t n=len() (total = x * 2)\n"},
		{"sort a DESC b\n", "sort a desc b\n"},
		{"\n\n\nshow\n\n\n\nhead 5\n\n", "show\n\nhead 5\n"},
		{"filter a > 1 \\\n   and b < 2\n", "filter a > 1 \\\n  and b < 2\n"},
		{"filtet  x\n", "filtet  x\n"},
		{"head 5 # a\nshow   # bb\n", "head 5 # a\nshow   # bb\n"},
		{"join x on id LEFT\n", "join x on id left\n"},
		{"with a = cut(x,[1,2],labels=['a','b','c'])\n", "with a = cut(x, [1, 2], labels=['a', 'b', 'c'])\n"},
	} {
		got := Format(tc.in)
		if got != tc.want {
			t.Errorf("Format(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
		if again := Format(got); again != got {
			t.Errorf("Format is not idempotent on %q: %q", got, again)
		}
	}
}

func TestFormatExamplesIdempotent(t *testing.T) {
	for path, src := range exampleScripts(t) {
		if Format(src) != src {
			t.Errorf("%s is not in canonical form: run golars fmt -w %s", path, path)
		}
		once := Format(src)
		if twice := Format(once); twice != once {
			t.Errorf("%s: format is not idempotent", path)
		}
		if a, b := structure(src), structure(once); a != b {
			t.Errorf("%s: formatting changed the statements:\n%s\n---\n%s", path, a, b)
		}
	}
}

// structure renders the statements of src without positions or
// spacing, so two scripts with the same meaning compare equal.
func structure(src string) string {
	f := Parse(src)
	var b strings.Builder
	for _, st := range f.Stmts {
		if len(st.Diags) > 0 {
			// Broken statements are kept as written (trimmed).
			fmt.Fprintf(&b, "broken %q\n", strings.Join(strings.Fields(st.Text), " "))
			continue
		}
		b.WriteString(st.Cmd.Value)
		for _, p := range st.Parts {
			if p.Kind == PartExpr {
				b.WriteString(" {" + dumpExpr(p) + "}")
				continue
			}
			if p.Kind == PartList {
				// `a b` and `a, b` name the same columns.
				for _, it := range p.Items {
					fmt.Fprintf(&b, " item:%s", it.Value)
				}
				continue
			}
			fmt.Fprintf(&b, " %d:%s", p.Kind, formatPart(p))
			for _, it := range p.Items {
				fmt.Fprintf(&b, "/%s", it.Value)
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func dumpExpr(p Part) string {
	// Formatting is canonical, so the formatted expression identifies
	// the tree.
	return formatExpr(p)
}
