// Command difftest runs random differential cases through golars and
// polars and reports every disagreement. See docs/testing.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Gaurav-Gosain/golars/internal/difftest"
)

func main() {
	var (
		n        = flag.Int("n", 1000, "number of cases")
		seed     = flag.Uint64("seed", 1, "first seed; case i uses seed+i")
		workers  = flag.Int("workers", 6, "parallel workers (one polars process each)")
		out      = flag.String("out", "", "output directory (default: a temp dir)")
		root     = flag.String("root", ".", "golars repository root")
		shrink   = flag.Bool("shrink", true, "shrink mismatches to minimal cases")
		maxShr   = flag.Int("max-shrink", 3, "shrink at most this many cases per signature")
		only     = flag.String("only", "", "run a single seed (overrides -n and -seed)")
		showKind = flag.String("show", "", "comma separated verdict kinds to print in full (default: all mismatches)")
		emit     = flag.String("emit", "", "write each shrunk mismatch as a regression JSON file into this directory")
		promote  = flag.String("promote", "", "re-check the regression files in this directory and mark passing ones fixed")
		emitName = flag.String("emit-name", "", "file name (without .json) for the regression file written in replay mode")
		litTrees = flag.Float64("literal-trees", difftest.LiteralTrees, "probability of keeping column-free subexpressions")
	)
	flag.Parse()
	difftest.LiteralTrees = *litTrees
	if *out == "" {
		d, err := os.MkdirTemp("", "difftest")
		if err != nil {
			fatal(err)
		}
		*out = d
	}
	python := difftest.DefaultPython(*root)
	runner := filepath.Join(*root, "bench", "polars-compare", "difftest", "runner.py")
	if *promote != "" {
		// Re-check regression files against golars (no Python needed)
		// and mark the ones that now pass as fixed.
		files, _ := filepath.Glob(filepath.Join(*promote, "*.json"))
		for _, f := range files {
			rc, err := difftest.LoadRegressCase(f)
			if err != nil {
				fatal(err)
			}
			c, pr, err := rc.Case()
			if err != nil {
				fatal(err)
			}
			c.Plan.Order = difftest.DeriveOrder(c.Plan.Ops)
			res := difftest.CheckWithOracleResult(c, pr)
			status := "known"
			if res.Verdict.OK() {
				status = "fixed"
			}
			if status != rc.Status {
				rc.Status = status
				if err := rc.Save(f); err != nil {
					fatal(err)
				}
			}
			fmt.Printf("%-6s %s %s\n", status, filepath.Base(f), firstLine(res.Verdict.Detail))
		}
		return
	}
	if flag.NArg() > 0 {
		// Replay case directories written by an earlier run.
		o, err := difftest.StartOracle(python, runner)
		if err != nil {
			fatal(err)
		}
		defer o.Close()
		for _, dir := range flag.Args() {
			c, err := difftest.LoadCaseDir(dir)
			if err != nil {
				fatal(err)
			}
			res := difftest.Check(c, filepath.Join(*out, "replay"), o)
			if *shrink && !res.Verdict.OK() {
				c = difftest.Shrink(c, res.Verdict, filepath.Join(*out, "replay"), o)
				res = difftest.Check(c, filepath.Join(*out, "replay"), o)
			}
			fmt.Printf("--- %s:\n%s\n", dir, report(c, res))
			if *emit != "" && res.Polars != nil {
				name := *emitName
				if name == "" {
					name = fmt.Sprintf("%s_%s", res.Verdict.Kind, filepath.Base(dir))
				}
				rc := difftest.NewRegressCase(name, c, res.Polars)
				rc.Note = firstLine(res.Verdict.Detail)
				if res.Verdict.OK() {
					rc.Status = "fixed"
				}
				_ = os.MkdirAll(*emit, 0o755)
				if err := rc.Save(filepath.Join(*emit, name+".json")); err != nil {
					fatal(err)
				}
			}
		}
		return
	}
	seeds := make([]uint64, *n)
	for i := range seeds {
		seeds[i] = *seed + uint64(i)
	}
	if *only != "" {
		var s uint64
		fmt.Sscan(*only, &s)
		seeds = []uint64{s}
	}

	type result struct {
		seed uint64
		c    *difftest.Case
		out  difftest.Outcome
	}
	jobs := make(chan uint64)
	results := make(chan result, 64)
	var wg sync.WaitGroup
	var done atomic.Int64
	start := time.Now()
	oracles := make([]*difftest.Oracle, *workers)
	for w := range *workers {
		o, err := difftest.StartOracle(python, runner)
		if err != nil {
			fatal(err)
		}
		oracles[w] = o
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir := filepath.Join(*out, fmt.Sprintf("w%d", w))
			for s := range jobs {
				c := difftest.NewCase(s)
				res := difftest.Check(c, dir, o)
				if !res.Verdict.OK() && res.Verdict.Kind != difftest.MUnsupported {
					// Keep a copy of the failing case.
					keep := filepath.Join(*out, "fail", string(res.Verdict.Kind), fmt.Sprint(s))
					_ = c.WriteDir(keep)
				}
				results <- result{seed: s, c: c, out: res}
				if d := done.Add(1); d%500 == 0 {
					fmt.Fprintf(os.Stderr, "%d cases, %.0f/s\n", d, float64(d)/time.Since(start).Seconds())
				}
			}
		}()
	}
	go func() {
		for _, s := range seeds {
			jobs <- s
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	counts := map[difftest.MismatchKind]int{}
	bySig := map[string][]result{}
	unsupported := map[string]int{}
	for r := range results {
		counts[r.out.Verdict.Kind]++
		switch {
		case r.out.Verdict.Kind == difftest.MUnsupported:
			unsupported[unsupportedKey(r.out.Verdict.Detail)]++
		case !r.out.Verdict.OK():
			sig := signature(r.out.Verdict)
			bySig[sig] = append(bySig[sig], r)
		}
	}
	elapsed := time.Since(start)

	show := map[string]bool{}
	for _, k := range strings.Split(*showKind, ",") {
		if k != "" {
			show[k] = true
		}
	}
	sigs := make([]string, 0, len(bySig))
	for s := range bySig {
		sigs = append(sigs, s)
	}
	sort.Slice(sigs, func(i, j int) bool { return len(bySig[sigs[i]]) > len(bySig[sigs[j]]) })
	type shrinkJob struct {
		sig string
		r   result
	}
	type shrunk struct {
		sig   string
		seed  uint64
		c     *difftest.Case
		final difftest.Outcome
		dir   string
	}
	var sjobs []shrinkJob
	for _, sig := range sigs {
		rs := bySig[sig]
		sort.Slice(rs, func(i, j int) bool { return rs[i].seed < rs[j].seed })
		if len(show) > 0 && !show[string(rs[0].out.Verdict.Kind)] {
			continue
		}
		limit := *maxShr
		switch rs[0].out.Verdict.Kind {
		case difftest.MValue, difftest.MSchema, difftest.MHeight:
			limit *= 10
		}
		for i, r := range rs {
			if i >= limit {
				break
			}
			sjobs = append(sjobs, shrinkJob{sig: sig, r: r})
		}
	}
	shrunkCh := make(chan shrunk, len(sjobs))
	jobCh := make(chan shrinkJob)
	var swg sync.WaitGroup
	for w, o := range oracles {
		swg.Add(1)
		go func() {
			defer swg.Done()
			dir := filepath.Join(*out, fmt.Sprintf("shrink%d", w))
			for j := range jobCh {
				c := j.r.c
				final := j.r.out
				if *shrink {
					c = difftest.Shrink(c, j.r.out.Verdict, dir, o)
					final = difftest.Check(c, dir, o)
				}
				if *emit != "" && final.Polars != nil && !final.Verdict.OK() {
					name := fmt.Sprintf("%s_%d", final.Verdict.Kind, j.r.seed)
					rc := difftest.NewRegressCase(name, c, final.Polars)
					rc.Note = firstLine(final.Verdict.Detail)
					_ = os.MkdirAll(*emit, 0o755)
					if err := rc.Save(filepath.Join(*emit, name+".json")); err != nil {
						fmt.Fprintln(os.Stderr, "emit:", err)
					}
				}
				keep := filepath.Join(*out, "min", string(final.Verdict.Kind), fmt.Sprint(j.r.seed))
				_ = c.WriteDir(keep)
				shrunkCh <- shrunk{sig: j.sig, seed: j.r.seed, c: c, final: final, dir: keep}
			}
		}()
	}
	for _, j := range sjobs {
		jobCh <- j
	}
	close(jobCh)
	swg.Wait()
	close(shrunkCh)
	groups := map[string][]shrunk{}
	var order []string
	for s := range shrunkCh {
		key := s.sig + " | " + shape(s.c, s.final.Verdict)
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], s)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i][:8] != order[j][:8] {
			return order[i] < order[j]
		}
		return len(groups[order[i]]) > len(groups[order[j]])
	})
	for _, key := range order {
		g := groups[key]
		sort.Slice(g, func(i, j int) bool { return len(g[i].c.Plan.Describe()) < len(g[j].c.Plan.Describe()) })
		var seeds []string
		for _, s := range g {
			seeds = append(seeds, fmt.Sprint(s.seed))
		}
		fmt.Printf("\n=== %s\n    %d shrunk cases (of %d with this signature), seeds %s\n", key, len(g), len(bySig[g[0].sig]), strings.Join(seeds, " "))
		fmt.Printf("--- smallest (%s):\n%s\n", g[0].dir, report(g[0].c, g[0].final))
	}
	for _, o := range oracles {
		o.Close()
	}

	fmt.Printf("\n%d cases in %s (%.0f cases/s), output in %s\n", len(seeds), elapsed.Round(time.Second), float64(len(seeds))/elapsed.Seconds(), *out)
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, string(k))
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		name := k
		if name == "" {
			name = "agree"
		}
		fmt.Printf("  %-16s %d\n", name, counts[difftest.MismatchKind(k)])
	}
	if len(unsupported) > 0 {
		fmt.Println("unsupported by golars:")
		keys := make([]string, 0, len(unsupported))
		for k := range unsupported {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return unsupported[keys[i]] > unsupported[keys[j]] })
		for _, k := range keys {
			fmt.Printf("  %5d  %s\n", unsupported[k], k)
		}
	}
}

func report(c *difftest.Case, o difftest.Outcome) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s\n", o.Verdict.Kind, trim(o.Verdict.Detail, 1500))
	b.WriteString(c.Plan.Describe())
	b.WriteString("\nleft:\n" + c.Left.Describe(40))
	if c.Right != nil {
		b.WriteString("right:\n" + c.Right.Describe(40))
	}
	if o.Polars != nil && o.Polars.OK {
		b.WriteString("polars result:\n" + describeCols(o.Polars.Cols))
	}
	if o.Golars != nil {
		b.WriteString("golars result:\n" + describeCols(o.Golars))
	}
	return b.String()
}

func describeCols(cols []difftest.ResultCol) string {
	var b strings.Builder
	for _, c := range cols {
		vals := c.Values
		suffix := ""
		if len(vals) > 30 {
			vals = vals[:30]
			suffix = fmt.Sprintf(" ... (%d rows)", len(c.Values))
		}
		fmt.Fprintf(&b, "  %s: %s = %s%s\n", c.Name, c.DType, difftest.FormatValue(vals), suffix)
	}
	return b.String()
}

func trim(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

var (
	numRe   = regexp.MustCompile(`-?\d+(\.\d+)?(e[-+]?\d+)?`)
	quoteRe = regexp.MustCompile(`"[^"]*"`)
	aliasRe = regexp.MustCompile(`\bo\d+\b|\bc\d+\b`)
)

// signature groups mismatches that are likely the same bug.
func signature(v difftest.Verdict) string {
	d := v.Detail
	if i := strings.Index(d, "\ngoroutine"); i >= 0 {
		d = d[:i]
	}
	if v.Kind == difftest.MPanic {
		// Group panics by message and the first golars frame.
		lines := strings.Split(v.Detail, "\n")
		loc, msg := "", ""
		for i, l := range lines {
			if msg == "" && strings.HasPrefix(l, "golars panicked:") {
				msg = numRe.ReplaceAllString(l, "N")
			}
			if strings.Contains(l, "Gaurav-Gosain/golars/") && !strings.Contains(l, "difftest") && i+1 < len(lines) && strings.Contains(lines[i+1], ".go:") {
				fn := strings.TrimSpace(l)
				if j := strings.IndexByte(fn, '('); j > 0 && !strings.HasPrefix(fn[j:], "(*") {
					fn = fn[:j]
				} else if j := strings.LastIndexByte(fn, '('); j > 0 {
					fn = fn[:j]
				}
				file := strings.TrimSpace(lines[i+1])
				if j := strings.LastIndex(file, "/golars/"); j >= 0 {
					file = file[j+len("/golars/"):]
				}
				if j := strings.Index(file, " +0x"); j >= 0 {
					file = file[:j]
				}
				loc = strings.TrimPrefix(fn, "github.com/Gaurav-Gosain/golars/") + " " + file
				break
			}
		}
		return string(v.Kind) + " " + msg + " @ " + loc
	}
	d = firstLine(d)
	d = quoteRe.ReplaceAllString(d, `"_"`)
	d = aliasRe.ReplaceAllString(d, "_")
	d = numRe.ReplaceAllString(d, "N")
	if v.Kind == difftest.MSchema || v.Kind == difftest.MValue || v.Kind == difftest.MHeight {
		d = ""
	}
	return string(v.Kind) + " " + trim(d, 200)
}

func unsupportedKey(detail string) string {
	if i := strings.Index(detail, " [in "); i >= 0 {
		detail = detail[:i]
	}
	return trim(detail, 120)
}

// shape summarises a minimal case: its ops and the functions it calls.
func shape(c *difftest.Case, _ difftest.Verdict) string {
	var ops []string
	fns := map[string]bool{}
	visit := func(e *difftest.Expr) {
		e.Walk(func(x *difftest.Expr) {
			switch x.K {
			case "call", "fn", "bin":
				if x.Name != "alias" {
					fns[x.Name] = true
				}
			case "when", "not", "neg":
				fns[x.K] = true
			}
		})
	}
	for _, op := range c.Plan.Ops {
		ops = append(ops, op.Op)
		if op.Pred != nil {
			visit(op.Pred)
		}
		for _, e := range op.Exprs {
			visit(e)
		}
	}
	names := make([]string, 0, len(fns))
	for k := range fns {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(ops, ",") + " [" + strings.Join(names, " ") + "]"
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "difftest:", err)
	os.Exit(1)
}
