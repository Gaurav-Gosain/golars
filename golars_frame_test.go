package golars_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/io/ipc"
)

func frameFixture(t *testing.T) *golars.DataFrame {
	t.Helper()
	a, _ := golars.FromInt64("a", []int64{1, 2, 3}, nil)
	s, _ := golars.FromString("s", []string{"x", "y", "z"}, nil)
	df, err := golars.NewDataFrame(a, s)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestFacadeSinksRoundTrip(t *testing.T) {
	ctx := context.Background()
	df := frameFixture(t)
	defer df.Release()
	dir := t.TempDir()
	lf := golars.Lazy(df).Filter(golars.Col("a").Gt(golars.Lit(int64(1))))
	want := "a:i64|s:str\n2|\"y\"\n3|\"z\""

	sinks := map[string]struct {
		sink func(string) error
		read func(string) (*golars.DataFrame, error)
	}{
		"csv": {func(p string) error { return golars.SinkCSV(ctx, lf, p) }, func(p string) (*golars.DataFrame, error) { return golars.ReadCSV(p) }},
		"parquet": {func(p string) error { return golars.SinkParquet(ctx, lf, p) }, func(p string) (*golars.DataFrame, error) {
			return golars.ReadParquet(p)
		}},
		"ipc":    {func(p string) error { return golars.SinkIPC(ctx, lf, p) }, func(p string) (*golars.DataFrame, error) { return golars.ReadIPC(p) }},
		"ndjson": {func(p string) error { return golars.SinkNDJSON(ctx, lf, p) }, func(p string) (*golars.DataFrame, error) { return golars.ReadNDJSON(p) }},
	}
	for name, s := range sinks {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "out."+name)
			if err := s.sink(path); err != nil {
				t.Fatal(err)
			}
			back, err := s.read(path)
			if err != nil {
				t.Fatal(err)
			}
			defer back.Release()
			// The NDJSON reader does not guarantee column order.
			ordered, err := back.Select("a", "s")
			if err != nil {
				t.Fatal(err)
			}
			defer ordered.Release()
			framerepr.AssertFrame(t, ordered, want)
		})
	}

	t.Run("ipc_stream", func(t *testing.T) {
		path := filepath.Join(dir, "out.arrows")
		if err := golars.WriteIPCStream(df, path); err != nil {
			t.Fatal(err)
		}
		if err := golars.SinkIPCStream(ctx, lf, filepath.Join(dir, "sink.arrows")); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{path, filepath.Join(dir, "sink.arrows")} {
			assertIPCStreamRows(t, p)
		}
	})
}

func assertIPCStreamRows(t *testing.T, path string) {
	t.Helper()
	f, err := openFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := ipc.NewStreamReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows := 0
	for batch, err := range r.Iter(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		rows += batch.Height()
		batch.Release()
	}
	if rows == 0 {
		t.Fatalf("%s: no rows read back", path)
	}
}

func TestFacadeSQL(t *testing.T) {
	ctx := context.Background()
	df := frameFixture(t)
	defer df.Release()
	out, err := golars.SQL(ctx, df, "SELECT a, s FROM self WHERE a >= 2")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	framerepr.AssertFrame(t, out, "a:i64|s:str\n2|\"y\"\n3|\"z\"")

	named, err := golars.SQL(ctx, df, "SELECT a FROM t WHERE a < 2", "t")
	if err != nil {
		t.Fatal(err)
	}
	defer named.Release()
	framerepr.AssertFrame(t, named, "a:i64\n1")

	lf := golars.SQLLazy(golars.Lazy(df).Filter(golars.Col("a").Gt(golars.Lit(int64(1)))), "SELECT s FROM self")
	cols, err := lf.Columns()
	if err != nil || len(cols) != 1 || cols[0] != "s" {
		t.Fatalf("SQLLazy columns = %v, %v", cols, err)
	}
	res, err := lf.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	framerepr.AssertFrame(t, res, "s:str\n\"y\"\n\"z\"")
}

func TestFacadeGroupByExprs(t *testing.T) {
	df := frameFixture(t)
	defer df.Release()
	out, err := golars.GroupByExprs(df, golars.Col("a").Gt(golars.Lit(int64(1))).Alias("big")).
		MaintainOrder().Len("").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	framerepr.AssertFrame(t, out, "big:bool|len:u32\nfalse|1\ntrue|2")
}

func openFile(path string) (*os.File, error) { return os.Open(path) }
