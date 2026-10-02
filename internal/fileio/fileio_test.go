package fileio

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func sampleFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	name, err := series.FromString("name", []string{"ada", "brian", ""}, []bool{true, true, false})
	if err != nil {
		t.Fatal(err)
	}
	age, err := series.FromInt64("age", []int64{27, 34, 19}, nil)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(name, age)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestFormatOf(t *testing.T) {
	cases := map[string]Format{
		"a.csv": CSV, "A.CSV": CSV, "x/y.tsv": TSV, "d.parquet": Parquet,
		"d.pq": Parquet, "d.arrow": IPC, "d.ipc": IPC, "d.json": JSON,
		"d.ndjson": NDJSON, "d.jsonl": NDJSON,
	}
	for path, want := range cases {
		got, ok := FormatOf(path)
		if !ok || got != want {
			t.Errorf("FormatOf(%q) = %q, %v; want %q", path, got, ok, want)
		}
	}
	for _, path := range []string{"noext", "a.txt", "a.csv.gz"} {
		if _, ok := FormatOf(path); ok {
			t.Errorf("FormatOf(%q) should fail", path)
		}
	}
	if f, ok := ParseFormat("arrow"); !ok || f != IPC {
		t.Errorf("ParseFormat(arrow) = %q, %v", f, ok)
	}
	if f, ok := ParseFormat(".jsonl"); !ok || f != NDJSON {
		t.Errorf("ParseFormat(.jsonl) = %q, %v", f, ok)
	}
}

func TestRoundTripEveryFormat(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	df := sampleFrame(t)
	defer df.Release()
	for _, ext := range Extensions() {
		t.Run(ext, func(t *testing.T) {
			path := filepath.Join(dir, "out."+ext)
			if err := Write(ctx, path, df); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := Read(ctx, path)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			defer got.Release()
			if got.Height() != 3 || got.Width() != 2 {
				t.Fatalf("shape = %dx%d, want 3x2", got.Height(), got.Width())
			}
			col, err := got.Column("name")
			if err != nil {
				t.Fatal(err)
			}
			// io/csv writes nulls as the literal "NULL" (polars writes an
			// empty field), so nulls do not survive a CSV round trip yet.
			if f, _ := FormatOf(path); f != CSV && f != TSV && col.NullCount() != 1 {
				t.Errorf("null count = %d, want 1", col.NullCount())
			}

			lf, err := Scan(path)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			scanned, err := lf.Collect(ctx)
			if err != nil {
				t.Fatalf("scan collect: %v", err)
			}
			defer scanned.Release()
			if scanned.Height() != 3 {
				t.Errorf("scan height = %d, want 3", scanned.Height())
			}
		})
	}
}

func TestUnsupportedExtension(t *testing.T) {
	ctx := context.Background()
	if _, err := Read(ctx, "data.txt"); err == nil || !strings.Contains(err.Error(), `".txt"`) {
		t.Errorf("Read error = %v", err)
	}
	if _, err := Read(ctx, "data"); err == nil || !strings.Contains(err.Error(), "no extension") {
		t.Errorf("Read error = %v", err)
	}
	if _, err := Scan("data.xlsx"); err == nil {
		t.Error("Scan should reject .xlsx")
	}
	df := sampleFrame(t)
	defer df.Release()
	if err := Write(ctx, filepath.Join(t.TempDir(), "x.bin"), df); err == nil {
		t.Error("Write should reject .bin")
	}
}

func TestWriteToFormats(t *testing.T) {
	ctx := context.Background()
	df := sampleFrame(t)
	defer df.Release()
	for _, f := range []Format{CSV, TSV, Parquet, IPC, JSON, NDJSON} {
		var buf bytes.Buffer
		if err := WriteTo(ctx, &buf, df, f); err != nil {
			t.Errorf("WriteTo(%s): %v", f, err)
			continue
		}
		if buf.Len() == 0 {
			t.Errorf("WriteTo(%s) wrote nothing", f)
		}
	}
	var buf bytes.Buffer
	if err := WriteTo(ctx, &buf, df, Format("xlsx")); err == nil {
		t.Error("WriteTo should reject unknown format")
	}
}
