package csv_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars: pl.read_csv("a,b\n") is an empty frame with two str columns.
func TestReadHeaderOnly(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df, err := csv.Read(context.Background(), strings.NewReader("a,b\n"), csv.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Height() != 0 || df.Width() != 2 {
		t.Fatalf("shape = %dx%d, want 0x2", df.Height(), df.Width())
	}
	if got := df.Schema().String(); got != "schema{a: str, b: str}" {
		t.Errorf("schema = %s", got)
	}
}

// polars write_csv writes a null as an empty field, not NULL:
// pl.DataFrame({"k": ["a", None], "n": [1, None]}).write_csv() == "k,n\na,1\n,\n".
func TestWriteNullAsEmptyField(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	k, _ := series.FromString("k", []string{"a", ""}, []bool{true, false}, series.WithAllocator(mem))
	n, _ := series.FromInt64("n", []int64{1, 0}, []bool{true, false}, series.WithAllocator(mem))
	df, err := dataframe.New(k, n)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	var buf bytes.Buffer
	if err := csv.Write(context.Background(), &buf, df); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "k,n\na,1\n,\n"; got != want {
		t.Errorf("csv = %q, want %q", got, want)
	}
	back, err := csv.Read(context.Background(), &buf, csv.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	col, _ := back.Column("n")
	if col.NullCount() != 1 {
		t.Errorf("round-trip n nulls = %d, want 1", col.NullCount())
	}
}
