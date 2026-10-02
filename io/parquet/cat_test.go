package parquet_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func catPQCells(s *series.Series) string {
	a := s.Chunk(0)
	out := make([]string, a.Len())
	for i := range out {
		if a.IsNull(i) {
			out[i] = "<null>"
		} else {
			out[i] = a.ValueStr(i)
		}
	}
	return strings.Join(out, ",")
}

func TestParquetCategoricalRoundTrip(t *testing.T) {
	ctx := context.Background()
	c, err := series.NewCategorical("c", []string{"b", "", "héllo", "b"}, []bool{true, false, true, true})
	if err != nil {
		t.Fatal(err)
	}
	e, err := series.NewEnum("e", []string{"lo", "hi", "lo", "hi"}, nil, dtype.Enum([]string{"lo", "mid", "hi"}))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(c, e)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	var buf bytes.Buffer
	if err := parquet.Write(ctx, &buf, df); err != nil {
		t.Fatal(err)
	}
	got, err := parquet.ReadBytes(ctx, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	gc, _ := got.Column("c")
	if !gc.DType().IsCategorical() {
		t.Fatalf("c dtype = %s", gc.DType())
	}
	if s := catPQCells(gc); s != "b,<null>,héllo,b" {
		t.Fatalf("c = %s", s)
	}
	ge, _ := got.Column("e")
	if !ge.DType().IsEnum() {
		t.Fatalf("e dtype = %s", ge.DType())
	}
	if s := catPQCells(ge); s != "lo,hi,lo,hi" {
		t.Fatalf("e = %s", s)
	}
}
