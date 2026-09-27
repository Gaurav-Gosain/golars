package json_test

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	"github.com/Gaurav-Gosain/golars/series"
)

func writeFixture(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	mem := testutil.NewCheckedAllocator(t)
	b, err := series.FromFloat64("b", []float64{1, math.NaN(), math.Inf(1)}, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	a, err := series.FromString("a", []string{"x", "", "z"}, []bool{true, false, true}, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(b, a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(df.Release)
	return df
}

// Keys follow column order (not alphabetical) and non-finite floats
// become null, as polars write_json does.
func TestWriteMatchesPolars(t *testing.T) {
	df := writeFixture(t)
	var buf bytes.Buffer
	if err := iojson.Write(context.Background(), &buf, df); err != nil {
		t.Fatal(err)
	}
	want := `[{"b":1,"a":"x"},{"b":null,"a":null},{"b":null,"a":"z"}]` + "\n"
	if buf.String() != want {
		t.Errorf("got  %s\nwant %s", buf.String(), want)
	}
}

func TestWriteNDJSONMatchesPolars(t *testing.T) {
	df := writeFixture(t)
	var buf bytes.Buffer
	if err := iojson.WriteNDJSON(context.Background(), &buf, df); err != nil {
		t.Fatal(err)
	}
	want := "{\"b\":1,\"a\":\"x\"}\n{\"b\":null,\"a\":null}\n{\"b\":null,\"a\":\"z\"}\n"
	if buf.String() != want {
		t.Errorf("got  %q\nwant %q", buf.String(), want)
	}
}

func TestWriteEmptyFrame(t *testing.T) {
	df := writeFixture(t)
	empty := df.Head(0)
	defer empty.Release()
	var buf bytes.Buffer
	if err := iojson.Write(context.Background(), &buf, empty); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "[]\n" {
		t.Errorf("got %q, want []", buf.String())
	}
	buf.Reset()
	if err := iojson.WriteNDJSON(context.Background(), &buf, empty); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("ndjson of an empty frame should be empty, got %q", buf.String())
	}
}

func TestWriteHonoursCancelledContext(t *testing.T) {
	df := writeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := iojson.Write(ctx, &bytes.Buffer{}, df); err == nil {
		t.Error("Write should fail on a cancelled context")
	}
}
