package json_test

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	"github.com/Gaurav-Gosain/golars/series"
)

func benchFrame(b *testing.B, n int) *dataframe.DataFrame {
	b.Helper()
	r := rand.New(rand.NewPCG(1, 2))
	a := make([]int64, n)
	x := make([]float64, n)
	s := make([]string, n)
	for i := range n {
		a[i] = r.Int64N(1 << 20)
		x[i] = r.Float64() * 100
		s[i] = "key" + strconv.Itoa(r.IntN(1000))
	}
	c1, _ := series.FromInt64("a", a, nil)
	c2, _ := series.FromFloat64("x", x, nil)
	c3, _ := series.FromString("s", s, nil)
	df, err := dataframe.New(c1, c2, c3)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func BenchmarkNDJSONWrite256K(b *testing.B) {
	df := benchFrame(b, 1<<18)
	defer df.Release()
	ctx := context.Background()
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		if err := iojson.WriteNDJSON(ctx, &buf, df); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(buf.Len()))
}

func BenchmarkNDJSONRead256K(b *testing.B) {
	df := benchFrame(b, 1<<18)
	ctx := context.Background()
	var buf bytes.Buffer
	if err := iojson.WriteNDJSON(ctx, &buf, df); err != nil {
		b.Fatal(err)
	}
	df.Release()
	b.SetBytes(int64(buf.Len()))
	b.ReportAllocs()
	for b.Loop() {
		got, err := iojson.ReadNDJSON(ctx, bytes.NewReader(buf.Bytes()))
		if err != nil {
			b.Fatal(err)
		}
		got.Release()
	}
}
