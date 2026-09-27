package parquet_test

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func benchNumericDF(b *testing.B, rows int) *dataframe.DataFrame {
	b.Helper()
	r := rand.New(rand.NewPCG(1, 2))
	id := make([]int64, rows)
	a := make([]int64, rows)
	x := make([]float64, rows)
	y := make([]float64, rows)
	for i := range rows {
		id[i] = int64(i)
		a[i] = r.Int64N(1 << 20)
		x[i] = r.Float64() * 1000
		y[i] = r.NormFloat64()
	}
	s1, _ := series.FromInt64("id", id, nil)
	s2, _ := series.FromInt64("a", a, nil)
	s3, _ := series.FromFloat64("x", x, nil)
	s4, _ := series.FromFloat64("y", y, nil)
	df, err := dataframe.New(s1, s2, s3, s4)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

var benchCities = []string{"tokyo", "paris", "new york", "berlin", "lima", "cairo", "oslo", "delhi"}

func benchMixedDF(b *testing.B, rows int) *dataframe.DataFrame {
	b.Helper()
	r := rand.New(rand.NewPCG(3, 4))
	id := make([]int64, rows)
	name := make([]string, rows)
	city := make([]string, rows)
	val := make([]float64, rows)
	for i := range rows {
		id[i] = int64(i)
		name[i] = "user_" + strconv.Itoa(r.IntN(100_000))
		city[i] = benchCities[r.IntN(len(benchCities))]
		val[i] = r.Float64() * 100
	}
	s1, _ := series.FromInt64("id", id, nil)
	s2, _ := series.FromString("name", name, nil)
	s3, _ := series.FromString("city", city, nil)
	s4, _ := series.FromFloat64("value", val, nil)
	df, err := dataframe.New(s1, s2, s3, s4)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func benchWrite(b *testing.B, df *dataframe.DataFrame) {
	defer df.Release()
	ctx := context.Background()
	path := filepath.Join(b.TempDir(), "bench.parquet")
	b.ReportAllocs()
	for b.Loop() {
		if err := parquet.WriteFile(ctx, path, df); err != nil {
			b.Fatal(err)
		}
	}
	if st, err := os.Stat(path); err == nil {
		b.ReportMetric(float64(st.Size())/1e6, "fileMB")
	}
}

func benchRead(b *testing.B, df *dataframe.DataFrame) {
	ctx := context.Background()
	path := filepath.Join(b.TempDir(), "bench.parquet")
	if err := parquet.WriteFile(ctx, path, df); err != nil {
		b.Fatal(err)
	}
	df.Release()
	b.ReportAllocs()
	for b.Loop() {
		got, err := parquet.ReadFile(ctx, path)
		if err != nil {
			b.Fatal(err)
		}
		got.Release()
	}
}

func BenchmarkWriteNumeric1M(b *testing.B) { benchWrite(b, benchNumericDF(b, 1<<20)) }
func BenchmarkWriteMixed1M(b *testing.B)   { benchWrite(b, benchMixedDF(b, 1<<20)) }
func BenchmarkReadNumeric1M(b *testing.B)  { benchRead(b, benchNumericDF(b, 1<<20)) }
func BenchmarkReadMixed1M(b *testing.B)    { benchRead(b, benchMixedDF(b, 1<<20)) }
