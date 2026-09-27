package csv_test

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
)

// genNumericCSV returns a CSV with an int64 id, an int64 value and two
// float64 columns.
func genNumericCSV(rows int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	var buf bytes.Buffer
	buf.Grow(rows * 48)
	buf.WriteString("id,a,x,y\n")
	var tmp []byte
	for i := range rows {
		tmp = strconv.AppendInt(tmp[:0], int64(i), 10)
		tmp = append(tmp, ',')
		tmp = strconv.AppendInt(tmp, r.Int64N(1<<20), 10)
		tmp = append(tmp, ',')
		tmp = strconv.AppendFloat(tmp, r.Float64()*1000, 'f', 4, 64)
		tmp = append(tmp, ',')
		tmp = strconv.AppendFloat(tmp, r.NormFloat64(), 'g', -1, 64)
		tmp = append(tmp, '\n')
		buf.Write(tmp)
	}
	return buf.Bytes()
}

var benchCities = []string{"tokyo", "paris", "new york", "berlin", "lima", "cairo", "oslo", "delhi"}

// genMixedCSV returns a CSV with ints, floats, strings of low and high
// cardinality and a bool column.
func genMixedCSV(rows int) []byte {
	r := rand.New(rand.NewPCG(3, 4))
	var buf bytes.Buffer
	buf.Grow(rows * 56)
	buf.WriteString("id,name,city,value,flag\n")
	var tmp []byte
	for i := range rows {
		tmp = strconv.AppendInt(tmp[:0], int64(i), 10)
		tmp = append(tmp, ",user_"...)
		tmp = strconv.AppendInt(tmp, r.Int64N(100_000), 10)
		tmp = append(tmp, ',')
		tmp = append(tmp, benchCities[r.IntN(len(benchCities))]...)
		tmp = append(tmp, ',')
		tmp = strconv.AppendFloat(tmp, r.Float64()*100, 'f', 2, 64)
		tmp = append(tmp, ',')
		tmp = strconv.AppendBool(tmp, r.IntN(2) == 0)
		tmp = append(tmp, '\n')
		buf.Write(tmp)
	}
	return buf.Bytes()
}

func benchReadFile(b *testing.B, data []byte) {
	path := filepath.Join(b.TempDir(), "bench.csv")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		df, err := iocsv.ReadFile(ctx, path)
		if err != nil {
			b.Fatal(err)
		}
		df.Release()
	}
}

func BenchmarkReadNumeric1M(b *testing.B) { benchReadFile(b, genNumericCSV(1<<20)) }
func BenchmarkReadMixed1M(b *testing.B)   { benchReadFile(b, genMixedCSV(1<<20)) }
