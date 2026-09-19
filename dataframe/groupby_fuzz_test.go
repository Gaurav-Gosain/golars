package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// FuzzGroupByMultiKey cross-checks multi-key sum/mean/min/max against
// a brute-force oracle on adversarial small inputs (ties, null keys,
// null values, empty groups impossible by construction).
func FuzzGroupByMultiKey(f *testing.F) {
	f.Add([]byte{1, 2, 1, 3, 2, 1, 0, 4, 2, 3, 1, 0})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 || len(data) > 40 {
			t.Skip()
		}
		n := len(data) / 3
		if n == 0 {
			t.Skip()
		}
		k1 := make([]int64, n)
		k1valid := make([]bool, n)
		k2 := make([]int64, n)
		k2valid := make([]bool, n)
		vals := make([]int64, n)
		vvalid := make([]bool, n)
		for i := range n {
			k1[i] = int64(data[3*i] % 4)
			k1valid[i] = data[3*i]%5 != 0
			k2[i] = int64(data[3*i+1] % 3)
			k2valid[i] = data[3*i+1]%5 != 0
			vals[i] = int64(data[3*i+2])
			vvalid[i] = data[3*i+2]%5 != 0
		}
		s1, err := series.FromInt64("a", k1, k1valid)
		if err != nil {
			t.Fatal(err)
		}
		defer s1.Release()
		s2, err := series.FromInt64("b", k2, k2valid)
		if err != nil {
			t.Fatal(err)
		}
		defer s2.Release()
		v, err := series.FromInt64("v", vals, vvalid)
		if err != nil {
			t.Fatal(err)
		}
		defer v.Release()
		df, err := dataframe.New(s1, s2, v)
		if err != nil {
			t.Fatal(err)
		}
		defer df.Release()

		type acc struct {
			sum   int64
			count int64
			minV  int64
			maxV  int64
			has   bool
		}
		type key struct {
			a, b       int64
			hasA, hasB bool
		}
		oracle := map[key]*acc{}
		for i := range n {
			kk := key{k1[i], k2[i], k1valid[i], k2valid[i]}
			if !kk.hasA {
				kk.a = 0
			}
			if !kk.hasB {
				kk.b = 0
			}
			a, ok := oracle[kk]
			if !ok {
				a = &acc{}
				oracle[kk] = a
			}
			if !vvalid[i] {
				continue
			}
			x := vals[i]
			a.sum += x
			a.count++
			if !a.has || x < a.minV {
				a.minV = x
			}
			if !a.has || x > a.maxV {
				a.maxV = x
			}
			a.has = true
		}

		out, err := df.GroupBy("a", "b").Agg(context.Background(), []expr.Expr{
			expr.Col("v").Sum().Alias("s"),
			expr.Col("v").Mean().Alias("m"),
			expr.Col("v").Min().Alias("mi"),
			expr.Col("v").Max().Alias("ma"),
			expr.Col("v").Count().Alias("c"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		if out.Height() != len(oracle) {
			t.Fatalf("height=%d want %d", out.Height(), len(oracle))
		}
		kc, _ := out.Column("a")
		bc, _ := out.Column("b")
		sc, _ := out.Column("s")
		mc, _ := out.Column("m")
		mic, _ := out.Column("mi")
		mac, _ := out.Column("ma")
		cc, _ := out.Column("c")
		kArr := kc.Chunk(0).(*array.Int64)
		bArr := bc.Chunk(0).(*array.Int64)
		sArr := sc.Chunk(0).(*array.Int64)
		mArr := mc.Chunk(0).(*array.Float64)
		miArr := mic.Chunk(0).(*array.Int64)
		maArr := mac.Chunk(0).(*array.Int64)
		cArr := cc.Chunk(0).(*array.Int64)
		seen := map[key]bool{}
		for i := range out.Height() {
			kk := key{0, 0, kArr.IsValid(i), bArr.IsValid(i)}
			if kk.hasA {
				kk.a = kArr.Value(i)
			}
			if kk.hasB {
				kk.b = bArr.Value(i)
			}
			if seen[kk] {
				t.Fatalf("duplicate group %+v", kk)
			}
			seen[kk] = true
			a, ok := oracle[kk]
			if !ok {
				t.Fatalf("unknown group %+v", kk)
			}
			if sArr.Value(i) != a.sum {
				t.Fatalf("group %+v sum=%d want %d", kk, sArr.Value(i), a.sum)
			}
			if cArr.Value(i) != a.count {
				t.Fatalf("group %+v count=%d want %d", kk, cArr.Value(i), a.count)
			}
			if !a.has {
				if mArr.IsValid(i) || miArr.IsValid(i) || maArr.IsValid(i) {
					t.Fatalf("group %+v all-null must emit nulls", kk)
				}
				continue
			}
			if miArr.Value(i) != a.minV || maArr.Value(i) != a.maxV {
				t.Fatalf("group %+v min/max=%d/%d want %d/%d",
					kk, miArr.Value(i), maArr.Value(i), a.minV, a.maxV)
			}
			wantMean := float64(a.sum) / float64(a.count)
			if !mArr.IsValid(i) || mArr.Value(i) != wantMean {
				t.Fatalf("group %+v mean=%v want %v", kk, mArr.Value(i), wantMean)
			}
		}
	})
}
