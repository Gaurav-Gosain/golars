//go:build arm64 && !noasm

package compute

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

// randFloat64Slice mixes ordinary values with the special cases the
// kernels must pass through untouched: signed zeros, infinities, NaN
// and repeated values that make equality compares hit.
func randFloat64Slice(r *rand.Rand, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		switch r.IntN(10) {
		case 0:
			out[i] = math.NaN()
		case 1:
			out[i] = math.Inf(1 - 2*r.IntN(2))
		case 2:
			out[i] = math.Copysign(0, float64(1-2*r.IntN(2)))
		case 3:
			out[i] = float64(r.IntN(3))
		default:
			out[i] = r.NormFloat64() * 100
		}
	}
	return out
}

// smallInt64Slice draws from a tiny range so equal pairs are common.
func smallInt64Slice(r *rand.Rand, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		if r.IntN(4) == 0 {
			out[i] = randInt64Slice(r, 1)[0]
		} else {
			out[i] = int64(r.IntN(5) - 2)
		}
	}
	return out
}

func sameFloatBits(a, b float64) bool {
	if a != a && b != b {
		return true
	}
	return math.Float64bits(a) == math.Float64bits(b)
}

func refCompare[T int64 | float64](x, y T, op compareOp) bool {
	switch op {
	case opEq:
		return x == y
	case opNe:
		return x != y
	case opLt:
		return x < y
	case opLe:
		return x <= y
	case opGt:
		return x > y
	case opGe:
		return x >= y
	}
	panic("bad op")
}

var allCompareOps = []compareOp{opEq, opNe, opLt, opLe, opGt, opGe}

// checkBits verifies the first done rows of bits against want and that
// the kernel did not touch any byte past done/8.
func checkBits(t *testing.T, name string, bits []byte, done, n int, want func(i int) bool) {
	t.Helper()
	if done != n&^15 {
		t.Fatalf("%s n=%d: processed %d, want %d", name, n, done, n&^15)
	}
	for i := range done {
		got := bits[i>>3]&(1<<(i&7)) != 0
		if got != want(i) {
			t.Fatalf("%s n=%d: row %d got %v want %v", name, n, i, got, want(i))
		}
	}
	for i := done / 8; i < len(bits); i++ {
		if bits[i] != 0xA5 {
			t.Fatalf("%s n=%d: byte %d past the processed rows was written", name, n, i)
		}
	}
}

func poisonBits(n int) []byte {
	b := make([]byte, (n+7)/8+2)
	for i := range b {
		b[i] = 0xA5
	}
	return b
}

func TestNEONCompareMatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	for _, n := range neonTestLengths() {
		ia, ib := smallInt64Slice(r, n+1)[1:], smallInt64Slice(r, n)
		fa, fb := randFloat64Slice(r, n+1)[1:], randFloat64Slice(r, n)
		ilit := int64(r.IntN(5) - 2)
		flit := float64(r.IntN(3))
		for _, op := range allCompareOps {
			bits := poisonBits(n)
			done := simdCompareInt64(ia, ib, bits, op)
			if n >= 16 {
				checkBits(t, fmt.Sprintf("int64 op=%d", op), bits, done, n, func(i int) bool { return refCompare(ia[i], ib[i], op) })
			}
			bits = poisonBits(n)
			done = simdCompareInt64Lit(ia, ilit, bits, op)
			if n >= 16 {
				checkBits(t, fmt.Sprintf("int64 lit op=%d", op), bits, done, n, func(i int) bool { return refCompare(ia[i], ilit, op) })
			}
			bits = poisonBits(n)
			done = simdCompareFloat64(fa, fb, bits, op)
			if n >= 16 {
				checkBits(t, fmt.Sprintf("float64 op=%d", op), bits, done, n, func(i int) bool { return refCompare(fa[i], fb[i], op) })
			}
			bits = poisonBits(n)
			done = simdCompareFloat64Lit(fa, flit, bits, op)
			if n >= 16 {
				checkBits(t, fmt.Sprintf("float64 lit op=%d", op), bits, done, n, func(i int) bool { return refCompare(fa[i], flit, op) })
			}
		}
	}
}

func TestNEONCompareInt64LitExtremes(t *testing.T) {
	vals := []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
	a := make([]int64, 32)
	for i := range a {
		a[i] = vals[i%len(vals)]
	}
	for _, lit := range vals {
		for _, op := range allCompareOps {
			bits := poisonBits(len(a))
			done := simdCompareInt64Lit(a, lit, bits, op)
			checkBits(t, fmt.Sprintf("extreme op=%d", op), bits, done, len(a), func(i int) bool { return refCompare(a[i], lit, op) })
		}
	}
}

func TestNEONArithMatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 14))
	for _, n := range neonTestLengths() {
		ia, ib := randInt64Slice(r, n+1)[1:], randInt64Slice(r, n)
		fa, fb := randFloat64Slice(r, n+1)[1:], randFloat64Slice(r, n)
		ilit := randInt64Slice(r, 1)[0]
		flit := r.NormFloat64()
		for _, op := range []arithOp{opAdd, opSub, opMul} {
			got, want := make([]int64, n), make([]int64, n)
			vecBinInt64(got, ia, ib, op)
			vecBinInt64Scalar(want, ia, ib, op, 0)
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("int64 op %d n=%d row %d: got %d want %d", op, n, i, got[i], want[i])
				}
			}
		}
		for _, op := range []arithOp{opAdd, opSub, opMul, opDiv} {
			got, want := make([]float64, n), make([]float64, n)
			vecBinFloat64(got, fa, fb, op)
			vecBinFloat64Scalar(want, fa, fb, op, 0)
			for i := range want {
				if !sameFloatBits(got[i], want[i]) {
					t.Fatalf("float64 op %d n=%d row %d: got %v want %v", op, n, i, got[i], want[i])
				}
			}
		}
		// Literal kernels return the processed prefix; the rest must
		// stay untouched for the caller's tail loop.
		for _, op := range []arithOp{opAdd, opSub} {
			out := make([]int64, n)
			done := neonLitInt64(ia, ilit, out, int(op))
			if done != n&^15 {
				t.Fatalf("lit int64 n=%d: processed %d", n, done)
			}
			for i := range n {
				want := int64(0)
				if i < done {
					want = ia[i] + ilit
					if op == opSub {
						want = ia[i] - ilit
					}
				}
				if out[i] != want {
					t.Fatalf("lit int64 op %d n=%d row %d: got %d want %d", op, n, i, out[i], want)
				}
			}
		}
		for _, op := range []arithOp{opAdd, opSub, opMul, opDiv} {
			out := make([]float64, n)
			done := neonLitFloat64(fa, flit, out, int(op))
			if done != n&^15 {
				t.Fatalf("lit float64 n=%d: processed %d", n, done)
			}
			for i := range n {
				want := 0.0
				if i < done {
					switch op {
					case opAdd:
						want = fa[i] + flit
					case opSub:
						want = fa[i] - flit
					case opMul:
						want = fa[i] * flit
					case opDiv:
						want = fa[i] / flit
					}
				}
				if !sameFloatBits(out[i], want) {
					t.Fatalf("lit float64 op %d n=%d row %d: got %v want %v", op, n, i, out[i], want)
				}
			}
		}
	}
}

func TestNEONBlendMatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 16))
	for _, n := range neonTestLengths() {
		cond := make([]byte, (n+7)/8)
		for i := range cond {
			cond[i] = byte(r.Uint32())
		}
		ia, ib := randInt64Slice(r, n+1)[1:], randInt64Slice(r, n)
		fa, fb := randFloat64Slice(r, n+1)[1:], randFloat64Slice(r, n)
		iout := make([]int64, n)
		fout := make([]float64, n)
		simdBlendInt64(cond, ia, ib, iout)
		simdBlendFloat64(cond, fa, fb, fout)
		for i := range n {
			pick := cond[i>>3]&(1<<(i&7)) != 0
			wi, wf := ib[i], fb[i]
			if pick {
				wi, wf = ia[i], fa[i]
			}
			if iout[i] != wi {
				t.Fatalf("blend int64 n=%d row %d: got %d want %d", n, i, iout[i], wi)
			}
			if !sameFloatBits(fout[i], wf) {
				t.Fatalf("blend float64 n=%d row %d: got %v want %v", n, i, fout[i], wf)
			}
		}
	}
}

func FuzzNEONCompareAndArith(f *testing.F) {
	f.Add(uint64(1), uint16(33), uint8(1), uint8(0))
	f.Add(uint64(9), uint16(700), uint8(2), uint8(5))
	f.Fuzz(func(t *testing.T, seed uint64, n uint16, off, opb uint8) {
		r := rand.New(rand.NewPCG(seed, seed+1))
		o := int(off % 4)
		size := int(n)
		ia, ib := smallInt64Slice(r, size+o)[o:], smallInt64Slice(r, size)
		fa, fb := randFloat64Slice(r, size+o)[o:], randFloat64Slice(r, size)
		op := allCompareOps[int(opb)%len(allCompareOps)]
		bits := poisonBits(size)
		if done := simdCompareInt64(ia, ib, bits, op); size >= 16 {
			checkBits(t, "int64", bits, done, size, func(i int) bool { return refCompare(ia[i], ib[i], op) })
		}
		bits = poisonBits(size)
		if done := simdCompareFloat64(fa, fb, bits, op); size >= 16 {
			checkBits(t, "float64", bits, done, size, func(i int) bool { return refCompare(fa[i], fb[i], op) })
		}
		got, want := make([]float64, size), make([]float64, size)
		aop := arithOp(int(opb) % 4)
		vecBinFloat64(got, fa, fb, aop)
		vecBinFloat64Scalar(want, fa, fb, aop, 0)
		for i := range want {
			if !sameFloatBits(got[i], want[i]) {
				t.Fatalf("float64 op %d row %d: got %v want %v", aop, i, got[i], want[i])
			}
		}
	})
}

func TestNEONCastInt64ToFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(17, 18))
	for _, n := range neonTestLengths() {
		src := randInt64Slice(r, n+1)[1:]
		// Values above 2^53 exercise round-to-nearest-even.
		if n > 2 {
			src[0] = 1<<53 + 1
			src[1] = -(1<<62 + 3)
		}
		out := make([]float64, n+1)
		simdCastInt64ToFloat64NT(out[:n], src)
		for i, v := range src {
			if out[i] != float64(v) {
				t.Fatalf("n=%d row %d: got %v want %v", n, i, out[i], float64(v))
			}
		}
		if out[n] != 0 {
			t.Fatalf("n=%d: wrote past the end", n)
		}
	}
}

func TestNEONFillNullMatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(19, 20))
	for _, n := range neonTestLengths() {
		bits := make([]byte, (n+7)/8)
		for i := range bits {
			switch r.IntN(4) {
			case 0:
				bits[i] = 0
			case 1:
				bits[i] = 0xFF
			default:
				bits[i] = byte(r.Uint32())
			}
		}
		src := randInt64Slice(r, n+1)[1:]
		lit := randInt64Slice(r, 1)[0]
		out := make([]int64, n+1)
		out[n] = 42
		done := simdFillNullInt64NEON(bits, src, lit, out[:n])
		if done != n&^7 {
			t.Fatalf("n=%d: processed %d", n, done)
		}
		for i := range done {
			want := lit
			if bits[i>>3]&(1<<(i&7)) != 0 {
				want = src[i]
			}
			if out[i] != want {
				t.Fatalf("n=%d row %d: got %d want %d", n, i, out[i], want)
			}
		}
		for i := done; i < n; i++ {
			if out[i] != 0 {
				t.Fatalf("n=%d: tail row %d written", n, i)
			}
		}
		if out[n] != 42 {
			t.Fatalf("n=%d: wrote past the end", n)
		}
	}
}
