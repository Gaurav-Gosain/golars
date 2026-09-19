//go:build goexperiment.simd && amd64

package compute

import (
	"simd/archsimd"
)

// simdAvailable is true when the build was produced with GOEXPERIMENT=simd
// on amd64. Kernels use this as a compile-time constant so the SIMD path is
// folded out of binaries that did not opt in, without runtime overhead.
const simdAvailable = true

// hasSIMDInt64 is the runtime gate for the SIMD fast paths. The compiler
// allows emitting AVX2 instructions unconditionally on amd64, but falling
// back on CPUs without AVX2 keeps us correct across older hardware.
func hasSIMDInt64() bool {
	return archsimd.X86.AVX2()
}

// simdAddInt64 writes out[i] = a[i] + b[i] using AVX2 when available.
// Caller guarantees len(a) == len(b) == len(out). No null handling; the
// scalar path handles null-aware cases.
func simdAddInt64(a, b, out []int64) {
	n := len(a)
	i := 0
	if archsimd.X86.AVX512() {
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadInt64x8(a[i:])
			vb := archsimd.LoadInt64x8(b[i:])
			va.Add(vb).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		for ; i+4 <= n; i += 4 {
			va := archsimd.LoadInt64x4(a[i:])
			vb := archsimd.LoadInt64x4(b[i:])
			va.Add(vb).Store(out[i:])
		}
	}
	for ; i < n; i++ {
		out[i] = a[i] + b[i]
	}
}

// simdAddFloat64 writes out[i] = a[i] + b[i] using AVX2 / AVX-512.
func simdAddFloat64(a, b, out []float64) {
	n := len(a)
	i := 0
	if archsimd.X86.AVX512() {
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadFloat64x8(a[i:])
			vb := archsimd.LoadFloat64x8(b[i:])
			va.Add(vb).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		for ; i+4 <= n; i += 4 {
			va := archsimd.LoadFloat64x4(a[i:])
			vb := archsimd.LoadFloat64x4(b[i:])
			va.Add(vb).Store(out[i:])
		}
	}
	for ; i < n; i++ {
		out[i] = a[i] + b[i]
	}
}

// simdSumInt64 returns the sum of every element in a using AVX2/AVX-512.
// 4-way unrolled with 4 parallel accumulators so the OoO core can keep
// multiple VPADDQ in flight per cycle instead of stalling on the
// acc = acc + v dependency chain. Attempted 8-way unroll measured 10%
// slower on AVX2 - the compiler couldn't keep all 16 YMM live across
// the iteration and spilled to the stack, wrecking the inner loop.
// AVX-512 iter handles 32 int64s, AVX2 iter handles 16.
func simdSumInt64(a []int64) int64 {
	n := len(a)
	i := 0
	var total int64
	if archsimd.X86.AVX512() {
		var a0, a1, a2, a3 archsimd.Int64x8
		for ; i+32 <= n; i += 32 {
			a0 = a0.Add(archsimd.LoadInt64x8(a[i:]))
			a1 = a1.Add(archsimd.LoadInt64x8(a[i+8:]))
			a2 = a2.Add(archsimd.LoadInt64x8(a[i+16:]))
			a3 = a3.Add(archsimd.LoadInt64x8(a[i+24:]))
		}
		acc := a0.Add(a1).Add(a2.Add(a3))
		for ; i+8 <= n; i += 8 {
			acc = acc.Add(archsimd.LoadInt64x8(a[i:]))
		}
		var tmp [8]int64
		acc.Store(tmp[:])
		for _, x := range tmp {
			total += x
		}
	} else if archsimd.X86.AVX2() {
		var a0, a1, a2, a3 archsimd.Int64x4
		for ; i+16 <= n; i += 16 {
			a0 = a0.Add(archsimd.LoadInt64x4(a[i:]))
			a1 = a1.Add(archsimd.LoadInt64x4(a[i+4:]))
			a2 = a2.Add(archsimd.LoadInt64x4(a[i+8:]))
			a3 = a3.Add(archsimd.LoadInt64x4(a[i+12:]))
		}
		acc := a0.Add(a1).Add(a2.Add(a3))
		for ; i+4 <= n; i += 4 {
			acc = acc.Add(archsimd.LoadInt64x4(a[i:]))
		}
		var tmp [4]int64
		acc.Store(tmp[:])
		for _, x := range tmp {
			total += x
		}
	}
	for ; i < n; i++ {
		total += a[i]
	}
	return total
}

// simdMinFloat64 returns (min, anyNaN) over vals in a single pass. NaN
// detection is fused into the MINPD reduction via IsNaN OR-reduce so the
// hot loop avoids a second scan. If anyNaN is true the returned min is
// not meaningful: the caller must fall back to scalar NaN-aware logic.
// AVX-512 uses 8-wide MINPD; AVX2 uses 4-wide.
func simdMinFloat64(vals []float64) (float64, bool) {
	n := len(vals)
	if n == 0 {
		return 0, false
	}
	i := 0
	if archsimd.X86.AVX512() && n >= 8 {
		acc := archsimd.LoadFloat64x8(vals[:8])
		anyNaN := acc.IsNaN()
		i = 8
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(vals[i:])
			anyNaN = anyNaN.Or(v.IsNaN())
			acc = acc.Min(v)
		}
		var tmp [8]float64
		acc.Store(tmp[:])
		hasNaN := anyNaN.ToBits() != 0
		best := tmp[0]
		for _, x := range tmp[1:] {
			if x < best {
				best = x
			}
		}
		for ; i < n; i++ {
			v := vals[i]
			if v != v {
				hasNaN = true
				continue
			}
			if v < best {
				best = v
			}
		}
		return best, hasNaN
	}
	if archsimd.X86.AVX2() && n >= 4 {
		acc := archsimd.LoadFloat64x4(vals[:4])
		anyNaN := acc.IsNaN()
		i = 4
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(vals[i:])
			anyNaN = anyNaN.Or(v.IsNaN())
			acc = acc.Min(v)
		}
		var tmp [4]float64
		acc.Store(tmp[:])
		hasNaN := anyNaN.ToBits() != 0
		best := tmp[0]
		for _, x := range tmp[1:] {
			if x < best {
				best = x
			}
		}
		for ; i < n; i++ {
			v := vals[i]
			if v != v {
				hasNaN = true
				continue
			}
			if v < best {
				best = v
			}
		}
		return best, hasNaN
	}
	best := vals[0]
	hasNaN := best != best
	for _, v := range vals[1:] {
		if v != v {
			hasNaN = true
			continue
		}
		if v < best {
			best = v
		}
	}
	return best, hasNaN
}

// simdMaxFloat64 mirrors simdMinFloat64 with MAXPD.
func simdMaxFloat64(vals []float64) (float64, bool) {
	n := len(vals)
	if n == 0 {
		return 0, false
	}
	i := 0
	if archsimd.X86.AVX512() && n >= 8 {
		acc := archsimd.LoadFloat64x8(vals[:8])
		anyNaN := acc.IsNaN()
		i = 8
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(vals[i:])
			anyNaN = anyNaN.Or(v.IsNaN())
			acc = acc.Max(v)
		}
		var tmp [8]float64
		acc.Store(tmp[:])
		hasNaN := anyNaN.ToBits() != 0
		best := tmp[0]
		for _, x := range tmp[1:] {
			if x > best {
				best = x
			}
		}
		for ; i < n; i++ {
			v := vals[i]
			if v != v {
				hasNaN = true
				continue
			}
			if v > best {
				best = v
			}
		}
		return best, hasNaN
	}
	if archsimd.X86.AVX2() && n >= 4 {
		acc := archsimd.LoadFloat64x4(vals[:4])
		anyNaN := acc.IsNaN()
		i = 4
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(vals[i:])
			anyNaN = anyNaN.Or(v.IsNaN())
			acc = acc.Max(v)
		}
		var tmp [4]float64
		acc.Store(tmp[:])
		hasNaN := anyNaN.ToBits() != 0
		best := tmp[0]
		for _, x := range tmp[1:] {
			if x > best {
				best = x
			}
		}
		for ; i < n; i++ {
			v := vals[i]
			if v != v {
				hasNaN = true
				continue
			}
			if v > best {
				best = v
			}
		}
		return best, hasNaN
	}
	best := vals[0]
	hasNaN := best != best
	for _, v := range vals[1:] {
		if v != v {
			hasNaN = true
			continue
		}
		if v > best {
			best = v
		}
	}
	return best, hasNaN
}

// simdCompareInt64 writes the bitmap for a[i] OP b[i] into bits[0..]. op
// is one of opGt / opLt / opEq. AVX-512 processes 8 lanes at a time with a
// single Int64x8.Greater → Mask.ToBits, producing one byte per 8 rows -
// exactly matches the bitmap layout. AVX2 produces 4-bit nibbles which we
// OR together into byte positions. Caller must ensure n is a multiple of 8
// unless handling the tail via the scalar fallback.
func simdCompareInt64(av, bv []int64, bits []byte, op compareOp) int {
	n := len(av)
	i := 0
	if archsimd.X86.AVX512() {
		// 8 lanes per iteration → one byte per iteration. n/8 full bytes.
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadInt64x8(av[i:])
			vb := archsimd.LoadInt64x8(bv[i:])
			var m archsimd.Mask64x8
			switch op {
			case opGt:
				m = va.Greater(vb)
			case opLt:
				m = va.Less(vb)
			case opEq:
				m = va.Equal(vb)
			case opGe:
				m = va.GreaterEqual(vb)
			case opLe:
				m = va.LessEqual(vb)
			case opNe:
				m = va.NotEqual(vb)
			}
			bits[i/8] = m.ToBits()
		}
	} else if archsimd.X86.AVX2() {
		// 4 lanes per iteration → 4 bits. Pair two iterations to fill a
		// byte so the write pattern still maps cleanly to the bitmap.
		for ; i+8 <= n; i += 8 {
			la := archsimd.LoadInt64x4(av[i:])
			lb := archsimd.LoadInt64x4(bv[i:])
			ha := archsimd.LoadInt64x4(av[i+4:])
			hb := archsimd.LoadInt64x4(bv[i+4:])
			var ml, mh archsimd.Mask64x4
			switch op {
			case opGt:
				ml = la.Greater(lb)
				mh = ha.Greater(hb)
			case opLt:
				ml = la.Less(lb)
				mh = ha.Less(hb)
			case opEq:
				ml = la.Equal(lb)
				mh = ha.Equal(hb)
			case opGe:
				ml = la.GreaterEqual(lb)
				mh = ha.GreaterEqual(hb)
			case opLe:
				ml = la.LessEqual(lb)
				mh = ha.LessEqual(hb)
			case opNe:
				ml = la.NotEqual(lb)
				mh = ha.NotEqual(hb)
			}
			bits[i/8] = ml.ToBits() | (mh.ToBits() << 4)
		}
	}
	return i
}

// simdCompareFloat64 is the float64 counterpart.
func simdCompareFloat64(av, bv []float64, bits []byte, op compareOp) int {
	n := len(av)
	i := 0
	if archsimd.X86.AVX512() {
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadFloat64x8(av[i:])
			vb := archsimd.LoadFloat64x8(bv[i:])
			var m archsimd.Mask64x8
			switch op {
			case opGt:
				m = va.Greater(vb)
			case opLt:
				m = va.Less(vb)
			case opEq:
				m = va.Equal(vb)
			case opGe:
				m = va.GreaterEqual(vb)
			case opLe:
				m = va.LessEqual(vb)
			case opNe:
				m = va.NotEqual(vb)
			}
			bits[i/8] = m.ToBits()
		}
	} else if archsimd.X86.AVX2() {
		for ; i+8 <= n; i += 8 {
			la := archsimd.LoadFloat64x4(av[i:])
			lb := archsimd.LoadFloat64x4(bv[i:])
			ha := archsimd.LoadFloat64x4(av[i+4:])
			hb := archsimd.LoadFloat64x4(bv[i+4:])
			var ml, mh archsimd.Mask64x4
			switch op {
			case opGt:
				ml = la.Greater(lb)
				mh = ha.Greater(hb)
			case opLt:
				ml = la.Less(lb)
				mh = ha.Less(hb)
			case opEq:
				ml = la.Equal(lb)
				mh = ha.Equal(hb)
			case opGe:
				ml = la.GreaterEqual(lb)
				mh = ha.GreaterEqual(hb)
			case opLe:
				ml = la.LessEqual(lb)
				mh = ha.LessEqual(hb)
			case opNe:
				ml = la.NotEqual(lb)
				mh = ha.NotEqual(hb)
			}
			bits[i/8] = ml.ToBits() | (mh.ToBits() << 4)
		}
	}
	return i
}

// simdBlendInt64 writes out[i] = aVals[i] if condBit i is set, else bVals[i].
// condBits is the packed boolean bitmap. Only AVX-512 supports the
// compact mask-register form; on AVX2-only CPUs we fall back to a
// dedicated assembly kernel (simdBlendInt64AVX2) that uses VPBLENDVB.
//
// Caller must guarantee len(aVals) == len(bVals) == len(out) == n.
func simdBlendInt64(condBits []byte, aVals, bVals, out []int64) {
	n := len(out)
	i := 0
	if archsimd.X86.AVX512() {
		// 8 lanes per iteration - one byte of the bitmap per step.
		for ; i+8 <= n; i += 8 {
			m := archsimd.Mask64x8FromBits(condBits[i>>3])
			va := archsimd.LoadInt64x8(aVals[i:])
			vb := archsimd.LoadInt64x8(bVals[i:])
			va.Merge(vb, m).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		// Delegate to the AVX2 asm kernel (4 int64s per iteration
		// via VPBLENDVB). Returns the number of elements processed.
		i = simdBlendInt64AVX2(condBits, aVals, bVals, out)
	}
	for ; i < n; i++ {
		bit := int64(condBits[i>>3]>>uint(i&7)) & 1
		mask := -bit
		out[i] = bVals[i] ^ ((aVals[i] ^ bVals[i]) & mask)
	}
}

// simdBlendFloat64 is the float64 counterpart of simdBlendInt64.
func simdBlendFloat64(condBits []byte, aVals, bVals, out []float64) {
	n := len(out)
	i := 0
	if archsimd.X86.AVX512() {
		for ; i+8 <= n; i += 8 {
			m := archsimd.Mask64x8FromBits(condBits[i>>3])
			va := archsimd.LoadFloat64x8(aVals[i:])
			vb := archsimd.LoadFloat64x8(bVals[i:])
			va.Merge(vb, m).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		// Int64 bit-blend works on float64 bit patterns too.
		ai := unsafeF64toI64(aVals)
		bi := unsafeF64toI64(bVals)
		oi := unsafeF64toI64(out)
		i = simdBlendInt64AVX2(condBits, ai, bi, oi)
	}
	for ; i < n; i++ {
		if condBits[i>>3]&(1<<(i&7)) != 0 {
			out[i] = aVals[i]
		} else {
			out[i] = bVals[i]
		}
	}
}

// simdCompareInt64Lit writes a > lit (or other op) bitmap for the
// full 8-aligned prefix of av. Returns the number of elements
// processed (always a multiple of 8).
func simdCompareInt64Lit(av []int64, lit int64, bits []byte, op compareOp) int {
	n := len(av)
	i := 0
	if archsimd.X86.AVX512() {
		litVec := archsimd.BroadcastInt64x8(lit)
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadInt64x8(av[i:])
			var m archsimd.Mask64x8
			switch op {
			case opGt:
				m = va.Greater(litVec)
			case opLt:
				m = va.Less(litVec)
			case opEq:
				m = va.Equal(litVec)
			case opGe:
				m = va.GreaterEqual(litVec)
			case opLe:
				m = va.LessEqual(litVec)
			case opNe:
				m = va.NotEqual(litVec)
			}
			bits[i/8] = m.ToBits()
		}
	} else if archsimd.X86.AVX2() {
		litVec := archsimd.BroadcastInt64x4(lit)
		for ; i+8 <= n; i += 8 {
			la := archsimd.LoadInt64x4(av[i:])
			ha := archsimd.LoadInt64x4(av[i+4:])
			var ml, mh archsimd.Mask64x4
			switch op {
			case opGt:
				ml = la.Greater(litVec)
				mh = ha.Greater(litVec)
			case opLt:
				ml = la.Less(litVec)
				mh = ha.Less(litVec)
			case opEq:
				ml = la.Equal(litVec)
				mh = ha.Equal(litVec)
			case opGe:
				ml = la.GreaterEqual(litVec)
				mh = ha.GreaterEqual(litVec)
			case opLe:
				ml = la.LessEqual(litVec)
				mh = ha.LessEqual(litVec)
			case opNe:
				ml = la.NotEqual(litVec)
				mh = ha.NotEqual(litVec)
			}
			bits[i/8] = ml.ToBits() | (mh.ToBits() << 4)
		}
	}
	return i
}

// simdCompareFloat64Lit is the float64 counterpart.
func simdCompareFloat64Lit(av []float64, lit float64, bits []byte, op compareOp) int {
	n := len(av)
	i := 0
	if archsimd.X86.AVX512() {
		litVec := archsimd.BroadcastFloat64x8(lit)
		for ; i+8 <= n; i += 8 {
			va := archsimd.LoadFloat64x8(av[i:])
			var m archsimd.Mask64x8
			switch op {
			case opGt:
				m = va.Greater(litVec)
			case opLt:
				m = va.Less(litVec)
			case opEq:
				m = va.Equal(litVec)
			case opGe:
				m = va.GreaterEqual(litVec)
			case opLe:
				m = va.LessEqual(litVec)
			case opNe:
				m = va.NotEqual(litVec)
			}
			bits[i/8] = m.ToBits()
		}
	} else if archsimd.X86.AVX2() {
		litVec := archsimd.BroadcastFloat64x4(lit)
		for ; i+8 <= n; i += 8 {
			la := archsimd.LoadFloat64x4(av[i:])
			ha := archsimd.LoadFloat64x4(av[i+4:])
			var ml, mh archsimd.Mask64x4
			switch op {
			case opGt:
				ml = la.Greater(litVec)
				mh = ha.Greater(litVec)
			case opLt:
				ml = la.Less(litVec)
				mh = ha.Less(litVec)
			case opEq:
				ml = la.Equal(litVec)
				mh = ha.Equal(litVec)
			case opGe:
				ml = la.GreaterEqual(litVec)
				mh = ha.GreaterEqual(litVec)
			case opLe:
				ml = la.LessEqual(litVec)
				mh = ha.LessEqual(litVec)
			case opNe:
				ml = la.NotEqual(litVec)
				mh = ha.NotEqual(litVec)
			}
			bits[i/8] = ml.ToBits() | (mh.ToBits() << 4)
		}
	}
	return i
}

// simdAddLitInt64 / simdSubLitInt64 / simdMulLitInt64 add/sub/mul a
// scalar literal into every element of src, writing to out. Returns
// the count of elements processed. All SIMD paths are AVX2-safe.
func simdAddLitInt64(src []int64, lit int64, out []int64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastInt64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadInt64x8(src[i:])
			v.Add(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastInt64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadInt64x4(src[i:])
			v.Add(litV).Store(out[i:])
		}
	}
	return i
}

func simdSubLitInt64(src []int64, lit int64, out []int64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastInt64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadInt64x8(src[i:])
			v.Sub(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastInt64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadInt64x4(src[i:])
			v.Sub(litV).Store(out[i:])
		}
	}
	return i
}

func simdAddLitFloat64(src []float64, lit float64, out []float64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastFloat64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(src[i:])
			v.Add(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastFloat64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(src[i:])
			v.Add(litV).Store(out[i:])
		}
	}
	return i
}

func simdSubLitFloat64(src []float64, lit float64, out []float64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastFloat64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(src[i:])
			v.Sub(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastFloat64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(src[i:])
			v.Sub(litV).Store(out[i:])
		}
	}
	return i
}

func simdMulLitFloat64(src []float64, lit float64, out []float64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastFloat64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(src[i:])
			v.Mul(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastFloat64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(src[i:])
			v.Mul(litV).Store(out[i:])
		}
	}
	return i
}

func simdDivLitFloat64(src []float64, lit float64, out []float64) int {
	n := len(src)
	i := 0
	if archsimd.X86.AVX512() {
		litV := archsimd.BroadcastFloat64x8(lit)
		for ; i+8 <= n; i += 8 {
			v := archsimd.LoadFloat64x8(src[i:])
			v.Div(litV).Store(out[i:])
		}
	} else if archsimd.X86.AVX2() {
		litV := archsimd.BroadcastFloat64x4(lit)
		for ; i+4 <= n; i += 4 {
			v := archsimd.LoadFloat64x4(src[i:])
			v.Div(litV).Store(out[i:])
		}
	}
	return i
}

// simdSumFloat64 returns the sum of every element in a using AVX2/AVX-512.
// 4-way unrolled with 4 parallel accumulators. VADDPD latency 3-4 cycles
// on Skylake/Zen means a single-accumulator chain only issues one add per
// 3-4 cycles; 4 accumulators saturate the FP-add pipes. Floating-point
// associativity changes with reduction order, but polars also reorders.
func simdSumFloat64(a []float64) float64 {
	n := len(a)
	i := 0
	var total float64
	if archsimd.X86.AVX512() {
		var a0, a1, a2, a3 archsimd.Float64x8
		for ; i+32 <= n; i += 32 {
			a0 = a0.Add(archsimd.LoadFloat64x8(a[i:]))
			a1 = a1.Add(archsimd.LoadFloat64x8(a[i+8:]))
			a2 = a2.Add(archsimd.LoadFloat64x8(a[i+16:]))
			a3 = a3.Add(archsimd.LoadFloat64x8(a[i+24:]))
		}
		acc := a0.Add(a1).Add(a2.Add(a3))
		for ; i+8 <= n; i += 8 {
			acc = acc.Add(archsimd.LoadFloat64x8(a[i:]))
		}
		var tmp [8]float64
		acc.Store(tmp[:])
		for _, x := range tmp {
			total += x
		}
	} else if archsimd.X86.AVX2() {
		var a0, a1, a2, a3 archsimd.Float64x4
		for ; i+16 <= n; i += 16 {
			a0 = a0.Add(archsimd.LoadFloat64x4(a[i:]))
			a1 = a1.Add(archsimd.LoadFloat64x4(a[i+4:]))
			a2 = a2.Add(archsimd.LoadFloat64x4(a[i+8:]))
			a3 = a3.Add(archsimd.LoadFloat64x4(a[i+12:]))
		}
		acc := a0.Add(a1).Add(a2.Add(a3))
		for ; i+4 <= n; i += 4 {
			acc = acc.Add(archsimd.LoadFloat64x4(a[i:]))
		}
		var tmp [4]float64
		acc.Store(tmp[:])
		for _, x := range tmp {
			total += x
		}
	}
	for ; i < n; i++ {
		total += a[i]
	}
	return total
}
