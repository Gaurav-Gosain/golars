package compute

import (
	"runtime"
	"sync"
	"unsafe"
)

// parallelRadixCutoff is the minimum input size for the parallel path.
// 11-bit digit histograms (2048 buckets) fit L1 per thread, so histogram
// zeroing costs ~5μs per pass vs ~100μs for 16-bit. With skip-pass on
// typical high-bit-zero data, only 2 passes actually run, so 4 barriers
// fire against a baseline of ~600μs serial; parallel wins at 256K+.
const parallelRadixCutoff = 256 * 1024

// parallelFloatRadixCutoff is the minimum input size for the parallel
// path for float64 sort. Lower than parallelRadixCutoff because f64
// data has no skip-pass opportunity (bits span the full dynamic
// range), so all 6 scatter passes always run: the parallel fan-out
// amortizes earlier than for int64 workloads where skip-pass can
// collapse work to 1-2 passes.
const parallelFloatRadixCutoff = 64 * 1024

// radixWorkspace holds a per-thread histogram + per-thread offset array.
// Reused via sync.Pool so the 32 KB allocation per call is amortized.
// 2048 entries ≡ 11-bit digit.
type radixWorkspace struct {
	hist [2048]int
	off  [2048]int
}

var radixWorkspacePool = sync.Pool{
	New: func() any { return new(radixWorkspace) },
}

// auxBufPool caches aux uint64 slices by capacity-bucket. The 8 MB
// allocation for a 1 M sort is a visible fraction (≈ 50 μs mallocgc zero)
// of the total bench iteration: pooling reuses the backing array across
// back-to-back sorts. Keyed by log2(capacity rounded up) so we share
// buffers across slightly-different sizes.
var auxBufPools sync.Map // int → *sync.Pool of []uint64

func auxBucket(n int) int {
	// Map n to next power-of-two log. For n=600K, bucket = 20 (1M items).
	if n <= 1 {
		return 0
	}
	shift := 0
	m := n - 1
	for m > 0 {
		m >>= 1
		shift++
	}
	return shift
}

func getAuxBuf(n int) []uint64 {
	b := auxBucket(n)
	if v, ok := auxBufPools.Load(b); ok {
		if buf := v.(*sync.Pool).Get(); buf != nil {
			s := buf.([]uint64)
			if cap(s) >= n {
				return s[:n]
			}
			// Too small (shouldn't happen since bucket is capacity-exact):
			// drop and allocate fresh.
		}
	}
	return make([]uint64, n)
}

func putAuxBuf(s []uint64) {
	if cap(s) == 0 {
		return
	}
	b := auxBucket(cap(s))
	v, _ := auxBufPools.LoadOrStore(b, &sync.Pool{})
	v.(*sync.Pool).Put(s[:cap(s)])
}

// radixSortInt64Parallel sorts vals in place using a parallel LSD radix
// sort. 16-bit digits, 4 passes, with per-thread partitioning:
//
//  1. Each worker counts digit occurrences over its input partition.
//  2. A serial prefix sum over the per-thread histograms computes the
//     starting offset for (thread, digit) pairs.
//  3. Each worker scatters its partition into the pre-computed offsets.
//
// This follows the same shape as DuckDB's and polars's parallel radix: the
// per-thread histograms avoid contention on a global counts array, and the
// prefix sum is cheap (K × 65536 = 524K ops for K=8) compared to the scan
// costs. Skip-pass runs when all values share a single digit bucket.
func radixSortInt64Parallel(vals []int64) {
	n := len(vals)
	k := min(runtime.GOMAXPROCS(0), 8)
	if n < parallelRadixCutoff || n < 64*k {
		radixSortInt64_16(vals)
		return
	}

	auxU := getAuxBuf(n)
	defer putAuxBuf(auxU)
	aux := int64sFromUint64s(auxU)
	const signBit uint64 = 1 << 63
	partSize := (n + k - 1) / k

	workspaces := make([]*radixWorkspace, k)
	for p := range k {
		workspaces[p] = radixWorkspacePool.Get().(*radixWorkspace)
	}
	defer func() {
		for _, w := range workspaces {
			radixWorkspacePool.Put(w)
		}
	}()

	src, dst := vals, aux
	swaps := 0
	var wg sync.WaitGroup

	// 6 passes of 11 bits each (last pass takes 9 bits to cover bit 63).
	// Histogram buckets: 2048, last-pass buckets: 512. Histograms are
	// zeroed inside each worker via compiler memset.
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}

		// 1. Parallel count.
		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				h := &workspaces[p].hist
				for i := range h {
					h[i] = 0
				}
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				for i := range s {
					d := int((uint64(s[i]) ^ signBit) >> shift & mask)
					h[d]++
				}
			}(p)
		}
		wg.Wait()

		// 2. Serial prefix sum + skip detection.
		nBuckets := 2048
		if pass == 5 {
			nBuckets = 512
		}
		var total int
		maxDigitCount := 0
		for d := 0; d < nBuckets; d++ {
			sum := 0
			for p := range k {
				workspaces[p].off[d] = total
				c := workspaces[p].hist[d]
				total += c
				sum += c
			}
			if sum > maxDigitCount {
				maxDigitCount = sum
			}
		}
		if maxDigitCount == n {
			continue
		}

		// 3. Parallel scatter. 2-way unrolled: two independent digit
		// streams expose more instruction-level parallelism per worker.
		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				off := &workspaces[p].off
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				i := 0
				for ; i+1 < len(s); i += 2 {
					v0 := s[i]
					v1 := s[i+1]
					d0 := int((uint64(v0) ^ signBit) >> shift & mask)
					d1 := int((uint64(v1) ^ signBit) >> shift & mask)
					p0 := off[d0]
					off[d0] = p0 + 1
					p1 := off[d1]
					off[d1] = p1 + 1
					dst[p0] = v0
					dst[p1] = v1
				}
				for ; i < len(s); i++ {
					v := s[i]
					d := int((uint64(v) ^ signBit) >> shift & mask)
					dst[off[d]] = v
					off[d]++
				}
			}(p)
		}
		wg.Wait()

		src, dst = dst, src
		swaps++
	}

	if swaps%2 != 0 {
		copy(vals, aux)
	}
}

// radixSortUint64FromFloatParallel takes float64 input and sorts by
// IEEE bit pattern into `out`. Fuses the first radix pass with the
// float→uint64 reinterpret so we skip the explicit bit-copy pass.
// Pass 0 also OR-reduces the sign bit and tracks the max exponent
// across the partition so the caller can skip a separate pre-scan
// for NaN/negative detection: returns true iff the input has any
// NaN, Inf, or negative value (signalling the caller should retry
// with the IEEE-transform fallback).
//
// Pass layout (6 passes total):
//
//	Pass 0: srcBits → aux      (reads input as uint64 bits)
//	Pass 1: aux     → out
//	Pass 2: out     → aux
//	Pass 3: aux     → out
//	Pass 4: out     → aux
//	Pass 5: aux     → out
//
// Final data is in `out`. For skip-pass triggered at pass p, we still
// perform a copy from src→dst for that pass so the downstream source
// buffer holds the right data; correctness first.
func radixSortUint64FromFloatParallel(src []float64, out []uint64) (needsIEEE bool) {
	n := len(src)
	if n != len(out) {
		panic("radixSortUint64FromFloatParallel: length mismatch")
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	srcBits := unsafe.Slice((*uint64)(unsafe.Pointer(&src[0])), n)
	if n < parallelFloatRadixCutoff || n < 64*k {
		// Serial fallback: detection happens inline in pass 0.
		return serialFromFloatWithDetect(srcBits, out)
	}

	aux := getAuxBuf(n)
	defer putAuxBuf(aux)
	partSize := (n + k - 1) / k

	workspaces := make([]*radixWorkspace, k)
	for p := range k {
		workspaces[p] = radixWorkspacePool.Get().(*radixWorkspace)
	}
	defer func() {
		for _, w := range workspaces {
			radixWorkspacePool.Put(w)
		}
	}()

	var wg sync.WaitGroup

	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		var passSrc, passDst []uint64
		switch pass {
		case 0:
			passSrc, passDst = srcBits, aux
		case 1:
			passSrc, passDst = aux, out
		case 2:
			passSrc, passDst = out, aux
		case 3:
			passSrc, passDst = aux, out
		case 4:
			passSrc, passDst = out, aux
		case 5:
			passSrc, passDst = aux, out
		}

		// Per-worker float-edge state is only populated during pass 0.
		// It stays zero on subsequent passes (we don't inspect it).
		edges := make([]floatEdge, k)
		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				h := &workspaces[p].hist
				clear(h[:])
				start := p * partSize
				end := min(start+partSize, n)
				s := passSrc[start:end]
				if pass == 0 {
					var orBits uint64
					for i := range s {
						b := s[i]
						h[int(b&mask)]++
						orBits |= b
					}
					edges[p] = floatEdge{orBits: orBits}
				} else {
					for i := range s {
						d := int(s[i] >> shift & mask)
						h[d]++
					}
				}
			}(p)
		}
		wg.Wait()

		if pass == 0 {
			var orBits uint64
			for _, e := range edges {
				orBits |= e.orBits
			}
			if orBits>>63 != 0 {
				// Any negative, Inf, or NaN → caller must run the IEEE
				// transform fallback. Aborting pass 0 wastes the
				// histograms but that's 1 µs; the fallback is rare.
				return true
			}
		}

		nBuckets := 2048
		if pass == 5 {
			nBuckets = 512
		}
		var total int
		maxDigitCount := 0
		for d := 0; d < nBuckets; d++ {
			sum := 0
			for p := range k {
				workspaces[p].off[d] = total
				c := workspaces[p].hist[d]
				total += c
				sum += c
			}
			if sum > maxDigitCount {
				maxDigitCount = sum
			}
		}
		if maxDigitCount == n {
			copy(passDst, passSrc)
			continue
		}

		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				off := &workspaces[p].off
				start := p * partSize
				end := min(start+partSize, n)
				s := passSrc[start:end]
				d := passDst
				// Branchless 2-way scatter. Per-thread output buffer
				// variant was tried (each worker scatters to private
				// region, merge step concatenates). Regressed 44% at
				// 1M: the merge doubled memory traffic (4n vs 2n) and
				// that dominates any coherence savings. Shared-dst
				// with disjoint sub-regions is actually optimal for
				// uniform-distribution int/float radix on AVX2.
				i := 0
				for ; i+1 < len(s); i += 2 {
					v0 := s[i]
					v1 := s[i+1]
					d0 := int(v0 >> shift & mask)
					d1 := int(v1 >> shift & mask)
					p0 := off[d0]
					off[d0] = p0 + 1
					p1 := off[d1]
					off[d1] = p1 + 1
					d[p0] = v0
					d[p1] = v1
				}
				for ; i < len(s); i++ {
					v := s[i]
					dig := int(v >> shift & mask)
					d[off[dig]] = v
					off[dig]++
				}
			}(p)
		}
		wg.Wait()
	}
	return false
}

// floatEdge holds pass-0 detection state built alongside the radix
// histogram. orBits's top bit flips iff any value in the partition
// had sign set: the only case that requires the IEEE fallback
// transform (NaN and +Inf sort correctly via raw uint64 bit order).
type floatEdge struct {
	orBits uint64
}

// serialFromFloatWithDetect runs the serial 8-bit radix over float
// bits. Returns true when the caller should run the IEEE-transform
// fallback.
//
// Detection is minimal: we only need to know whether ANY value has the
// sign bit set. For all-non-negative input (including positive NaN and
// +Inf), raw uint64 bit order is the correct IEEE order: finite
// values sort by magnitude, then +Inf, then positive NaN: matching
// polars' nulls_last/NaN_last convention. Only negatives require the
// IEEE bit transform.
//
// 8-bit digits produce 256-bucket histograms (2 KB each, 16 KB for
// all 8 passes) that fit L1 comfortably. Measured 20 % faster than
// the prior 11-bit variant at N=16K because the smaller counts array
// keeps accesses L1-hot and the scatter's 256 buckets produce long
// sequential write streams (avg 64 elements/bucket at 16K vs 8 at
// 11-bit/2048-bucket).
func serialFromFloatWithDetect(srcBits, out []uint64) (needsIEEE bool) {
	n := len(srcBits)
	auxU := getAuxBuf(n)
	defer putAuxBuf(auxU)

	var counts [8][256]int
	for _, b := range srcBits {
		counts[0][b&0xFF]++
		counts[1][(b>>8)&0xFF]++
		counts[2][(b>>16)&0xFF]++
		counts[3][(b>>24)&0xFF]++
		counts[4][(b>>32)&0xFF]++
		counts[5][(b>>40)&0xFF]++
		counts[6][(b>>48)&0xFF]++
		counts[7][(b>>56)&0xFF]++
	}
	// Sign bit is bit 63, i.e. the high bit of pass 7's digit. Any
	// non-zero count in counts[7][128..256] means a negative input
	// was seen and the caller must take the IEEE transform fallback.
	for _, c := range counts[7][128:] {
		if c > 0 {
			return true
		}
	}

	// Route through (srcBits → auxU → out → auxU → ... → out). 8
	// passes, alternating. Final data lands in `out` because 8 is
	// even and the sequence (srcBits→auxU, auxU→out, out→auxU, ...)
	// with 8 steps terminates at auxU → out.
	//
	// All 8 passes go through the 2-way PREFETCHT0-tagged asm scatter
	// on amd64; fallback forwards to the generic scatterUint64 on
	// other arches. The asm handles shift=0 (pass 0) correctly.
	// 2-way unrolled: ~30 % faster than Go's generated scatter.
	//
	// A byte position where every key has the same digit is a no-op
	// pass (for floats in a narrow range the top exponent byte usually
	// is), so it is skipped. The first real pass is aimed at out or
	// auxU depending on how many real passes remain, so the last one
	// always lands in out without a trailing copy.
	var live [8]uint
	nLive := 0
	for p := range 8 {
		if counts[p][(srcBits[0]>>(uint(p)*8))&0xFF] != n {
			live[nLive] = uint(p)
			nLive++
		}
	}
	if nLive == 0 {
		copy(out, srcBits)
		return false
	}
	src := srcBits
	dst, next := auxU, out
	if nLive%2 == 1 {
		dst, next = out, auxU
	}
	for _, p := range live[:nLive] {
		scatterUint64_8_prefetch2(src, dst, counts[p][:], p)
		src, dst, next = dst, next, dst
	}
	return false
}

// radixSortUint64Parallel mirrors radixSortInt64Parallel but skips the
// sign-bit flip. Used by SortFloat64 after its IEEE 754 transform.
func radixSortUint64Parallel(vals []uint64) {
	n := len(vals)
	k := min(runtime.GOMAXPROCS(0), 8)
	if n < parallelRadixCutoff || n < 64*k {
		radixSortUint64_11(vals)
		return
	}

	aux := getAuxBuf(n)
	defer putAuxBuf(aux)
	partSize := (n + k - 1) / k

	workspaces := make([]*radixWorkspace, k)
	for p := range k {
		workspaces[p] = radixWorkspacePool.Get().(*radixWorkspace)
	}
	defer func() {
		for _, w := range workspaces {
			radixWorkspacePool.Put(w)
		}
	}()

	src, dst := vals, aux
	swaps := 0
	var wg sync.WaitGroup

	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}

		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				h := &workspaces[p].hist
				for i := range h {
					h[i] = 0
				}
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				for i := range s {
					d := int(s[i] >> shift & mask)
					h[d]++
				}
			}(p)
		}
		wg.Wait()

		nBuckets := 2048
		if pass == 5 {
			nBuckets = 512
		}
		var total int
		maxDigitCount := 0
		for d := 0; d < nBuckets; d++ {
			sum := 0
			for p := range k {
				workspaces[p].off[d] = total
				c := workspaces[p].hist[d]
				total += c
				sum += c
			}
			if sum > maxDigitCount {
				maxDigitCount = sum
			}
		}
		if maxDigitCount == n {
			continue
		}

		for p := range k {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				off := &workspaces[p].off
				start := p * partSize
				end := min(start+partSize, n)
				s := src[start:end]
				i := 0
				for ; i+1 < len(s); i += 2 {
					v0 := s[i]
					v1 := s[i+1]
					d0 := int(v0 >> shift & mask)
					d1 := int(v1 >> shift & mask)
					p0 := off[d0]
					off[d0] = p0 + 1
					p1 := off[d1]
					off[d1] = p1 + 1
					dst[p0] = v0
					dst[p1] = v1
				}
				for ; i < len(s); i++ {
					v := s[i]
					d := int(v >> shift & mask)
					dst[off[d]] = v
					off[d]++
				}
			}(p)
		}
		wg.Wait()

		src, dst = dst, src
		swaps++
	}

	if swaps%2 != 0 {
		copy(vals, aux)
	}
}
