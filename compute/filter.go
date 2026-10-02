package compute

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// ErrMaskNotBool indicates a filter mask was not a boolean Series.
var ErrMaskNotBool = fmt.Errorf("%w: filter mask must be Bool", ErrDTypeMismatch)

// Filter returns a Series containing only the positions i where mask[i] is
// true. Null mask entries are treated as false.
func Filter(ctx context.Context, s, mask *series.Series, opts ...Option) (*series.Series, error) {
	if !mask.DType().IsBool() {
		return nil, ErrMaskNotBool
	}
	if s.Len() != mask.Len() {
		return nil, fmt.Errorf("%w: series=%d mask=%d", ErrLengthMismatch, s.Len(), mask.Len())
	}
	cfg := resolve(opts)

	sArr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer sArr.Release()
	mArr, err := extractChunk(mask, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer mArr.Release()

	mBool := mArr.(*array.Boolean)
	name := cfg.outName(s.Name())
	n := sArr.Len()

	// Ultra-fast path: no-null source + no-null mask + int64/float64 dtype.
	// Skip the []int indices allocation entirely and walk the bitmap in a
	// single fused pass that writes directly into the output buffer. This
	// mirrors polars' bitmap-aware gather. Checked BEFORE any indices work
	// so we do not pay for bitmap scan twice.
	if sArr.NullN() == 0 && mArr.NullN() == 0 {
		data := mArr.Data()
		if data.Offset() == 0 {
			buffers := data.Buffers()
			if len(buffers) >= 2 && buffers[1] != nil {
				maskBytes := buffers[1].Bytes()
				switch s.DType().ID() {
				case arrow.INT64:
					return fusedFilterInt64(name, int64Values(sArr), maskBytes, n, poolingMem(cfg.alloc))
				case arrow.FLOAT64:
					return fusedFilterFloat64(name, float64Values(sArr), maskBytes, n, poolingMem(cfg.alloc))
				}
			}
		}
	}

	// Strings: walk the mask words directly into offset and data
	// buffers, skipping the []int index list.
	if s.DType().ID() == arrow.STRING && sArr.NullN() == 0 && mArr.NullN() == 0 && mArr.Data().Offset() == 0 {
		if bufs := mArr.Data().Buffers(); len(bufs) >= 2 && bufs[1] != nil {
			return filterStringByBitmap(name, sArr.(*array.String), bufs[1].Bytes(), cfg.alloc)
		}
	}

	// Fallback: precompute indices then gather. Used for Boolean / String
	// dtypes, non-zero mask offsets, or when the source has nulls we must
	// preserve.
	indices := collectMaskIndices(mArr, mBool, n)

	// No-null fast path (take values via precomputed indices): one allocator
	// tracked output buffer, no memcpy through the arrow builder.
	if sArr.NullN() == 0 {
		switch s.DType().ID() {
		case arrow.INT64:
			src := int64Values(sArr)
			return series.BuildInt64Direct(name, len(indices), cfg.alloc, func(out []int64) {
				for j, i := range indices {
					out[j] = src[i]
				}
			})
		case arrow.FLOAT64:
			src := float64Values(sArr)
			return series.BuildFloat64Direct(name, len(indices), cfg.alloc, func(out []float64) {
				for j, i := range indices {
					out[j] = src[i]
				}
			})
		}
	}

	switch s.DType().ID() {
	case arrow.INT32:
		return takeNumeric(name, sArr, int32Values(sArr), indices, cfg.alloc, fromInt32Result)
	case arrow.INT64:
		return takeNumeric(name, sArr, int64Values(sArr), indices, cfg.alloc, fromInt64Result)
	case arrow.UINT32:
		return takeNumeric(name, sArr, uint32Values(sArr), indices, cfg.alloc, fromUint32Result)
	case arrow.UINT64:
		return takeNumeric(name, sArr, uint64Values(sArr), indices, cfg.alloc, fromUint64Result)
	case arrow.FLOAT32:
		return takeNumeric(name, sArr, float32Values(sArr), indices, cfg.alloc, fromFloat32Result)
	case arrow.FLOAT64:
		return takeNumeric(name, sArr, float64Values(sArr), indices, cfg.alloc, fromFloat64Result)
	case arrow.BOOL:
		return takeBool(name, sArr, indices, cfg.alloc)
	case arrow.STRING:
		return takeString(name, sArr, indices, cfg.alloc)
	case arrow.TIMESTAMP, arrow.DATE64, arrow.TIME64, arrow.DURATION:
		return takeInt64Typed(name, sArr, indices, cfg.alloc)
	case arrow.DATE32, arrow.TIME32:
		return takeInt32Typed(name, sArr, indices, cfg.alloc)
	case arrow.DICTIONARY:
		return catTake(name, sArr, indices, cfg.alloc)
	}
	// Small ints, lists, structs and other less common dtypes go through
	// the generic gather, as in Take.
	out, err := s.Gather(indices, series.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	if out.Name() != name {
		renamed := out.Rename(name)
		out.Release()
		return renamed, nil
	}
	return out, nil
}

// Take returns a Series assembled by gathering rows at the given indices.
// Negative or out-of-range indices return an error. A null at indices[i]
// position is not possible (the caller provides plain ints); to materialize
// a null use the mask form via Filter or rely on gather primitives in the
// expression layer.
func Take(ctx context.Context, s *series.Series, indices []int, opts ...Option) (_ *series.Series, err error) {
	cfg := resolve(opts)

	// Catch any out-of-range slice access from the typed fast paths below
	// and convert it into the documented error. This replaces a pre-loop
	// that scanned every index up-front - at small N the pre-loop alone
	// was 15-20% of total Take time, completely wasted since Go's
	// intrinsic bounds check already fires on src[indices[j]]. For the
	// benchmark (in-bounds permutation) this is free; only OOB callers
	// pay the defer+recover cost.
	n := s.Len()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("compute.Take: index out of range for Series of length %d (%v)", n, r)
		}
	}()

	sArr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer sArr.Release()

	name := cfg.outName(s.Name())

	switch s.DType().ID() {
	case arrow.INT32:
		return takeNumeric(name, sArr, int32Values(sArr), indices, cfg.alloc, fromInt32Result)
	case arrow.INT64:
		if sArr.NullN() == 0 {
			return takeInt64Direct(name, int64Values(sArr), indices, cfg.alloc)
		}
		return takeNumeric(name, sArr, int64Values(sArr), indices, cfg.alloc, fromInt64Result)
	case arrow.UINT32:
		return takeNumeric(name, sArr, uint32Values(sArr), indices, cfg.alloc, fromUint32Result)
	case arrow.UINT64:
		return takeNumeric(name, sArr, uint64Values(sArr), indices, cfg.alloc, fromUint64Result)
	case arrow.FLOAT32:
		return takeNumeric(name, sArr, float32Values(sArr), indices, cfg.alloc, fromFloat32Result)
	case arrow.FLOAT64:
		if sArr.NullN() == 0 {
			return takeFloat64Direct(name, float64Values(sArr), indices, cfg.alloc)
		}
		return takeNumeric(name, sArr, float64Values(sArr), indices, cfg.alloc, fromFloat64Result)
	case arrow.BOOL:
		return takeBool(name, sArr, indices, cfg.alloc)
	case arrow.STRING:
		return takeString(name, sArr, indices, cfg.alloc)
	case arrow.TIMESTAMP, arrow.DATE64, arrow.TIME64, arrow.DURATION:
		return takeInt64Typed(name, sArr, indices, cfg.alloc)
	case arrow.DATE32, arrow.TIME32:
		return takeInt32Typed(name, sArr, indices, cfg.alloc)
	case arrow.DICTIONARY:
		return catTake(name, sArr, indices, cfg.alloc)
	}
	// Nested and less common dtypes (lists, structs, small ints) go
	// through the generic gather.
	out, err := s.Gather(indices, series.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	if out.Name() != name {
		renamed := out.Rename(name)
		out.Release()
		return renamed, nil
	}
	return out, nil
}

// takeInt64Typed gathers rows from an int64-backed array (TIMESTAMP, DATE64,
// TIME64, DURATION) at indices, preserving the source dtype on the output.
func takeInt64Typed(name string, src arrow.Array, indices []int, mem memory.Allocator) (*series.Series, error) {
	n := len(indices)
	dt := src.DataType()
	var values []int64
	if src.Len() > 0 {
		srcBuf := src.Data().Buffers()[1].Bytes()
		srcOffset := src.Data().Offset()
		values = unsafe.Slice((*int64)(unsafe.Pointer(&srcBuf[0])), srcOffset+src.Len())[srcOffset:]
	}

	hasNulls := src.NullN() > 0
	var valid []bool
	if hasNulls {
		valid = make([]bool, n)
	}
	return series.BuildTypedInt64Direct(name, n, mem, dt, valid, func(out []int64) {
		if hasNulls {
			for j, i := range indices {
				out[j] = values[i]
				valid[j] = src.IsValid(i)
			}
			return
		}
		for j, i := range indices {
			out[j] = values[i]
		}
	})
}

// takeInt32Typed is the int32-backed sibling for DATE32 / TIME32.
func takeInt32Typed(name string, src arrow.Array, indices []int, mem memory.Allocator) (*series.Series, error) {
	n := len(indices)
	dt := src.DataType()
	var values []int32
	if src.Len() > 0 {
		srcBuf := src.Data().Buffers()[1].Bytes()
		srcOffset := src.Data().Offset()
		values = unsafe.Slice((*int32)(unsafe.Pointer(&srcBuf[0])), srcOffset+src.Len())[srcOffset:]
	}

	hasNulls := src.NullN() > 0
	var valid []bool
	if hasNulls {
		valid = make([]bool, n)
	}
	return series.BuildTypedInt32Direct(name, n, mem, dt, valid, func(out []int32) {
		if hasNulls {
			for j, i := range indices {
				out[j] = values[i]
				valid[j] = src.IsValid(i)
			}
			return
		}
		for j, i := range indices {
			out[j] = values[i]
		}
	})
}

// takeInt64Direct gathers indices into a freshly allocated (pool-backed)
// arrow int64 buffer, bypassing the []int64 → memcpy → arrow path used by
// takeNumeric. At 256K+ this saves one 2 MB memcpy per call.
func takeInt64Direct(name string, src []int64, indices []int, mem memory.Allocator) (*series.Series, error) {
	return series.BuildInt64Direct(name, len(indices), poolingMem(mem), func(out []int64) {
		parallelGather(out, src, indices)
	})
}

func takeFloat64Direct(name string, src []float64, indices []int, mem memory.Allocator) (*series.Series, error) {
	return series.BuildFloat64Direct(name, len(indices), poolingMem(mem), func(out []float64) {
		parallelGather(out, src, indices)
	})
}

// takeParallelCutoff is the output length above which a gather fans out.
// Below it, goroutine startup costs more than the gather itself.
const takeParallelCutoff = 64 * 1024

// parallelGather runs gatherInto over chunks of indices in parallel for
// large outputs. A panic in a worker (out-of-range index) is re-raised
// on the calling goroutine so Take's recover still sees it.
func parallelGather[T gatherElem](out, src []T, indices []int) {
	n := len(indices)
	if n < takeParallelCutoff {
		gatherInto(out, src, indices)
		return
	}
	var bad atomic.Value
	_ = pool.ParallelFor(context.Background(), n, 0, func(_ context.Context, s, e int) error {
		defer func() {
			if r := recover(); r != nil {
				bad.Store(fmt.Sprint(r))
			}
		}()
		gatherInto(out[s:e], src, indices[s:e])
		return nil
	})
	if r := bad.Load(); r != nil {
		panic(r)
	}
}

// collectMaskIndices is the hot path of Filter. Profiling showed ~30% of
// Filter's CPU in per-row bitmap accessors (bitutil.BitIsSet via
// array.Boolean.Value + array.IsValid). This byte-scan version reads the
// raw validity and data bitmaps one byte at a time, skipping entire 8-row
// blocks when no bits are set. It falls back to the accessor loop when the
// mask has a non-zero bit offset, which the byte-scan math would misalign.
func collectMaskIndices(mArr arrow.Array, mBool *array.Boolean, n int) []int {
	data := mArr.Data()
	if data.Offset() != 0 {
		return collectMaskIndicesSlow(mArr, mBool, n)
	}
	buffers := data.Buffers()
	if len(buffers) < 2 || buffers[1] == nil {
		return collectMaskIndicesSlow(mArr, mBool, n)
	}
	valueBytes := buffers[1].Bytes()
	var validBytes []byte
	if mArr.NullN() > 0 && buffers[0] != nil {
		validBytes = buffers[0].Bytes()
	}

	estimate := max(n/4, 16)
	indices := make([]int, 0, estimate)

	nFull := n / 8
	for byteIdx := range nFull {
		b := valueBytes[byteIdx]
		if validBytes != nil {
			b &= validBytes[byteIdx]
		}
		if b == 0 {
			continue
		}
		base := byteIdx * 8
		for bit := range 8 {
			if b&(1<<bit) != 0 {
				indices = append(indices, base+bit)
			}
		}
	}
	if rem := n % 8; rem > 0 {
		b := valueBytes[nFull]
		if validBytes != nil {
			b &= validBytes[nFull]
		}
		base := nFull * 8
		for bit := range rem {
			if b&(1<<bit) != 0 {
				indices = append(indices, base+bit)
			}
		}
	}
	return indices
}

// countSetBitsInMask returns the number of set bits in the first n bits of
// maskBytes, processing 64 bits at a time via POPCNT.
func countSetBitsInMask(maskBytes []byte, n int) int {
	nFullWords := n / 64
	count := 0
	for wordIdx := range nFullWords {
		w := *(*uint64)(unsafe.Pointer(&maskBytes[wordIdx*8]))
		count += bits.OnesCount64(w)
	}
	// Tail: remaining bits within the last partial word.
	rem := n - nFullWords*64
	if rem > 0 {
		// Read remaining bytes safely.
		var w uint64
		base := nFullWords * 8
		for i := 0; i < (rem+7)/8; i++ {
			w |= uint64(maskBytes[base+i]) << (i * 8)
		}
		// Mask off bits beyond rem.
		w &= (uint64(1) << rem) - 1
		count += bits.OnesCount64(w)
	}
	return count
}

// fusedFilterInt64 scans the mask bitmap and writes surviving src values
// directly into the output buffer in one pass. Processes 64 bits at a time
// via POPCNT (sizing) and TZCNT (gather). For n>=256K we parallelize:
// 1) per-chunk POPCNT to compute chunk output sizes,
// 2) exclusive prefix-sum → per-chunk output offset,
// 3) parallel per-chunk scatter into pre-sized output.
// Mirrors polars' bitmap-aware filter: one allocation, sequential reads,
// no intermediate []int indices.
// FusedFilterInt64ByBitmap is the exported entry point for kernels
// (e.g. DropNulls) that already own a packed bitmap matching the
// mask semantics expected by fusedFilterInt64. Skips the mask wrap
// and the Filter dispatch; writes straight to an arrow-backed buffer.
func FusedFilterInt64ByBitmap(name string, src []int64, maskBytes []byte, n int, mem memory.Allocator) (*series.Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	// poolingMem recycles arrow buffers between calls - eliminates the
	// mallocgc + memset for every output allocation. DropNulls microbench
	// profiling on Mac showed 71% of time in runtime.gcStart from the
	// allocator dropping fresh buffers every iteration; pooling closes
	// that directly.
	return fusedFilterInt64(name, src, maskBytes, n, poolingMem(mem))
}

// FusedFilterFloat64ByBitmap mirrors FusedFilterInt64ByBitmap for float64.
func FusedFilterFloat64ByBitmap(name string, src []float64, maskBytes []byte, n int, mem memory.Allocator) (*series.Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return fusedFilterFloat64(name, src, maskBytes, n, poolingMem(mem))
}

func fusedFilterInt64(name string, src []int64, maskBytes []byte, n int, mem memory.Allocator) (*series.Series, error) {
	count := countSetBitsInMask(maskBytes, n)

	if n >= parallelFilterCutoff {
		return parallelFusedFilterInt64(name, src, maskBytes, n, count, mem)
	}
	return series.BuildInt64Direct(name, count, mem, func(out []int64) {
		fusedFilterInt64Scatter(src, maskBytes, out, 0, n, 0)
	})
}

// fusedFilterInt64Scatter is the inner scatter kernel. It reads mask bits
// in [wordBitStart, wordBitEnd) aligned to 64-bit words and writes matched
// values to out[outOffset:]. Returns the number of values written.
//
// The inner loop uses unsafe pointer arithmetic to bypass Go's
// bounds checks on both src and out. Caller guarantees:
//   - maskBytes has at least ceil(wordBitEnd/8) bytes
//   - src has at least wordBitEnd elements
//   - out has outOffset + popcount(mask[wordBitStart:wordBitEnd]) slots
//
// Measured at ~18% faster on DropNulls 262K - the prior bounds-check
// elision happened sporadically depending on inliner state; the
// explicit unsafe path is consistent.
// compactWord64 copies the 8-byte elements src[base+b] for every set
// bit b of the non-zero mask word w to out[idx...], returning the new
// idx. Three shapes:
//   - all 64 bits set: one bulk copy.
//   - sparse words: iterate set bits with trailing-zero counts.
//   - dense words: a branchless loop that stores every element up to
//     the highest set bit and advances idx by the bit. Unselected
//     elements land in the slot the next selected element overwrites,
//     and stopping at the highest set bit means no store goes past the
//     word's last output slot, so neighbouring workers never collide.
//
// The branchless form removes the data-dependent loop exit of the
// trailing-zero loop, which mispredicts constantly on random masks of
// middling density (DropNulls on 30% nulls is the benchmark case).
func compactWord64(srcPtr, outPtr unsafe.Pointer, w uint64, base, idx int) int {
	if w == ^uint64(0) {
		copy(unsafe.Slice((*uint64)(unsafe.Add(outPtr, idx*8)), 64),
			unsafe.Slice((*uint64)(unsafe.Add(srcPtr, base*8)), 64))
		return idx + 64
	}
	if bits.OnesCount64(w) < 16 {
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			*(*uint64)(unsafe.Add(outPtr, idx*8)) = *(*uint64)(unsafe.Add(srcPtr, (base+bit)*8))
			idx++
			w &= w - 1
		}
		return idx
	}
	hi := 64 - bits.LeadingZeros64(w)
	s := unsafe.Slice((*uint64)(unsafe.Add(srcPtr, base*8)), hi)
	for b, v := range s {
		*(*uint64)(unsafe.Add(outPtr, idx*8)) = v
		idx += int(w >> uint(b) & 1)
	}
	return idx
}

func fusedFilterInt64Scatter(src []int64, maskBytes []byte, out []int64, wordBitStart, wordBitEnd, outOffset int) int {
	_ = src[wordBitEnd-1] // bounds-check hoist: compiler may elide inner check
	_ = out[outOffset:]
	srcPtr := unsafe.Pointer(unsafe.SliceData(src))
	outPtr := unsafe.Pointer(unsafe.SliceData(out))
	idx := outOffset
	w0 := wordBitStart / 64
	w1 := wordBitEnd / 64
	for wordIdx := w0; wordIdx < w1; wordIdx++ {
		w := *(*uint64)(unsafe.Pointer(&maskBytes[wordIdx*8]))
		if w == 0 {
			continue
		}
		idx = compactWord64(srcPtr, outPtr, w, wordIdx*64, idx)
	}
	// Tail bits (only used when wordBitEnd is not a multiple of 64, e.g.
	// at the very end of the array).
	rem := wordBitEnd - w1*64
	if rem > 0 {
		var w uint64
		base := w1 * 8
		for i := range (rem + 7) / 8 {
			w |= uint64(maskBytes[base+i]) << (i * 8)
		}
		w &= (uint64(1) << rem) - 1
		baseIdx := w1 * 64
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			v := *(*int64)(unsafe.Add(srcPtr, (baseIdx+bit)*8))
			*(*int64)(unsafe.Add(outPtr, idx*8)) = v
			idx++
			w &= w - 1
		}
	}
	return idx - outOffset
}

func parallelFusedFilterInt64(name string, src []int64, maskBytes []byte, n, count int, mem memory.Allocator) (*series.Series, error) {
	// Pick worker count by row size: goroutine startup is ~50-100 us
	// each, so 8 workers at 262K (≈30 us real work per worker) was net
	// negative on the DropNulls bench. 4 workers up to ~500K, then 8
	// above that - measured sweet spot on the i7-10700.
	k := 4
	if n >= 512*1024 {
		k = 8
	}
	// Align chunk boundaries to 64-bit-word multiples so neither POPCNT nor
	// scatter crosses a mask word.
	nWords := n / 64
	wordsPerWorker := (nWords + k - 1) / k
	// Phase 1: per-worker popcount over its word range.
	chunkCounts := make([]int, k)
	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := w * wordsPerWorker
			we := min(ws+wordsPerWorker, nWords)
			sum := 0
			for wi := ws; wi < we; wi++ {
				word := *(*uint64)(unsafe.Pointer(&maskBytes[wi*8]))
				sum += bits.OnesCount64(word)
			}
			chunkCounts[w] = sum
		}(w)
	}
	wg.Wait()
	// Account for trailing partial word in worker k-1.
	tailBits := n - nWords*64
	if tailBits > 0 {
		var w uint64
		base := nWords * 8
		for i := 0; i < (tailBits+7)/8; i++ {
			w |= uint64(maskBytes[base+i]) << (i * 8)
		}
		w &= (uint64(1) << tailBits) - 1
		chunkCounts[k-1] += bits.OnesCount64(w)
	}
	// Phase 2: exclusive prefix sum over chunkCounts.
	chunkOffsets := make([]int, k+1)
	cum := 0
	for w := range k {
		chunkOffsets[w] = cum
		cum += chunkCounts[w]
	}
	chunkOffsets[k] = cum

	return series.BuildInt64Direct(name, count, mem, func(out []int64) {
		for w := range k {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				ws := w * wordsPerWorker
				we := min(ws+wordsPerWorker, nWords)
				wordBitStart := ws * 64
				wordBitEnd := we * 64
				// Last worker also handles the trailing partial word.
				if w == k-1 && tailBits > 0 {
					wordBitEnd = n
				}
				fusedFilterInt64Scatter(src, maskBytes, out, wordBitStart, wordBitEnd, chunkOffsets[w])
			}(w)
		}
		wg.Wait()
	})
}

// fusedFilterFloat64 is the float64 variant.
func fusedFilterFloat64(name string, src []float64, maskBytes []byte, n int, mem memory.Allocator) (*series.Series, error) {
	count := countSetBitsInMask(maskBytes, n)

	if n >= parallelFilterCutoff {
		return parallelFusedFilterFloat64(name, src, maskBytes, n, count, mem)
	}
	return series.BuildFloat64Direct(name, count, mem, func(out []float64) {
		fusedFilterFloat64Scatter(src, maskBytes, out, 0, n, 0)
	})
}

func fusedFilterFloat64Scatter(src []float64, maskBytes []byte, out []float64, wordBitStart, wordBitEnd, outOffset int) int {
	// Same unsafe-gather pattern as the int64 variant - see
	// fusedFilterInt64Scatter for the invariants the caller must hold.
	_ = src[wordBitEnd-1]
	_ = out[outOffset:]
	srcPtr := unsafe.Pointer(unsafe.SliceData(src))
	outPtr := unsafe.Pointer(unsafe.SliceData(out))
	idx := outOffset
	w0 := wordBitStart / 64
	w1 := wordBitEnd / 64
	for wordIdx := w0; wordIdx < w1; wordIdx++ {
		w := *(*uint64)(unsafe.Pointer(&maskBytes[wordIdx*8]))
		if w == 0 {
			continue
		}
		idx = compactWord64(srcPtr, outPtr, w, wordIdx*64, idx)
	}
	rem := wordBitEnd - w1*64
	if rem > 0 {
		var w uint64
		base := w1 * 8
		for i := range (rem + 7) / 8 {
			w |= uint64(maskBytes[base+i]) << (i * 8)
		}
		w &= (uint64(1) << rem) - 1
		baseIdx := w1 * 64
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			v := *(*float64)(unsafe.Add(srcPtr, (baseIdx+bit)*8))
			*(*float64)(unsafe.Add(outPtr, idx*8)) = v
			idx++
			w &= w - 1
		}
	}
	return idx - outOffset
}

func parallelFusedFilterFloat64(name string, src []float64, maskBytes []byte, n, count int, mem memory.Allocator) (*series.Series, error) {
	k := 4
	if n >= 512*1024 {
		k = 8
	}
	nWords := n / 64
	wordsPerWorker := (nWords + k - 1) / k
	chunkCounts := make([]int, k)
	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := w * wordsPerWorker
			we := min(ws+wordsPerWorker, nWords)
			sum := 0
			for wi := ws; wi < we; wi++ {
				word := *(*uint64)(unsafe.Pointer(&maskBytes[wi*8]))
				sum += bits.OnesCount64(word)
			}
			chunkCounts[w] = sum
		}(w)
	}
	wg.Wait()
	tailBits := n - nWords*64
	if tailBits > 0 {
		var w uint64
		base := nWords * 8
		for i := 0; i < (tailBits+7)/8; i++ {
			w |= uint64(maskBytes[base+i]) << (i * 8)
		}
		w &= (uint64(1) << tailBits) - 1
		chunkCounts[k-1] += bits.OnesCount64(w)
	}
	chunkOffsets := make([]int, k+1)
	cum := 0
	for w := range k {
		chunkOffsets[w] = cum
		cum += chunkCounts[w]
	}
	chunkOffsets[k] = cum

	return series.BuildFloat64Direct(name, count, mem, func(out []float64) {
		for w := range k {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				ws := w * wordsPerWorker
				we := min(ws+wordsPerWorker, nWords)
				wordBitStart := ws * 64
				wordBitEnd := we * 64
				if w == k-1 && tailBits > 0 {
					wordBitEnd = n
				}
				fusedFilterFloat64Scatter(src, maskBytes, out, wordBitStart, wordBitEnd, chunkOffsets[w])
			}(w)
		}
		wg.Wait()
	})
}

// collectMaskIndicesSlow uses arrow accessors and handles the non-zero
// bit-offset case correctly.
func collectMaskIndicesSlow(mArr arrow.Array, mBool *array.Boolean, n int) []int {
	indices := make([]int, 0, n/4)
	for i := range n {
		if mArr.IsValid(i) && mBool.Value(i) {
			indices = append(indices, i)
		}
	}
	return indices
}

func takeNumeric[T Numeric](
	name string,
	src arrow.Array,
	vals []T,
	indices []int,
	mem memory.Allocator,
	build func(string, []T, []bool, memory.Allocator) (*series.Series, error),
) (*series.Series, error) {
	n := len(indices)
	out := make([]T, n)
	var valid []bool
	if src.NullN() > 0 {
		valid = make([]bool, n)
	}
	// Parallelize the gather at 64K+: the inner loop is a pointer-chasing
	// random read against the source array, so the wall-clock win scales
	// with memory-controller parallelism rather than CPU throughput.
	// 8-way batched gather: snapshot 8 consecutive indices upfront, then
	// issue 8 potentially-random loads so the CPU's out-of-order window
	// can overlap their cache-miss latency. Same technique as the join
	// gatherNullable fast path.
	if n >= 64*1024 && valid == nil {
		_ = pool.ParallelFor(context.Background(), n, 0, func(_ context.Context, s, e int) error {
			j := s
			for ; j+8 <= e; j += 8 {
				i0 := indices[j]
				i1 := indices[j+1]
				i2 := indices[j+2]
				i3 := indices[j+3]
				i4 := indices[j+4]
				i5 := indices[j+5]
				i6 := indices[j+6]
				i7 := indices[j+7]
				out[j] = vals[i0]
				out[j+1] = vals[i1]
				out[j+2] = vals[i2]
				out[j+3] = vals[i3]
				out[j+4] = vals[i4]
				out[j+5] = vals[i5]
				out[j+6] = vals[i6]
				out[j+7] = vals[i7]
			}
			for ; j < e; j++ {
				out[j] = vals[indices[j]]
			}
			return nil
		})
		return build(name, out, valid, mem)
	}
	for j, i := range indices {
		out[j] = vals[i]
		if valid != nil {
			valid[j] = src.IsValid(i)
		}
	}
	return build(name, out, valid, mem)
}

func takeBool(name string, src arrow.Array, indices []int, mem memory.Allocator) (*series.Series, error) {
	boolArr := src.(*array.Boolean)
	out := make([]bool, len(indices))
	var valid []bool
	if src.NullN() > 0 {
		valid = make([]bool, len(indices))
	}
	for j, i := range indices {
		out[j] = boolArr.Value(i)
		if valid != nil {
			valid[j] = src.IsValid(i)
		}
	}
	return fromBoolResult(name, out, valid, mem)
}
