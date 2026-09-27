package compute

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// takeStringParallelCutoff is the output length above which the string
// gather runs its two passes on several goroutines.
const takeStringParallelCutoff = 64 * 1024

// takeString gathers string rows at indices into freshly built arrow
// buffers: one pass sums the selected lengths, a second writes the
// offsets and copies each value's bytes straight into the data buffer.
// That replaces a []string of views plus a BinaryBuilder append per row
// (with its growth copies), which profiled at about 20% of filter-heavy
// string pipelines. Null source rows produce null, zero-length outputs.
//
// Large gathers split both passes over row chunks; each chunk writes a
// disjoint byte range found by a prefix sum over chunk totals.
func takeString(name string, src arrow.Array, indices []int, mem memory.Allocator) (*series.Series, error) {
	sa := src.(*array.String)
	n := len(indices)
	offs := sa.ValueOffsets()
	var data []byte
	if bufs := sa.Data().Buffers(); len(bufs) > 2 && bufs[2] != nil {
		data = bufs[2].Bytes()
	}
	hasNulls := sa.NullN() > 0
	mem = poolingMem(mem)

	k := 1
	if n >= takeStringParallelCutoff {
		k = max(min(8, n/(takeStringParallelCutoff/4)), 1)
	}
	// Chunks start on multiples of 8 rows so each worker owns whole
	// validity bytes and never races on a shared byte.
	chunk := ((n+k-1)/k + 7) &^ 7
	totals := make([]int, k+1)
	parallelChunks(k, func(p int) {
		s, e := p*chunk, min((p+1)*chunk, n)
		if s >= e {
			return
		}
		t := 0
		for _, i := range indices[s:e] {
			if hasNulls && sa.IsNull(i) {
				continue
			}
			t += int(offs[i+1] - offs[i])
		}
		totals[p+1] = t
	})
	for p := range k {
		totals[p+1] += totals[p]
	}
	total := totals[k]
	if total > int(^uint32(0)>>1) {
		// Beyond int32 offsets: keep the generic builder path, which
		// reports the overflow.
		return takeStringBuilder(name, sa, indices, mem)
	}

	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * arrow.Int32SizeBytes)
	defer offBuf.Release()
	outOffs := arrow.Int32Traits.CastFromBytes(offBuf.Bytes())[:n+1]
	dataBuf := memory.NewResizableBuffer(mem)
	dataBuf.Resize(total)
	defer dataBuf.Release()
	outData := dataBuf.Bytes()[:total]

	var validBuf *memory.Buffer
	var validBits []byte
	if hasNulls {
		validBuf = memory.NewResizableBuffer(mem)
		validBuf.Resize(int(bitutil.BytesForBits(int64(n))))
		defer validBuf.Release()
		validBits = validBuf.Bytes()
		clear(validBits)
	}
	nullCounts := make([]int, k)
	parallelChunks(k, func(p int) {
		s, e := p*chunk, min((p+1)*chunk, n)
		if s >= e {
			return
		}
		pos := totals[p]
		nulls := 0
		for j := s; j < e; j++ {
			i := indices[j]
			outOffs[j] = int32(pos)
			if hasNulls {
				if sa.IsNull(i) {
					nulls++
					continue
				}
				bitutil.SetBit(validBits, j)
			}
			pos += copy(outData[pos:], data[offs[i]:offs[i+1]])
		}
		nullCounts[p] = nulls
	})
	outOffs[n] = int32(total)
	nullCount := 0
	for _, c := range nullCounts {
		nullCount += c
	}
	arrData := array.NewData(arrow.BinaryTypes.String, n,
		[]*memory.Buffer{validBuf, offBuf, dataBuf}, nil, nullCount, 0)
	defer arrData.Release()
	return series.New(name, array.NewStringData(arrData))
}

// filterStringByBitmap keeps the rows of a no-null string array whose
// mask bit is set, reading the mask 64 rows at a time and writing the
// offsets and bytes directly (no []int index list). Large inputs split
// on whole mask words: a first pass counts kept rows and bytes per
// chunk, a prefix sum places each chunk, a second pass copies.
func filterStringByBitmap(name string, sa *array.String, maskBytes []byte, mem memory.Allocator) (*series.Series, error) {
	n := sa.Len()
	offs := sa.ValueOffsets()
	var data []byte
	if bufs := sa.Data().Buffers(); len(bufs) > 2 && bufs[2] != nil {
		data = bufs[2].Bytes()
	}
	mem = poolingMem(mem)
	nWords := (n + 63) / 64
	word := func(w int) uint64 {
		var x uint64
		if (w+1)*8 <= len(maskBytes) {
			x = binary.LittleEndian.Uint64(maskBytes[w*8:])
		} else {
			for b := 0; w*8+b < len(maskBytes); b++ {
				x |= uint64(maskBytes[w*8+b]) << (8 * b)
			}
		}
		if rem := n - w*64; rem < 64 {
			x &= (uint64(1) << rem) - 1
		}
		return x
	}
	k := 1
	if n >= takeStringParallelCutoff {
		k = max(min(8, n/(takeStringParallelCutoff/4)), 1)
	}
	wordsPer := (nWords + k - 1) / k
	rows := make([]int, k+1)
	byteTotals := make([]int, k+1)
	parallelChunks(k, func(p int) {
		ws, we := p*wordsPer, min((p+1)*wordsPer, nWords)
		r, b := 0, 0
		for w := ws; w < we; w++ {
			x := word(w)
			r += bits.OnesCount64(x)
			for x != 0 {
				i := w*64 + bits.TrailingZeros64(x)
				b += int(offs[i+1] - offs[i])
				x &= x - 1
			}
		}
		rows[p+1], byteTotals[p+1] = r, b
	})
	for p := range k {
		rows[p+1] += rows[p]
		byteTotals[p+1] += byteTotals[p]
	}
	m, total := rows[k], byteTotals[k]
	if total > int(^uint32(0)>>1) {
		return nil, fmt.Errorf("compute.Filter: string output exceeds int32 offsets (%d bytes)", total)
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((m + 1) * arrow.Int32SizeBytes)
	defer offBuf.Release()
	outOffs := arrow.Int32Traits.CastFromBytes(offBuf.Bytes())[:m+1]
	dataBuf := memory.NewResizableBuffer(mem)
	dataBuf.Resize(total)
	defer dataBuf.Release()
	outData := dataBuf.Bytes()[:total]
	parallelChunks(k, func(p int) {
		ws, we := p*wordsPer, min((p+1)*wordsPer, nWords)
		j, pos := rows[p], byteTotals[p]
		for w := ws; w < we; w++ {
			x := word(w)
			for x != 0 {
				i := w*64 + bits.TrailingZeros64(x)
				outOffs[j] = int32(pos)
				pos += copy(outData[pos:], data[offs[i]:offs[i+1]])
				j++
				x &= x - 1
			}
		}
	})
	outOffs[m] = int32(total)
	arrData := array.NewData(arrow.BinaryTypes.String, m,
		[]*memory.Buffer{nil, offBuf, dataBuf}, nil, 0, 0)
	defer arrData.Release()
	return series.New(name, array.NewStringData(arrData))
}

// parallelChunks runs fn(0..k-1) concurrently, chunk 0 on the caller.
func parallelChunks(k int, fn func(p int)) {
	if k <= 1 {
		fn(0)
		return
	}
	var wg sync.WaitGroup
	for p := 1; p < k; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(p)
		}()
	}
	fn(0)
	wg.Wait()
}

// takeStringBuilder is the plain gather through FromString, kept for
// outputs too large for int32 offsets.
func takeStringBuilder(name string, sa *array.String, indices []int, mem memory.Allocator) (*series.Series, error) {
	out := make([]string, len(indices))
	var valid []bool
	if sa.NullN() > 0 {
		valid = make([]bool, len(indices))
	}
	for j, i := range indices {
		out[j] = sa.Value(i)
		if valid != nil {
			valid[j] = sa.IsValid(i)
		}
	}
	return series.FromString(name, out, valid, series.WithAllocator(mem))
}
