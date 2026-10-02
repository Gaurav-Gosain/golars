package dataframe

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkStrMerge compares the serial and hash-partitioned merges of
// per-partition string dictionaries on the UniqueStr bench shape.
func BenchmarkStrMerge(b *testing.B) {
	const n = 1 << 20
	for _, card := range []int{16, 100_000, n / 4} {
		vals := make([]string, n)
		for i := range vals {
			h := (uint64(i) * 2654435761) & 0xFFFFFFFF
			vals[i] = fmt.Sprintf("key_%07d", h%uint64(card))
		}
		s, _ := series.FromString("k", vals, nil)
		arr := s.Chunk(0).(*array.String)
		const k = 8
		chunk := (n + k - 1) / k
		parts := make([]*strEncoder, k)
		codes := make([]int32, n)
		for p := range k {
			e := newStrEncoder(arr, 64)
			e.trackHashes = true
			e.encodeRange(p*chunk, min((p+1)*chunk, n), codes[p*chunk:])
			parts[p] = e
		}
		b.Run(fmt.Sprintf("serial/card=%d", card), func(b *testing.B) {
			for b.Loop() {
				mergeStrPartsSerial(arr, parts)
			}
		})
		b.Run(fmt.Sprintf("parallel/card=%d", card), func(b *testing.B) {
			for b.Loop() {
				mergeStrPartsParallel(arr, parts, n)
			}
		})
		b.Run(fmt.Sprintf("full/card=%d", card), func(b *testing.B) {
			for b.Loop() {
				kc := encodeStringCodes(arr)
				int32Scratch.put(kc.codes)
			}
		})
		s.Release()
	}
}
