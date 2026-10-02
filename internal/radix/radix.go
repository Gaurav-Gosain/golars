// Package radix holds an LSD radix sort for uint64 keys whose
// significant bits are known up front. Callers pack a sort key and a
// row index into one word (key in the high bits, index in the low
// bits), so a single unsigned sort yields a stable argsort with no
// indirect key lookups: every pass streams the packed words.
package radix

import (
	"runtime"
	"sync"
)

const (
	digitBits = 11
	buckets   = 1 << digitBits
	digitMask = buckets - 1
)

// parallelCutoff is the input length above which passes run on
// several goroutines with per-worker histograms.
const parallelCutoff = 128 * 1024

var auxPool sync.Pool // *[]uint64

func getAux(n int) []uint64 {
	if v := auxPool.Get(); v != nil {
		s := *(v.(*[]uint64))
		if cap(s) >= n {
			return s[:n]
		}
	}
	return make([]uint64, n)
}

func putAux(s []uint64) {
	auxPool.Put(&s)
}

// SortUint64 sorts vals ascending, looking only at the low sigBits bits
// (higher bits must be zero). Passes whose digit is constant across the
// input are skipped. The result is written back into vals.
func SortUint64(vals []uint64, sigBits int) {
	n := len(vals)
	if n < 2 || sigBits <= 0 {
		return
	}
	passes := (sigBits + digitBits - 1) / digitBits
	if n < 256 {
		insertion(vals)
		return
	}
	aux := getAux(n)
	defer putAux(aux)
	var src, dst []uint64
	if n >= parallelCutoff && runtime.GOMAXPROCS(0) > 1 {
		src, dst = sortParallel(vals, aux, passes)
	} else {
		src, dst = sortSerial(vals, aux, passes)
	}
	_ = dst
	if &src[0] != &vals[0] {
		copy(vals, src)
	}
}

// sortSerial runs the passes with one histogram read up front for all
// digits. Returns the buffer holding the sorted data first.
func sortSerial(vals, aux []uint64, passes int) ([]uint64, []uint64) {
	counts := make([][buckets]int, passes)
	for _, v := range vals {
		for p := range passes {
			counts[p][(v>>(uint(p)*digitBits))&digitMask]++
		}
	}
	n := len(vals)
	src, dst := vals, aux
	for p := range passes {
		c := &counts[p]
		shift := uint(p) * digitBits
		if c[(src[0]>>shift)&digitMask] == n {
			continue
		}
		off := 0
		for d := range c {
			k := c[d]
			c[d] = off
			off += k
		}
		scatter(src, dst, c, shift)
		src, dst = dst, src
	}
	return src, dst
}

func scatter(src, dst []uint64, off *[buckets]int, shift uint) {
	i := 0
	for ; i+1 < len(src); i += 2 {
		v0, v1 := src[i], src[i+1]
		d0 := (v0 >> shift) & digitMask
		d1 := (v1 >> shift) & digitMask
		p0 := off[d0]
		off[d0] = p0 + 1
		p1 := off[d1]
		off[d1] = p1 + 1
		dst[p0] = v0
		dst[p1] = v1
	}
	for ; i < len(src); i++ {
		v := src[i]
		d := (v >> shift) & digitMask
		dst[off[d]] = v
		off[d]++
	}
}

// sortParallel splits every pass across workers: each counts its slice,
// a serial prefix sum assigns every (worker, digit) pair a disjoint
// output range, then each worker scatters its slice. Order within a
// digit follows worker order, so the sort stays stable.
func sortParallel(vals, aux []uint64, passes int) ([]uint64, []uint64) {
	n := len(vals)
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	hists := make([][buckets]int, k)
	src, dst := vals, aux
	var wg sync.WaitGroup
	for p := range passes {
		shift := uint(p) * digitBits
		for w := range k {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := &hists[w]
				clear(h[:])
				start := w * chunk
				end := min(start+chunk, n)
				for _, v := range src[start:end] {
					h[(v>>shift)&digitMask]++
				}
			}()
		}
		wg.Wait()
		total := 0
		trivial := false
		for d := range buckets {
			sum := 0
			for w := range k {
				c := hists[w][d]
				hists[w][d] = total
				total += c
				sum += c
			}
			if sum == n {
				trivial = true
			}
		}
		if trivial {
			continue
		}
		for w := range k {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := w * chunk
				end := min(start+chunk, n)
				if start < end {
					scatter(src[start:end], dst, &hists[w], shift)
				}
			}()
		}
		wg.Wait()
		src, dst = dst, src
	}
	return src, dst
}

func insertion(vals []uint64) {
	for i := 1; i < len(vals); i++ {
		v := vals[i]
		j := i - 1
		for j >= 0 && vals[j] > v {
			vals[j+1] = vals[j]
			j--
		}
		vals[j+1] = v
	}
}
