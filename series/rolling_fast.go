package series

import "unsafe"

// markValidFrom sets the validity bits for rows [from, n) and returns
// the null count, from. Rolling kernels without input nulls produce
// nulls only for the leading min_periods-1 rows, so the bitmap is a
// run of zeros followed by ones and can be written a byte at a time
// instead of one read-modify-write per row.
func markValidFrom(validBits []byte, from, n int) int {
	if from >= n {
		return n
	}
	i := from
	for ; i < n && i&7 != 0; i++ {
		validBits[i>>3] |= 1 << (i & 7)
	}
	full := n >> 3
	if b := i >> 3; b < full {
		bytes := validBits[b:full]
		for k := range bytes {
			bytes[k] = 0xFF
		}
		i = full << 3
	}
	for ; i < n; i++ {
		validBits[i>>3] |= 1 << (i & 7)
	}
	return from
}

// rollingLead is the number of leading rows that fall short of mp
// observations in a column without nulls.
func rollingLead(n, mp int) int { return min(mp-1, n) }

// rollingMinMaxIntKernel fills out[i] with the min (or max) of
// vals[max(0, i-w+1) .. i] for integer columns without nulls. It is
// the van Herk / Gil-Werman scheme: split the rows into blocks of w,
// take running extremes forward within each block (prefix) and
// backward within each block (suffix); a full window starting at j
// then spans at most two blocks and its extreme is
// op(suffix[j], prefix[j+w-1]). That is three compares per row with
// no data-dependent branches, against the deque's unpredictable pops.
//
// The prefix pass stores int64 results in out's memory. The suffix
// pass walks j downwards and reads prefix[j+w-1] just before it
// overwrites that slot with the float result; every later read is at
// a smaller index, so it still sees a prefix value. Rows before w-1
// are partial windows whose answer is the block-0 prefix itself.
func rollingMinMaxIntKernel[T int64 | int32](out []float64, vals []T, w int, isMin bool) {
	n := len(out)
	if n == 0 {
		return
	}
	vals = vals[:n]
	pre := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(out))), n)
	for bs := 0; bs < n; bs += w {
		be := min(bs+w, n)
		blk, dst := vals[bs:be], pre[bs:be]
		dst = dst[:len(blk)]
		run := int64(blk[0])
		if isMin {
			for k, v := range blk {
				run = min(run, int64(v))
				dst[k] = run
			}
		} else {
			for k, v := range blk {
				run = max(run, int64(v))
				dst[k] = run
			}
		}
	}
	// Last row index whose window start j = i-w+1 is valid.
	lastJ := n - w
	for bs := (n - 1) / w * w; bs >= 0; bs -= w {
		be := min(bs+w, n)
		j := be - 1
		run := int64(vals[j])
		// Suffix values for j > lastJ have no output row.
		for ; j > lastJ && j >= bs; j-- {
			if isMin {
				run = min(run, int64(vals[j]))
			} else {
				run = max(run, int64(vals[j]))
			}
		}
		if j < bs {
			continue
		}
		src := vals[bs : j+1]
		dst := out[bs+w-1 : j+w]
		pp := pre[bs+w-1 : j+w]
		dst = dst[:len(src)]
		pp = pp[:len(src)]
		if isMin {
			for k := len(src) - 1; k >= 0; k-- {
				run = min(run, int64(src[k]))
				dst[k] = float64(min(run, pp[k]))
			}
		} else {
			for k := len(src) - 1; k >= 0; k-- {
				run = max(run, int64(src[k]))
				dst[k] = float64(max(run, pp[k]))
			}
		}
	}
	head := min(w-1, n)
	for i := range head {
		out[i] = float64(pre[i])
	}
}

// idxRing is a fixed-capacity deque of row indices for the monotonic
// rolling min/max drivers. A window never holds more than w+1
// indices, so a power-of-two ring sized once replaces the old
// slice-and-append deque, which reallocated every time the slice's
// start crept past its capacity.
type idxRing struct {
	buf        []int
	mask       int
	head, tail int
}

func newIdxRing(capacity int) idxRing {
	size := 1
	for size < capacity {
		size <<= 1
	}
	return idxRing{buf: make([]int, size), mask: size - 1}
}

func (r *idxRing) empty() bool    { return r.head == r.tail }
func (r *idxRing) front() int     { return r.buf[r.head&r.mask] }
func (r *idxRing) back() int      { return r.buf[(r.tail-1)&r.mask] }
func (r *idxRing) popFront()      { r.head++ }
func (r *idxRing) popBack()       { r.tail-- }
func (r *idxRing) pushBack(i int) { r.buf[r.tail&r.mask] = i; r.tail++ }
func (r *idxRing) size() int      { return r.tail - r.head }
