package compute

import "unsafe"

// gatherElem lists the pointer-free element types gatherInto accepts.
// The 8-byte fast path copies raw bits, which would skip the write
// barrier for pointer types.
type gatherElem interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// gatherInto writes src[indices[j]] into out[j] for every j. out must be
// at least len(indices) long. Out-of-range indices panic (Take converts
// the panic into its documented error).
//
// 8-byte element types go through gather64Accel first where the
// platform has an assembly kernel. The Go loop finishes whatever the
// kernel leaves, which includes any block holding a bad index, so the
// panic still comes from a normal bounds check.
//
// The Go loop works in blocks of eight through array-pointer views of
// the index and output slices, which lets the compiler drop their
// bounds checks: only the eight src lookups keep one. Eight independent
// loads per block also keep several cache misses in flight on random
// gathers.
func gatherInto[T gatherElem](out, src []T, indices []int) {
	n := len(indices)
	out = out[:n]
	j := 0
	var zero T
	if unsafe.Sizeof(zero) == 8 && n >= 8 {
		o := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(out))), n)
		s := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(src))), len(src))
		j = gather64Accel(o, s, indices)
	}
	for ; j+8 <= n; j += 8 {
		idx := (*[8]int)(indices[j : j+8])
		o := (*[8]T)(out[j : j+8])
		o[0] = src[idx[0]]
		o[1] = src[idx[1]]
		o[2] = src[idx[2]]
		o[3] = src[idx[3]]
		o[4] = src[idx[4]]
		o[5] = src[idx[5]]
		o[6] = src[idx[6]]
		o[7] = src[idx[7]]
	}
	for ; j < n; j++ {
		out[j] = src[indices[j]]
	}
}
