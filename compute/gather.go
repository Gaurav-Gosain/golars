package compute

// gatherInto writes src[indices[j]] into out[j] for every j. out must be
// at least len(indices) long. Out-of-range indices panic (Take converts
// the panic into its documented error).
//
// The loop works in blocks of eight through array-pointer views of the
// index and output slices, which lets the compiler drop their bounds
// checks: only the eight src lookups keep one. Eight independent loads
// per block also keep several cache misses in flight on random gathers.
func gatherInto[T any](out, src []T, indices []int) {
	n := len(indices)
	out = out[:n]
	j := 0
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
