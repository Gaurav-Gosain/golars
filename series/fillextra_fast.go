package series

import "unsafe"

// as64 reinterprets a slice of 8-byte values as []uint64 so one
// branchless kernel serves int64 and float64. Only bits move; no
// arithmetic is done on the values.
func as64[T int64 | float64](s []T) []uint64 {
	return unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(s))), len(s))
}

// fillForwardNoLimit writes out[i] = the last valid src[j] with j <= i,
// for rows at or after the first valid row, and returns that first
// valid row (n when every row is null). bits is the source validity
// bitmap with element 0 at bit off.
//
// The hot loop selects with a mask instead of branching on the bit,
// so a random null pattern costs no mispredicts; bytes that are all
// valid or all null skip the per-bit work.
func fillForwardNoLimit(out, src []uint64, bits []byte, off int) int {
	n := len(out)
	src = src[:n]
	first := firstValid(bits, off, n)
	if first == n {
		copy(out, src)
		return n
	}
	copy(out[:first], src[:first])
	last := src[first]
	i := first
	for ; i < n && (i+off)&7 != 0; i++ {
		m := -uint64(bits[(i+off)>>3] >> ((i + off) & 7) & 1)
		last ^= (src[i] ^ last) & m
		out[i] = last
	}
	for ; i+8 <= n; i += 8 {
		s, o := src[i:i+8:i+8], out[i:i+8:i+8]
		switch b := bits[(i+off)>>3]; b {
		case 0xFF:
			copy(o, s)
			last = s[7]
		case 0:
			for k := range o {
				o[k] = last
			}
		default:
			for k := range 8 {
				m := -uint64(b >> k & 1)
				last ^= (s[k] ^ last) & m
				o[k] = last
			}
		}
	}
	for ; i < n; i++ {
		m := -uint64(bits[(i+off)>>3] >> ((i + off) & 7) & 1)
		last ^= (src[i] ^ last) & m
		out[i] = last
	}
	return first
}

// fillBackwardNoLimit mirrors fillForwardNoLimit: out[i] = the next
// valid src[j] with j >= i. It returns the last valid row plus one
// (0 when every row is null); rows from there on stay null.
func fillBackwardNoLimit(out, src []uint64, bits []byte, off int) int {
	n := len(out)
	src = src[:n]
	end := lastValid(bits, off, n) + 1
	copy(out[end:], src[end:])
	if end == 0 {
		return 0
	}
	next := src[end-1]
	for i := end - 1; i >= 0; i-- {
		m := -uint64(bits[(i+off)>>3] >> ((i + off) & 7) & 1)
		next ^= (src[i] ^ next) & m
		out[i] = next
	}
	return end
}

// firstValid returns the first row in [0, n) whose bit is set, or n.
func firstValid(bits []byte, off, n int) int {
	i := 0
	for ; i < n && (i+off)&7 != 0; i++ {
		if bits[(i+off)>>3]>>((i+off)&7)&1 != 0 {
			return i
		}
	}
	for ; i+8 <= n; i += 8 {
		if bits[(i+off)>>3] != 0 {
			break
		}
	}
	for ; i < n; i++ {
		if bits[(i+off)>>3]>>((i+off)&7)&1 != 0 {
			return i
		}
	}
	return n
}

// lastValid returns the last row in [0, n) whose bit is set, or -1.
func lastValid(bits []byte, off, n int) int {
	for i := n - 1; i >= 0; i-- {
		if bits[(i+off)>>3]>>((i+off)&7)&1 != 0 {
			return i
		}
	}
	return -1
}

// directionalFillNoLimit runs the unlimited forward or backward fill
// and writes the output validity, which is one contiguous valid run:
// from the first valid row on (forward) or up to the last (backward).
// It returns the null count.
func directionalFillNoLimit(out, src []uint64, bits []byte, off int, forward bool, validBits []byte) int {
	n := len(out)
	if forward {
		first := fillForwardNoLimit(out, src, bits, off)
		return markValidFrom(validBits, first, n)
	}
	end := fillBackwardNoLimit(out, src, bits, off)
	markValidFrom(validBits, 0, end)
	return n - end
}
