// Package strhash is a fast, non-cryptographic string hash for in-memory
// hash tables (group-by keys, dictionary encoding, joins).
//
// It follows the wyhash construction: short inputs are read with at most
// two overlapping word loads and folded with a 64x64->128 multiply. It is
// several times cheaper than hash/maphash for the short keys typical of
// categorical columns because it inlines into the caller's loop and has
// no seed state to thread through. It is not DoS resistant; callers that
// hash untrusted input at scale should resolve collisions by comparing
// keys (all callers in this module do).
package strhash

import (
	"encoding/binary"
	"math/bits"
	"unsafe"
)

const (
	p0 = 0xa0761d6478bd642f
	p1 = 0xe7037ed1a0b428db
	p2 = 0x8ebc6af09c88c6e3
)

func mix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func load64(p unsafe.Pointer) uint64 {
	return binary.LittleEndian.Uint64((*[8]byte)(p)[:])
}

func load32(p unsafe.Pointer) uint64 {
	return uint64(binary.LittleEndian.Uint32((*[4]byte)(p)[:]))
}

// String hashes s. Equal strings always hash equal; the value is stable
// within a process run and across runs.
func String(s string) uint64 {
	n := len(s)
	if n == 0 {
		return mix(p0, p1)
	}
	p := unsafe.Pointer(unsafe.StringData(s))
	var a, b uint64
	switch {
	case n < 4:
		// Three byte reads cover lengths 1..3 without overreading.
		a = uint64(s[0])<<16 | uint64(s[n>>1])<<8 | uint64(s[n-1])
	case n <= 8:
		a = load32(p)
		b = load32(unsafe.Add(p, n-4))
	case n <= 16:
		a = load64(p)
		b = load64(unsafe.Add(p, n-8))
	default:
		return long(p, n)
	}
	return mix(mix(a^p1, b^p0)^uint64(n), p2)
}

func long(p unsafe.Pointer, n int) uint64 {
	seed := uint64(p0)
	i := 0
	for ; n-i > 16; i += 16 {
		seed = mix(load64(unsafe.Add(p, i))^p1, load64(unsafe.Add(p, i+8))^seed)
	}
	a := load64(unsafe.Add(p, n-16))
	b := load64(unsafe.Add(p, n-8))
	return mix(mix(a^p1, b^seed)^uint64(n), p2)
}
