package parquet

import (
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

var errRLE = errors.New("parquet: corrupt RLE/bit-packed data")

// rleReader decodes the parquet RLE / bit-packing hybrid encoding used
// for definition levels, dictionary indices and RLE booleans.
type rleReader struct {
	b   []byte
	pos int
	bw  int
	// Current run: an RLE run of rleLeft copies of rleVal, or a bit-packed
	// run of packLeft values starting at value packIdx of packed.
	rleLeft  int
	rleVal   uint32
	packLeft int
	packIdx  int
	packed   []byte
}

func newRLEReader(b []byte, bw int) rleReader {
	return rleReader{b: b, bw: bw}
}

// nextRun loads the next run header. It returns false at the end of the
// data or on corrupt input (r.pos is set past the end in that case).
func (r *rleReader) nextRun() bool {
	if r.pos >= len(r.b) {
		return false
	}
	h, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 || h > 1<<40 {
		r.pos = len(r.b) + 1
		return false
	}
	r.pos += n
	if h&1 == 0 {
		nb := (r.bw + 7) / 8
		if r.pos+nb > len(r.b) {
			r.pos = len(r.b) + 1
			return false
		}
		var v uint32
		for i := range nb {
			v |= uint32(r.b[r.pos+i]) << (8 * i)
		}
		r.pos += nb
		r.rleLeft = int(h >> 1)
		r.rleVal = v
		return true
	}
	groups := int(h >> 1)
	nbytes := groups * r.bw
	// The last group of a page may be truncated by writers that know the
	// value count; clamp to the available bytes.
	avail := len(r.b) - r.pos
	if nbytes > avail {
		nbytes = avail
	}
	r.packed = r.b[r.pos:]
	r.packIdx = 0
	if r.bw == 0 {
		r.packLeft = groups * 8
	} else {
		r.packLeft = min(groups*8, nbytes*8/r.bw)
	}
	r.pos += nbytes
	return true
}

// readBits decodes n values of a bit width 1 stream into dst starting
// at bit off and returns the number of ones.
func (r *rleReader) readBits(dst []byte, off, n int) (int, error) {
	ones := 0
	for n > 0 {
		switch {
		case r.rleLeft > 0:
			k := min(r.rleLeft, n)
			if r.rleVal != 0 {
				bitutil.SetBitsTo(dst, int64(off), int64(k), true)
				ones += k
			} else {
				bitutil.SetBitsTo(dst, int64(off), int64(k), false)
			}
			r.rleLeft -= k
			off += k
			n -= k
		case r.packLeft > 0:
			k := min(r.packLeft, n)
			if r.bw == 1 {
				bitutil.CopyBitmap(r.packed, r.packIdx, k, dst, off)
				ones += bitutil.CountSetBits(dst, off, k)
			} else {
				for i := range k {
					v := unpackOne(r.packed, r.packIdx+i, r.bw)
					if v != 0 {
						bitutil.SetBit(dst, off+i)
						ones++
					} else {
						bitutil.ClearBit(dst, off+i)
					}
				}
			}
			r.packIdx += k
			r.packLeft -= k
			off += k
			n -= k
		default:
			if !r.nextRun() {
				return ones, errRLE
			}
		}
	}
	return ones, nil
}

// readU32 decodes len(dst) values.
func (r *rleReader) readU32(dst []uint32) error {
	for len(dst) > 0 {
		switch {
		case r.rleLeft > 0:
			k := min(r.rleLeft, len(dst))
			v := r.rleVal
			for i := range dst[:k] {
				dst[i] = v
			}
			r.rleLeft -= k
			dst = dst[k:]
		case r.packLeft > 0:
			k := min(r.packLeft, len(dst))
			unpack32(r.packed, r.packIdx, r.bw, dst[:k])
			r.packIdx += k
			r.packLeft -= k
			dst = dst[k:]
		default:
			if !r.nextRun() {
				return errRLE
			}
		}
	}
	return nil
}

// unpackOne extracts value i of width w from LSB-first packed bits.
func unpackOne(src []byte, i, w int) uint32 {
	bit := i * w
	var v uint64
	byteOff := bit >> 3
	for j := 0; j < 8 && byteOff+j < len(src); j++ {
		v |= uint64(src[byteOff+j]) << (8 * j)
	}
	return uint32(v>>(bit&7)) & uint32((uint64(1)<<w)-1)
}

// unpack32 extracts len(dst) values of width w starting at value start.
func unpack32(src []byte, start, w int, dst []uint32) {
	if w == 0 {
		clear(dst)
		return
	}
	mask := uint64(1)<<w - 1
	bit := start * w
	i := 0
	// Fast path: an unaligned 8-byte load covers any value of width up
	// to 32 at any bit shift.
	for ; i < len(dst); i++ {
		b := bit >> 3
		if b+8 > len(src) {
			break
		}
		dst[i] = uint32(binary.LittleEndian.Uint64(src[b:]) >> (bit & 7) & mask)
		bit += w
	}
	for ; i < len(dst); i++ {
		dst[i] = unpackOne(src, start+i, w)
	}
}

// bitWidth returns the number of bits needed for values up to v.
func bitWidth(v uint64) int { return bits.Len64(v) }
