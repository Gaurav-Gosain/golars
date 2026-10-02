package csv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/bits"
)

const (
	swarOnes    = 0x0101010101010101
	swarHigh    = 0x8080808080808080
	swarNewline = uint64('\n') * swarOnes
)

// swarZero flags (with the high bit) the zero bytes of v.
func swarZero(v uint64) uint64 { return (v - swarOnes) &^ v & swarHigh }

var (
	errBareQuote    = errors.New(`extraneous or missing " in quoted-field`)
	errUnterminated = errors.New("unterminated quoted field")
)

// tokenizer splits RFC 4180 records out of a byte slice. Fields are
// returned as subslices of the input when possible; quoted fields with
// "" escapes or CRLF inside are unescaped into a scratch buffer that is
// only valid until the next call.
type tokenizer struct {
	data    []byte
	delim   byte
	quotes  bool
	scratch []byte
}

// field reads one field starting at pos. It returns the field bytes, the
// position just past the terminator, and whether that terminator ended
// the record (newline or end of input).
func (t *tokenizer) field(pos int) ([]byte, int, bool, error) {
	d := t.data
	n := len(d)
	if t.quotes && pos < n && d[pos] == '"' {
		return t.quoted(pos)
	}
	delim := t.delim
	i := pos
	// Scan 8 bytes at a time for the delimiter or a newline. The lowest
	// flagged byte of the zero-byte test is always exact.
	dm := uint64(delim) * swarOnes
	for i+8 <= n {
		v := binary.LittleEndian.Uint64(d[i:])
		if m := swarZero(v^dm) | swarZero(v^swarNewline); m != 0 {
			i += bits.TrailingZeros64(m) >> 3
			break
		}
		i += 8
	}
	for ; i < n; i++ {
		c := d[i]
		if c == delim {
			return d[pos:i], i + 1, false, nil
		}
		if c == '\n' {
			end := i
			if end > pos && d[end-1] == '\r' {
				end--
			}
			return d[pos:end], i + 1, true, nil
		}
	}
	end := n
	if end > pos && d[end-1] == '\r' {
		end--
	}
	return d[pos:end], n, true, nil
}

func (t *tokenizer) quoted(pos int) ([]byte, int, bool, error) {
	d := t.data
	n := len(d)
	start := pos + 1
	i := start
	escaped := false
	for {
		j := bytes.IndexByte(d[i:], '"')
		if j < 0 {
			return nil, n, true, errUnterminated
		}
		i += j
		if i+1 < n && d[i+1] == '"' {
			escaped = true
			i += 2
			continue
		}
		break
	}
	f := d[start:i]
	if escaped || bytes.IndexByte(f, '\r') >= 0 {
		t.scratch = unescapeQuoted(t.scratch[:0], f)
		f = t.scratch
	}
	i++ // closing quote
	if i >= n {
		return f, n, true, nil
	}
	switch d[i] {
	case t.delim:
		return f, i + 1, false, nil
	case '\n':
		return f, i + 1, true, nil
	case '\r':
		if i+1 >= n {
			return f, n, true, nil
		}
		if d[i+1] == '\n' {
			return f, i + 2, true, nil
		}
	}
	return nil, i, false, errBareQuote
}

// unescapeQuoted collapses "" to " and CRLF to LF, matching
// encoding/csv.
func unescapeQuoted(dst, f []byte) []byte {
	for i := 0; i < len(f); i++ {
		c := f[i]
		if c == '"' && i+1 < len(f) && f[i+1] == '"' {
			i++
		} else if c == '\r' && i+1 < len(f) && f[i+1] == '\n' {
			continue
		}
		dst = append(dst, c)
	}
	return dst
}

// skipBlank advances past empty lines, which encoding/csv (and so the
// previous arrow-based reader) ignores.
func skipBlank(d []byte, pos int) int {
	for pos < len(d) {
		switch {
		case d[pos] == '\n':
			pos++
		case d[pos] == '\r' && pos+1 < len(d) && d[pos+1] == '\n':
			pos += 2
		default:
			return pos
		}
	}
	return pos
}

// record reads a whole record into dst as strings (copies). Used for the
// header and the inference sample, never on the hot path.
func (t *tokenizer) record(pos int, dst []string) ([]string, int, error) {
	dst = dst[:0]
	for {
		f, next, eor, err := t.field(pos)
		if err != nil {
			return nil, next, err
		}
		dst = append(dst, string(f))
		pos = next
		if eor {
			return dst, pos, nil
		}
	}
}

// splitPoints returns up to n+1 offsets that cut d into record-aligned
// pieces. Without quotes any newline is a record boundary. With quotes a
// newline is a boundary when the number of quote characters before it is
// even, which holds for RFC 4180 input; callers must re-parse
// sequentially if a split produces an error.
func splitPoints(d []byte, n int, quotes bool) []int {
	out := []int{0}
	if n <= 1 || len(d) == 0 {
		return append(out, len(d))
	}
	seg := len(d) / n
	var parity []int
	if quotes {
		// Quote parity at each segment start, from per-segment counts.
		counts := make([]int, n)
		parallelFor(n, func(i int) {
			lo, hi := i*seg, (i+1)*seg
			if i == n-1 {
				hi = len(d)
			}
			counts[i] = bytes.Count(d[lo:hi], []byte{'"'})
		})
		parity = make([]int, n)
		acc := 0
		for i := range n {
			parity[i] = acc & 1
			acc += counts[i]
		}
	}
	for i := 1; i < n; i++ {
		t := i * seg
		prev := out[len(out)-1]
		if t <= prev {
			continue
		}
		p := 0
		if quotes {
			p = parity[i]
		}
		for {
			j := bytes.IndexByte(d[t:], '\n')
			if j < 0 {
				t = -1
				break
			}
			if quotes {
				p ^= bytes.Count(d[t:t+j], []byte{'"'}) & 1
			}
			t += j + 1
			if p == 0 {
				break
			}
		}
		if t < 0 || t >= len(d) {
			break
		}
		if t > prev {
			out = append(out, t)
		}
	}
	return append(out, len(d))
}
