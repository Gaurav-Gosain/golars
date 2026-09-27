package csv

import (
	"encoding/binary"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"sync"
)

// parseFloat64 parses a float with exactly the result of
// strconv.ParseFloat(s, 64). Plain decimals (optional sign, digits, one
// dot, optional exponent) with at most 19 significant digits are handled
// here; everything else (hex floats, inf, nan, underscores, very long
// mantissas) defers to strconv.
func parseFloat64(b []byte) (float64, bool) {
	n := len(b)
	i := 0
	neg := false
	if n > 0 {
		if c := b[0]; c == '-' || c == '+' {
			neg = c == '-'
			i = 1
		}
	}
	start := i
	mant, i := scanDigits(b, i, 0)
	nd := i - start
	frac := 0
	if i < n && b[i] == '.' {
		fs := i + 1
		mant, i = scanDigits(b, fs, mant)
		frac = i - fs
	}
	// Only the plain form with at most 19 digits (so mant cannot
	// overflow) takes this path; the rest goes through the general
	// parser.
	if i != n || nd+frac == 0 || nd+frac > 19 {
		return parseFloatGeneral(b)
	}
	return finishFloat(mant, -frac, neg, b)
}

// scanDigits accumulates the decimal digits starting at i into mant,
// eight at a time when possible, and returns the index of the first
// non-digit.
func scanDigits(b []byte, i int, mant uint64) (uint64, int) {
	for i+8 <= len(b) {
		v := binary.LittleEndian.Uint64(b[i:])
		if (v&0xF0F0F0F0F0F0F0F0)|(((v+0x0606060606060606)&0xF0F0F0F0F0F0F0F0)>>4) != 0x3333333333333333 {
			break
		}
		v -= 0x3030303030303030
		v = (v * 10) + (v >> 8)
		v = (((v & 0x000000FF000000FF) * (100 + (1000000 << 32))) + (((v >> 16) & 0x000000FF000000FF) * (1 + (10000 << 32)))) >> 32 & 0xFFFFFFFF
		mant = mant*100000000 + v
		i += 8
	}
	for ; i < len(b); i++ {
		d := b[i] - '0'
		if d > 9 {
			break
		}
		mant = mant*10 + uint64(d)
	}
	return mant, i
}

// parseFloatGeneral handles exponents, leading zeros beyond 19 digits
// and defers the rest to strconv.
func parseFloatGeneral(b []byte) (float64, bool) {
	n := len(b)
	if n == 0 {
		return 0, false
	}
	i := 0
	neg := false
	if c := b[0]; c == '-' || c == '+' {
		neg = c == '-'
		i = 1
	}
	var mant uint64
	nd := 0 // digits consumed into mant, leading zeros excluded
	sawAny := false
	dropExp := 0 // decimal exponent adjustment from fraction digits
	sawDot := false
	for ; i < n; i++ {
		c := b[i]
		if d := c - '0'; d <= 9 {
			sawAny = true
			if nd == 0 && d == 0 {
				if sawDot {
					dropExp--
				}
				continue
			}
			if nd >= 19 {
				return slowFloat(b)
			}
			mant = mant*10 + uint64(d)
			nd++
			if sawDot {
				dropExp--
			}
			continue
		}
		if c == '.' && !sawDot {
			sawDot = true
			continue
		}
		break
	}
	if !sawAny {
		return slowFloat(b)
	}
	exp := 0
	if i < n {
		if c := b[i]; c != 'e' && c != 'E' {
			return slowFloat(b)
		}
		i++
		eneg := false
		if i < n && (b[i] == '-' || b[i] == '+') {
			eneg = b[i] == '-'
			i++
		}
		if i == n {
			return slowFloat(b)
		}
		for ; i < n; i++ {
			d := b[i] - '0'
			if d > 9 || exp > 10000 {
				return slowFloat(b)
			}
			exp = exp*10 + int(d)
		}
		if eneg {
			exp = -exp
		}
	}
	exp += dropExp
	return finishFloat(mant, exp, neg, b)
}

// finishFloat converts mant * 10^exp to the nearest float64. b is the
// original text for the strconv fallback.
func finishFloat(mant uint64, exp int, neg bool, b []byte) (float64, bool) {
	if mant == 0 {
		if neg {
			return math.Copysign(0, -1), true
		}
		return 0, true
	}
	// Exact path: mantissa and power of ten are both exact in float64,
	// so one multiply or divide rounds correctly.
	if mant < 1<<53 && exp >= -22 && exp <= 22 {
		f := float64(mant)
		if exp < 0 {
			f /= pow10[-exp]
		} else {
			f *= pow10[exp]
		}
		if neg {
			f = -f
		}
		return f, true
	}
	if f, ok := eiselLemire64(mant, exp, neg); ok {
		return f, true
	}
	return slowFloat(b)
}

func slowFloat(b []byte) (float64, bool) {
	v, err := strconv.ParseFloat(unsafeString(b), 64)
	if err != nil {
		// strconv reports out-of-range values as an error but still
		// returns +-Inf or 0; arrow and polars both accept those.
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
			return v, true
		}
		return 0, false
	}
	return v, true
}

var pow10 = [...]float64{1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22}

const (
	powMinExp10 = -348
	powMaxExp10 = 347
)

// powersOfTen holds 128-bit mantissas of 10^e (rounded down, top bit
// set) for e in [powMinExp10, powMaxExp10], as {lo, hi}. It is computed
// once with math/big instead of being checked in as a table.
var (
	powersOfTen     [powMaxExp10 - powMinExp10 + 1][2]uint64
	powersOfTenOnce sync.Once
)

func initPowersOfTen() {
	ten := big.NewInt(10)
	mask := new(big.Int).SetUint64(math.MaxUint64)
	for e := powMinExp10; e <= powMaxExp10; e++ {
		var v big.Int
		if e >= 0 {
			v.Exp(ten, big.NewInt(int64(e)), nil)
			if n := v.BitLen(); n <= 128 {
				v.Lsh(&v, uint(128-n))
			} else {
				v.Rsh(&v, uint(n-128))
			}
		} else {
			var d big.Int
			d.Exp(ten, big.NewInt(int64(-e)), nil)
			v.Lsh(big.NewInt(1), uint(128+d.BitLen()))
			v.Quo(&v, &d)
			if n := v.BitLen(); n > 128 {
				v.Rsh(&v, uint(n-128))
			}
		}
		var lo, hi big.Int
		lo.And(&v, mask)
		hi.Rsh(&v, 64)
		powersOfTen[e-powMinExp10] = [2]uint64{lo.Uint64(), hi.Uint64()}
	}
}

// eiselLemire64 is the Eisel-Lemire algorithm as used by Go's strconv
// before Go 1.27 (and by fast_float). It returns ok=false for the rare
// inputs it cannot round with certainty; callers then use strconv.
func eiselLemire64(man uint64, exp10 int, neg bool) (float64, bool) {
	if exp10 < powMinExp10 || powMaxExp10 < exp10 {
		return 0, false
	}
	powersOfTenOnce.Do(initPowersOfTen)
	clz := bits.LeadingZeros64(man)
	man <<= uint(clz)
	const float64ExponentBias = 1023
	retExp2 := uint64(217706*exp10>>16+64+float64ExponentBias) - uint64(clz)

	pw := &powersOfTen[exp10-powMinExp10]
	xHi, xLo := bits.Mul64(man, pw[1])
	if xHi&0x1FF == 0x1FF && xLo+man < man {
		yHi, yLo := bits.Mul64(man, pw[0])
		mergedHi, mergedLo := xHi, xLo+yHi
		if mergedLo < xLo {
			mergedHi++
		}
		if mergedHi&0x1FF == 0x1FF && mergedLo+1 == 0 && yLo+man < man {
			return 0, false
		}
		xHi, xLo = mergedHi, mergedLo
	}
	msb := xHi >> 63
	retMantissa := xHi >> (msb + 9)
	retExp2 -= 1 ^ msb
	if xLo == 0 && xHi&0x1FF == 0 && retMantissa&3 == 1 {
		return 0, false
	}
	retMantissa += retMantissa & 1
	retMantissa >>= 1
	if retMantissa>>53 > 0 {
		retMantissa >>= 1
		retExp2++
	}
	if retExp2-1 >= 0x7FF-1 {
		return 0, false
	}
	retBits := retExp2<<52 | retMantissa&0x000FFFFFFFFFFFFF
	if neg {
		retBits |= 0x8000000000000000
	}
	return math.Float64frombits(retBits), true
}
