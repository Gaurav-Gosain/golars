package strhash

import (
	"strconv"
	"strings"
	"testing"
)

func TestStringEqualInputsHashEqual(t *testing.T) {
	t.Parallel()
	for n := range 70 {
		a := strings.Repeat("x", n)
		b := string([]byte(a)) // distinct backing array
		if String(a) != String(b) {
			t.Fatalf("len %d: equal strings hash differently", n)
		}
	}
}

func TestStringFewCollisions(t *testing.T) {
	t.Parallel()
	seen := make(map[uint64]string)
	check := func(s string) {
		h := String(s)
		if prev, ok := seen[h]; ok && prev != s {
			t.Fatalf("collision: %q and %q", prev, s)
		}
		seen[h] = s
	}
	for i := range 200_000 {
		check(strconv.Itoa(i))
		check("key_" + strconv.Itoa(i))
		check(strings.Repeat("ab", i%23) + strconv.Itoa(i))
	}
	// Every length boundary and a single differing byte at each position.
	base := []byte(strings.Repeat("q", 40))
	for n := range len(base) {
		for pos := range n {
			b := append([]byte(nil), base[:n]...)
			b[pos] = 'r'
			check(string(b))
		}
		check(string(base[:n]))
	}
}

func TestStringLowBitsSpread(t *testing.T) {
	t.Parallel()
	// Linear probing uses the low bits; keys that share a long prefix
	// and differ only in their last bytes must still spread. Covers the
	// 1-3, 4-8, 9-16 and long code paths.
	formats := map[string]func(i int) string{
		"short":  func(i int) string { return "r" + strconv.Itoa(i) },
		"9to16":  func(i int) string { return "key_" + leftPad(i, 7) },
		"long":   func(i int) string { return "customer#" + leftPad(i, 12) },
		"suffix": func(i int) string { return leftPad(i, 6) + "_constant_suffix_value" },
	}
	for name, f := range formats {
		const buckets = 1 << 10
		var counts [buckets]int
		const n = buckets * 64
		for i := range n {
			counts[String(f(i))&(buckets-1)]++
		}
		for b, c := range counts {
			if c < 16 || c > 160 {
				t.Fatalf("%s: bucket %d has %d entries, want about 64", name, b, c)
			}
		}
	}
}

func leftPad(i, width int) string {
	s := strconv.Itoa(i)
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func BenchmarkString8(b *testing.B) {
	s := "abcdefgh"
	var sink uint64
	for b.Loop() {
		sink += String(s)
	}
	_ = sink
}
