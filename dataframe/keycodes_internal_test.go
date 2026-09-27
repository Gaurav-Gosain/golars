package dataframe

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// equalUniques compares two keyUniques column by column, treating a
// nil validity slice as all-valid.
func equalUniques(t *testing.T, got, want *keyUniques) {
	t.Helper()
	if groupLen(got) != groupLen(want) {
		t.Fatalf("group count = %d, want %d", groupLen(got), groupLen(want))
	}
	for g := range groupLen(want) {
		gv := got.valid == nil || got.valid[g]
		wv := want.valid == nil || want.valid[g]
		if gv != wv {
			t.Fatalf("group %d validity = %v, want %v", g, gv, wv)
		}
		if !wv {
			continue
		}
		switch {
		case want.i64 != nil:
			if got.i64[g] != want.i64[g] {
				t.Fatalf("group %d i64 = %d, want %d", g, got.i64[g], want.i64[g])
			}
		case want.i32 != nil:
			if got.i32[g] != want.i32[g] {
				t.Fatalf("group %d i32 = %d, want %d", g, got.i32[g], want.i32[g])
			}
		case want.str != nil:
			if got.str[g] != want.str[g] {
				t.Fatalf("group %d str = %q, want %q", g, got.str[g], want.str[g])
			}
		default:
			if got.boolean[g] != want.boolean[g] {
				t.Fatalf("group %d bool = %v, want %v", g, got.boolean[g], want.boolean[g])
			}
		}
	}
}

// TestAssignGroupsViaCodesMatchesTuplePath checks the dictionary-code
// assignment against the byte-tuple path, which it replaces: identical
// first-seen group ids and identical unique keys, across dtypes, null
// patterns, cardinalities, and sizes on both sides of the parallel
// string cutoff.
func TestAssignGroupsViaCodesMatchesTuplePath(t *testing.T) {
	t.Parallel()
	sizes := []int{0, 1, 7, 1000, strPartThreshold - 1, strPartThreshold, strPartThreshold + 3, 300_001}
	for _, n := range sizes {
		for _, card := range []int{1, 5, 3000} {
			for _, nullEvery := range []int{0, 1, 7} {
				name := fmt.Sprintf("n=%d/card=%d/nulls=%d", n, card, nullEvery)
				t.Run(name, func(t *testing.T) {
					rng := rand.New(rand.NewSource(int64(n + card + nullEvery)))
					strs := make([]string, n)
					sv := make([]bool, n)
					i64s := make([]int64, n)
					iv := make([]bool, n)
					i32s := make([]int32, n)
					bools := make([]bool, n)
					bv := make([]bool, n)
					for i := range n {
						// Mix short keys (packed path) with long ones
						// (byte-hash path), including near the 7-byte edge.
						switch r := rng.Intn(card); r % 3 {
						case 0:
							strs[i] = fmt.Sprintf("k%d", r)
						case 1:
							strs[i] = fmt.Sprintf("%07d", r)[:7]
						default:
							strs[i] = fmt.Sprintf("long-key-%d-é", r)
						}
						if rng.Intn(50) == 0 {
							strs[i] = "" // empty string is a real key, not null
						}
						i64s[i] = int64(rng.Intn(card)) * 1_000_003 // wide range: hash path
						i32s[i] = int32(rng.Intn(card)) - int32(card/2)
						bools[i] = rng.Intn(2) == 0
						sv[i] = nullEvery == 0 || rng.Intn(nullEvery+1) != 0
						iv[i] = nullEvery == 0 || rng.Intn(nullEvery+1) != 0
						bv[i] = nullEvery == 0 || rng.Intn(nullEvery+1) != 0
					}
					if nullEvery == 1 && n > 0 {
						// All-null string column.
						for i := range sv {
							sv[i] = false
						}
					}
					ss, _ := series.FromString("s", strs, sv)
					defer ss.Release()
					is, _ := series.FromInt64("i", i64s, iv)
					defer is.Release()
					i3, _ := series.FromInt32("j", i32s, nil)
					defer i3.Release()
					bs, _ := series.FromBool("b", bools, bv)
					defer bs.Release()

					combos := [][]multiKeyCol{
						{{name: "s", str: ss.Chunk(0).(*array.String)}, {name: "i", i64: is.Chunk(0).(*array.Int64)}},
						{{name: "i", i64: is.Chunk(0).(*array.Int64)}, {name: "j", i32: i3.Chunk(0).(*array.Int32)}, {name: "b", bl: bs.Chunk(0).(*array.Boolean)}},
						{{name: "b", bl: bs.Chunk(0).(*array.Boolean)}, {name: "s", str: ss.Chunk(0).(*array.String)}},
					}
					for ci, cols := range combos {
						wantIDs, wantU := assignGroupsMultiKeyRange(cols, 0, n)
						gotIDs, gotU, ok := assignGroupsViaCodes(cols, n)
						if !ok {
							t.Fatalf("combo %d: codes path declined", ci)
						}
						if !slices.Equal(gotIDs, wantIDs) {
							t.Fatalf("combo %d: group ids differ", ci)
						}
						for k := range cols {
							equalUniques(t, gotU[k], wantU[k])
						}
					}
				})
			}
		}
	}
}

// TestEncodeStringCodesHighCardinality drives the hash-partitioned
// parallel merge (many distinct keys per partition) and checks codes
// and first rows against a map oracle, with nulls, empty strings and a
// mix of short and long keys.
func TestEncodeStringCodesHighCardinality(t *testing.T) {
	t.Parallel()
	for _, card := range []int{20_000, 150_000} {
		const n = 400_000
		rng := rand.New(rand.NewSource(int64(card)))
		vals := make([]string, n)
		valid := make([]bool, n)
		for i := range vals {
			r := rng.Intn(card)
			if r%2 == 0 {
				vals[i] = fmt.Sprintf("%x", r)
			} else {
				vals[i] = fmt.Sprintf("long-key-number-%d", r)
			}
			if r == 7 {
				vals[i] = ""
			}
			valid[i] = rng.Intn(97) != 0
		}
		s, _ := series.FromString("s", vals, valid)
		kc := encodeStringCodes(s.Chunk(0).(*array.String))
		type key struct {
			v     string
			valid bool
		}
		seen := map[key]int32{}
		var firstRows []int32
		for i := range n {
			k := key{vals[i], valid[i]}
			if !k.valid {
				k.v = ""
			}
			code, ok := seen[k]
			if !ok {
				code = int32(len(seen))
				seen[k] = code
				firstRows = append(firstRows, int32(i))
			}
			if kc.codes[i] != code {
				t.Fatalf("card=%d row %d: code %d, want %d", card, i, kc.codes[i], code)
			}
		}
		if !slices.Equal(kc.firstRows, firstRows) {
			t.Fatalf("card=%d: firstRows differ", card)
		}
		s.Release()
	}
}

// TestMergeStrPartsParallelMatchesSerial checks the hash-partitioned
// merge of chunk dictionaries against the serial merge directly (the
// sampling dispatcher rarely routes high-cardinality input here).
func TestMergeStrPartsParallelMatchesSerial(t *testing.T) {
	t.Parallel()
	const n = 200_000
	rng := rand.New(rand.NewSource(11))
	vals := make([]string, n)
	valid := make([]bool, n)
	for i := range vals {
		r := rng.Intn(30_000)
		vals[i] = fmt.Sprintf("v%d", r)
		if r%3 == 0 {
			vals[i] += "-with-a-longer-tail"
		}
		valid[i] = rng.Intn(50) != 0
	}
	s, _ := series.FromString("s", vals, valid)
	defer s.Release()
	arr := s.Chunk(0).(*array.String)
	const k = 6
	chunk := (n + k - 1) / k
	parts := make([]*strEncoder, k)
	codes := make([]int32, n)
	for p := range k {
		e := newStrEncoder(arr, 64)
		e.trackHashes = true
		e.encodeRange(p*chunk, min((p+1)*chunk, n), codes[p*chunk:])
		parts[p] = e
	}
	wantRemaps, wantFirst := mergeStrPartsSerial(arr, parts)
	gotRemaps, gotFirst := mergeStrPartsParallel(arr, parts, n)
	if !slices.Equal(gotFirst, wantFirst) {
		t.Fatal("first rows differ")
	}
	for p := range k {
		want := wantRemaps[p]
		if want == nil { // identity
			want = make([]int32, len(parts[p].firstRows))
			for i := range want {
				want[i] = int32(i)
			}
		}
		if !slices.Equal(gotRemaps[p], want) {
			t.Fatalf("part %d remap differs", p)
		}
	}
}

// TestPackShortLoads checks the three load strategies (forward word,
// backward word, byte loop) agree and that distinct short strings pack
// to distinct words.
func TestPackShortLoads(t *testing.T) {
	t.Parallel()
	words := []string{"", "a", "b", "ab", "ba", "a\x00", "\x00", "\x00\x00", "abcdefg", "abcdef", "zzzzzzz"}
	seen := map[uint64]string{}
	for _, w := range words {
		// Forward: plenty of trailing bytes.
		fwd := []byte(w + "XXXXXXXX")
		pf := packShort(fwd, 0, int32(len(w)))
		// Backward: string at the end of a buffer with 8+ leading bytes.
		bwd := []byte("YYYYYYYY" + w)
		pb := packShort(bwd, 8, int32(8+len(w)))
		// Tiny buffer: neither word load fits.
		tiny := []byte(w)
		pt := packShort(tiny, 0, int32(len(w)))
		if pf != pb || pf != pt {
			t.Fatalf("%q: forward %x backward %x tiny %x", w, pf, pb, pt)
		}
		if prev, ok := seen[pf]; ok {
			t.Fatalf("%q and %q pack to the same word", prev, w)
		}
		seen[pf] = w
	}
}

// TestEncodeStringCodesSliced covers an array with a non-zero offset,
// which the parallel partitions index into directly.
func TestEncodeStringCodesSliced(t *testing.T) {
	t.Parallel()
	const n = 3 * strPartThreshold
	vals := make([]string, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("v%d", (i*7919)%1013)
	}
	s, _ := series.FromString("s", vals, nil)
	defer s.Release()
	full := s.Chunk(0).(*array.String)
	sliced := array.NewSlice(full, 5, int64(n-11)).(*array.String)
	defer sliced.Release()

	kc := encodeStringCodes(sliced)
	seen := map[string]int32{}
	for i := range sliced.Len() {
		v := sliced.Value(i)
		code, ok := seen[v]
		if !ok {
			code = int32(len(seen))
			seen[v] = code
			if kc.firstRows[code] != int32(i) {
				t.Fatalf("firstRows[%d] = %d, want %d", code, kc.firstRows[code], i)
			}
		}
		if kc.codes[i] != code {
			t.Fatalf("row %d code = %d, want %d", i, kc.codes[i], code)
		}
	}
	if kc.card() != len(seen) {
		t.Fatalf("card = %d, want %d", kc.card(), len(seen))
	}
}
