package dataframe

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// TestAssignGroupsMultiKeyParallel verifies the parallel assignment
// against an oracle map on input large enough to fan out, with nulls
// on every key column to exercise validity materialization in merge.
func TestAssignGroupsMultiKeyParallel(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	const n = 130000
	regions := make([]string, n)
	regValid := make([]bool, n)
	years := make([]int64, n)
	yearValid := make([]bool, n)
	words := []string{"n", "s", "e", "w", "ne", "nw", "se", "sw", ""}
	for i := range regions {
		regions[i] = words[rng.Intn(len(words))]
		regValid[i] = rng.Intn(8) != 0
		years[i] = 2020 + int64(rng.Intn(6))
		yearValid[i] = rng.Intn(8) != 0
	}
	rs, _ := series.FromString("region", regions, regValid)
	defer rs.Release()
	ys, _ := series.FromInt64("year", years, yearValid)
	defer ys.Release()

	cols := []multiKeyCol{
		{name: "region", str: rs.Chunk(0).(*array.String)},
		{name: "year", i64: ys.Chunk(0).(*array.Int64)},
	}
	ids, uniques := assignGroupsMultiKey(cols, n)

	type key struct {
		region  string
		hasReg  bool
		year    int64
		hasYear bool
	}
	oracle := map[key]int{}
	for i := range n {
		k := key{regions[i], regValid[i], years[i], yearValid[i]}
		if !k.hasReg {
			k.region = ""
		}
		if !k.hasYear {
			k.year = 0
		}
		if _, ok := oracle[k]; !ok {
			oracle[k] = len(oracle)
		}
	}
	if len(uniques) != 2 {
		t.Fatalf("uniques cols = %d, want 2", len(uniques))
	}
	ng := groupLen(uniques[0])
	if ng != len(oracle) {
		t.Fatalf("groups = %d, want %d", ng, len(oracle))
	}
	// Every row's id must point at uniques matching its own tuple.
	ru, yu := uniques[0], uniques[1]
	for i := range n {
		gid := ids[i]
		if gid < 0 || gid >= ng {
			t.Fatalf("row %d id %d out of range", i, gid)
		}
		if got := ru.valid == nil || ru.valid[gid]; got != regValid[i] {
			t.Fatalf("row %d region validity mismatch", i)
		}
		if regValid[i] && ru.str[gid] != regions[i] {
			t.Fatalf("row %d region value mismatch", i)
		}
		if got := yu.valid == nil || yu.valid[gid]; got != yearValid[i] {
			t.Fatalf("row %d year validity mismatch", i)
		}
		if yearValid[i] && yu.i64[gid] != years[i] {
			t.Fatalf("row %d year value mismatch", i)
		}
	}
	// Same-tuple rows share an id; different tuples differ.
	seen := map[key]int{}
	for i := range n {
		k := key{regions[i], regValid[i], years[i], yearValid[i]}
		if !k.hasReg {
			k.region = ""
		}
		if !k.hasYear {
			k.year = 0
		}
		if prev, ok := seen[k]; ok {
			if prev != ids[i] {
				t.Fatalf("same tuple split across groups")
			}
		} else {
			seen[k] = ids[i]
		}
	}
	if len(seen) != ng {
		t.Fatalf("distinct ids = %d, want %d", len(seen), ng)
	}
}
