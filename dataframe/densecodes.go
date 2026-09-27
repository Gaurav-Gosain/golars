package dataframe

import (
	"runtime"
	"sync"
)

// denseParThreshold is the row count above which dense key encoding
// fans out across workers.
const denseParThreshold = 64 * 1024

// denseParMaxRange caps the key range for the parallel dense encoder.
// Every worker keeps a private table of this many int32 slots, so wide
// ranges stay serial to bound memory and cache pressure.
const denseParMaxRange = 1 << 18

// runChunks splits [0, n) into k contiguous chunks and runs fn on each
// concurrently, returning when all are done.
func runChunks(n, k int, fn func(p, start, end int)) {
	chunk := (n + k - 1) / k
	var wg sync.WaitGroup
	for p := range k {
		start := p * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(p, start, end)
		}()
	}
	wg.Wait()
}

func denseWorkers(n int) int {
	return min(runtime.GOMAXPROCS(0), 8, max(n/(denseParThreshold/4), 1))
}

// denseEncode rewrites keys (each in [0, r)) into dense codes numbered by
// first appearance and returns the first row of every code. Large inputs
// run in two parallel passes: workers record the keys first seen in
// their chunk, a serial merge numbers them in chunk order (preserving
// global first-seen order), then workers rewrite their chunk.
func denseEncode[T int32 | int](keys []T, r int) []int32 {
	n := len(keys)
	k := denseWorkers(n)
	if n < denseParThreshold || r > denseParMaxRange || k < 2 {
		return denseEncodeSerial(keys, r)
	}
	type local struct {
		order []T     // keys in first-seen order within the chunk
		rows  []int32 // matching first rows
	}
	locals := make([]local, k)
	runChunks(n, k, func(p, start, end int) {
		seen := int32Scratch.get(r)
		clear(seen)
		var l local
		for i := start; i < end; i++ {
			key := keys[i]
			if seen[key] == 0 {
				seen[key] = 1
				l.order = append(l.order, key)
				l.rows = append(l.rows, int32(i))
			}
		}
		int32Scratch.put(seen)
		locals[p] = l
	})
	table := int32Scratch.get(r)
	clear(table)
	var firstRows []int32
	for _, l := range locals {
		for j, key := range l.order {
			if table[key] == 0 {
				firstRows = append(firstRows, l.rows[j])
				table[key] = int32(len(firstRows))
			}
		}
	}
	runChunks(n, k, func(_, start, end int) {
		seg := keys[start:end]
		for i, key := range seg {
			seg[i] = T(table[key] - 1)
		}
	})
	int32Scratch.put(table)
	return firstRows
}

func denseEncodeSerial[T int32 | int](keys []T, r int) []int32 {
	table := int32Scratch.get(r)
	clear(table)
	var firstRows []int32
	for i, key := range keys {
		slot := &table[key]
		if *slot == 0 {
			firstRows = append(firstRows, int32(i))
			*slot = int32(len(firstRows))
		}
		keys[i] = T(*slot - 1)
	}
	int32Scratch.put(table)
	return firstRows
}

// validInt64Range returns the min and max of the valid values; ok=false when
// there are none. Large inputs scan chunks in parallel.
func validInt64Range(vals []int64, isNull func(int) bool, hasNulls bool) (lo, hi int64, ok bool) {
	scan := func(start, end int) (int64, int64, bool) {
		var lo, hi int64
		found := false
		if !hasNulls {
			if start >= end {
				return 0, 0, false
			}
			lo, hi = vals[start], vals[start]
			for _, v := range vals[start+1 : end] {
				lo = min(lo, v)
				hi = max(hi, v)
			}
			return lo, hi, true
		}
		for i := start; i < end; i++ {
			if isNull(i) {
				continue
			}
			v := vals[i]
			if !found {
				lo, hi, found = v, v, true
				continue
			}
			lo = min(lo, v)
			hi = max(hi, v)
		}
		return lo, hi, found
	}
	n := len(vals)
	k := denseWorkers(n)
	if n < denseParThreshold || k < 2 {
		return scan(0, n)
	}
	type res struct {
		lo, hi int64
		ok     bool
	}
	parts := make([]res, k)
	runChunks(n, k, func(p, start, end int) {
		l, h, o := scan(start, end)
		parts[p] = res{l, h, o}
	})
	for _, p := range parts {
		if !p.ok {
			continue
		}
		if !ok {
			lo, hi, ok = p.lo, p.hi, true
			continue
		}
		lo = min(lo, p.lo)
		hi = max(hi, p.hi)
	}
	return lo, hi, ok
}
