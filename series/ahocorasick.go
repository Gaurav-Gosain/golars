package series

// A small Aho-Corasick automaton used by the multi-pattern string
// kernels (contains_any, extract_many, find_many, replace_many).
//
// Polars builds these kernels on the aho-corasick crate with the
// "standard" match semantics: scanning left to right, a match is
// reported as soon as it ends, and when several patterns end at the
// same byte the longest one wins. Non-overlapping search restarts at
// the root after each match. That is what this automaton implements.
//
// The automaton is compiled to a dense DFA over byte classes: bytes
// that appear in no pattern share class 0, so the transition table
// stays small even for large alphabets.

type acMatch struct {
	pattern int
	start   int
	end     int
}

type ahoCorasick struct {
	classes   [256]uint16
	alpha     int
	trans     []int32   // state*alpha + class -> next state
	outputs   [][]int32 // pattern ids, longest first
	patLens   []int
	emptyPat  int // id of an empty pattern, or -1
	caseInsen bool
}

func newAhoCorasick(patterns []string, asciiCaseInsensitive bool) *ahoCorasick {
	ac := &ahoCorasick{emptyPat: -1, caseInsen: asciiCaseInsensitive}
	fold := func(b byte) byte {
		if asciiCaseInsensitive && b >= 'A' && b <= 'Z' {
			return b + 32
		}
		return b
	}
	// Byte classes.
	next := uint16(1)
	for _, p := range patterns {
		for i := 0; i < len(p); i++ {
			b := fold(p[i])
			if ac.classes[b] == 0 {
				ac.classes[b] = next
				next++
			}
		}
	}
	if asciiCaseInsensitive {
		for b := 'A'; b <= 'Z'; b++ {
			ac.classes[b] = ac.classes[b+32]
		}
	}
	ac.alpha = int(next)

	// Trie.
	type node struct {
		children map[uint16]int32
		own      int32 // pattern id ending here, -1 if none
	}
	nodes := []node{{children: map[uint16]int32{}, own: -1}}
	ac.patLens = make([]int, len(patterns))
	for id, p := range patterns {
		ac.patLens[id] = len(p)
		if len(p) == 0 {
			if ac.emptyPat < 0 {
				ac.emptyPat = id
			}
			continue
		}
		cur := int32(0)
		for i := 0; i < len(p); i++ {
			c := ac.classes[fold(p[i])]
			nxt, ok := nodes[cur].children[c]
			if !ok {
				nodes = append(nodes, node{children: map[uint16]int32{}, own: -1})
				nxt = int32(len(nodes) - 1)
				nodes[cur].children[c] = nxt
			}
			cur = nxt
		}
		if nodes[cur].own < 0 {
			nodes[cur].own = int32(id)
		}
	}
	ns := len(nodes)
	ac.trans = make([]int32, ns*ac.alpha)
	ac.outputs = make([][]int32, ns)
	fail := make([]int32, ns)
	// BFS over the trie to fill failure links, outputs and the DFA.
	queue := make([]int32, 0, ns)
	if nodes[0].own >= 0 {
		ac.outputs[0] = []int32{nodes[0].own}
	}
	for c := 0; c < ac.alpha; c++ {
		if nxt, ok := nodes[0].children[uint16(c)]; ok {
			ac.trans[c] = nxt
			fail[nxt] = 0
			queue = append(queue, nxt)
		} else {
			ac.trans[c] = 0
		}
	}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		var out []int32
		if nodes[s].own >= 0 {
			out = append(out, nodes[s].own)
		}
		out = append(out, ac.outputs[fail[s]]...)
		ac.outputs[s] = out
		for c := 0; c < ac.alpha; c++ {
			if nxt, ok := nodes[s].children[uint16(c)]; ok {
				fail[nxt] = ac.trans[int(fail[s])*ac.alpha+c]
				ac.trans[int(s)*ac.alpha+c] = nxt
				queue = append(queue, nxt)
			} else {
				ac.trans[int(s)*ac.alpha+c] = ac.trans[int(fail[s])*ac.alpha+c]
			}
		}
	}
	return ac
}

// isMatch reports whether any pattern occurs in hay.
func (ac *ahoCorasick) isMatch(hay string) bool {
	if ac.emptyPat >= 0 {
		return true
	}
	s := int32(0)
	for i := 0; i < len(hay); i++ {
		s = ac.trans[int(s)*ac.alpha+int(ac.classes[hay[i]])]
		if len(ac.outputs[s]) > 0 {
			return true
		}
	}
	return false
}

// each walks the matches of hay in order and calls fn for each. With
// overlapping=false the standard non-overlapping semantics apply.
func (ac *ahoCorasick) each(hay string, overlapping bool, fn func(m acMatch)) {
	if ac.emptyPat >= 0 && !overlapping {
		// Every position yields the empty match first, which blocks
		// all longer matches under standard semantics.
		for i := 0; i <= len(hay); i++ {
			fn(acMatch{pattern: ac.emptyPat, start: i, end: i})
		}
		return
	}
	s := int32(0)
	if overlapping && ac.emptyPat >= 0 {
		fn(acMatch{pattern: ac.emptyPat, start: 0, end: 0})
	}
	for i := 0; i < len(hay); i++ {
		s = ac.trans[int(s)*ac.alpha+int(ac.classes[hay[i]])]
		out := ac.outputs[s]
		if len(out) == 0 {
			continue
		}
		end := i + 1
		if overlapping {
			for _, p := range out {
				fn(acMatch{pattern: int(p), start: end - ac.patLens[p], end: end})
			}
			continue
		}
		p := out[0]
		fn(acMatch{pattern: int(p), start: end - ac.patLens[p], end: end})
		s = 0
	}
}
