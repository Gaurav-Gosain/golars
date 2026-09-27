package syntax

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// PartKind is the role of an argument in its statement.
type PartKind uint8

// Part kinds.
const (
	PartText      PartKind = iota // unclassified text (arguments of an unknown command)
	PartCommand                   // the command word
	PartKeyword                   // a structural keyword: as, on, by, every, asc, ...
	PartOption                    // one of a fixed set of words: inner, backward, sum, ...
	PartPath                      // a file path
	PartFrame                     // a staged frame name
	PartTarget                    // a staged frame name or a file path
	PartNewFrame                  // a frame name being defined
	PartColumn                    // a column reference
	PartNewColumn                 // a column name being defined
	PartList                      // a comma-separated list; Items are its columns
	PartAgg                       // a col:op[:alias] shorthand; Items are its pieces
	PartCount                     // a non-negative integer
	PartNumber                    // an integer or float
	PartValue                     // a scalar value
	PartDType                     // a dtype name
	PartDuration                  // a duration such as 1h or 30m
	PartWord                      // a free word (transpose prefix, ...)
	PartExpr                      // an expression; Expr is its tree
	PartPunct                     // = , or :
)

// Part is one classified piece of a statement. Offsets are bytes
// into Stmt.Text.
type Part struct {
	Kind     PartKind
	Off, End int
	// Text is the source text; Value is the text with quotes removed
	// (and lower-cased for keywords and options).
	Text, Value string
	// Items are the elements of a PartList or PartAgg.
	Items []Part
	// Expr is the tree of a PartExpr, with offsets relative to Off.
	Expr *exprparse.Node
	// Named is true for a PartExpr that follows `name =`.
	Named bool
}

// tok is a whitespace-separated argument.
type tok struct {
	off, end int
	text     string
}

// splitArgs splits text (starting at offset base of the statement)
// into whitespace-separated arguments. Quotes, parentheses and
// brackets group, and arguments joined by a comma merge into one, so
// `a, b c` gives the two arguments `a, b` and `c`.
func splitArgs(text string, base int) []tok {
	var raw []tok
	depth := 0
	quote := byte(0)
	start := -1
	for i := 0; i <= len(text); i++ {
		if i == len(text) {
			if start >= 0 {
				raw = append(raw, tok{base + start, base + i, text[start:i]})
			}
			break
		}
		c := text[i]
		if start < 0 {
			if isSpace(c) {
				continue
			}
			start = i
		}
		switch {
		case quote != 0:
			if c == '\\' && i+1 < len(text) {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			if depth > 0 {
				depth--
			}
		case depth == 0 && isSpace(c):
			raw = append(raw, tok{base + start, base + i, text[start:i]})
			start = -1
		}
	}
	var out []tok
	for _, t := range raw {
		if n := len(out); n > 0 && (strings.HasSuffix(out[n-1].text, ",") || strings.HasPrefix(t.text, ",")) {
			prev := &out[n-1]
			prev.text = text[prev.off-base : t.end-base]
			prev.end = t.end
			continue
		}
		out = append(out, t)
	}
	return out
}

// SplitArgs splits the arguments of a statement the way the parser
// does: on whitespace outside quotes and brackets, with comma lists
// kept together (`a, b` is one argument). Surrounding quotes are
// removed. Hosts use it so every tool agrees on argument boundaries.
func SplitArgs(rest string) []string {
	toks := splitArgs(rest, 0)
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = unquote(t.text)
	}
	return out
}

// unquote strips one pair of matching quotes and resolves escapes.
func unquote(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		var b strings.Builder
		for i := 1; i < len(s)-1; i++ {
			if s[i] == '\\' && i+1 < len(s)-1 {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				case 't':
					b.WriteByte('\t')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		return b.String()
	}
	return s
}

// listItems splits a comma list token into column parts.
func listItems(t tok) []Part {
	var out []Part
	start := 0
	quote := byte(0)
	for i := 0; i <= len(t.text); i++ {
		if i < len(t.text) {
			c := t.text[i]
			if quote != 0 {
				if c == '\\' && i+1 < len(t.text) {
					i++
				} else if c == quote {
					quote = 0
				}
				continue
			}
			if c == '"' || c == '\'' {
				quote = c
				continue
			}
			if c != ',' {
				continue
			}
		}
		item := t.text[start:i]
		lead := len(item) - len(strings.TrimLeft(item, " \t"))
		item = strings.TrimSpace(item)
		if item != "" {
			off := t.off + start + lead
			out = append(out, Part{Kind: PartColumn, Off: off, End: off + len(item), Text: item, Value: unquote(item)})
		}
		start = i + 1
	}
	return out
}

// --- grammar patterns -------------------------------------------

type gKind uint8

const (
	gSlot gKind = iota
	gKeyword
	gEnum
	gGroup
)

type gItem struct {
	kind  gKind
	slot  string   // gSlot
	words []string // gKeyword (one word) and gEnum
	items []gItem  // gGroup
}

var (
	grammarMu    sync.Mutex
	grammarCache = map[string][]gItem{}
)

// compileGrammar parses a CommandSpec.Grammar pattern.
func compileGrammar(pattern string) []gItem {
	grammarMu.Lock()
	defer grammarMu.Unlock()
	if g, found := grammarCache[pattern]; found {
		return g
	}
	items, _ := parsePattern(strings.Fields(strings.NewReplacer("[", " [ ", "]", " ] ").Replace(pattern)), 0)
	grammarCache[pattern] = items
	return items
}

func parsePattern(words []string, i int) ([]gItem, int) {
	var out []gItem
	for i < len(words) {
		w := words[i]
		switch {
		case w == "[":
			sub, next := parsePattern(words, i+1)
			out = append(out, gItem{kind: gGroup, items: sub})
			i = next
			continue
		case w == "]":
			return out, i + 1
		case strings.HasPrefix(w, "'"):
			out = append(out, gItem{kind: gKeyword, words: []string{strings.Trim(w, "'")}})
		case strings.Contains(w, "|"):
			out = append(out, gItem{kind: gEnum, words: strings.Split(w, "|")})
		default:
			out = append(out, gItem{kind: gSlot, slot: w})
		}
		i++
	}
	return out, i
}

// firstWords lists the literal words that can start items, used to
// stop a greedy `cols` slot before a keyword that follows it.
func firstWords(items []gItem) []string {
	var out []string
	for _, it := range items {
		switch it.kind {
		case gKeyword, gEnum:
			out = append(out, it.words...)
		case gGroup:
			out = append(out, firstWords(it.items)...)
		}
	}
	return out
}

// --- statement parsing ------------------------------------------

func parseStmt(s *Stmt) {
	text := s.Text
	i := 0
	for i < len(text) && isSpace(text[i]) {
		i++
	}
	if i < len(text) && text[i] == '.' {
		s.Dot = true
		i++
	}
	j := i
	for j < len(text) && !isSpace(text[j]) {
		j++
	}
	word := text[i:j]
	// Value is the spelling FindCommand resolves (`..h` is `.h`).
	s.Cmd = Part{Kind: PartCommand, Off: i, End: j, Text: word, Value: strings.ToLower(strings.TrimPrefix(word, "."))}
	s.Spec = script.FindCommand(word)
	rest, restOff := s.Rest()
	if s.Spec == nil {
		d := s.diagAt(i, j, SevError, "unknown-command", "unknown command %q", word)
		if best := script.SuggestCommand(word); best != "" {
			d.Hint = "did you mean \"" + best + "\"?"
			d.Fix = &Fix{Title: "Change to " + best, Text: best}
		}
		s.Diags = append(s.Diags, d)
		for _, t := range splitArgs(rest, restOff) {
			s.Parts = append(s.Parts, Part{Kind: PartText, Off: t.off, End: t.end, Text: t.text, Value: unquote(t.text)})
		}
		return
	}
	p := &stmtParser{s: s, rest: rest, restOff: restOff}
	switch s.Spec.Grammar {
	case "@select", "@filter", "@with", "@groupby", "@dynamic":
		// Expressions report their own bracket errors.
	default:
		if at, what := unbalanced(rest); at >= 0 {
			d := p.errAt(restOff+at, restOff+len(rest), "syntax", "unclosed %s", what)
			d.Hint = "close it, or quote the name"
			for _, t := range splitArgs(rest, restOff) {
				s.Parts = append(s.Parts, Part{Kind: PartText, Off: t.off, End: t.end, Text: t.text, Value: unquote(t.text)})
			}
			return
		}
	}
	switch s.Spec.Grammar {
	case "@select":
		p.parseSelect()
	case "@filter":
		p.parseFilter()
	case "@with":
		p.parseWith()
	case "@sort":
		p.parseSort()
	case "@groupby":
		p.parseGroupBy()
	case "@dynamic":
		p.parseDynamic()
	case "@asof":
		p.parseAsof()
	default:
		p.toks = splitArgs(rest, restOff)
		p.match(compileGrammar(s.Spec.Grammar), true, nil)
		p.extra()
	}
}

type stmtParser struct {
	s       *Stmt
	rest    string
	restOff int
	toks    []tok
	i       int
}

func (p *stmtParser) add(part Part) { p.s.Parts = append(p.s.Parts, part) }

func (p *stmtParser) errAt(off, end int, code, format string, args ...any) *Diag {
	p.s.Diags = append(p.s.Diags, p.s.diagAt(off, end, SevError, code, format, args...))
	return &p.s.Diags[len(p.s.Diags)-1]
}

// missing reports a required argument that is absent, pointing just
// past the end of the statement.
func (p *stmtParser) missing(what string) {
	end := len(strings.TrimRight(p.s.Text, " \t"))
	d := p.errAt(end, end, "missing-argument", "%s needs %s", p.s.Spec.Name, what)
	d.Hint = "usage: " + p.s.Spec.Signature
}

// extra reports arguments left over after the grammar matched.
func (p *stmtParser) extra() {
	if p.i >= len(p.toks) {
		return
	}
	first, last := p.toks[p.i], p.toks[len(p.toks)-1]
	for _, t := range p.toks[p.i:] {
		p.add(Part{Kind: PartText, Off: t.off, End: t.end, Text: t.text, Value: unquote(t.text)})
	}
	d := p.errAt(first.off, last.end, "extra-argument", "unexpected argument %q", first.text)
	d.Hint = "usage: " + p.s.Spec.Signature
}

// addList adds a comma list token, reporting one without columns.
func (p *stmtParser) addList(t tok) {
	items := listItems(t)
	if len(items) == 0 {
		p.errAt(t.off, t.end, "missing-argument", "expected a column, got %q", t.text)
	}
	p.add(Part{Kind: PartList, Off: t.off, End: t.end, Text: t.text, Value: t.text, Items: items})
}

func slotName(slot string) string {
	switch slot {
	case "path":
		return "a file path"
	case "frame":
		return "a frame name"
	case "target":
		return "a frame name or file path"
	case "newframe":
		return "a name for the frame"
	case "col":
		return "a column"
	case "newcol":
		return "a name for the new column"
	case "cols", "collist":
		return "one or more columns"
	case "count":
		return "a row count"
	case "int":
		return "an integer"
	case "number":
		return "a number"
	case "value":
		return "a value"
	case "dtype":
		return "a dtype"
	case "dur":
		return "a duration such as 1h"
	}
	return "an argument"
}

// match applies grammar items to the remaining tokens. required says
// whether a missing leading item is an error (false inside an
// optional group, which match only enters when its first item fits).
func (p *stmtParser) match(items []gItem, required bool, follow []string) bool {
	for k, it := range items {
		stop := append(firstWords(items[k+1:]), follow...)
		switch it.kind {
		case gGroup:
			if p.groupApplies(it.items, stop) {
				p.match(it.items, true, stop)
			}
		case gKeyword, gEnum:
			if p.i >= len(p.toks) {
				if required {
					if it.kind == gKeyword {
						p.missing("`" + it.words[0] + "`")
					} else {
						p.missing("one of " + strings.Join(it.words, ", "))
					}
				}
				return false
			}
			t := p.toks[p.i]
			if !p.wordMatches(it, t) {
				if required || it.kind == gEnum {
					p.badWord(it, t)
					p.i++
				}
				return false
			}
			kind := PartKeyword
			if it.kind == gEnum {
				kind = PartOption
			}
			p.add(Part{Kind: kind, Off: t.off, End: t.end, Text: t.text, Value: strings.ToLower(t.text)})
			p.i++
		case gSlot:
			if p.i >= len(p.toks) {
				if required {
					p.missing(slotName(it.slot))
				}
				return false
			}
			p.slot(it.slot, stop)
		}
		required = true
	}
	return true
}

func (p *stmtParser) wordMatches(it gItem, t tok) bool {
	for _, w := range it.words {
		if strings.EqualFold(t.text, w) {
			return true
		}
	}
	return false
}

// groupApplies decides whether an optional group consumes the next
// token: a group that starts with a word applies when the token is
// that word (or a near miss of it), one that starts with a slot
// applies when a token that is not a later keyword remains.
func (p *stmtParser) groupApplies(items []gItem, stop []string) bool {
	if p.i >= len(p.toks) || len(items) == 0 {
		return false
	}
	t := p.toks[p.i]
	switch first := items[0]; first.kind {
	case gKeyword, gEnum:
		if p.wordMatches(first, t) {
			return true
		}
		// A last word that fits no option is reported against the
		// option list (`join x on k lefft`: did you mean left), and a
		// near miss of a keyword against that keyword.
		if p.i != len(p.toks)-1 {
			return false
		}
		return first.kind == gEnum || script.Closest(t.text, first.words) != ""
	case gGroup:
		return p.groupApplies(first.items, stop)
	}
	for _, w := range stop {
		if strings.EqualFold(t.text, w) {
			return false
		}
	}
	return true
}

func (p *stmtParser) badWord(it gItem, t tok) {
	var d *Diag
	if it.kind == gKeyword {
		d = p.errAt(t.off, t.end, "unexpected-word", "expected `%s`, got %q", it.words[0], t.text)
	} else {
		d = p.errAt(t.off, t.end, "unknown-option", "unknown %s option %q: want %s",
			p.s.Spec.Name, t.text, strings.Join(it.words, ", "))
	}
	if best := script.Closest(t.text, it.words); best != "" {
		d.Hint = "did you mean \"" + best + "\"?"
		d.Fix = &Fix{Title: "Change to " + best, Text: best}
	} else if it.kind == gKeyword {
		d.Hint = "usage: " + p.s.Spec.Signature
	}
}

// slot consumes the token(s) for one grammar slot.
func (p *stmtParser) slot(slot string, stop []string) {
	t := p.toks[p.i]
	part := Part{Off: t.off, End: t.end, Text: t.text, Value: unquote(t.text)}
	switch slot {
	case "path":
		part.Kind = PartPath
	case "frame":
		part.Kind = PartFrame
	case "target":
		part.Kind = PartTarget
	case "newframe":
		part.Kind = PartNewFrame
	case "col":
		part.Kind = PartColumn
	case "newcol":
		part.Kind = PartNewColumn
	case "collist":
		part.Kind = PartList
		part.Items = listItems(t)
		if len(part.Items) == 0 {
			p.errAt(t.off, t.end, "missing-argument", "expected a column, got %q", t.text)
		}
	case "cols":
		// Greedy: every remaining token up to a keyword of a later
		// item, each token a column or a comma list.
		for p.i < len(p.toks) {
			t := p.toks[p.i]
			if slicesContainsFold(stop, t.text) {
				break
			}
			p.addList(t)
			p.i++
		}
		return
	case "count", "int":
		part.Kind = PartCount
		if slot == "int" {
			part.Kind = PartNumber
		}
		if _, err := strconv.ParseInt(t.text, 10, 64); err != nil || (slot == "count" && strings.HasPrefix(t.text, "-")) {
			want := "an integer"
			if slot == "count" {
				want = "a non-negative integer"
			}
			p.errAt(t.off, t.end, "bad-number", "expected %s, got %q", want, t.text)
		}
	case "number":
		part.Kind = PartNumber
		if _, err := strconv.ParseFloat(t.text, 64); err != nil {
			p.errAt(t.off, t.end, "bad-number", "expected a number, got %q", t.text)
		}
	case "value":
		// A value is the rest of the statement, as in fill_null "n/a".
		last := p.toks[len(p.toks)-1]
		text := p.s.Text[t.off:last.end]
		p.add(Part{Kind: PartValue, Off: t.off, End: last.end, Text: text, Value: unquote(text)})
		p.i = len(p.toks)
		return
	case "dtype":
		part.Kind = PartDType
		if _, err := exprparse.ParseDType(t.text); err != nil {
			d := p.errAt(t.off, t.end, "unknown-dtype", "unknown dtype %q", t.text)
			if best := script.Closest(t.text, exprparse.DTypeNames()); best != "" {
				d.Hint = "did you mean \"" + best + "\"?"
				d.Fix = &Fix{Title: "Change to " + best, Text: best}
			} else {
				d.Hint = "want one of " + strings.Join(exprparse.DTypeNames(), ", ")
			}
		}
	case "dur":
		part.Kind = PartDuration
		if !durationRE.MatchString(t.text) {
			d := p.errAt(t.off, t.end, "bad-duration", "invalid duration %q", t.text)
			d.Hint = "write a number and a unit, as in 30s, 15m, 1h, 1d, 1w, 1mo or 1y"
		}
	default:
		part.Kind = PartWord
	}
	p.add(part)
	p.i++
}

// durationRE matches polars duration strings such as 1h, 1d12h, 3i
// or -2mo.
var durationRE = regexp.MustCompile(`^-?(\d+(ns|us|µs|ms|s|m|h|d|w|mo|q|y|i))+$`)

func slicesContainsFold(list []string, s string) bool {
	for _, w := range list {
		if strings.EqualFold(w, s) {
			return true
		}
	}
	return false
}

// --- expression statements ---------------------------------------

// addExpr parses text (at statement offset off) as an expression part.
func (p *stmtParser) addExpr(text string, off int, named bool) {
	lead := len(text) - len(strings.TrimLeft(text, " \t"))
	text = strings.TrimSpace(text)
	off += lead
	if text == "" {
		d := p.errAt(off, off, "missing-expression", "missing expression")
		d.Hint = "usage: " + p.s.Spec.Signature
		return
	}
	n, err := exprparse.ParseTree(text)
	p.add(Part{Kind: PartExpr, Off: off, End: off + len(text), Text: text, Value: text, Expr: n, Named: named})
	if err != nil {
		p.exprError(err, off)
	}
}

// exprError turns an exprparse error at base offset off into a
// statement diagnostic.
func (p *stmtParser) exprError(err error, off int) {
	p.s.Diags = append(p.s.Diags, ExprDiag(p.s, err, off, "syntax"))
}

// ExprDiag converts an exprparse error from an expression that
// starts at offset off of s.Text into a diagnostic.
func ExprDiag(s *Stmt, err error, off int, code string) Diag {
	var pe *exprparse.Error
	if !errors.As(err, &pe) {
		return s.diagAt(off, len(s.Text), SevError, code, "%s", err.Error())
	}
	d := s.diagAt(off+pe.Pos, off+pe.End, SevError, code, "%s", pe.Msg)
	d.Hint = pe.Hint
	if pe.Fix != "" {
		d.Fix = &Fix{Title: "Change to " + pe.Fix, Text: pe.Fix}
		if pe.End == pe.Pos {
			d.Fix.Title = "Insert " + pe.Fix
		}
	}
	return d
}

func (p *stmtParser) parseFilter() {
	if p.rest == "" {
		p.missing("a predicate")
		return
	}
	p.addExpr(p.rest, p.restOff, false)
}

// assignRE matches the `name =` prefix of an assignment (but not ==).
var assignRE = regexp.MustCompile(`^\s*([A-Za-z_][\w.\-]*|"[^"]*"|'[^']*')\s*=([^=]|$)`)

// splitTop splits text at top-level commas, returning each item's
// text and offset relative to text.
func splitTop(text string) (items []string, offs []int) {
	depth := 0
	quote := byte(0)
	start := 0
	for i := 0; i <= len(text); i++ {
		if i < len(text) {
			c := text[i]
			switch {
			case quote != 0:
				if c == '\\' && i+1 < len(text) {
					i++
				} else if c == quote {
					quote = 0
				}
				continue
			case c == '"' || c == '\'':
				quote = c
				continue
			case c == '(' || c == '[':
				depth++
				continue
			case c == ')' || c == ']':
				if depth > 0 {
					depth--
				}
				continue
			case c != ',' || depth > 0:
				continue
			}
		}
		items = append(items, text[start:min(i, len(text))])
		offs = append(offs, start)
		start = i + 1
	}
	return items, offs
}

// assignment parses `name = expr` at statement offset off. It
// returns false when item is not an assignment.
func (p *stmtParser) assignment(item string, off int) bool {
	m := assignRE.FindStringSubmatchIndex(item)
	if m == nil {
		return false
	}
	name := item[m[2]:m[3]]
	p.add(Part{Kind: PartNewColumn, Off: off + m[2], End: off + m[3], Text: name, Value: unquote(name)})
	eq := strings.IndexByte(item[m[3]:], '=') + m[3]
	p.add(Part{Kind: PartPunct, Off: off + eq, End: off + eq + 1, Text: "=", Value: "="})
	p.addExpr(item[eq+1:], off+eq+1, true)
	return true
}

func (p *stmtParser) parseWith() {
	if p.rest == "" {
		p.missing("an assignment NAME = EXPR")
		return
	}
	items, offs := splitTop(p.rest)
	for k, item := range items {
		off := p.restOff + offs[k]
		if !p.assignment(item, off) {
			lead := len(item) - len(strings.TrimLeft(item, " \t"))
			t := strings.TrimSpace(item)
			d := p.errAt(off+lead, off+lead+len(t), "bad-assignment", "expected NAME = EXPR, got %q", t)
			d.Hint = "as in with total = price * qty"
			if strings.Contains(t, "==") {
				d.Hint = "use a single '=' to name the column"
			}
		}
		if k > 0 {
			p.add(Part{Kind: PartPunct, Off: off - 1, End: off, Text: ",", Value: ","})
		}
	}
}

// SelectIsPlain reports whether a select list is a plain column list
// (names such as my-col, separated by commas or spaces) rather than a
// list of expressions. It is plain when it holds no quotes,
// parentheses, assignments or operators.
func SelectIsPlain(rest string) bool {
	if strings.ContainsAny(rest, "(=\"'+*/%<>![") {
		return false
	}
	for f := range strings.FieldsSeq(strings.ReplaceAll(rest, ",", " ")) {
		// A plain name starts like an identifier; `-x` or `2` is an
		// expression.
		c := f[0]
		if !(c == '_' || c >= 0x80 || (c|0x20 >= 'a' && c|0x20 <= 'z')) {
			return false
		}
	}
	return true
}

func (p *stmtParser) parseSelect() {
	if p.rest == "" {
		p.missing("one or more columns or expressions")
		return
	}
	if SelectIsPlain(p.rest) {
		for _, t := range splitArgs(p.rest, p.restOff) {
			p.addList(t)
		}
		return
	}
	items, offs := splitTop(p.rest)
	for k, item := range items {
		off := p.restOff + offs[k]
		if k > 0 {
			p.add(Part{Kind: PartPunct, Off: off - 1, End: off, Text: ",", Value: ","})
		}
		if !p.assignment(item, off) {
			p.addExpr(item, off, false)
		}
	}
}

func (p *stmtParser) parseSort() {
	p.toks = splitArgs(p.rest, p.restOff)
	if len(p.toks) == 0 {
		p.missing("a column")
		return
	}
	seenCol := false
	for _, t := range p.toks {
		items := listItems(t)
		if len(items) == 0 {
			p.errAt(t.off, t.end, "missing-argument", "expected a column, got %q", t.text)
		}
		for _, it := range items {
			switch strings.ToLower(it.Value) {
			case "asc", "desc":
				it.Kind, it.Value = PartKeyword, strings.ToLower(it.Value)
				if !seenCol {
					p.errAt(it.Off, it.End, "unexpected-word", "direction %q must follow a column", it.Text)
				}
			default:
				seenCol = true
			}
			p.add(it)
		}
	}
}

// aggShortRE matches `col:op[:alias]`.
var aggShortRE = regexp.MustCompile(`^([^\s:=(),]+):([A-Za-z_]+)(?::([^\s:=(),]+))?$`)

// AggOps lists the operations of the col:op[:alias] shorthand.
var AggOps = []string{"sum", "mean", "avg", "min", "max", "count", "null_count", "first", "last",
	"median", "std", "var", "n_unique"}

// parseAggs parses aggregation items from text at statement offset
// off: `col:op[:alias]`, `name = expr` (spaces allowed around '='
// and inside the expression) or `(name = expr)`.
func (p *stmtParser) parseAggs(text string, off int) {
	i := 0
	for {
		for i < len(text) && isSpace(text[i]) {
			i++
		}
		if i >= len(text) {
			return
		}
		rest := text[i:]
		if rest[0] == '(' {
			end := matchingParen(rest)
			if end < 0 {
				d := p.errAt(off+i, off+len(text), "bad-aggregation", "unclosed '(' in aggregation")
				d.Hint = "as in (n = len())"
				p.add(Part{Kind: PartText, Off: off + i, End: off + len(text), Text: rest, Value: rest})
				return
			}
			inner := rest[1:end]
			if !p.assignment(inner, off+i+1) {
				d := p.errAt(off+i, off+i+end+1, "bad-aggregation", "expected (name = expr), got %q", rest[:min(end+1, len(rest))])
				d.Hint = "as in (n = len())"
			}
			i += min(end+1, len(rest))
			continue
		}
		if m := assignRE.FindStringSubmatchIndex(rest); m != nil {
			name := rest[m[2]:m[3]]
			p.add(Part{Kind: PartNewColumn, Off: off + i + m[2], End: off + i + m[3], Text: name, Value: unquote(name)})
			eq := strings.IndexByte(rest[m[3]:], '=') + m[3]
			p.add(Part{Kind: PartPunct, Off: off + i + eq, End: off + i + eq + 1, Text: "=", Value: "="})
			exprText := rest[eq+1:]
			lead := len(exprText) - len(strings.TrimLeft(exprText, " \t"))
			start := off + i + eq + 1 + lead
			n, used, err := exprparse.ParsePrefix(exprText[lead:])
			if err != nil {
				p.add(Part{Kind: PartExpr, Off: start, End: off + len(text), Text: exprText[lead:], Value: exprText[lead:], Named: true})
				p.exprError(err, start)
				return
			}
			etext := strings.TrimRight(exprText[lead:lead+used], " \t")
			p.add(Part{Kind: PartExpr, Off: start, End: start + len(etext), Text: etext, Value: etext, Expr: n, Named: true})
			i += eq + 1 + lead + used
			continue
		}
		j := 0
		for j < len(rest) && !isSpace(rest[j]) {
			j++
		}
		word := rest[:j]
		p.aggShorthand(word, off+i)
		i += j
	}
}

func matchingParen(s string) int {
	depth := 0
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && i+1 < len(s) {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func (p *stmtParser) aggShorthand(word string, off int) {
	m := aggShortRE.FindStringSubmatchIndex(word)
	if m == nil {
		d := p.errAt(off, off+len(word), "bad-aggregation", "expected col:op[:alias] or name = expr, got %q", word)
		d.Hint = "as in amount:sum:total or n = len()"
		p.add(Part{Kind: PartText, Off: off, End: off + len(word), Text: word, Value: word})
		return
	}
	agg := Part{Kind: PartAgg, Off: off, End: off + len(word), Text: word, Value: word}
	col := word[m[2]:m[3]]
	agg.Items = append(agg.Items, Part{Kind: PartColumn, Off: off + m[2], End: off + m[3], Text: col, Value: col})
	op := word[m[4]:m[5]]
	agg.Items = append(agg.Items, Part{Kind: PartOption, Off: off + m[4], End: off + m[5], Text: op, Value: strings.ToLower(op)})
	if !slicesContainsFold(AggOps, op) {
		d := p.errAt(off+m[4], off+m[5], "unknown-option", "unknown aggregation %q: want %s", op, strings.Join(AggOps, ", "))
		if best := script.Closest(op, AggOps); best != "" {
			d.Hint = "did you mean \"" + best + "\"?"
			d.Fix = &Fix{Title: "Change to " + best, Text: best}
		}
	}
	if m[6] >= 0 {
		alias := word[m[6]:m[7]]
		agg.Items = append(agg.Items, Part{Kind: PartNewColumn, Off: off + m[6], End: off + m[7], Text: alias, Value: alias})
	}
	p.add(agg)
}

func (p *stmtParser) parseGroupBy() {
	toks := splitArgs(p.rest, p.restOff)
	if len(toks) == 0 {
		p.missing("group keys and aggregations")
		return
	}
	keys := toks[0]
	p.addList(keys)
	aggOff := keys.end - p.restOff
	if strings.TrimSpace(p.rest[aggOff:]) == "" {
		p.missing("at least one aggregation")
		return
	}
	p.parseAggs(p.rest[aggOff:], keys.end)
}

// DynamicOptions are the option words of group_by_dynamic.
var DynamicOptions = []string{"every", "period", "offset", "by", "closed", "label", "start_by"}

func (p *stmtParser) parseDynamic() {
	toks := splitArgs(p.rest, p.restOff)
	if len(toks) == 0 {
		p.missing("a time column")
		return
	}
	p.add(Part{Kind: PartColumn, Off: toks[0].off, End: toks[0].end, Text: toks[0].text, Value: unquote(toks[0].text)})
	i := 1
	sawEvery := false
	for i+1 < len(toks) && slicesContainsFold(DynamicOptions, toks[i].text) {
		kw, v := toks[i], toks[i+1]
		opt := strings.ToLower(kw.text)
		p.add(Part{Kind: PartKeyword, Off: kw.off, End: kw.end, Text: kw.text, Value: opt})
		val := Part{Off: v.off, End: v.end, Text: v.text, Value: unquote(v.text)}
		switch opt {
		case "every", "period", "offset":
			val.Kind = PartDuration
			sawEvery = sawEvery || opt == "every"
			if !durationRE.MatchString(v.text) {
				d := p.errAt(v.off, v.end, "bad-duration", "invalid duration %q", v.text)
				d.Hint = "write a number and a unit, as in 30m, 1h, 1d, 1w, 1mo or 3i"
			}
		case "by":
			val.Kind = PartList
			val.Items = listItems(v)
			if len(val.Items) == 0 {
				p.errAt(v.off, v.end, "missing-argument", "expected a column, got %q", v.text)
			}
		case "closed":
			val.Kind = PartOption
			p.checkWord(v, []string{"left", "right", "both", "none"}, "closed")
		case "label":
			val.Kind = PartOption
			p.checkWord(v, []string{"left", "right", "datapoint"}, "label")
		default:
			val.Kind = PartWord
		}
		p.add(val)
		i += 2
	}
	if !sawEvery {
		end := len(strings.TrimRight(p.s.Text, " \t"))
		at := end
		if i < len(toks) {
			at = toks[i].off
		}
		d := p.errAt(at, at, "missing-argument", "group_by_dynamic needs `every DURATION`")
		d.Hint = "as in group_by_dynamic ts every 1h amount:sum"
	}
	if i >= len(toks) {
		if sawEvery {
			p.missing("at least one aggregation")
		}
		return
	}
	p.parseAggs(p.rest[toks[i].off-p.restOff:], toks[i].off)
}

func (p *stmtParser) checkWord(v tok, words []string, what string) {
	if slicesContainsFold(words, v.text) {
		return
	}
	d := p.errAt(v.off, v.end, "unknown-option", "unknown %s value %q: want %s", what, v.text, strings.Join(words, ", "))
	if best := script.Closest(v.text, words); best != "" {
		d.Hint = "did you mean \"" + best + "\"?"
		d.Fix = &Fix{Title: "Change to " + best, Text: best}
	}
}

// AsofStrategies are the strategy words of join_asof.
var AsofStrategies = []string{"backward", "forward", "nearest"}

func (p *stmtParser) parseAsof() {
	p.toks = splitArgs(p.rest, p.restOff)
	if !p.match(compileGrammar("target 'on' col"), true, nil) {
		return
	}
	for p.i < len(p.toks) {
		t := p.toks[p.i]
		w := strings.ToLower(t.text)
		switch {
		case slicesContainsFold(AsofStrategies, w):
			p.add(Part{Kind: PartOption, Off: t.off, End: t.end, Text: t.text, Value: w})
			p.i++
		case w == "by" || w == "tolerance":
			p.add(Part{Kind: PartKeyword, Off: t.off, End: t.end, Text: t.text, Value: w})
			p.i++
			if p.i >= len(p.toks) {
				p.missing("a value after " + w)
				return
			}
			v := p.toks[p.i]
			part := Part{Kind: PartValue, Off: v.off, End: v.end, Text: v.text, Value: unquote(v.text)}
			if w == "by" {
				part.Kind, part.Items = PartList, listItems(v)
				if len(part.Items) == 0 {
					p.errAt(v.off, v.end, "missing-argument", "expected a column, got %q", v.text)
				}
			}
			p.add(part)
			p.i++
		default:
			words := append([]string{"by", "tolerance"}, AsofStrategies...)
			d := p.errAt(t.off, t.end, "unexpected-word", "unexpected %q: want by, tolerance, backward, forward or nearest", t.text)
			if best := script.Closest(t.text, words); best != "" {
				d.Hint = "did you mean \"" + best + "\"?"
				d.Fix = &Fix{Title: "Change to " + best, Text: best}
			}
			p.add(Part{Kind: PartText, Off: t.off, End: t.end, Text: t.text, Value: t.text})
			p.i++
		}
	}
}

// unbalanced returns the offset and a description of the first
// bracket or quote in s that is never closed, or -1.
func unbalanced(s string) (int, string) {
	var stack []int
	quote, qpos := byte(0), -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && i+1 < len(s) {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote, qpos = c, i
		case c == '(' || c == '[':
			stack = append(stack, i)
		case c == ')' || c == ']':
			if len(stack) == 0 {
				return i, "unmatched " + string(c)
			}
			stack = stack[:len(stack)-1]
		}
	}
	if quote != 0 {
		return qpos, "string"
	}
	if len(stack) > 0 {
		return stack[0], string(s[stack[0]])
	}
	return -1, ""
}
