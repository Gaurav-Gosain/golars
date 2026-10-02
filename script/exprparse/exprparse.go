// Package exprparse turns a short text expression into an expr.Expr.
//
// Grammar (identifiers are case-sensitive, keywords are not):
//
//	expr     := orExpr
//	orExpr   := andExpr ("or" andExpr)*
//	andExpr  := notExpr ("and" notExpr)*
//	notExpr  := "not" notExpr | cmpExpr
//	cmpExpr  := addExpr ( cmpOp addExpr
//	                    | "is_null" | "is_not_null"
//	                    | strOp addExpr
//	                    | ["not"] "in" list )?
//	cmpOp    := "==" | "!=" | "<" | "<=" | ">" | ">="
//	strOp    := "contains" | "starts_with" | "ends_with" | "like" | "not_like"
//	addExpr  := mulExpr (("+" | "-") mulExpr)*
//	mulExpr  := unary (("*" | "/" | "//" | "%") unary)*
//	unary    := "-" unary | power
//	power    := postfix ("**" unary)?
//	postfix  := primary ("." member)*
//	member   := ns "." ident args? | ident args?
//	primary  := literal | list | "(" expr ")" | when
//	          | ns "." ident args | ident args | ident
//	when     := "when" orExpr "then" orExpr ("when" orExpr "then" orExpr)*
//	            ("otherwise" orExpr)?
//	args     := "(" (arg ("," arg)*)? ")"
//	arg      := ident "=" expr | expr
//	list     := "[" (expr ("," expr)*)? "]"
//	literal  := number | quoted-string | "true" | "false" | "null"
//	ns       := "str" | "dt" | "list" | "arr" | "struct" | "name" | "bin" | "cat"
//
// Bare identifiers are column references. Every function has one
// regular spelling in two equivalent forms: a method on a value
// (`ts.dt.year()`, `x.round(2)`) or a call whose first argument is
// the receiver (`dt.year(ts)`, `round(x, 2)`). Calls resolve against
// the public golars expression API through reflection, so a new
// method on expr.Expr (or on one of its namespaces) is reachable
// from glr without touching this package. The tables in funcs.go only
// add aliases, polars-style default arguments and free functions.
package exprparse

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/script"
)

// Parse builds an expr.Expr from s. Errors are *Error values that
// point at the offending span of s.
func Parse(s string) (expr.Expr, error) {
	v, err := compileSource(s)
	if err != nil {
		return expr.Expr{}, err
	}
	return v.e, nil
}

// GoSource parses s and returns Go source that rebuilds the same
// expression with the golars expr (and dtype) packages. The glr to Go
// transpiler uses it so generated programs call the real API.
func GoSource(s string) (string, error) {
	v, err := compileSource(s)
	if err != nil {
		return "", err
	}
	return v.src, nil
}

// ParseTree parses s into a syntax tree without resolving functions
// against the expression API. Editors use it for highlighting,
// completion and formatting; Compile turns the tree into an Expr.
func ParseTree(s string) (*Node, error) {
	n, _, err := parse(s, false)
	if err != nil {
		return nil, attachSource(err, s)
	}
	return n, nil
}

// ParsePrefix parses the longest expression at the start of s and
// returns it with the byte offset where it stopped. It lets statement
// grammars embed expressions without a delimiter, as in
// `groupby k total = amount.sum() n = len()`.
func ParsePrefix(s string) (*Node, int, error) {
	n, end, err := parse(s, true)
	if err != nil {
		return nil, end, attachSource(err, s)
	}
	return n, end, nil
}

// Compile resolves a tree from ParseTree (or ParsePrefix) into an
// expression. src is the text the tree was parsed from; it is only
// used to report error columns.
func Compile(n *Node, src string) (expr.Expr, error) {
	v, err := safeCompile(n)
	if err != nil {
		return expr.Expr{}, attachSource(err, src)
	}
	return v.e, nil
}

func compileSource(s string) (val, error) {
	n, _, err := parse(s, false)
	if err != nil {
		return val{}, attachSource(err, s)
	}
	v, err := safeCompile(n)
	if err != nil {
		return val{}, attachSource(err, s)
	}
	return v, nil
}

// safeCompile compiles n, turning a panic inside an expression
// constructor (reached through reflection with user arguments) into
// an error instead of crashing the host.
func safeCompile(n *Node) (v val, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errAt(n, "internal: %v", r)
		}
	}()
	return compile(n)
}

func parse(s string, prefix bool) (*Node, int, error) {
	toks, err := tokenize(s)
	if err != nil {
		return nil, 0, err
	}
	p := &parser{toks: toks, src: s}
	n, err := p.parseExpr()
	if err != nil {
		return nil, p.peek().start, err
	}
	t := p.peek()
	if prefix || t.kind == tokEOF {
		return n, t.start, nil
	}
	return nil, t.start, p.trailingError(t)
}

// trailingError explains a token left over after a complete
// expression.
func (p *parser) trailingError(t token) error {
	if t.kind == tokOp && t.text == "=" {
		e := errSpan(t.start, t.end, "unexpected '='")
		e.Fix = "=="
		return e.withHint("did you mean '=='?")
	}
	e := errSpan(t.start, t.end, "unexpected %s", describe(t))
	if t.kind == tokIdent {
		if kw := script.Closest(strings.ToLower(t.text), infixKeywords); kw != "" {
			e.Fix = kw
			return e.withHint("did you mean %q?", kw)
		}
		return e.withHint("expected an operator such as ==, and, or +")
	}
	if t.kind == tokRParen || t.kind == tokRBrack {
		return e.withHint("it has no matching opening bracket")
	}
	return e
}

// describe names a token for an error message.
func describe(t token) string {
	switch t.kind {
	case tokEOF:
		return "end of input"
	case tokIdent:
		return "identifier " + strconv.Quote(t.text)
	case tokNumber:
		return "number " + t.text
	case tokString:
		return "string " + t.text
	}
	return strconv.Quote(t.text)
}

// infixKeywords are the words that may follow a complete operand.
var infixKeywords = []string{"and", "or", "not", "in", "is_null", "is_not_null",
	"contains", "starts_with", "ends_with", "like", "not_like", "then", "otherwise"}

// Keywords lists the reserved words of the expression language, for
// editors.
func Keywords() []string {
	return []string{"and", "or", "not", "in", "is_null", "is_not_null", "contains",
		"starts_with", "ends_with", "like", "not_like", "when", "then", "otherwise",
		"true", "false", "null"}
}

// Namespaces lists the method namespaces in display order.
func Namespaces() []string {
	return []string{"str", "dt", "list", "arr", "struct", "name", "bin", "cat"}
}

// IsNamespace reports whether s is one of the method namespaces.
func IsNamespace(s string) bool {
	_, found := namespaces[s]
	return found
}

// --- tokens ------------------------------------------------------

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp
	tokLParen
	tokRParen
	tokLBrack
	tokRBrack
	tokComma
	tokDot
)

type token struct {
	kind       tokKind
	text       string // identifier, operator or number text; unescaped string value
	start, end int
}

func tokenize(s string) ([]token, error) {
	var toks []token
	i := 0
	add := func(k tokKind, text string, n int) {
		toks = append(toks, token{k, text, i, i + n})
		i += n
	}
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			add(tokLParen, "(", 1)
		case c == ')':
			add(tokRParen, ")", 1)
		case c == '[':
			add(tokLBrack, "[", 1)
		case c == ']':
			add(tokRBrack, "]", 1)
		case c == ',':
			add(tokComma, ",", 1)
		case c == '.' && (i+1 >= len(s) || !isDigit(s[i+1])):
			add(tokDot, ".", 1)
		case c == '"' || c == '\'':
			end, lit, ok := readString(s, i)
			if !ok {
				e := errSpan(i, len(s), "unterminated string")
				return nil, e.withHint("close it with %c", c)
			}
			toks = append(toks, token{tokString, lit, i, end})
			i = end
		case isDigit(c) || (c == '.' && i+1 < len(s) && isDigit(s[i+1])):
			end, lit := readNumber(s, i)
			toks = append(toks, token{tokNumber, lit, i, end})
			i = end
		case identStartAt(s, i) > 0:
			end, lit := readIdent(s, i)
			toks = append(toks, token{tokIdent, lit, i, end})
			i = end
		default:
			if i+1 < len(s) {
				switch two := s[i : i+2]; two {
				case "==", "!=", "<=", ">=", "//", "**":
					add(tokOp, two, 2)
					continue
				}
			}
			// Unknown characters become stray operators so the parser
			// can report them in context.
			_, size := utf8.DecodeRuneInString(s[i:])
			add(tokOp, s[i:i+size], size)
		}
	}
	toks = append(toks, token{tokEOF, "", len(s), len(s)})
	return toks, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// identStartAt returns the byte length of the identifier-start rune
// at s[i], or 0 when there is none. Letters of any script and '_'
// start an identifier.
func identStartAt(s string, i int) int {
	c := s[i]
	if c < utf8.RuneSelf {
		if c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') {
			return 1
		}
		return 0
	}
	r, size := utf8.DecodeRuneInString(s[i:])
	if r != utf8.RuneError && unicode.IsLetter(r) {
		return size
	}
	return 0
}

func readIdent(s string, i int) (int, string) {
	j := i
	for j < len(s) {
		if n := identStartAt(s, j); n > 0 {
			j += n
			continue
		}
		if isDigit(s[j]) {
			j++
			continue
		}
		if s[j] >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[j:])
			if r != utf8.RuneError && unicode.IsDigit(r) {
				j += size
				continue
			}
		}
		break
	}
	return j, s[i:j]
}

func readNumber(s string, i int) (int, string) {
	j := i
	for j < len(s) && isDigit(s[j]) {
		j++
	}
	if j < len(s) && s[j] == '.' {
		j++
		for j < len(s) && isDigit(s[j]) {
			j++
		}
	}
	// An exponent needs at least one digit; otherwise `1e` is the
	// number 1 followed by an identifier.
	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		k := j + 1
		if k < len(s) && (s[k] == '+' || s[k] == '-') {
			k++
		}
		if k < len(s) && isDigit(s[k]) {
			for k < len(s) && isDigit(s[k]) {
				k++
			}
			j = k
		}
	}
	return j, s[i:j]
}

func readString(s string, i int) (int, string, bool) {
	quote := s[i]
	j := i + 1
	var b strings.Builder
	for j < len(s) {
		c := s[j]
		if c == '\\' && j+1 < len(s) {
			switch s[j+1] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '"', '\'', '\\':
				b.WriteByte(s[j+1])
			default:
				b.WriteByte('\\')
				b.WriteByte(s[j+1])
			}
			j += 2
			continue
		}
		if c == quote {
			return j + 1, b.String(), true
		}
		b.WriteByte(c)
		j++
	}
	return j, "", false
}

// --- syntax tree -------------------------------------------------

// NodeKind tags a syntax tree node.
type NodeKind uint8

// Node kinds.
const (
	KindLit    NodeKind = iota // Lit holds int64, float64, string, bool or nil
	KindCol                    // Name is the column
	KindList                   // Args are the elements
	KindBinary                 // Name is the operator; Args are the operands
	KindNot                    // Args[0]
	KindNeg                    // Args[0]
	KindCall                   // NS.Name(Recv?, Args..., Kw...)
	KindWhen                   // Args are pred/then pairs; Final is the otherwise branch
)

// Node is one node of a parsed glr expression. Offsets are bytes into
// the parsed text.
type Node struct {
	Kind NodeKind
	// Pos and End span the node's source text, parentheses included.
	Pos, End int
	// Lit is the literal value of a KindLit node.
	Lit any
	// Raw is the source spelling of a literal (quotes included).
	Raw string
	// Name is the column, operator or function name.
	Name string
	// NamePos is the offset of Name for columns and calls (for binary
	// nodes, of the operator).
	NamePos int
	// NS is the namespace of a call (str, dt, ...), "" for none.
	NS    string
	Recv  *Node
	Args  []*Node
	Kw    []KwArg
	Final *Node
	// Parens counts the parentheses written around the node.
	Parens int
	// CallParens is false for a method written without parentheses,
	// as in `price.sum`.
	CallParens bool
	// Infix is the operator spelling of a call written infix: `in`,
	// `is_null`, `contains`, ... For a KindNot node it is "not in"
	// when the source was `x not in [...]`.
	Infix string
	// ambig marks `ns.fn(args)` where ns could also be a column name.
	// The compiler falls back to `col(ns).fn(args)` when the
	// namespace reading does not compile.
	ambig bool
}

// NameEnd returns the end offset of the node's name.
func (n *Node) NameEnd() int { return n.NamePos + len(n.Name) }

// KwArg is a keyword argument `name=value`.
type KwArg struct {
	Name    string
	NamePos int
	Val     *Node
}

// Walk calls fn for n and every node below it in source order,
// skipping the children of nodes for which fn returns false.
func (n *Node) Walk(fn func(*Node) bool) {
	if n == nil || !fn(n) {
		return
	}
	n.Recv.Walk(fn)
	for _, a := range n.Args {
		a.Walk(fn)
	}
	for _, k := range n.Kw {
		k.Val.Walk(fn)
	}
	n.Final.Walk(fn)
}

// litNode wraps a Go value (a table default) as a glr literal. A
// []any becomes a list literal.
func litNode(v any) *Node {
	switch x := v.(type) {
	case int:
		v = int64(x)
	case []any:
		list := &Node{Kind: KindList}
		for _, e := range x {
			list.Args = append(list.Args, litNode(e))
		}
		return list
	}
	return &Node{Kind: KindLit, Lit: v}
}

// namespaces maps each glr namespace to its accessor on expr.Expr.
var namespaces = map[string]string{
	"str": "Str", "dt": "Dt", "list": "List", "arr": "Arr",
	"struct": "Struct", "name": "Name", "bin": "Bin", "cat": "Cat",
}

// --- parser ------------------------------------------------------

// maxDepth bounds nesting so hostile input cannot exhaust the stack.
const maxDepth = 200

type parser struct {
	toks  []token
	pos   int
	src   string
	depth int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) peekAt(k int) token {
	if p.pos+k < len(p.toks) {
		return p.toks[p.pos+k]
	}
	return p.toks[len(p.toks)-1]
}

func (p *parser) advance() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) expect(kind tokKind, context string) (token, error) {
	t := p.peek()
	if t.kind != kind {
		e := errSpan(t.start, t.end, "expected %s %s, got %s", kindName(kind), context, describe(t))
		if kind == tokRParen || kind == tokRBrack {
			e.Fix = kindName(kind)[1:2]
			if t.kind == tokEOF {
				e.Hint = "add the missing " + kindName(kind)
			}
		}
		return t, e
	}
	p.pos++
	return t, nil
}

func kindName(k tokKind) string {
	switch k {
	case tokIdent:
		return "a name"
	case tokNumber:
		return "a number"
	case tokString:
		return "a string"
	case tokOp:
		return "an operator"
	case tokLParen:
		return "'('"
	case tokRParen:
		return "')'"
	case tokLBrack:
		return "'['"
	case tokRBrack:
		return "']'"
	case tokComma:
		return "','"
	case tokDot:
		return "'.'"
	}
	return "a token"
}

// isKeyword reports whether t is the identifier kw, ignoring case.
func isKeyword(t token, kw string) bool {
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) enter() error {
	p.depth++
	if p.depth > maxDepth {
		t := p.peek()
		return errSpan(t.start, t.end, "expression nested too deeply")
	}
	return nil
}

func (p *parser) parseExpr() (*Node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	return p.parseOr()
}

func (p *parser) binary(op token, left, right *Node) *Node {
	return &Node{Kind: KindBinary, Pos: left.Pos, End: right.End, Name: strings.ToLower(op.text),
		NamePos: op.start, Args: []*Node{left, right}}
}

func (p *parser) parseOr() (*Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.peek(), "or") {
		t := p.advance()
		right, err := p.operand(t, p.parseAnd)
		if err != nil {
			return nil, err
		}
		left = p.binary(t, left, right)
	}
	return left, nil
}

func (p *parser) parseAnd() (*Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.peek(), "and") {
		t := p.advance()
		right, err := p.operand(t, p.parseNot)
		if err != nil {
			return nil, err
		}
		left = p.binary(t, left, right)
	}
	return left, nil
}

// operand parses the right-hand side of an operator, turning a
// missing operand into an error that names the operator.
func (p *parser) operand(op token, parse func() (*Node, error)) (*Node, error) {
	if t := p.peek(); t.kind == tokEOF {
		return nil, errSpan(op.start, op.end, "missing operand after %s", strings.ToLower(op.text)).
			withHint("the expression ends here")
	}
	return parse()
}

func (p *parser) parseNot() (*Node, error) {
	if t := p.peek(); isKeyword(t, "not") {
		p.advance()
		if err := p.enter(); err != nil {
			return nil, err
		}
		inner, err := p.operand(t, p.parseNot)
		p.depth--
		if err != nil {
			return nil, err
		}
		return &Node{Kind: KindNot, Pos: t.start, End: inner.End, NamePos: t.start, Args: []*Node{inner}}, nil
	}
	return p.parseCmp()
}

// strInfix are the predicate keywords that desugar to str methods.
var strInfix = []string{"contains", "starts_with", "ends_with", "like", "not_like"}

func (p *parser) parseCmp() (*Node, error) {
	left, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	switch {
	case t.kind == tokOp:
		switch t.text {
		case "==", "!=", "<", "<=", ">", ">=":
			p.advance()
			right, err := p.operand(t, p.parseAdd)
			if err != nil {
				return nil, err
			}
			return p.binary(t, left, right), nil
		}
	case isKeyword(t, "is_null"), isKeyword(t, "is_not_null"):
		p.advance()
		name := strings.ToLower(t.text)
		return &Node{Kind: KindCall, Pos: left.Pos, End: t.end, Name: name, NamePos: t.start,
			Recv: left, Infix: name, CallParens: true}, nil
	case isKeyword(t, "in"):
		p.advance()
		return p.parseIn(left, t)
	case isKeyword(t, "not") && isKeyword(p.peekAt(1), "in"):
		p.advance()
		in, err := p.parseIn(left, p.advance())
		if err != nil {
			return nil, err
		}
		return &Node{Kind: KindNot, Pos: left.Pos, End: in.End, NamePos: t.start, Args: []*Node{in}, Infix: "not in"}, nil
	case t.kind == tokIdent:
		for _, op := range strInfix {
			if isKeyword(t, op) {
				p.advance()
				right, err := p.operand(t, p.parseAdd)
				if err != nil {
					return nil, err
				}
				return &Node{Kind: KindCall, Pos: left.Pos, End: right.End, NS: "str", Name: op, NamePos: t.start,
					Recv: left, Args: []*Node{right}, Infix: op, CallParens: true}, nil
			}
		}
	}
	return left, nil
}

func (p *parser) parseIn(left *Node, t token) (*Node, error) {
	if p.peek().kind != tokLBrack {
		nt := p.peek()
		return nil, errSpan(nt.start, nt.end, "`in` must be followed by a [list]").
			withHint(`write it as x in ["a", "b"]`)
	}
	list, err := p.parseList()
	if err != nil {
		return nil, err
	}
	return &Node{Kind: KindCall, Pos: left.Pos, End: list.End, Name: "is_in", NamePos: t.start,
		Recv: left, Args: []*Node{list}, Infix: "in", CallParens: true}, nil
}

func (p *parser) parseAdd() (*Node, error) {
	left, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != tokOp || (t.text != "+" && t.text != "-") {
			return left, nil
		}
		p.advance()
		right, err := p.operand(t, p.parseMul)
		if err != nil {
			return nil, err
		}
		left = p.binary(t, left, right)
	}
}

func (p *parser) parseMul() (*Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != tokOp {
			return left, nil
		}
		switch t.text {
		case "*", "/", "//", "%":
		default:
			return left, nil
		}
		p.advance()
		right, err := p.operand(t, p.parseUnary)
		if err != nil {
			return nil, err
		}
		left = p.binary(t, left, right)
	}
}

func (p *parser) parseUnary() (*Node, error) {
	if t := p.peek(); t.kind == tokOp && t.text == "-" {
		p.advance()
		if err := p.enter(); err != nil {
			return nil, err
		}
		inner, err := p.operand(t, p.parseUnary)
		p.depth--
		if err != nil {
			return nil, err
		}
		// Fold negative numeric literals so `shift(-1)` passes a
		// literal rather than a negation expression.
		if inner.Kind == KindLit && inner.Parens == 0 {
			switch v := inner.Lit.(type) {
			case int64:
				return &Node{Kind: KindLit, Pos: t.start, End: inner.End, Lit: -v, Raw: "-" + inner.Raw}, nil
			case float64:
				return &Node{Kind: KindLit, Pos: t.start, End: inner.End, Lit: -v, Raw: "-" + inner.Raw}, nil
			}
		}
		return &Node{Kind: KindNeg, Pos: t.start, End: inner.End, NamePos: t.start, Args: []*Node{inner}}, nil
	}
	return p.parsePower()
}

func (p *parser) parsePower() (*Node, error) {
	base, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tokOp && t.text == "**" {
		p.advance()
		if err := p.enter(); err != nil {
			return nil, err
		}
		exp, err := p.operand(t, p.parseUnary)
		p.depth--
		if err != nil {
			return nil, err
		}
		return p.binary(t, base, exp), nil
	}
	return base, nil
}

func (p *parser) parsePrimary() (*Node, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.advance()
		lit, err := parseNumber(t)
		if err != nil {
			return nil, err
		}
		return p.parsePostfix(lit)
	case tokString:
		p.advance()
		return p.parsePostfix(&Node{Kind: KindLit, Pos: t.start, End: t.end, Lit: t.text, Raw: p.src[t.start:t.end]})
	case tokLBrack:
		return p.parseList()
	case tokLParen:
		p.advance()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		cl, err := p.expect(tokRParen, "to close the '(' at column "+strconv.Itoa(utf8.RuneCountInString(p.src[:t.start])+1))
		if err != nil {
			return nil, err
		}
		inner.Parens++
		inner.Pos, inner.End = t.start, cl.end
		return p.parsePostfix(inner)
	case tokIdent:
		return p.parseIdent()
	case tokEOF:
		return nil, errSpan(t.start, t.end, "unexpected end of input").withHint("an expression is missing here")
	}
	e := errSpan(t.start, t.end, "unexpected %s", describe(t))
	switch t.text {
	case "=":
		e.Fix = "=="
		e.Hint = "did you mean '=='?"
	case "&", "&&":
		e.Fix = "and"
		e.Hint = "use `and`"
	case "|", "||":
		e.Fix = "or"
		e.Hint = "use `or`"
	case "!":
		e.Fix = "not"
		e.Hint = "use `not`"
	}
	return nil, e
}

func parseNumber(t token) (*Node, error) {
	if strings.ContainsAny(t.text, ".eE") {
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, errSpan(t.start, t.end, "invalid number %s", t.text)
		}
		return &Node{Kind: KindLit, Pos: t.start, End: t.end, Lit: f, Raw: t.text}, nil
	}
	n, err := strconv.ParseInt(t.text, 10, 64)
	if err != nil {
		return nil, errSpan(t.start, t.end, "integer %s is out of range", t.text).
			withHint("write it as a float, as in %s.0", t.text)
	}
	return &Node{Kind: KindLit, Pos: t.start, End: t.end, Lit: n, Raw: t.text}, nil
}

func (p *parser) parseIdent() (*Node, error) {
	t := p.advance()
	lit := func(v any) (*Node, error) {
		return p.parsePostfix(&Node{Kind: KindLit, Pos: t.start, End: t.end, Lit: v, Raw: strings.ToLower(t.text)})
	}
	switch strings.ToLower(t.text) {
	case "true":
		return lit(true)
	case "false":
		return lit(false)
	case "null":
		return lit(nil)
	case "when":
		return p.parseWhen(t)
	}
	// `ns.fn(args)`: a namespaced function whose first argument is
	// the receiver. `ns.<namespace>...` is a column called ns instead
	// (so a column named `name` still reads `name.str.upper()`).
	if _, isNS := namespaces[t.text]; isNS && p.peek().kind == tokDot &&
		p.peekAt(1).kind == tokIdent && p.peekAt(2).kind == tokLParen {
		if _, memberIsNS := namespaces[p.peekAt(1).text]; !memberIsNS {
			p.advance()
			fn := p.advance()
			args, kw, end, err := p.parseCallArgs(fn.text)
			if err != nil {
				return nil, err
			}
			call := &Node{Kind: KindCall, Pos: t.start, End: end, NS: t.text, Name: fn.text, NamePos: fn.start,
				Args: args, Kw: kw, CallParens: true, ambig: true}
			return p.parsePostfix(call)
		}
	}
	if p.peek().kind == tokLParen {
		args, kw, end, err := p.parseCallArgs(t.text)
		if err != nil {
			return nil, err
		}
		return p.parsePostfix(&Node{Kind: KindCall, Pos: t.start, End: end, Name: t.text, NamePos: t.start,
			Args: args, Kw: kw, CallParens: true})
	}
	return p.parsePostfix(&Node{Kind: KindCol, Pos: t.start, End: t.end, Name: t.text, NamePos: t.start})
}

func (p *parser) parseWhen(first token) (*Node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	w := &Node{Kind: KindWhen, Pos: first.start, NamePos: first.start}
	kw := first
	for {
		pred, err := p.operand(kw, p.parseOr)
		if err != nil {
			return nil, err
		}
		t := p.peek()
		if !isKeyword(t, "then") {
			e := errSpan(t.start, t.end, "expected `then` after the `when` condition, got %s", describe(t))
			if t.kind == tokIdent && script.Closest(strings.ToLower(t.text), []string{"then"}) != "" {
				e.Fix = "then"
				return nil, e.withHint(`did you mean "then"?`)
			}
			return nil, e.withHint("write it as when COND then VALUE otherwise VALUE")
		}
		p.advance()
		then, err := p.operand(t, p.parseOr)
		if err != nil {
			return nil, err
		}
		w.Args = append(w.Args, pred, then)
		w.End = then.End
		if !isKeyword(p.peek(), "when") {
			break
		}
		kw = p.advance()
	}
	if t := p.peek(); isKeyword(t, "otherwise") {
		p.advance()
		other, err := p.operand(t, p.parseOr)
		if err != nil {
			return nil, err
		}
		w.Final = other
		w.End = other.End
	}
	return w, nil
}

func (p *parser) parseList() (*Node, error) {
	open, err := p.expect(tokLBrack, "")
	if err != nil {
		return nil, err
	}
	list := &Node{Kind: KindList, Pos: open.start}
	if t := p.peek(); t.kind == tokRBrack {
		p.advance()
		list.End = t.end
		return list, nil
	}
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list.Args = append(list.Args, e)
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
		if p.peek().kind == tokRBrack {
			break // trailing comma
		}
	}
	cl, err := p.expect(tokRBrack, "to close the list")
	if err != nil {
		return nil, err
	}
	list.End = cl.end
	return list, nil
}

func (p *parser) parsePostfix(base *Node) (*Node, error) {
	for p.peek().kind == tokDot {
		dot := p.advance()
		member := p.peek()
		if member.kind != tokIdent {
			return nil, errSpan(member.start, member.end, "expected a method name after '.', got %s", describe(member))
		}
		p.advance()
		call := &Node{Kind: KindCall, Pos: base.Pos, End: member.end, Name: member.text, NamePos: member.start, Recv: base}
		if _, isNS := namespaces[member.text]; isNS {
			if p.peek().kind != tokDot {
				return nil, errSpan(dot.start, member.end, ".%s must be followed by a method", member.text).
					withHint("as in x.%s.%s()", member.text, sampleMethod(member.text))
			}
			p.advance()
			method := p.peek()
			if method.kind != tokIdent {
				return nil, errSpan(method.start, method.end, "expected a %s method name, got %s", member.text, describe(method))
			}
			p.advance()
			call.NS, call.Name, call.NamePos, call.End = member.text, method.text, method.start, method.end
		}
		// Parentheses are optional for a call without arguments:
		// `price.sum` reads the same as `price.sum()`.
		if p.peek().kind == tokLParen {
			args, kw, end, err := p.parseCallArgs(call.Name)
			if err != nil {
				return nil, err
			}
			call.Args, call.Kw, call.End, call.CallParens = args, kw, end, true
		}
		base = call
	}
	return base, nil
}

func sampleMethod(ns string) string {
	switch ns {
	case "str":
		return "to_uppercase"
	case "dt":
		return "year"
	case "list", "arr":
		return "len"
	case "name":
		return "suffix(\"_x\")"
	}
	return "method"
}

// parseCallArgs parses `( arg, name=arg, ... )` and leaves the parser
// after the close paren, whose end offset it returns.
func (p *parser) parseCallArgs(fn string) ([]*Node, []KwArg, int, error) {
	open, err := p.expect(tokLParen, "")
	if err != nil {
		return nil, nil, 0, err
	}
	var args []*Node
	var kw []KwArg
	if t := p.peek(); t.kind == tokRParen {
		p.advance()
		return nil, nil, t.end, nil
	}
	for {
		if p.peek().kind == tokIdent && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "=" {
			name := p.advance()
			eq := p.advance()
			v, err := p.operand(eq, p.parseExpr)
			if err != nil {
				return nil, nil, 0, err
			}
			for _, prev := range kw {
				if prev.Name == name.text {
					return nil, nil, 0, errSpan(name.start, name.end, "keyword argument %q given twice", name.text)
				}
			}
			kw = append(kw, KwArg{Name: name.text, NamePos: name.start, Val: v})
		} else {
			if len(kw) > 0 {
				t := p.peek()
				return nil, nil, 0, errSpan(t.start, t.end, "positional argument after keyword argument").
					withHint("move it before %s=", kw[0].Name)
			}
			a, err := p.parseExpr()
			if err != nil {
				return nil, nil, 0, err
			}
			args = append(args, a)
		}
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
		if p.peek().kind == tokRParen {
			break // trailing comma
		}
	}
	cl, err := p.expect(tokRParen, "to close the call to "+fn+" at column "+strconv.Itoa(utf8.RuneCountInString(p.src[:open.start])+1))
	if err != nil {
		return nil, nil, 0, err
	}
	return args, kw, cl.end, nil
}
