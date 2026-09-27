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
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/Gaurav-Gosain/golars/expr"
)

// Parse builds an expr.Expr from s. Errors point at the offending
// byte where the parser knows it.
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

func compileSource(s string) (val, error) {
	p := &parser{toks: tokenize(s)}
	n, err := p.parseExpr()
	if err != nil {
		return val{}, err
	}
	if t := p.peek(); t.kind != tokEOF {
		if t.kind == tokOp && t.text == "=" {
			return val{}, fmt.Errorf("unexpected '=' at byte %d; did you mean '=='?", t.start)
		}
		return val{}, fmt.Errorf("unexpected trailing token %q at byte %d", t.text, t.start)
	}
	return compile(n)
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
	kind  tokKind
	text  string
	start int
}

func tokenize(s string) []token {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, token{tokLParen, "(", i})
			i++
		case c == ')':
			toks = append(toks, token{tokRParen, ")", i})
			i++
		case c == '[':
			toks = append(toks, token{tokLBrack, "[", i})
			i++
		case c == ']':
			toks = append(toks, token{tokRBrack, "]", i})
			i++
		case c == ',':
			toks = append(toks, token{tokComma, ",", i})
			i++
		case c == '.' && (i+1 >= len(s) || !isDigit(s[i+1])):
			toks = append(toks, token{tokDot, ".", i})
			i++
		case c == '"' || c == '\'':
			end, lit, ok := readString(s, i)
			if !ok {
				toks = append(toks, token{tokOp, "!badstring", i})
				return toks
			}
			toks = append(toks, token{tokString, lit, i})
			i = end
		case isDigit(c) || (c == '.' && i+1 < len(s) && isDigit(s[i+1])):
			end, lit := readNumber(s, i)
			toks = append(toks, token{tokNumber, lit, i})
			i = end
		case isIdentStart(c):
			end, lit := readIdent(s, i)
			toks = append(toks, token{tokIdent, lit, i})
			i = end
		default:
			if i+1 < len(s) {
				switch two := s[i : i+2]; two {
				case "==", "!=", "<=", ">=", "//", "**":
					toks = append(toks, token{tokOp, two, i})
					i += 2
					continue
				}
			}
			// Unknown bytes become stray operators so the parser can
			// report a useful error rather than hang.
			toks = append(toks, token{tokOp, string(c), i})
			i++
		}
	}
	return append(toks, token{tokEOF, "", len(s)})
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return unicode.IsLetter(rune(c)) || c == '_' }
func isIdentCont(c byte) bool  { return isIdentStart(c) || isDigit(c) }

func readIdent(s string, i int) (int, string) {
	j := i
	for j < len(s) && isIdentCont(s[j]) {
		j++
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
	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		j++
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		for j < len(s) && isDigit(s[j]) {
			j++
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

type nodeKind uint8

const (
	nLit    nodeKind = iota // lit holds int64, float64, string, bool or nil
	nCol                    // name is the column
	nList                   // args are the elements
	nBinary                 // name is the operator; args are the operands
	nNot                    // args[0]
	nNeg                    // args[0]
	nCall                   // ns.name(recv?, args..., kw...)
	nWhen                   // args are pred/then pairs; final is the otherwise branch
)

type node struct {
	kind  nodeKind
	pos   int
	lit   any
	name  string
	ns    string
	recv  *node
	args  []*node
	kw    []kwarg
	final *node
	// ambig marks `ns.fn(args)` where ns could also be a column name.
	// The compiler falls back to `col(ns).fn(args)` when the
	// namespace reading does not compile.
	ambig bool
}

type kwarg struct {
	name string
	val  *node
}

// litNode wraps a Go value (a table default) as a glr literal. A
// []any becomes a list literal.
func litNode(v any) *node {
	switch x := v.(type) {
	case int:
		v = int64(x)
	case []any:
		list := &node{kind: nList}
		for _, e := range x {
			list.args = append(list.args, litNode(e))
		}
		return list
	}
	return &node{kind: nLit, lit: v}
}

// namespaces maps each glr namespace to its accessor on expr.Expr.
var namespaces = map[string]string{
	"str": "Str", "dt": "Dt", "list": "List", "arr": "Arr",
	"struct": "Struct", "name": "Name", "bin": "Bin", "cat": "Cat",
}

// --- parser ------------------------------------------------------

type parser struct {
	toks []token
	pos  int
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

func (p *parser) expect(kind tokKind) (token, error) {
	t := p.peek()
	if t.kind != kind {
		if t.kind == tokEOF {
			return t, fmt.Errorf("expected %s, got end of input", kindName(kind))
		}
		return t, fmt.Errorf("expected %s, got %q at byte %d", kindName(kind), t.text, t.start)
	}
	p.pos++
	return t, nil
}

func kindName(k tokKind) string {
	switch k {
	case tokIdent:
		return "identifier"
	case tokNumber:
		return "number"
	case tokString:
		return "string"
	case tokOp:
		return "operator"
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
	return "token"
}

// isKeyword reports whether t is the identifier kw, ignoring case.
func isKeyword(t token, kw string) bool {
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) parseExpr() (*node, error) { return p.parseOr() }

func (p *parser) parseOr() (*node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.peek(), "or") {
		t := p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nBinary, pos: t.start, name: "or", args: []*node{left, right}}
	}
	return left, nil
}

func (p *parser) parseAnd() (*node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.peek(), "and") {
		t := p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nBinary, pos: t.start, name: "and", args: []*node{left, right}}
	}
	return left, nil
}

func (p *parser) parseNot() (*node, error) {
	if t := p.peek(); isKeyword(t, "not") {
		p.advance()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &node{kind: nNot, pos: t.start, args: []*node{inner}}, nil
	}
	return p.parseCmp()
}

// strInfix are the predicate keywords that desugar to str methods.
var strInfix = []string{"contains", "starts_with", "ends_with", "like", "not_like"}

func (p *parser) parseCmp() (*node, error) {
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
			right, err := p.parseAdd()
			if err != nil {
				return nil, err
			}
			return &node{kind: nBinary, pos: t.start, name: t.text, args: []*node{left, right}}, nil
		}
	case isKeyword(t, "is_null"), isKeyword(t, "is_not_null"):
		p.advance()
		return &node{kind: nCall, pos: t.start, name: strings.ToLower(t.text), recv: left}, nil
	case isKeyword(t, "in"):
		p.advance()
		return p.parseIn(left, t)
	case isKeyword(t, "not") && isKeyword(p.peekAt(1), "in"):
		p.advance()
		p.advance()
		in, err := p.parseIn(left, t)
		if err != nil {
			return nil, err
		}
		return &node{kind: nNot, pos: t.start, args: []*node{in}}, nil
	case t.kind == tokIdent:
		for _, op := range strInfix {
			if isKeyword(t, op) {
				p.advance()
				right, err := p.parseAdd()
				if err != nil {
					return nil, err
				}
				return &node{kind: nCall, pos: t.start, ns: "str", name: op, recv: left, args: []*node{right}}, nil
			}
		}
	}
	return left, nil
}

func (p *parser) parseIn(left *node, t token) (*node, error) {
	if p.peek().kind != tokLBrack {
		return nil, fmt.Errorf("`in` at byte %d must be followed by a [list]", t.start)
	}
	list, err := p.parseList()
	if err != nil {
		return nil, err
	}
	return &node{kind: nCall, pos: t.start, name: "is_in", recv: left, args: []*node{list}}, nil
}

func (p *parser) parseAdd() (*node, error) {
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
		right, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nBinary, pos: t.start, name: t.text, args: []*node{left, right}}
	}
}

func (p *parser) parseMul() (*node, error) {
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
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nBinary, pos: t.start, name: t.text, args: []*node{left, right}}
	}
}

func (p *parser) parseUnary() (*node, error) {
	if t := p.peek(); t.kind == tokOp && t.text == "-" {
		p.advance()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		// Fold negative numeric literals so `shift(-1)` passes a
		// literal rather than a negation expression.
		if inner.kind == nLit {
			switch v := inner.lit.(type) {
			case int64:
				return &node{kind: nLit, pos: t.start, lit: -v}, nil
			case float64:
				return &node{kind: nLit, pos: t.start, lit: -v}, nil
			}
		}
		return &node{kind: nNeg, pos: t.start, args: []*node{inner}}, nil
	}
	return p.parsePower()
}

func (p *parser) parsePower() (*node, error) {
	base, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tokOp && t.text == "**" {
		p.advance()
		exp, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &node{kind: nBinary, pos: t.start, name: "**", args: []*node{base, exp}}, nil
	}
	return base, nil
}

func (p *parser) parsePrimary() (*node, error) {
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
		return p.parsePostfix(&node{kind: nLit, pos: t.start, lit: t.text})
	case tokLBrack:
		return p.parseList()
	case tokLParen:
		p.advance()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return p.parsePostfix(inner)
	case tokIdent:
		return p.parseIdent()
	case tokEOF:
		return nil, fmt.Errorf("unexpected end of input")
	}
	return nil, fmt.Errorf("unexpected token %q at byte %d", t.text, t.start)
}

func parseNumber(t token) (*node, error) {
	if strings.ContainsAny(t.text, ".eE") {
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", t.text, err)
		}
		return &node{kind: nLit, pos: t.start, lit: f}, nil
	}
	n, err := strconv.ParseInt(t.text, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q: %w", t.text, err)
	}
	return &node{kind: nLit, pos: t.start, lit: n}, nil
}

func (p *parser) parseIdent() (*node, error) {
	t := p.advance()
	switch strings.ToLower(t.text) {
	case "true":
		return p.parsePostfix(&node{kind: nLit, pos: t.start, lit: true})
	case "false":
		return p.parsePostfix(&node{kind: nLit, pos: t.start, lit: false})
	case "null":
		return p.parsePostfix(&node{kind: nLit, pos: t.start, lit: nil})
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
			args, kw, err := p.parseCallArgs()
			if err != nil {
				return nil, err
			}
			call := &node{kind: nCall, pos: t.start, ns: t.text, name: fn.text, args: args, kw: kw, ambig: true}
			return p.parsePostfix(call)
		}
	}
	if p.peek().kind == tokLParen {
		args, kw, err := p.parseCallArgs()
		if err != nil {
			return nil, err
		}
		return p.parsePostfix(&node{kind: nCall, pos: t.start, name: t.text, args: args, kw: kw})
	}
	return p.parsePostfix(&node{kind: nCol, pos: t.start, name: t.text})
}

func (p *parser) parseWhen(first token) (*node, error) {
	w := &node{kind: nWhen, pos: first.start}
	for {
		pred, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if t := p.peek(); !isKeyword(t, "then") {
			return nil, fmt.Errorf("expected `then` after `when` condition, got %q", t.text)
		}
		p.advance()
		then, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		w.args = append(w.args, pred, then)
		if !isKeyword(p.peek(), "when") {
			break
		}
		p.advance()
	}
	if isKeyword(p.peek(), "otherwise") {
		p.advance()
		other, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		w.final = other
	}
	return w, nil
}

func (p *parser) parseList() (*node, error) {
	open, err := p.expect(tokLBrack)
	if err != nil {
		return nil, err
	}
	list := &node{kind: nList, pos: open.start}
	if p.peek().kind == tokRBrack {
		p.advance()
		return list, nil
	}
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list.args = append(list.args, e)
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tokRBrack); err != nil {
		return nil, err
	}
	return list, nil
}

func (p *parser) parsePostfix(base *node) (*node, error) {
	for p.peek().kind == tokDot {
		p.advance()
		member, err := p.expect(tokIdent)
		if err != nil {
			return nil, err
		}
		call := &node{kind: nCall, pos: member.start, name: member.text, recv: base}
		if _, isNS := namespaces[member.text]; isNS {
			if p.peek().kind != tokDot {
				return nil, fmt.Errorf(".%s must be followed by a method", member.text)
			}
			p.advance()
			method, err := p.expect(tokIdent)
			if err != nil {
				return nil, err
			}
			call.ns, call.name = member.text, method.text
		}
		// Parentheses are optional for a call without arguments:
		// `price.sum` reads the same as `price.sum()`.
		if p.peek().kind == tokLParen {
			if call.args, call.kw, err = p.parseCallArgs(); err != nil {
				return nil, err
			}
		}
		base = call
	}
	return base, nil
}

// parseCallArgs parses `( arg, name=arg, ... )` and leaves the parser
// after the close paren.
func (p *parser) parseCallArgs() ([]*node, []kwarg, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, nil, err
	}
	var args []*node
	var kw []kwarg
	if p.peek().kind == tokRParen {
		p.advance()
		return nil, nil, nil
	}
	for {
		if p.peek().kind == tokIdent && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "=" {
			name := p.advance()
			p.advance()
			v, err := p.parseExpr()
			if err != nil {
				return nil, nil, err
			}
			kw = append(kw, kwarg{name: name.text, val: v})
		} else {
			if len(kw) > 0 {
				t := p.peek()
				return nil, nil, fmt.Errorf("positional argument after keyword argument at byte %d", t.start)
			}
			a, err := p.parseExpr()
			if err != nil {
				return nil, nil, err
			}
			args = append(args, a)
		}
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, nil, err
	}
	return args, kw, nil
}
