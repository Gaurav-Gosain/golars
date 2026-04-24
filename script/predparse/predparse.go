// Package predparse parses the `.filter` predicate DSL used by the
// golars REPL, the script runner, and the glr-to-Go transpiler into
// an [expr.Expr] tree.
//
// Grammar:
//
//	pred   := term (('and'|'or') term)*
//	term   := operand op operand
//	        | operand 'is_null'
//	        | operand 'is_not_null'
//	        | operand 'contains' string
//	        | operand 'starts_with' string
//	        | operand 'ends_with' string
//	        | operand 'like' string
//	        | operand 'not_like' string
//	op     := '==' | '!=' | '<' | '<=' | '>' | '>='
//	operand := identifier | integer | float | string | 'true' | 'false'
//
// Combining is left-associative; parentheses are not supported.
// Identifiers become column references. The string-op forms desugar
// to Col(x).Str().<op>(s) so the optimiser and evaluator see a
// regular FunctionNode.
package predparse

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/Gaurav-Gosain/golars/expr"
)

// Parse builds an expr.Expr from a filter predicate string.
func Parse(input string) (expr.Expr, error) {
	tokens, err := tokenize(input)
	if err != nil {
		return expr.Expr{}, err
	}
	p := &parser{tokens: tokens}
	e, err := p.parseOr()
	if err != nil {
		return expr.Expr{}, err
	}
	if p.pos < len(p.tokens) {
		return expr.Expr{}, fmt.Errorf("unexpected %q", p.tokens[p.pos].lex)
	}
	return e, nil
}

type tokKind uint8

const (
	tkIdent tokKind = iota
	tkInt
	tkFloat
	tkString
	tkBool
	tkOp
	tkKeyword
)

type token struct {
	kind    tokKind
	lex     string
	intVal  int64
	fltVal  float64
	boolVal bool
}

type parser struct {
	tokens []token
	pos    int
}

func tokenize(in string) ([]token, error) {
	var out []token
	i := 0
	for i < len(in) {
		c := in[i]
		if unicode.IsSpace(rune(c)) {
			i++
			continue
		}
		if c == '"' {
			end := i + 1
			var sb strings.Builder
			for end < len(in) && in[end] != '"' {
				if in[end] == '\\' && end+1 < len(in) {
					sb.WriteByte(in[end+1])
					end += 2
					continue
				}
				sb.WriteByte(in[end])
				end++
			}
			if end >= len(in) {
				return nil, fmt.Errorf("unterminated string")
			}
			out = append(out, token{kind: tkString, lex: sb.String()})
			i = end + 1
			continue
		}
		if isDigit(c) || (c == '-' && i+1 < len(in) && isDigit(in[i+1])) {
			end := i + 1
			isFloat := false
			for end < len(in) && (isDigit(in[end]) || in[end] == '.' || in[end] == 'e' || in[end] == '-') {
				if in[end] == '.' || in[end] == 'e' {
					isFloat = true
				}
				end++
			}
			lex := in[i:end]
			if isFloat {
				v, err := strconv.ParseFloat(lex, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number %q", lex)
				}
				out = append(out, token{kind: tkFloat, lex: lex, fltVal: v})
			} else {
				v, err := strconv.ParseInt(lex, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid integer %q", lex)
				}
				out = append(out, token{kind: tkInt, lex: lex, intVal: v})
			}
			i = end
			continue
		}
		if c == '=' || c == '!' || c == '<' || c == '>' {
			end := i + 1
			if end < len(in) && (in[end] == '=') {
				end++
			}
			lex := in[i:end]
			if lex == "=" {
				return nil, fmt.Errorf("unexpected '='; did you mean '=='?")
			}
			out = append(out, token{kind: tkOp, lex: lex})
			i = end
			continue
		}
		if isIdentStart(c) {
			end := i + 1
			for end < len(in) && isIdentCont(in[end]) {
				end++
			}
			lex := in[i:end]
			lc := strings.ToLower(lex)
			switch lc {
			case "and", "or", "is_null", "is_not_null",
				"contains", "starts_with", "ends_with", "like", "not_like":
				out = append(out, token{kind: tkKeyword, lex: lc})
			case "true":
				out = append(out, token{kind: tkBool, lex: lex, boolVal: true})
			case "false":
				out = append(out, token{kind: tkBool, lex: lex, boolVal: false})
			default:
				out = append(out, token{kind: tkIdent, lex: lex})
			}
			i = end
			continue
		}
		return nil, fmt.Errorf("unexpected character %q", string(c))
	}
	return out, nil
}

func (p *parser) parseOr() (expr.Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return expr.Expr{}, err
	}
	for p.peekKeyword("or") {
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return expr.Expr{}, err
		}
		left = left.Or(right)
	}
	return left, nil
}

func (p *parser) parseAnd() (expr.Expr, error) {
	left, err := p.parseTerm()
	if err != nil {
		return expr.Expr{}, err
	}
	for p.peekKeyword("and") {
		p.pos++
		right, err := p.parseTerm()
		if err != nil {
			return expr.Expr{}, err
		}
		left = left.And(right)
	}
	return left, nil
}

func (p *parser) parseTerm() (expr.Expr, error) {
	if p.pos >= len(p.tokens) {
		return expr.Expr{}, fmt.Errorf("unexpected end of input")
	}
	startKind := p.tokens[p.pos].kind
	left, err := p.parseOperand()
	if err != nil {
		return expr.Expr{}, err
	}
	if p.peekKeyword("is_null") {
		p.pos++
		return left.IsNull(), nil
	}
	if p.peekKeyword("is_not_null") {
		p.pos++
		return left.IsNotNull(), nil
	}
	for _, op := range []string{"contains", "starts_with", "ends_with", "like", "not_like"} {
		if p.peekKeyword(op) {
			p.pos++
			s, err := p.parseStringArg(op)
			if err != nil {
				return expr.Expr{}, err
			}
			return applyStrOp(left, op, s), nil
		}
	}
	// A bare column reference (e.g. `filter matches_vowel`) is valid:
	// the boolean column itself is the predicate. Anything else must
	// have an operator follow so the predicate is actually a test.
	if p.pos >= len(p.tokens) {
		if startKind == tkIdent {
			return left, nil
		}
		return expr.Expr{}, fmt.Errorf("expected operator after %q", p.tokens[p.pos-1].lex)
	}
	op := p.tokens[p.pos]
	if op.kind != tkOp {
		if startKind == tkIdent {
			return left, nil
		}
		return expr.Expr{}, fmt.Errorf("expected operator, got %q", op.lex)
	}
	p.pos++
	right, err := p.parseOperand()
	if err != nil {
		return expr.Expr{}, err
	}
	return applyBinaryOp(left, op.lex, right)
}

func (p *parser) parseOperand() (expr.Expr, error) {
	if p.pos >= len(p.tokens) {
		return expr.Expr{}, fmt.Errorf("unexpected end of input")
	}
	t := p.tokens[p.pos]
	p.pos++
	switch t.kind {
	case tkIdent:
		return expr.Col(t.lex), nil
	case tkInt:
		return expr.LitInt64(t.intVal), nil
	case tkFloat:
		return expr.LitFloat64(t.fltVal), nil
	case tkString:
		return expr.LitString(t.lex), nil
	case tkBool:
		return expr.LitBool(t.boolVal), nil
	}
	return expr.Expr{}, fmt.Errorf("expected operand, got %q", t.lex)
}

func (p *parser) peekKeyword(s string) bool {
	if p.pos >= len(p.tokens) {
		return false
	}
	t := p.tokens[p.pos]
	return t.kind == tkKeyword && t.lex == s
}

func (p *parser) parseStringArg(op string) (string, error) {
	if p.pos >= len(p.tokens) {
		return "", fmt.Errorf("%s: expected string argument", op)
	}
	t := p.tokens[p.pos]
	if t.kind != tkString {
		return "", fmt.Errorf("%s: expected string argument, got %q", op, t.lex)
	}
	p.pos++
	return t.lex, nil
}

func applyStrOp(left expr.Expr, op, arg string) expr.Expr {
	s := left.Str()
	switch op {
	case "contains":
		return s.Contains(arg)
	case "starts_with":
		return s.StartsWith(arg)
	case "ends_with":
		return s.EndsWith(arg)
	case "like":
		return s.Like(arg)
	case "not_like":
		return s.NotLike(arg)
	}
	return left.Eq(expr.LitString(arg))
}

func applyBinaryOp(left expr.Expr, op string, right expr.Expr) (expr.Expr, error) {
	switch op {
	case "==":
		return left.Eq(right), nil
	case "!=":
		return left.Ne(right), nil
	case "<":
		return left.Lt(right), nil
	case "<=":
		return left.Le(right), nil
	case ">":
		return left.Gt(right), nil
	case ">=":
		return left.Ge(right), nil
	}
	return expr.Expr{}, fmt.Errorf("unknown operator %q", op)
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isIdentCont(c byte) bool  { return isIdentStart(c) || isDigit(c) }
