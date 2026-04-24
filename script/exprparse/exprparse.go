// Package exprparse turns a short text expression into an expr.Expr.
//
// Grammar (whitespace-separated, case-sensitive for identifiers):
//
//	expr    := orExpr
//	orExpr  := andExpr ("or" andExpr)*
//	andExpr := notExpr ("and" notExpr)*
//	notExpr := "not" notExpr | cmpExpr
//	cmpExpr := addExpr ((== | != | < | <= | > | >=) addExpr)?
//	addExpr := mulExpr ((+ | -) mulExpr)*
//	mulExpr := unary ((* | /) unary)*
//	unary   := "-" unary | primary
//	primary := literal | "(" expr ")" | methodChain
//	methodChain := ident ("." call)*
//	call    := ident "(" argList? ")"
//	argList := expr ("," expr)*
//	literal := number | quoted-string | "true" | "false" | "null"
//
// Bare identifiers resolve to column references (`expr.Col`). Chained
// calls dispatch to method namespaces: `col.str.upper()` lowers to
// `expr.Col("col").Str().ToUppercase()`. The supported surface
// mirrors the `.glr` scripting language's audience; not every golars
// operator is reachable. See the `dispatch*` helpers below for the
// full list.
package exprparse

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
)

// Parse builds an expr.Expr from s. Trailing whitespace tolerated.
// Errors point at the offending byte so callers can flag the cursor
// in editor contexts.
func Parse(s string) (expr.Expr, error) {
	p := &parser{src: s, toks: tokenize(s)}
	e, err := p.parseExpr()
	if err != nil {
		return expr.Expr{}, err
	}
	if p.pos < len(p.toks) && p.toks[p.pos].kind != tokEOF {
		return expr.Expr{}, fmt.Errorf("unexpected trailing token %q at byte %d",
			p.toks[p.pos].text, p.toks[p.pos].start)
	}
	return e, nil
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
		case isDigit(c) || (c == '-' && i+1 < len(s) && isDigit(s[i+1]) && (len(toks) == 0 || lastIsOperand(toks) == false)):
			end, lit := readNumber(s, i)
			toks = append(toks, token{tokNumber, lit, i})
			i = end
		case isIdentStart(c):
			end, lit := readIdent(s, i)
			toks = append(toks, token{tokIdent, lit, i})
			i = end
		default:
			// Multi-char operators first: ==, !=, <=, >=
			if i+1 < len(s) {
				two := s[i : i+2]
				switch two {
				case "==", "!=", "<=", ">=":
					toks = append(toks, token{tokOp, two, i})
					i += 2
					continue
				}
			}
			switch c {
			case '+', '-', '*', '/', '<', '>', '=':
				toks = append(toks, token{tokOp, string(c), i})
				i++
			default:
				// Unknown byte: emit as stray op so the parser can
				// report a useful error rather than hang.
				toks = append(toks, token{tokOp, string(c), i})
				i++
			}
		}
	}
	toks = append(toks, token{tokEOF, "", len(s)})
	return toks
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return unicode.IsLetter(rune(c)) || c == '_' }
func isIdentCont(c byte) bool  { return isIdentStart(c) || isDigit(c) }

// lastIsOperand reports whether the token stream currently ends at
// an operand (literal / identifier / close paren), which lets the
// tokenizer tell unary minus apart from binary minus.
func lastIsOperand(toks []token) bool {
	if len(toks) == 0 {
		return false
	}
	switch toks[len(toks)-1].kind {
	case tokNumber, tokString, tokIdent, tokRParen:
		return true
	}
	return false
}

func readIdent(s string, i int) (int, string) {
	j := i
	for j < len(s) && isIdentCont(s[j]) {
		j++
	}
	return j, s[i:j]
}

func readNumber(s string, i int) (int, string) {
	j := i
	if s[j] == '-' {
		j++
	}
	for j < len(s) && isDigit(s[j]) {
		j++
	}
	if j < len(s) && s[j] == '.' {
		j++
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

// --- parser ------------------------------------------------------

type parser struct {
	src  string
	toks []token
	pos  int
}

func (p *parser) peek() token {
	return p.toks[p.pos]
}

func (p *parser) advance() token {
	t := p.toks[p.pos]
	p.pos++
	return t
}

func (p *parser) expect(kind tokKind) (token, error) {
	t := p.peek()
	if t.kind != kind {
		return t, fmt.Errorf("expected %s, got %q at byte %d",
			kindName(kind), t.text, t.start)
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
	case tokComma:
		return "','"
	case tokDot:
		return "'.'"
	}
	return "token"
}

func (p *parser) parseExpr() (expr.Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (expr.Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return expr.Expr{}, err
	}
	for p.peek().kind == tokIdent && p.peek().text == "or" {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return expr.Expr{}, err
		}
		left = left.Or(right)
	}
	return left, nil
}

func (p *parser) parseAnd() (expr.Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return expr.Expr{}, err
	}
	for p.peek().kind == tokIdent && p.peek().text == "and" {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return expr.Expr{}, err
		}
		left = left.And(right)
	}
	return left, nil
}

func (p *parser) parseNot() (expr.Expr, error) {
	if p.peek().kind == tokIdent && p.peek().text == "not" {
		p.advance()
		inner, err := p.parseNot()
		if err != nil {
			return expr.Expr{}, err
		}
		return inner.Not(), nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (expr.Expr, error) {
	left, err := p.parseAdd()
	if err != nil {
		return expr.Expr{}, err
	}
	t := p.peek()
	if t.kind == tokOp {
		switch t.text {
		case "==", "!=", "<", "<=", ">", ">=":
			p.advance()
			right, err := p.parseAdd()
			if err != nil {
				return expr.Expr{}, err
			}
			return applyBinary(left, right, t.text), nil
		}
	}
	return left, nil
}

func (p *parser) parseAdd() (expr.Expr, error) {
	left, err := p.parseMul()
	if err != nil {
		return expr.Expr{}, err
	}
	for {
		t := p.peek()
		if t.kind != tokOp || (t.text != "+" && t.text != "-") {
			break
		}
		p.advance()
		right, err := p.parseMul()
		if err != nil {
			return expr.Expr{}, err
		}
		left = applyBinary(left, right, t.text)
	}
	return left, nil
}

func (p *parser) parseMul() (expr.Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return expr.Expr{}, err
	}
	for {
		t := p.peek()
		if t.kind != tokOp || (t.text != "*" && t.text != "/") {
			break
		}
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return expr.Expr{}, err
		}
		left = applyBinary(left, right, t.text)
	}
	return left, nil
}

func (p *parser) parseUnary() (expr.Expr, error) {
	if t := p.peek(); t.kind == tokOp && t.text == "-" {
		p.advance()
		inner, err := p.parseUnary()
		if err != nil {
			return expr.Expr{}, err
		}
		return inner.Neg(), nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (expr.Expr, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.advance()
		if strings.Contains(t.text, ".") {
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return expr.Expr{}, fmt.Errorf("invalid number %q: %w", t.text, err)
			}
			return expr.LitFloat64(f), nil
		}
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return expr.Expr{}, fmt.Errorf("invalid number %q: %w", t.text, err)
		}
		return expr.LitInt64(n), nil
	case tokString:
		p.advance()
		return expr.LitString(t.text), nil
	case tokIdent:
		switch t.text {
		case "true":
			p.advance()
			return expr.LitBool(true), nil
		case "false":
			p.advance()
			return expr.LitBool(false), nil
		case "null":
			p.advance()
			return expr.LitNull(dtype.Null()), nil
		}
		return p.parseMethodChain()
	case tokLParen:
		p.advance()
		inner, err := p.parseExpr()
		if err != nil {
			return expr.Expr{}, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return expr.Expr{}, err
		}
		return inner, nil
	}
	return expr.Expr{}, fmt.Errorf("unexpected token %q at byte %d", t.text, t.start)
}

// parseMethodChain handles `ident (. ident (args?))*` where the
// first ident is a column name and subsequent chained calls dispatch
// to a namespace. Special-case the namespace itself so
// `col.str.upper()` works (skip the intermediate "str" ident).
func (p *parser) parseMethodChain() (expr.Expr, error) {
	head, err := p.expect(tokIdent)
	if err != nil {
		return expr.Expr{}, err
	}
	// Function-style call at the head: allow `sum(col)` / `abs(col)`
	// for aggregate / scalar ops that would otherwise require a bare
	// column start.
	if p.peek().kind == tokLParen {
		return p.parseFreeFunction(head.text)
	}
	base := expr.Col(head.text)
	for p.peek().kind == tokDot {
		p.advance()
		member, err := p.expect(tokIdent)
		if err != nil {
			return expr.Expr{}, err
		}
		next, err := p.applyMember(base, member.text)
		if err != nil {
			return expr.Expr{}, err
		}
		base = next
	}
	return base, nil
}

// applyMember turns `base.member(args?)` into the right Expr call.
// Namespaces (`str`) consume the next dot+member pair.
func (p *parser) applyMember(base expr.Expr, member string) (expr.Expr, error) {
	if member == "str" {
		// Consume the next method in the str namespace.
		if p.peek().kind != tokDot {
			return expr.Expr{}, fmt.Errorf(".str must be followed by a method")
		}
		p.advance()
		methodTok, err := p.expect(tokIdent)
		if err != nil {
			return expr.Expr{}, err
		}
		args, err := p.parseCallArgs()
		if err != nil {
			return expr.Expr{}, err
		}
		return dispatchStr(base, methodTok.text, args)
	}
	// Non-namespaced method: allow both arg-less aggregates (sum,
	// mean, ...) and param methods (fill_null, shift, ...).
	if p.peek().kind == tokLParen {
		args, err := p.parseCallArgs()
		if err != nil {
			return expr.Expr{}, err
		}
		return dispatchMethod(base, member, args)
	}
	// Property-like usage without parens: alias fast path for
	// `col.sum`, etc.
	return dispatchMethod(base, member, nil)
}

// parseFreeFunction handles `name(...)` calls where `name` is a
// top-level function (not attached to a column). Supports the
// handful of polars-style free constructors that make sense here:
// col("x"), lit(v), sum("x"), etc.
func (p *parser) parseFreeFunction(name string) (expr.Expr, error) {
	args, err := p.parseCallArgs()
	if err != nil {
		return expr.Expr{}, err
	}
	return dispatchFreeFn(name, args)
}

// parseCallArgs parses `( e, e, ... )`. Leaves the parser positioned
// after the close paren. Returns a nil slice for `()`.
func (p *parser) parseCallArgs() ([]expr.Expr, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	if p.peek().kind == tokRParen {
		p.advance()
		return nil, nil
	}
	var args []expr.Expr
	for {
		a, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, a)
		if p.peek().kind == tokComma {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return args, nil
}

// --- dispatch ----------------------------------------------------

func applyBinary(left, right expr.Expr, op string) expr.Expr {
	switch op {
	case "+":
		return left.Add(right)
	case "-":
		return left.Sub(right)
	case "*":
		return left.Mul(right)
	case "/":
		return left.Div(right)
	case "==":
		return left.Eq(right)
	case "!=":
		return left.Ne(right)
	case "<":
		return left.Lt(right)
	case "<=":
		return left.Le(right)
	case ">":
		return left.Gt(right)
	case ">=":
		return left.Ge(right)
	}
	panic("unreachable: unknown operator " + op)
}

// dispatchStr routes a `col.str.METHOD(args...)` call. Argument
// count mismatches return a descriptive error rather than panicking.
func dispatchStr(base expr.Expr, method string, args []expr.Expr) (expr.Expr, error) {
	switch method {
	case "upper", "to_upper", "to_uppercase":
		return base.Str().ToUpper(), nil
	case "lower", "to_lower", "to_lowercase":
		return base.Str().ToLower(), nil
	case "trim":
		return base.Str().Trim(), nil
	case "len_bytes":
		return base.Str().LenBytes(), nil
	case "len_chars":
		return base.Str().LenChars(), nil
	case "contains":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Contains(s), nil
	case "starts_with":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().StartsWith(s), nil
	case "ends_with":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().EndsWith(s), nil
	case "like":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Like(s), nil
	case "not_like":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().NotLike(s), nil
	case "contains_regex":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().ContainsRegex(s), nil
	case "strip_prefix":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().StripPrefix(s), nil
	case "strip_suffix":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().StripSuffix(s), nil
	case "replace":
		a, b, err := twoStrings(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Replace(a, b), nil
	case "replace_all":
		a, b, err := twoStrings(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().ReplaceAll(a, b), nil
	case "slice":
		start, length, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Slice(start, length), nil
	case "head":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Head(n), nil
	case "tail":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Tail(n), nil
	case "count_matches":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().CountMatches(s), nil
	case "find":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().Find(s), nil
	case "split_exact":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Str().SplitExact(s), nil
	}
	return expr.Expr{}, fmt.Errorf("unknown str method %q", method)
}

// dispatchMethod handles non-namespaced methods on a column or
// derived expression: aggregates (.sum, .mean, ...), casts, shape
// ops (.shift, .reverse, ...).
func dispatchMethod(base expr.Expr, method string, args []expr.Expr) (expr.Expr, error) {
	switch method {
	case "sum":
		return base.Sum(), nil
	case "mean":
		return base.Mean(), nil
	case "min":
		return base.Min(), nil
	case "max":
		return base.Max(), nil
	case "count":
		return base.Count(), nil
	case "null_count":
		return base.NullCount(), nil
	case "first":
		return base.First(), nil
	case "last":
		return base.Last(), nil
	case "std":
		return base.Std(), nil
	case "var":
		return base.Var(), nil
	case "median":
		return base.Median(), nil
	case "quantile":
		q, err := oneFloat(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Quantile(q), nil
	case "skew":
		return base.Skew(), nil
	case "kurtosis":
		return base.Kurtosis(), nil
	case "n_unique":
		return base.NUnique(), nil
	case "approx_n_unique":
		return base.ApproxNUnique(), nil
	case "is_null":
		return base.IsNull(), nil
	case "is_not_null":
		return base.IsNotNull(), nil
	case "abs":
		return base.Abs(), nil
	case "neg":
		return base.Neg(), nil
	case "not":
		return base.Not(), nil
	case "round":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Round(n), nil
	case "floor":
		return base.Floor(), nil
	case "ceil":
		return base.Ceil(), nil
	case "sqrt":
		return base.Sqrt(), nil
	case "exp":
		return base.Exp(), nil
	case "log":
		return base.Log(), nil
	case "log10":
		return base.Log10(), nil
	case "log2":
		return base.Log2(), nil
	case "sign":
		return base.Sign(), nil
	case "reverse":
		return base.Reverse(), nil
	case "head":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Head(n), nil
	case "tail":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Tail(n), nil
	case "shift":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Shift(n), nil
	case "diff":
		n, err := oneInt(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Diff(n), nil
	case "cum_sum":
		return base.CumSum(), nil
	case "cum_min":
		return base.CumMin(), nil
	case "cum_max":
		return base.CumMax(), nil
	case "fill_null":
		if len(args) != 1 {
			return expr.Expr{}, fmt.Errorf("fill_null takes 1 argument, got %d", len(args))
		}
		return base.FillNullExpr(args[0]), nil
	case "alias":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Alias(s), nil
	case "cast":
		s, err := oneString(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		dt, err := dtypeByName(s)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Cast(dt), nil
	case "rolling_sum":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingSum(w, mp), nil
	case "rolling_mean":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingMean(w, mp), nil
	case "rolling_min":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingMin(w, mp), nil
	case "rolling_max":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingMax(w, mp), nil
	case "rolling_std":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingStd(w, mp), nil
	case "rolling_var":
		w, mp, err := twoInts(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.RollingVar(w, mp), nil
	case "ewm_mean":
		a, err := oneFloat(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.EWMMean(a), nil
	case "ewm_std":
		a, err := oneFloat(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.EWMStd(a), nil
	case "ewm_var":
		a, err := oneFloat(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.EWMVar(a), nil
	case "forward_fill":
		n, err := oneIntOrZero(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.ForwardFill(n), nil
	case "backward_fill":
		n, err := oneIntOrZero(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.BackwardFill(n), nil
	case "over":
		keys, err := stringList(method, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return base.Over(keys...), nil
	case "between":
		if len(args) != 2 {
			return expr.Expr{}, fmt.Errorf("between takes 2 arguments, got %d", len(args))
		}
		loV, err := literalValue(args[0])
		if err != nil {
			return expr.Expr{}, fmt.Errorf("between: lo %w", err)
		}
		hiV, err := literalValue(args[1])
		if err != nil {
			return expr.Expr{}, fmt.Errorf("between: hi %w", err)
		}
		return base.Between(loV, hiV), nil
	}
	return expr.Expr{}, fmt.Errorf("unknown method %q", method)
}

// dispatchFreeFn covers top-level constructors reachable without a
// column receiver: `col(x)`, `lit(v)`, `sum("x")`, and the
// when/then/otherwise chain.
func dispatchFreeFn(name string, args []expr.Expr) (expr.Expr, error) {
	switch name {
	case "col":
		s, err := oneString(name, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return expr.Col(s), nil
	case "lit":
		if len(args) != 1 {
			return expr.Expr{}, fmt.Errorf("lit takes 1 argument, got %d", len(args))
		}
		v, err := literalValue(args[0])
		if err != nil {
			return expr.Expr{}, err
		}
		return expr.Lit(v), nil
	case "sum", "mean", "min", "max", "count", "first", "last",
		"median", "std", "var", "n_unique":
		s, err := oneString(name, args)
		if err != nil {
			return expr.Expr{}, err
		}
		return aggByName(expr.Col(s), name)
	case "abs", "sqrt", "exp", "log", "log2", "log10", "sign",
		"floor", "ceil":
		if len(args) != 1 {
			return expr.Expr{}, fmt.Errorf("%s takes 1 argument, got %d", name, len(args))
		}
		return dispatchMethod(args[0], name, nil)
	case "coalesce":
		if len(args) == 0 {
			return expr.Expr{}, fmt.Errorf("coalesce requires at least one argument")
		}
		return expr.Coalesce(args...), nil
	}
	return expr.Expr{}, fmt.Errorf("unknown function %q", name)
}

func aggByName(base expr.Expr, name string) (expr.Expr, error) {
	return dispatchMethod(base, name, nil)
}

// --- arg helpers ------------------------------------------------

func oneString(op string, args []expr.Expr) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("%s takes 1 argument, got %d", op, len(args))
	}
	return stringArg(op, args[0])
}

func twoStrings(op string, args []expr.Expr) (string, string, error) {
	if len(args) != 2 {
		return "", "", fmt.Errorf("%s takes 2 arguments, got %d", op, len(args))
	}
	a, err := stringArg(op, args[0])
	if err != nil {
		return "", "", err
	}
	b, err := stringArg(op, args[1])
	if err != nil {
		return "", "", err
	}
	return a, b, nil
}

func oneInt(op string, args []expr.Expr) (int, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("%s takes 1 argument, got %d", op, len(args))
	}
	return intArg(op, args[0])
}

func oneIntOrZero(op string, args []expr.Expr) (int, error) {
	if len(args) == 0 {
		return 0, nil
	}
	return oneInt(op, args)
}

func twoInts(op string, args []expr.Expr) (int, int, error) {
	if len(args) != 2 {
		return 0, 0, fmt.Errorf("%s takes 2 arguments, got %d", op, len(args))
	}
	a, err := intArg(op, args[0])
	if err != nil {
		return 0, 0, err
	}
	b, err := intArg(op, args[1])
	if err != nil {
		return 0, 0, err
	}
	return a, b, nil
}

func oneFloat(op string, args []expr.Expr) (float64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("%s takes 1 argument, got %d", op, len(args))
	}
	return floatArg(op, args[0])
}

func stringInt(op string, args []expr.Expr) (string, int, error) {
	if len(args) != 2 {
		return "", 0, fmt.Errorf("%s takes 2 arguments, got %d", op, len(args))
	}
	s, err := stringArg(op, args[0])
	if err != nil {
		return "", 0, err
	}
	i, err := intArg(op, args[1])
	if err != nil {
		return "", 0, err
	}
	return s, i, nil
}

func stringList(op string, args []expr.Expr) ([]string, error) {
	out := make([]string, len(args))
	for i, a := range args {
		s, err := stringArg(op, a)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}

func stringArg(op string, e expr.Expr) (string, error) {
	lit, ok := asLiteral(e)
	if !ok {
		return "", fmt.Errorf("%s: expected string literal", op)
	}
	s, ok := lit.(string)
	if !ok {
		return "", fmt.Errorf("%s: expected string, got %T", op, lit)
	}
	return s, nil
}

func intArg(op string, e expr.Expr) (int, error) {
	lit, ok := asLiteral(e)
	if !ok {
		return 0, fmt.Errorf("%s: expected integer literal", op)
	}
	switch v := lit.(type) {
	case int64:
		return int(v), nil
	case int:
		return v, nil
	}
	return 0, fmt.Errorf("%s: expected integer, got %T", op, lit)
}

func floatArg(op string, e expr.Expr) (float64, error) {
	lit, ok := asLiteral(e)
	if !ok {
		return 0, fmt.Errorf("%s: expected numeric literal", op)
	}
	switch v := lit.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	}
	return 0, fmt.Errorf("%s: expected numeric, got %T", op, lit)
}

func asLiteral(e expr.Expr) (any, bool) {
	n, ok := e.Node().(expr.LitNode)
	if !ok {
		return nil, false
	}
	return n.Value, true
}

func literalValue(e expr.Expr) (any, error) {
	v, ok := asLiteral(e)
	if !ok {
		return nil, fmt.Errorf("expected a literal")
	}
	return v, nil
}

func dtypeByName(name string) (dtype.DType, error) {
	switch strings.ToLower(name) {
	case "i64", "int64":
		return dtype.Int64(), nil
	case "i32", "int32":
		return dtype.Int32(), nil
	case "f64", "float64":
		return dtype.Float64(), nil
	case "f32", "float32":
		return dtype.Float32(), nil
	case "bool":
		return dtype.Bool(), nil
	case "str", "string", "utf8":
		return dtype.String(), nil
	}
	return dtype.DType{}, fmt.Errorf("unknown dtype %q", name)
}
