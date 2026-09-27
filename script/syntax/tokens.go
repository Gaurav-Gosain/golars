package syntax

import (
	"sort"
	"strings"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// TokenClass is the highlighting class of a token. The names match
// LSP semantic token types.
type TokenClass string

// Token classes.
const (
	TokKeyword    TokenClass = "keyword"
	TokFunction   TokenClass = "function"
	TokMethod     TokenClass = "method"
	TokNamespace  TokenClass = "namespace"
	TokProperty   TokenClass = "property"
	TokParameter  TokenClass = "parameter"
	TokString     TokenClass = "string"
	TokNumber     TokenClass = "number"
	TokOperator   TokenClass = "operator"
	TokComment    TokenClass = "comment"
	TokType       TokenClass = "type"
	TokClass      TokenClass = "class"
	TokEnumMember TokenClass = "enumMember"
)

// Token is a highlighted span on one physical line (byte columns).
type Token struct {
	Line, Col, EndCol int
	Class             TokenClass
	// Decl marks a name being defined (a new column or frame).
	Decl bool
}

// Tokens classifies the text of a parsed file for highlighting. The
// result is sorted and free of overlaps.
func (f *File) Tokens() []Token {
	var toks []Token
	for _, c := range f.Comments {
		toks = append(toks, Token{Line: c.Line, Col: c.Col, EndCol: len(f.Lines[c.Line]), Class: TokComment})
	}
	for _, st := range f.Stmts {
		toks = append(toks, st.Tokens()...)
	}
	sort.SliceStable(toks, func(i, j int) bool {
		if toks[i].Line != toks[j].Line {
			return toks[i].Line < toks[j].Line
		}
		return toks[i].Col < toks[j].Col
	})
	out := toks[:0]
	for _, t := range toks {
		if n := len(out); n > 0 && out[n-1].Line == t.Line && t.Col < out[n-1].EndCol {
			continue
		}
		out = append(out, t)
	}
	return out
}

// Tokens classifies one statement.
func (s *Stmt) Tokens() []Token {
	var toks []Token
	span := func(off, end int, class TokenClass, decl bool) {
		l, c := s.Position(off)
		el, ec := s.Position(end)
		if l != el || ec <= c {
			return
		}
		toks = append(toks, Token{Line: l, Col: c, EndCol: ec, Class: class, Decl: decl})
	}
	span(s.Cmd.Off, s.Cmd.End, TokKeyword, false)
	for _, p := range s.Parts {
		partTokens(p, span)
	}
	return toks
}

func partTokens(p Part, span func(off, end int, class TokenClass, decl bool)) {
	switch p.Kind {
	case PartKeyword:
		span(p.Off, p.End, TokKeyword, false)
	case PartOption:
		span(p.Off, p.End, TokEnumMember, false)
	case PartPath, PartValue:
		span(p.Off, p.End, TokString, false)
	case PartFrame, PartTarget:
		span(p.Off, p.End, TokClass, false)
	case PartNewFrame:
		span(p.Off, p.End, TokClass, true)
	case PartColumn:
		span(p.Off, p.End, TokProperty, false)
	case PartNewColumn:
		span(p.Off, p.End, TokProperty, true)
	case PartList, PartAgg:
		for _, it := range p.Items {
			partTokens(it, span)
		}
	case PartCount, PartNumber, PartDuration:
		span(p.Off, p.End, TokNumber, false)
	case PartDType:
		span(p.Off, p.End, TokType, false)
	case PartPunct:
		span(p.Off, p.End, TokOperator, false)
	case PartExpr:
		if p.Expr != nil {
			exprTokens(p.Expr, p.Off, p.Text, span)
		}
	}
}

func exprTokens(root *exprparse.Node, base int, text string, span func(off, end int, class TokenClass, decl bool)) {
	at := func(off, end int, class TokenClass) { span(base+off, base+end, class, false) }
	root.Walk(func(n *exprparse.Node) bool {
		switch n.Kind {
		case exprparse.KindCol:
			at(n.NamePos, n.NameEnd(), TokProperty)
		case exprparse.KindLit:
			start, end := n.Pos, n.End
			for k := 0; k < n.Parens && start < end && text[start] == '('; k++ {
				start++
				end--
			}
			switch n.Lit.(type) {
			case string:
				at(start, end, TokString)
			case bool, nil:
				at(start, end, TokKeyword)
			default:
				at(start, end, TokNumber)
			}
		case exprparse.KindBinary:
			class := TokOperator
			if n.Name == "and" || n.Name == "or" {
				class = TokKeyword
			}
			at(n.NamePos, n.NamePos+len(n.Name), class)
		case exprparse.KindNot:
			at(n.NamePos, n.NamePos+3, TokKeyword)
			if n.Infix == "not in" {
				if in := n.Args[0]; in.NamePos >= 0 {
					at(in.NamePos, in.NamePos+2, TokKeyword)
				}
			}
		case exprparse.KindNeg:
			at(n.NamePos, n.NamePos+1, TokOperator)
		case exprparse.KindWhen:
			whenKeywords(text, n, at)
		case exprparse.KindCall:
			if n.Infix != "" {
				at(n.NamePos, n.NamePos+len(n.Infix), TokKeyword)
				break
			}
			if n.NS != "" {
				nsPos := n.NamePos - len(n.NS) - 1
				if nsPos >= 0 && strings.HasPrefix(text[nsPos:], n.NS+".") {
					at(nsPos, nsPos+len(n.NS), TokNamespace)
				}
			}
			class := TokFunction
			if n.Recv != nil {
				class = TokMethod
			}
			at(n.NamePos, n.NameEnd(), class)
			for _, k := range n.Kw {
				at(k.NamePos, k.NamePos+len(k.Name), TokParameter)
			}
		}
		return true
	})
}

// whenKeywords marks the when/then/otherwise words of a when node.
// The tree keeps only the first `when`, so the others are found in
// the text between the branch expressions.
func whenKeywords(text string, n *exprparse.Node, at func(off, end int, class TokenClass)) {
	mark := func(from, to int, word string) {
		if from < 0 || to > len(text) || from >= to {
			return
		}
		if i := strings.Index(strings.ToLower(text[from:to]), word); i >= 0 {
			at(from+i, from+i+len(word), TokKeyword)
		}
	}
	prev := n.Pos
	for i := 0; i+1 < len(n.Args); i += 2 {
		mark(prev, n.Args[i].Pos, "when")
		mark(n.Args[i].End, n.Args[i+1].Pos, "then")
		prev = n.Args[i+1].End
	}
	if n.Final != nil {
		mark(prev, n.Final.Pos, "otherwise")
	}
}
