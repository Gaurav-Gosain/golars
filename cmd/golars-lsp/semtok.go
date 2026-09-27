package main

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// semanticTypes is the token legend; indexes are the tokenType values.
var semanticTypes = []string{
	"keyword", "function", "method", "namespace", "property", "parameter",
	"string", "number", "operator", "comment", "type", "class", "enumMember",
}

var semanticModifiers = []string{"declaration", "defaultLibrary"}

const (
	tokKeyword = iota
	tokFunction
	tokMethod
	tokNamespace
	tokProperty
	tokParameter
	tokString
	tokNumber
	tokOperator
	tokComment
	tokType
	tokClass
	tokEnumMember
)

const modDeclaration = 1

type semToken struct {
	line, col, end int // byte columns
	typ, mods      int
}

func (s *server) semanticTokens(params json.RawMessage) (any, error) {
	d, err := s.doc(params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": encodeTokens(d, semanticTokensOf(d))}, nil
}

func semanticTokensOf(d *document) []semToken {
	r := d.analysis()
	var toks []semToken
	for _, c := range r.File.Comments {
		toks = append(toks, semToken{line: c.Line, col: c.Col, end: len(d.line(c.Line)), typ: tokComment})
	}
	for _, st := range r.File.Stmts {
		span := func(off, end, typ, mods int) {
			l, c := st.Position(off)
			el, ec := st.Position(end)
			if l != el || ec <= c {
				return
			}
			toks = append(toks, semToken{line: l, col: c, end: ec, typ: typ, mods: mods})
		}
		span(st.Cmd.Off, st.Cmd.End, tokKeyword, 0)
		for _, p := range st.Parts {
			partTokens(p, span)
		}
	}
	sort.Slice(toks, func(i, j int) bool {
		if toks[i].line != toks[j].line {
			return toks[i].line < toks[j].line
		}
		return toks[i].col < toks[j].col
	})
	// Drop overlaps (a later token starting inside an earlier one).
	out := toks[:0]
	for _, t := range toks {
		if n := len(out); n > 0 && out[n-1].line == t.line && t.col < out[n-1].end {
			continue
		}
		out = append(out, t)
	}
	return out
}

func partTokens(p syntax.Part, span func(off, end, typ, mods int)) {
	switch p.Kind {
	case syntax.PartKeyword:
		span(p.Off, p.End, tokKeyword, 0)
	case syntax.PartOption:
		span(p.Off, p.End, tokEnumMember, 0)
	case syntax.PartPath:
		span(p.Off, p.End, tokString, 0)
	case syntax.PartFrame, syntax.PartTarget:
		span(p.Off, p.End, tokClass, 0)
	case syntax.PartNewFrame:
		span(p.Off, p.End, tokClass, modDeclaration)
	case syntax.PartColumn:
		span(p.Off, p.End, tokProperty, 0)
	case syntax.PartNewColumn:
		span(p.Off, p.End, tokProperty, modDeclaration)
	case syntax.PartList, syntax.PartAgg:
		for _, it := range p.Items {
			partTokens(it, span)
		}
	case syntax.PartCount, syntax.PartNumber, syntax.PartDuration:
		span(p.Off, p.End, tokNumber, 0)
	case syntax.PartValue:
		span(p.Off, p.End, tokString, 0)
	case syntax.PartDType:
		span(p.Off, p.End, tokType, 0)
	case syntax.PartPunct:
		span(p.Off, p.End, tokOperator, 0)
	case syntax.PartExpr:
		if p.Expr != nil {
			exprTokens(p.Expr, p.Off, p.Text, span)
		}
	}
}

func exprTokens(root *exprparse.Node, base int, text string, span func(off, end, typ, mods int)) {
	root.Walk(func(n *exprparse.Node) bool {
		switch n.Kind {
		case exprparse.KindCol:
			span(base+n.NamePos, base+n.NameEnd(), tokProperty, 0)
		case exprparse.KindLit:
			start, end := n.Pos, n.End
			// Parenthesised literals: color the literal only.
			for n.Parens > 0 && start < end && text[start] == '(' {
				start++
				end--
			}
			switch n.Lit.(type) {
			case string:
				span(base+start, base+end, tokString, 0)
			case bool, nil:
				span(base+start, base+end, tokKeyword, 0)
			default:
				span(base+start, base+end, tokNumber, 0)
			}
		case exprparse.KindBinary:
			typ := tokOperator
			if n.Name == "and" || n.Name == "or" {
				typ = tokKeyword
			}
			span(base+n.NamePos, base+n.NamePos+len(n.Name), typ, 0)
		case exprparse.KindNot:
			span(base+n.NamePos, base+n.NamePos+3, tokKeyword, 0)
		case exprparse.KindNeg:
			span(base+n.NamePos, base+n.NamePos+1, tokOperator, 0)
		case exprparse.KindWhen:
			keywordsIn(text, n, base, span)
		case exprparse.KindCall:
			if n.Infix != "" {
				span(base+n.NamePos, base+n.NamePos+len(n.Infix), tokKeyword, 0)
				break
			}
			if n.NS != "" {
				nsPos := n.NamePos - len(n.NS) - 1
				if nsPos >= 0 && strings.HasPrefix(text[nsPos:], n.NS+".") {
					span(base+nsPos, base+nsPos+len(n.NS), tokNamespace, 0)
				}
			}
			typ := tokFunction
			if n.Recv != nil {
				typ = tokMethod
			}
			span(base+n.NamePos, base+n.NameEnd(), typ, 0)
			for _, k := range n.Kw {
				span(base+k.NamePos, base+k.NamePos+len(k.Name), tokParameter, 0)
			}
		}
		return true
	})
}

// keywordsIn marks the when/then/otherwise words of a when node. The
// tree keeps only the first `when`, so the others are found between
// the branch expressions.
func keywordsIn(text string, n *exprparse.Node, base int, span func(off, end, typ, mods int)) {
	mark := func(from, to int, word string) {
		if from < 0 || to > len(text) || from >= to {
			return
		}
		if i := strings.Index(strings.ToLower(text[from:to]), word); i >= 0 {
			span(base+from+i, base+from+i+len(word), tokKeyword, 0)
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

// encodeTokens produces the relative LSP encoding.
func encodeTokens(d *document, toks []semToken) []int {
	out := make([]int, 0, len(toks)*5)
	prevLine, prevCol := 0, 0
	for _, t := range toks {
		col := d.utf16Col(t.line, t.col)
		length := d.utf16Col(t.line, t.end) - col
		if length <= 0 {
			continue
		}
		dl := t.line - prevLine
		dc := col
		if dl == 0 {
			dc = col - prevCol
		}
		out = append(out, dl, dc, length, t.typ, t.mods)
		prevLine, prevCol = t.line, col
	}
	return out
}
