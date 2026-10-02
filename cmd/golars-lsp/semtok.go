package main

import (
	"encoding/json"
	"slices"

	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// semanticTypes is the token legend; indexes are the tokenType values.
var semanticTypes = []string{
	string(syntax.TokKeyword), string(syntax.TokFunction), string(syntax.TokMethod),
	string(syntax.TokNamespace), string(syntax.TokProperty), string(syntax.TokParameter),
	string(syntax.TokString), string(syntax.TokNumber), string(syntax.TokOperator),
	string(syntax.TokComment), string(syntax.TokType), string(syntax.TokClass),
	string(syntax.TokEnumMember),
}

var semanticModifiers = []string{"declaration"}

func (s *server) semanticTokens(params json.RawMessage) (any, error) {
	d, err := s.doc(params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": encodeTokens(d, d.analysis().File.Tokens())}, nil
}

// encodeTokens produces the relative LSP encoding.
func encodeTokens(d *document, toks []syntax.Token) []int {
	out := make([]int, 0, len(toks)*5)
	prevLine, prevCol := 0, 0
	for _, t := range toks {
		col := d.utf16Col(t.Line, t.Col)
		length := d.utf16Col(t.Line, t.EndCol) - col
		if length <= 0 {
			continue
		}
		dl := t.Line - prevLine
		dc := col
		if dl == 0 {
			dc = col - prevCol
		}
		mods := 0
		if t.Decl {
			mods = 1
		}
		out = append(out, dl, dc, length, slices.Index(semanticTypes, string(t.Class)), mods)
		prevLine, prevCol = t.Line, col
	}
	return out
}
