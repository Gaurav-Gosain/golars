package exprparse

import (
	"strings"
)

// FormatSource parses s and prints it in canonical form: one space
// around binary operators, `, ` between arguments, lower-case
// keywords, parentheses and literal spellings as written.
func FormatSource(s string) (string, error) {
	n, err := ParseTree(s)
	if err != nil {
		return "", err
	}
	return Format(n), nil
}

// Format prints a syntax tree in canonical form. Parsing the result
// gives the same tree.
func Format(n *Node) string {
	var b strings.Builder
	writeNode(&b, n)
	return b.String()
}

func writeNode(b *strings.Builder, n *Node) {
	for range n.Parens {
		b.WriteByte('(')
	}
	writeBare(b, n)
	for range n.Parens {
		b.WriteByte(')')
	}
}

func writeBare(b *strings.Builder, n *Node) {
	switch n.Kind {
	case KindLit:
		if n.Raw != "" {
			b.WriteString(n.Raw)
		} else {
			b.WriteString(glrLiteral(n.Lit))
		}
	case KindCol:
		b.WriteString(n.Name)
	case KindList:
		b.WriteByte('[')
		for i, a := range n.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			writeNode(b, a)
		}
		b.WriteByte(']')
	case KindBinary:
		writeNode(b, n.Args[0])
		b.WriteByte(' ')
		b.WriteString(n.Name)
		b.WriteByte(' ')
		writeNode(b, n.Args[1])
	case KindNot:
		if n.Infix == "not in" {
			in := n.Args[0]
			writeNode(b, in.Recv)
			b.WriteString(" not in ")
			writeNode(b, in.Args[0])
			return
		}
		b.WriteString("not ")
		writeNode(b, n.Args[0])
	case KindNeg:
		b.WriteByte('-')
		writeNode(b, n.Args[0])
	case KindWhen:
		for i := 0; i+1 < len(n.Args); i += 2 {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString("when ")
			writeNode(b, n.Args[i])
			b.WriteString(" then ")
			writeNode(b, n.Args[i+1])
		}
		if n.Final != nil {
			b.WriteString(" otherwise ")
			writeNode(b, n.Final)
		}
	case KindCall:
		writeCall(b, n)
	}
}

func writeCall(b *strings.Builder, n *Node) {
	switch n.Infix {
	case "":
	case "is_null", "is_not_null":
		writeNode(b, n.Recv)
		b.WriteString(" " + n.Infix)
		return
	default:
		writeNode(b, n.Recv)
		b.WriteString(" " + n.Infix + " ")
		writeNode(b, n.Args[0])
		return
	}
	if n.Recv != nil {
		writeNode(b, n.Recv)
		b.WriteByte('.')
	}
	if n.NS != "" {
		b.WriteString(n.NS)
		b.WriteByte('.')
	}
	b.WriteString(n.Name)
	if !n.CallParens {
		return
	}
	b.WriteByte('(')
	for i, a := range n.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		writeNode(b, a)
	}
	for i, k := range n.Kw {
		if i > 0 || len(n.Args) > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k.Name)
		b.WriteByte('=')
		writeNode(b, k.Val)
	}
	b.WriteByte(')')
}
