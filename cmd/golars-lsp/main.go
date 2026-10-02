// Command golars-lsp is a minimal Language Server for golars .glr
// scripts. It speaks JSON-RPC 2.0 over stdio with Content-Length
// framing and supports:
//
//   - initialize, shutdown, exit
//   - textDocument/didOpen, didChange, didClose
//   - textDocument/completion  (commands, keywords, frame names, paths)
//   - textDocument/hover       (command summary + signature + long doc)
//   - textDocument/publishDiagnostics (unknown commands, missing args)
//
// Drop into any editor that speaks LSP. See editors/nvim-golars for
// the Neovim plugin; the full install guide lives in
// editors/tree-sitter-golars/README.md + editors/nvim-golars/README.md.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	logOut := io.Writer(os.Stderr)
	if path := os.Getenv("GOLARS_LSP_LOG"); path != "" {
		if f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			logOut = io.MultiWriter(os.Stderr, f)
		}
	}
	srv := newServer(os.Stdin, os.Stdout, logOut)
	if err := srv.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "golars-lsp:", err)
		os.Exit(1)
	}
}
