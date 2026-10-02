// Command golars is the golars CLI: an interactive REPL for exploring
// DataFrames, a runner for .glr scripts, and one-shot subcommands
// (schema, sql, head, convert, ...) for working with data files from
// the shell.
//
// The REPL and scripts share one dispatcher (dispatch.go). Command
// names, signatures and help text come from script.Commands, so the
// REPL, the LSP, the Jupyter kernel and the docs stay in step.
//
// Files are grouped by purpose:
//
//	cli.go, subcmd_*.go   cobra command tree and one-shot subcommands
//	repl.go               interactive loop, prompt and completion
//	dispatch.go           statement dispatcher and argument helpers
//	state.go              session state (focused frame, named frames)
//	cmds_*.go             script command handlers, one file per category
//	table.go, output.go   terminal table rendering and output formats
//	styles.go             colour styles and the NO_COLOR policy
package main

import "os"

const version = "0.1.0"

func main() { os.Exit(execCLI()) }
