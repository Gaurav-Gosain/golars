package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/Gaurav-Gosain/golars/script"
)

// withStdoutSilenced runs fn with os.Stdout pointed at the null
// device, so the dispatcher's success lines do not pollute
// machine-readable output. Stdout is restored before returning.
func withStdoutSilenced(fn func() error) error {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return fn()
	}
	real := os.Stdout
	os.Stdout = devNull
	defer func() {
		os.Stdout = real
		_ = devNull.Close()
	}()
	return fn()
}

// runPreview executes a .glr script silently and prints only the head
// of the focused frame. Editor integrations use it (nvim-golars
// renders the output as virtual text below `# ^?` probes), so stdout
// holds just the table: no banner, no trace, no success lines.
//
// format "markdown" prints a GitHub-flavoured markdown table for hosts
// that render markdown (Zed and VS Code hovers); anything else prints
// the box-drawn table the REPL uses.
func runPreview(s *state, path string, n int, format string) error {
	if n <= 0 {
		n = 10
	}
	err := withStdoutSilenced(func() error {
		runner := script.Runner{Exec: script.ExecutorFunc(s.handle)}
		return runner.RunFile(path)
	})
	if err != nil && !errors.Is(err, errExit) {
		return err
	}
	if !s.hasFocus() {
		return errNoFrame
	}
	df, err := s.currentLazy().Head(n).Collect(s.ctx)
	if err != nil {
		return err
	}
	defer df.Release()
	if format == "markdown" {
		return writeMarkdown(os.Stdout, df)
	}
	fmt.Println(df)
	return nil
}
