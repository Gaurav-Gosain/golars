package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Gaurav-Gosain/golars/script"
)

// runPreview executes a .glr script silently and prints only the
// focused frame's head at the end. Designed for editor integrations
// (nvim-golars renders the output as virtual text below `# ^?`
// probes). Emits no banner, no per-statement trace, no success
// messages: stdout contains just the table.
//
// When format == "markdown" a GitHub-flavored markdown table is
// produced instead of the box-drawing form, so hosts that render
// markdown (Zed / VS Code hover popups, Fumadocs) get a native
// table layout rather than a literal ASCII rectangle.
//
// Errors during script execution are written to stderr; the preview
// output on stdout is whatever partial state existed before the
// error, which is usually what the user wants when they're still
// typing the script.
func runPreview(s *state, path string, n int, format string) int {
	if n <= 0 {
		n = 10
	}

	// Redirect stdout to /dev/null while the script runs so internal
	// "loaded foo.csv" messages don't pollute the preview output.
	// We restore stdout before rendering the final frame so the
	// preview reaches the caller verbatim.
	realStdout := os.Stdout
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		// If /dev/null is unavailable (rare), fall back to a tee to
		// an anonymous pipe and discard. This keeps the function
		// working on constrained environments.
		devNull = io.Discard.(*os.File)
	}
	os.Stdout = devNull

	runner := script.Runner{
		Exec: script.ExecutorFunc(s.handle),
		// No Trace: silence is the point of preview mode.
	}
	runErr := runner.RunFile(path)

	os.Stdout = realStdout
	if devNull != nil && devNull != (*os.File)(nil) {
		_ = devNull.Close()
	}

	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}

	// Collect the final focused pipeline without going through the
	// dispatcher (which would print success chrome).
	if s.lf == nil && s.df == nil {
		fmt.Fprintln(os.Stderr, "golars: no frame in focus")
		return 1
	}
	lf := s.currentLazy().Head(n)
	df, err := lf.Collect(s.ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer df.Release()

	if format == "markdown" {
		if err := writeMarkdown(os.Stdout, df); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}

	// Frame.String() uses the rounded-corner format the REPL prints
	// via `.show`. Writing it verbatim keeps preview output visually
	// identical to what the user gets at the prompt.
	fmt.Println(df)
	return 0
}
