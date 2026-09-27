package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/script"
)

// errExit is returned by the exit command. The REPL quits on it, a
// script stops cleanly, and the kernel host ignores it.
var errExit = errors.New("exit")

// traceLine prints a script statement before it runs so a script run
// reads like a REPL transcript.
func traceLine(line string) {
	fmt.Println(promptStyle.Render("golars") + dimStyle.Render(" > ") + line)
}

// runScript runs a .glr file through the dispatcher. An exit
// statement stops the script without an error.
func runScript(s *state, path string) error {
	runner := script.Runner{Exec: script.ExecutorFunc(s.handle), Trace: traceLine}
	if err := runner.RunFile(path); err != nil && !errors.Is(err, errExit) {
		return err
	}
	return nil
}

// cmdSource runs another script against the current session.
func cmdSource(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	runner := script.Runner{Exec: script.ExecutorFunc(s.handle), Trace: traceLine}
	if err := runner.RunFile(c.args[0]); err != nil {
		return err
	}
	ok("script complete: %s", c.args[0])
	return nil
}

// cmdHelp prints every command from script.Commands grouped by
// category, so the table cannot drift from the dispatcher.
func cmdHelp(_ *state, _ *call) error {
	const sigWidth = 34
	fmt.Println()
	for _, cat := range script.Categories {
		fmt.Println(titleStyle.Render(cat))
		for _, c := range script.Commands {
			if c.Category != cat {
				continue
			}
			sig, summary := c.Signature, c.Summary
			if len(c.Aliases) > 0 {
				summary += " (alias: " + strings.Join(c.Aliases, ", ") + ")"
			}
			if len(sig) > sigWidth {
				fmt.Printf("  %s\n  %s  %s\n", cmdStyle.Render(sig), padRight("", sigWidth), dimStyle.Render(summary))
				continue
			}
			fmt.Printf("  %s  %s\n", cmdStyle.Render(padRight(sig, sigWidth)), dimStyle.Render(summary))
		}
		fmt.Println()
	}
	fmt.Println(titleStyle.Render("predicates (filter)"))
	fmt.Println("  " + dimStyle.Render("col op value [and|or col op value]...  no parentheses, left to right"))
	fmt.Println("  " + dimStyle.Render("ops: == != < <= > >= is_null is_not_null contains starts_with ends_with like not_like"))
	fmt.Println("  " + dimStyle.Render(`values: integers, floats, "double-quoted strings", true, false`))
	fmt.Println()
	fmt.Println(dimStyle.Render("  The leading dot is optional: `load x.csv` and `.load x.csv` are the same."))
	fmt.Println()
	return nil
}

func cmdExit(_ *state, _ *call) error { return errExit }

func cmdTiming(s *state, _ *call) error {
	s.showTiming = !s.showTiming
	if s.showTiming {
		ok("timing on")
	} else {
		ok("timing off")
	}
	return nil
}

func cmdClear(_ *state, _ *call) error {
	fmt.Print("\033[H\033[2J")
	return nil
}

func cmdInfo(s *state, _ *call) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	rows := [][2]string{
		{"version", version},
		{"go", runtime.Version()},
		{"arch", runtime.GOOS + "/" + runtime.GOARCH},
		{"heap", fmt.Sprintf("%.2f MB", float64(ms.HeapAlloc)/(1<<20))},
		{"gc runs", strconv.FormatUint(uint64(ms.NumGC), 10)},
		{"uptime", time.Since(s.startTime).Round(time.Second).String()},
		{"commands", strconv.Itoa(s.evalCount)},
		{"frames", strconv.Itoa(len(s.frames))},
	}
	if s.hasFocus() {
		rows = append(rows, [2]string{"source", s.path})
		if s.lf == nil {
			rows = append(rows, [2]string{"shape", shape(s.df)})
		}
	}
	fmt.Println()
	fmt.Println(titleStyle.Render("runtime"))
	fmt.Println()
	for _, r := range rows {
		fmt.Printf("  %s  %s\n", dimStyle.Render(padRight(r[0], 12)), r[1])
	}
	fmt.Println()
	return nil
}

func cmdPwd(_ *state, _ *call) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	fmt.Println(wd)
	return nil
}

func cmdLs(_ *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	dir := "."
	if len(c.args) == 1 {
		dir = c.args[0]
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			name = cmdStyle.Render(name + "/")
		}
		fmt.Println(name)
	}
	return nil
}

// cmdCd changes the process working directory; with no argument it
// goes to the home directory.
func cmdCd(_ *state, c *call) error {
	if len(c.args) > 1 {
		return c.usage()
	}
	dir := ""
	if len(c.args) == 1 {
		dir = c.args[0]
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		dir = home
	}
	return os.Chdir(dir)
}
