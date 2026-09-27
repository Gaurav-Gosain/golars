package main

import (
	"os"
	"slices"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

var (
	primaryColor = lipgloss.Color("#8B5CF6")
	accentColor  = lipgloss.Color("#10B981")
	errorColor   = lipgloss.Color("#EF4444")
	warningColor = lipgloss.Color("#F59E0B")
	infoColor    = lipgloss.Color("#3B82F6")
	dimColor     = lipgloss.Color("#6B7280")
	headerColor  = lipgloss.Color("#F472B6")

	logoStyle    = lipgloss.NewStyle().Foreground(primaryColor).Bold(true)
	promptStyle  = lipgloss.NewStyle().Foreground(primaryColor).Bold(true)
	errStyle     = lipgloss.NewStyle().Foreground(errorColor).Bold(true)
	errMsgStyle  = lipgloss.NewStyle().Foreground(errorColor)
	warnStyle    = lipgloss.NewStyle().Foreground(warningColor)
	successStyle = lipgloss.NewStyle().Foreground(accentColor)
	infoStyle    = lipgloss.NewStyle().Foreground(infoColor)
	dimStyle     = lipgloss.NewStyle().Foreground(dimColor)
	cmdStyle     = lipgloss.NewStyle().Foreground(warningColor)
	titleStyle   = lipgloss.NewStyle().Foreground(primaryColor).Bold(true).Underline(true)
	headerStyle  = lipgloss.NewStyle().Foreground(headerColor).Bold(true)
)

// colorsEnabled reports whether golars should emit ANSI color
// escapes. The rules, in priority order:
//
//  1. `--no-color` anywhere in argv.
//  2. `NO_COLOR` env var set (https://no-color.org convention).
//  3. `FORCE_COLOR` or `CLICOLOR_FORCE` set to a non-zero value.
//  4. Stdout is a terminal.
func colorsEnabled(args []string) bool {
	if slices.Contains(args, "--no-color") {
		return false
	}
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		return false
	}
	// The Jupyter kernel sets one of these in the kernel-host
	// subprocess so styled output lands as ANSI in the cell output,
	// which JupyterLab renders inline.
	for _, key := range []string{"FORCE_COLOR", "CLICOLOR_FORCE"} {
		if v, ok := os.LookupEnv(key); ok && v != "" && v != "0" {
			return true
		}
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// applyColorPolicy decides whether Render calls emit ANSI escapes.
// When color is off every style is replaced with a plain one so
// Render returns its input unchanged.
//
// When color is on the writer profile is pinned to TrueColor:
// lipgloss would otherwise detect a non-TTY stdout and drop colour,
// which defeats FORCE_COLOR for piped output (Jupyter, log capture).
func applyColorPolicy(enable bool) {
	if enable {
		lipgloss.Writer.Profile = colorprofile.TrueColor
		return
	}
	lipgloss.Writer.Profile = colorprofile.Ascii
	plain := lipgloss.NewStyle()
	for _, st := range []*lipgloss.Style{
		&logoStyle, &promptStyle, &errStyle, &errMsgStyle, &warnStyle,
		&successStyle, &infoStyle, &dimStyle, &cmdStyle, &titleStyle,
		&headerStyle, &connectorStyle,
	} {
		*st = plain
	}
	for i := range graphStyles {
		graphStyles[i].style = plain
	}
}
