package repl

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
)

// ErrCanceled is returned by ReadLine when the user pressed Ctrl+C.
// Typical consumers continue the input loop instead of exiting.
var ErrCanceled = errors.New("repl: canceled")

// ErrEOF is returned on Ctrl+D with an empty buffer or when the non-TTY
// input is exhausted. Typical consumers exit the loop.
var ErrEOF = errors.New("repl: eof")

// DefaultMaxHistory is the default cap on in-memory and persisted history.
const DefaultMaxHistory = 1000

// Options configures a Prompt.
type Options struct {
	// Prompt is the static prompt string. Ignored if PromptFunc is set.
	Prompt string

	// PromptFunc, if set, is called every ReadLine to produce a dynamic
	// prompt (e.g. with status info). Takes precedence over Prompt.
	PromptFunc func() string

	// Suggester computes inline ghost-text completions. nil disables
	// suggestions entirely.
	Suggester Suggester

	// Highlighter, if set, colours the input as it is typed.
	Highlighter Highlighter

	// GhostStyle styles the inline completion text. Defaults to a dim
	// gray.
	GhostStyle lipgloss.Style

	// HintStyle styles the right-side hint annotation. Defaults to a
	// dim gray italic.
	HintStyle lipgloss.Style

	// HistoryPath, if non-empty, is where history is loaded on New and
	// saved on Close. Missing files are treated as empty history.
	HistoryPath string

	// MaxHistory caps in-memory and persisted history. Zero means
	// DefaultMaxHistory.
	MaxHistory int

	// Input overrides the input reader. If nil, os.Stdin is used and
	// TTY detection is performed. If set, the non-TTY fallback
	// (bufio.Scanner) is always used: convenient for tests and
	// non-interactive pipelines.
	Input io.Reader

	// Output overrides the output writer. If nil, os.Stdout is used.
	Output io.Writer
}

// Prompt is a reusable inline-input widget. Safe for sequential use
// from a single goroutine; not safe for concurrent calls to ReadLine.
type Prompt struct {
	opts    Options
	history []string
	isTTY   bool
	scanner *bufio.Scanner
}

// New constructs a Prompt. The history file, if any, is loaded
// immediately; further loads happen only via explicit ReloadHistory.
func New(opts Options) *Prompt {
	if opts.MaxHistory <= 0 {
		opts.MaxHistory = DefaultMaxHistory
	}
	if _, unset := opts.GhostStyle.GetForeground().(lipgloss.NoColor); unset {
		opts.GhostStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#4A5568"))
	}
	if _, unset := opts.HintStyle.GetForeground().(lipgloss.NoColor); unset {
		opts.HintStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#6B7280")).Italic(true)
	}
	p := &Prompt{opts: opts}

	if opts.Input == nil {
		p.isTTY = term.IsTerminal(os.Stdin.Fd())
	}
	if !p.isTTY {
		in := opts.Input
		if in == nil {
			in = os.Stdin
		}
		p.scanner = bufio.NewScanner(in)
		p.scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	}
	if opts.HistoryPath != "" {
		p.history = loadHistoryFile(opts.HistoryPath, opts.MaxHistory)
	}
	return p
}

// History returns a defensive copy of the in-memory history.
func (p *Prompt) History() []string {
	out := make([]string, len(p.history))
	copy(out, p.history)
	return out
}

// AddHistory appends a line to history, deduplicating against the
// immediately-previous entry and enforcing MaxHistory. Empty lines are
// ignored. Callers who want to log a command manually can call this
// directly; ReadLine calls it for every non-empty line it returns.
func (p *Prompt) AddHistory(line string) {
	line = strings.TrimRight(line, "\r\n")
	if strings.TrimSpace(line) == "" {
		return
	}
	if n := len(p.history); n > 0 && p.history[n-1] == line {
		return
	}
	p.history = append(p.history, line)
	if len(p.history) > p.opts.MaxHistory {
		p.history = p.history[len(p.history)-p.opts.MaxHistory:]
	}
}

// Save writes history to disk. No-op if HistoryPath is empty.
// Safe to call repeatedly; writes are atomic on platforms where
// os.WriteFile is atomic.
func (p *Prompt) Save() error {
	if p.opts.HistoryPath == "" || len(p.history) == 0 {
		return nil
	}
	return os.WriteFile(p.opts.HistoryPath,
		[]byte(strings.Join(p.history, "\n")+"\n"), 0o600)
}

// Close persists history. Equivalent to Save. Safe to call more than
// once. Returned error is from the final write attempt.
func (p *Prompt) Close() error { return p.Save() }

func (p *Prompt) promptString() string {
	if p.opts.PromptFunc != nil {
		return p.opts.PromptFunc()
	}
	return p.opts.Prompt
}

// ReadLine reads one line of input. Returns ErrCanceled on Ctrl+C and
// ErrEOF on Ctrl+D (with an empty buffer) or when non-TTY input is
// exhausted. Non-empty returned lines are automatically appended to
// history; empty lines are ignored.
func (p *Prompt) ReadLine() (string, error) {
	var (
		line string
		err  error
	)
	if !p.isTTY {
		line, err = p.readLinePlain()
	} else {
		line, err = p.readLineTTY()
	}
	if err != nil {
		return "", err
	}
	p.AddHistory(line)
	return line, nil
}

func (p *Prompt) readLinePlain() (string, error) {
	if !p.scanner.Scan() {
		if e := p.scanner.Err(); e != nil {
			return "", e
		}
		return "", ErrEOF
	}
	return p.scanner.Text(), nil
}

func (p *Prompt) readLineTTY() (string, error) {
	out := p.opts.Output
	if out == nil {
		out = os.Stdout
	}
	in := p.opts.Input
	if in == nil {
		in = os.Stdin
	}

	// Ctrl+L re-enters this loop with the buffer preserved, after
	// emitting a real clear-screen + clear-scrollback ANSI sequence.
	// Bubbletea v2's tea.ClearScreen only homes the cursor; it
	// doesn't repaint the terminal below the prompt, which visually
	// looks like nothing happened.
	preserved := ""
	for {
		ti := textinput.New()
		ti.Prompt = p.promptString()
		ti.Focus()
		styles := ti.Styles()
		styles.Focused.Prompt = lipgloss.NewStyle()
		styles.Focused.Text = lipgloss.NewStyle()
		styles.Focused.Suggestion = p.opts.GhostStyle
		ti.SetStyles(styles)
		ti.CharLimit = 0
		ti.SetWidth(0)
		ti.ShowSuggestions = true
		if preserved != "" {
			ti.SetValue(preserved)
			ti.SetCursor(len(preserved))
		}

		m := &model{
			ti:          ti,
			highlighter: p.opts.Highlighter,
			ghostStyle:  p.opts.GhostStyle,
			prompt:      ti.Prompt,
			suggester:   p.opts.Suggester,
			history:     p.history,
			historyIdx:  len(p.history),
			hintStyle:   p.opts.HintStyle,
		}
		m.refreshGhost()

		prog := tea.NewProgram(m, tea.WithInput(in), tea.WithOutput(out))
		final, err := prog.Run()
		if err != nil {
			return "", err
		}
		fin := final.(*model)
		switch {
		case fin.canceled:
			return "", ErrCanceled
		case fin.eof:
			return "", ErrEOF
		case fin.clearReq:
			// ESC [2J clears the viewport, ESC [3J clears scrollback
			// on xterm-compatible emulators, ESC [H homes the cursor.
			_, _ = io.WriteString(out, "\x1b[2J\x1b[3J\x1b[H")
			preserved = fin.ti.Value()
			continue
		}
		// Bubbletea clears the input line when the program exits, so
		// echo the accepted line to keep a readable transcript.
		value := fin.ti.Value()
		echo := value
		if p.opts.Highlighter != nil {
			echo = renderPlain(value, p.opts.Highlighter(value))
		}
		_, _ = io.WriteString(out, fin.prompt+echo+"\n")
		return value, nil
	}
}

// loadHistoryFile reads path and returns up to max most-recent lines.
// Missing files, read errors, and empty files all return nil: history
// is a best-effort UX feature, not a contract.
func loadHistoryFile(path string, max int) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if line != "" {
			out = append(out, line)
		}
	}
	if len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}
