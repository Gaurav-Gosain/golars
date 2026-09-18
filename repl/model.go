package repl

import (
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// model is the tea.Model that drives a single ReadLine. It wraps
// textinput with ghost-text completions and history navigation.
type model struct {
	ti         textinput.Model
	suggester  Suggester
	history    []string
	historyIdx int // one past last when not browsing
	ghost      string
	hint       string
	hintStyle  lipgloss.Style
	canceled   bool
	eof        bool
	// clearReq is true when the user pressed Ctrl+L. The caller is
	// expected to emit a real clear-screen ANSI sequence and restart
	// the prompt with the input buffer intact; bubbletea v2's
	// tea.ClearScreen only homes the cursor in inline mode, which
	// looks like nothing happened on a busy scrollback.
	clearReq bool
}

func (m *model) Init() tea.Cmd { return textinput.Blink }

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if km, ok := msg.(tea.KeyPressMsg); ok {
		if handled, next, cmd := m.handleKey(km); handled {
			return next, cmd
		}
	}
	var cmd tea.Cmd
	m.ti, cmd = m.ti.Update(msg)
	m.refreshGhost()
	return m, cmd
}

// handleKey returns (true, model, cmd) if the key was consumed and the
// textinput-default update should be skipped. Extracted so tests can
// exercise the key-routing logic without running a tea.Program.
//
// Shortcuts: enter submits, ctrl+c cancels, ctrl+d on empty line sends
// EOF, right-arrow or tab accept the ghost, up/down browse history,
// ctrl+l clears the screen, ctrl+u kills to line start.
func (m *model) handleKey(msg tea.KeyPressMsg) (bool, tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		return true, m, tea.Quit
	case "ctrl+c":
		m.canceled = true
		return true, m, tea.Quit
	case "ctrl+d":
		if m.ti.Value() == "" {
			m.eof = true
			return true, m, tea.Quit
		}
	case "ctrl+l":
		// Quit this Program; readLineTTY re-starts a fresh prompt with
		// the input buffer preserved after emitting a real clear-screen
		// escape to the terminal.
		m.clearReq = true
		return true, m, tea.Quit
	case "ctrl+u":
		// Kill to line start.
		if m.ti.Value() != "" {
			tail := string([]rune(m.ti.Value())[m.ti.Position():])
			m.ti.SetValue(tail)
			m.ti.SetCursor(0)
			m.refreshGhost()
		}
		return true, m, nil
	case "right":
		if m.ghost != "" && m.ti.Position() == len([]rune(m.ti.Value())) {
			m.ti.SetValue(m.ti.Value() + m.ghost)
			m.ti.SetCursor(len(m.ti.Value()))
			m.refreshGhost()
			return true, m, nil
		}
	case "up":
		if m.historyIdx > 0 {
			m.historyIdx--
			m.ti.SetValue(m.history[m.historyIdx])
			m.ti.SetCursor(len(m.ti.Value()))
			m.refreshGhost()
		}
		return true, m, nil
	case "down":
		if m.historyIdx < len(m.history) {
			m.historyIdx++
			if m.historyIdx == len(m.history) {
				m.ti.SetValue("")
			} else {
				m.ti.SetValue(m.history[m.historyIdx])
			}
			m.ti.SetCursor(len(m.ti.Value()))
			m.refreshGhost()
		}
		return true, m, nil
	}
	return false, m, nil
}

func (m *model) View() tea.View {
	base := m.ti.View()
	if m.hint != "" {
		base += "  " + m.hintStyle.Render(m.hint)
	}
	return tea.NewView(base)
}

// refreshGhost consults the suggester and updates textinput's
// suggestion list so the built-in View renders the block cursor
// overlaying the first ghost character (in CompletionStyle).
func (m *model) refreshGhost() {
	if m.suggester == nil {
		m.ghost, m.hint = "", ""
		m.ti.SetSuggestions(nil)
		return
	}
	g, h := m.suggester.Suggest(m.ti.Value())
	m.ghost, m.hint = g, h
	if g != "" {
		m.ti.SetSuggestions([]string{m.ti.Value() + g})
	} else {
		m.ti.SetSuggestions(nil)
	}
}
