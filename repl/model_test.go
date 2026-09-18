package repl

import (
	"testing"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// These tests exercise the tea.Model internals directly so they don't
// need a real TTY. They live in the package (not _test) to reach
// unexported fields.

func newTestModel(sugg Suggester, history []string) *model {
	ti := textinput.New()
	ti.Focus()
	ti.ShowSuggestions = true
	return &model{
		ti:         ti,
		suggester:  sugg,
		history:    history,
		historyIdx: len(history),
		hintStyle:  lipgloss.NewStyle(),
	}
}

func TestModelEnterQuits(t *testing.T) {
	m := newTestModel(nil, nil)
	m.ti.SetValue("hi")
	handled, _, cmd := m.handleKey(tea.KeyPressMsg{Code: tea.KeyEnter})
	if !handled || cmd == nil {
		t.Fatal("Enter should produce a tea.Quit")
	}
	if m.canceled || m.eof {
		t.Fatal("Enter must not flag canceled/eof")
	}
}

func TestModelCtrlCCancels(t *testing.T) {
	m := newTestModel(nil, nil)
	handled, _, cmd := m.handleKey(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
	if !handled || cmd == nil || !m.canceled {
		t.Fatalf("Ctrl+C did not cancel: handled=%v cmd=%v canceled=%v", handled, cmd, m.canceled)
	}
}

func TestModelCtrlDOnEmptyIsEOF(t *testing.T) {
	m := newTestModel(nil, nil)
	handled, _, cmd := m.handleKey(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
	if !handled || cmd == nil || !m.eof {
		t.Fatal("Ctrl+D on empty should quit with eof flag")
	}
}

func TestModelCtrlDOnNonEmptyPassesThrough(t *testing.T) {
	m := newTestModel(nil, nil)
	m.ti.SetValue("x")
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
	if handled {
		t.Fatal("Ctrl+D on non-empty buffer must pass through to textinput (delete-forward)")
	}
	if m.eof {
		t.Fatal("Ctrl+D must not flag EOF when buffer is non-empty")
	}
}

// Ctrl+L returns tea.ClearScreen; bubbletea treats that as a repaint
// directive before the next update. Input buffer is preserved.
func TestModelCtrlLIssuesClearScreen(t *testing.T) {
	m := newTestModel(nil, nil)
	m.ti.SetValue("load foo")
	handled, _, cmd := m.handleKey(tea.KeyPressMsg{Code: 'l', Mod: tea.ModCtrl})
	if !handled || cmd == nil {
		t.Fatalf("Ctrl+L did not return a clear cmd: handled=%v cmd=%v", handled, cmd)
	}
	// Buffer must survive the clear.
	if m.ti.Value() != "load foo" {
		t.Fatalf("Ctrl+L wiped input buffer: %q", m.ti.Value())
	}
}

// Ctrl+U kills to the line start, preserving the tail from the cursor.
func TestModelCtrlUKillsToStart(t *testing.T) {
	m := newTestModel(nil, nil)
	m.ti.SetValue("hello world")
	m.ti.SetCursor(6) // caret at the 'w'
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl})
	if !handled {
		t.Fatal("Ctrl+U not handled")
	}
	if got := m.ti.Value(); got != "world" {
		t.Fatalf("after Ctrl+U got %q, want %q", got, "world")
	}
}

func TestModelRightArrowAcceptsGhost(t *testing.T) {
	m := newTestModel(SuggesterFunc(func(v string) (string, string) {
		if v == "hel" {
			return "lo", ""
		}
		return "", ""
	}), nil)
	m.ti.SetValue("hel")
	m.ti.SetCursor(3)
	m.refreshGhost()
	if m.ghost != "lo" {
		t.Fatalf("ghost precondition: got %q", m.ghost)
	}
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyRight})
	if !handled {
		t.Fatal("right-arrow at end of line should accept ghost")
	}
	if m.ti.Value() != "hello" {
		t.Fatalf("after accept: got %q", m.ti.Value())
	}
}

func TestModelUnicodeEditing(t *testing.T) {
	m := newTestModel(SuggesterFunc(func(string) (string, string) { return "é", "" }), nil)
	m.ti.SetValue("é中 tail")
	m.ti.SetCursor(3)
	m.handleKey(tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl})
	if got := m.ti.Value(); got != "tail" {
		t.Fatalf("Ctrl+U: got %q", got)
	}
	m.ti.SetValue("é中")
	m.ti.CursorEnd()
	m.refreshGhost()
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyRight})
	if !handled || m.ti.Value() != "é中é" {
		t.Fatalf("completion: handled=%v value=%q", handled, m.ti.Value())
	}
}

func TestModelRightArrowMidLinePassesThrough(t *testing.T) {
	// If caret isn't at end, right-arrow should move the cursor, not
	// accept a ghost.
	m := newTestModel(SuggesterFunc(func(v string) (string, string) {
		return "xx", ""
	}), nil)
	m.ti.SetValue("abcdef")
	m.ti.SetCursor(2)
	m.refreshGhost()
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyRight})
	if handled {
		t.Fatal("mid-line right-arrow should pass through")
	}
}

func TestModelUpArrowHistory(t *testing.T) {
	m := newTestModel(nil, []string{"one", "two", "three"})
	m.ti.SetValue("current")
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if !handled {
		t.Fatal("Up should be consumed")
	}
	if m.ti.Value() != "three" {
		t.Fatalf("up 1: got %q", m.ti.Value())
	}
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.ti.Value() != "two" {
		t.Fatalf("up 2: got %q", m.ti.Value())
	}
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.ti.Value() != "one" {
		t.Fatalf("up 3: got %q", m.ti.Value())
	}
	// At top of history: another up is a no-op
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.ti.Value() != "one" {
		t.Fatalf("up 4 (clamp): got %q", m.ti.Value())
	}
}

func TestModelDownArrowHistory(t *testing.T) {
	m := newTestModel(nil, []string{"one", "two"})
	// Navigate up then back down to a blank (one past last).
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.ti.Value() != "one" {
		t.Fatalf("setup: got %q", m.ti.Value())
	}
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.ti.Value() != "two" {
		t.Fatalf("down 1: got %q", m.ti.Value())
	}
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.ti.Value() != "" {
		t.Fatalf("down 2 (past end): got %q", m.ti.Value())
	}
	// Down past end is a no-op.
	m.handleKey(tea.KeyPressMsg{Code: tea.KeyDown})
	if m.ti.Value() != "" {
		t.Fatal("down past end should stay blank")
	}
}

func TestModelDownWithoutUp(t *testing.T) {
	m := newTestModel(nil, []string{"a", "b"})
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyDown})
	if !handled {
		t.Fatal("Down should be consumed even without prior up")
	}
	// Already at one-past-end; should remain blank (SetValue("")).
	if m.ti.Value() != "" {
		t.Fatalf("down from blank: got %q", m.ti.Value())
	}
}

func TestModelEmptyHistoryUp(t *testing.T) {
	m := newTestModel(nil, nil)
	handled, _, _ := m.handleKey(tea.KeyPressMsg{Code: tea.KeyUp})
	if !handled {
		t.Fatal("Up should still be consumed on empty history")
	}
	if m.ti.Value() != "" {
		t.Fatalf("up empty: got %q", m.ti.Value())
	}
}

func TestModelGhostNilSuggester(t *testing.T) {
	m := newTestModel(nil, nil)
	m.ti.SetValue("anything")
	m.refreshGhost()
	if m.ghost != "" || m.hint != "" {
		t.Fatalf("nil suggester should produce no ghost/hint: %q %q", m.ghost, m.hint)
	}
}

func TestModelViewContainsHint(t *testing.T) {
	m := newTestModel(SuggesterFunc(func(v string) (string, string) {
		return "", "hello-hint"
	}), nil)
	m.refreshGhost()
	if got := m.View(); !containsString(got.Content, "hello-hint") {
		t.Fatalf("view missing hint: %q", got.Content)
	}
}

func containsString(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
