package main

import (
	"slices"
	"testing"

	"github.com/go-zeromq/zmq4"
)

type recordingSocket struct {
	zmq4.Socket
	messages []zmq4.Msg
}

func (s *recordingSocket) Send(msg zmq4.Msg) error {
	s.messages = append(s.messages, msg)
	return nil
}

func TestNotebookCursorPositions(t *testing.T) {
	for _, tc := range []struct {
		code  string
		pos   float64
		start float64
		end   float64
		match string
	}{
		{"# café\nsh", 9, 7, 9, "show"},
		{"sh", 2, 0, 2, "show"},
		{"sh", -1, 0, 0, "show"},
		{"sh", 100, 0, 2, "show"},
		{".sh", 3, 0, 3, ".show"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			sock := &recordingSocket{}
			k := &kernel{shell: sock}
			k.handleComplete(message{Content: map[string]any{"code": tc.code, "cursor_pos": tc.pos}})
			got, err := decode(sock.messages[0].Frames, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Content["cursor_start"] != tc.start || got.Content["cursor_end"] != tc.end {
				t.Fatalf("wrong range: %v", got.Content)
			}
			matches := got.Content["matches"].([]any)
			if !slices.Contains(matches, any(tc.match)) {
				t.Fatalf("missing %q in %v", tc.match, matches)
			}
		})
	}
}

func TestNotebookInspectUnicode(t *testing.T) {
	for _, pos := range []float64{9, 100, -1} {
		sock := &recordingSocket{}
		k := &kernel{shell: sock}
		k.handleInspect(message{Content: map[string]any{"code": "# café\nshow", "cursor_pos": pos}})
		got, err := decode(sock.messages[0].Frames, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.Content["found"] != (pos >= 0) {
			t.Fatalf("position %v: %v", pos, got.Content)
		}
	}
}
