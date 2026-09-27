package main

import (
	"slices"
	"strings"
	"testing"
)

func TestNotebookCompletionAndInspectUseSpecAliases(t *testing.T) {
	sock := &recordingSocket{}
	k := &kernel{shell: sock}
	k.handleComplete(message{Content: map[string]any{"code": "scan_a", "cursor_pos": float64(6)}})
	got, err := decode(sock.messages[0].Frames, nil)
	if err != nil {
		t.Fatal(err)
	}
	matches := got.Content["matches"].([]any)
	for _, want := range []string{"scan_auto", "scan_arrow"} {
		if !slices.Contains(matches, any(want)) {
			t.Errorf("missing %q in %v", want, matches)
		}
	}

	sock = &recordingSocket{}
	k = &kernel{shell: sock}
	k.handleInspect(message{Content: map[string]any{"code": "write out.csv", "cursor_pos": float64(2)}})
	got, err = decode(sock.messages[0].Frames, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := got.Content["data"].(map[string]any)
	md, _ := data["text/markdown"].(string)
	if !strings.Contains(md, "### `save`") || !strings.Contains(md, "`write`") {
		t.Errorf("inspect of alias write should document save, got %q", md)
	}
}
