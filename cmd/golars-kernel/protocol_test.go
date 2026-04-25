package main

import (
	"os"
	"strings"
	"testing"
)

// TestSignAndVerify: encode produces frames whose signature decode
// accepts, and a tampered content frame trips the HMAC check.
func TestSignAndVerify(t *testing.T) {
	key := []byte("test-key-not-secret")
	msg := message{
		Identities: [][]byte{[]byte("client-1")},
		Header: header{
			MsgID:   "abc",
			Session: "sess",
			MsgType: "kernel_info_request",
			Version: "5.3",
		},
		Content: map[string]any{"foo": "bar"},
	}
	frames, err := encode(msg, key)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decode(frames, key)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Header.MsgID != "abc" || got.Header.MsgType != "kernel_info_request" {
		t.Fatalf("header mismatch: %#v", got.Header)
	}
	if got.Content["foo"] != "bar" {
		t.Fatalf("content mismatch: %#v", got.Content)
	}
	if string(got.Identities[0]) != "client-1" {
		t.Fatalf("identity mismatch: %q", got.Identities[0])
	}

	// Tamper with the content frame: signature must reject.
	for i, f := range frames {
		if strings.Contains(string(f), `"foo":"bar"`) {
			frames[i] = []byte(`{"foo":"BAR"}`)
		}
	}
	if _, err := decode(frames, key); err == nil {
		t.Fatalf("expected signature rejection on tampered content")
	}
}

// TestEndsWithDisplayCommand: heuristic for suppressing the auto-HTML
// when the user already explicitly displayed via show/head/etc.
func TestEndsWithDisplayCommand(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"load x.csv\nshow", true},
		{"load x.csv\nhead 5", true},
		{"load x.csv\n# done\ntail 3 # comment", true},
		{"load x.csv\nfilter age > 25", false},
		{"load x.csv", false},
		{"", false},
		{"# comment only", false},
		{"load x.csv\n.show", true}, // dot-prefix REPL form
	}
	for _, c := range cases {
		if got := endsWithDisplayCommand(c.code); got != c.want {
			t.Errorf("endsWithDisplayCommand(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}

// TestConnectionFile: defaults applied + bad scheme rejected.
func TestConnectionFile(t *testing.T) {
	tmp := t.TempDir() + "/c.json"
	if err := writeFile(tmp, []byte(`{
		"ip": "127.0.0.1",
		"shell_port": 1, "iopub_port": 2, "stdin_port": 3,
		"control_port": 4, "hb_port": 5,
		"key": "abc"
	}`)); err != nil {
		t.Fatal(err)
	}
	cf, err := loadConnectionFile(tmp)
	if err != nil {
		t.Fatalf("loadConnectionFile: %v", err)
	}
	if cf.Transport != "tcp" {
		t.Errorf("default transport not applied: %q", cf.Transport)
	}
	if cf.SignatureScheme != "hmac-sha256" {
		t.Errorf("default signature scheme not applied: %q", cf.SignatureScheme)
	}
	if cf.addr(cf.ShellPort) != "tcp://127.0.0.1:1" {
		t.Errorf("addr wrong: %q", cf.addr(cf.ShellPort))
	}

	// Unsupported scheme is rejected.
	bad := t.TempDir() + "/bad.json"
	_ = writeFile(bad, []byte(`{
		"ip": "127.0.0.1", "shell_port": 1, "iopub_port": 2,
		"stdin_port": 3, "control_port": 4, "hb_port": 5,
		"key": "k", "signature_scheme": "not-real"
	}`))
	if _, err := loadConnectionFile(bad); err == nil {
		t.Errorf("expected unsupported-scheme error")
	}
}

func writeFile(path string, b []byte) error {
	return os.WriteFile(path, b, 0o644)
}
