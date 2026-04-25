package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// connectionFile is the JSON payload Jupyter writes to disk and
// passes to the kernel binary as argv[1]. Layout matches the public
// jupyter-client protocol spec.
type connectionFile struct {
	IP              string `json:"ip"`
	Transport       string `json:"transport"`
	SignatureScheme string `json:"signature_scheme"`
	Key             string `json:"key"`

	ShellPort   int `json:"shell_port"`
	IOPubPort   int `json:"iopub_port"`
	StdinPort   int `json:"stdin_port"`
	ControlPort int `json:"control_port"`
	HBPort      int `json:"hb_port"`

	KernelName string `json:"kernel_name,omitempty"`
}

func loadConnectionFile(path string) (*connectionFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cf connectionFile
	if err := json.Unmarshal(raw, &cf); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if cf.Transport == "" {
		cf.Transport = "tcp"
	}
	if cf.SignatureScheme == "" {
		cf.SignatureScheme = "hmac-sha256"
	}
	if cf.SignatureScheme != "hmac-sha256" {
		return nil, fmt.Errorf("unsupported signature scheme %q", cf.SignatureScheme)
	}
	return &cf, nil
}

// addr formats an endpoint like "tcp://127.0.0.1:1234" for ZMQ.
func (cf *connectionFile) addr(port int) string {
	return fmt.Sprintf("%s://%s:%d", cf.Transport, cf.IP, port)
}
