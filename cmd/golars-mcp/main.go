// Command golars-mcp is a Model Context Protocol server that
// exposes a read-only subset of golars as tools an LLM host
// (Claude Desktop, Cursor, Windsurf, ...) can invoke.
//
// The protocol is JSON-RPC 2.0 over stdio with a small extension set
// defined by MCP (https://modelcontextprotocol.io). This file
// implements the base protocol plus the four core methods a host
// uses to discover and call tools:
//
//	initialize          handshake, exchange server info + capabilities
//	tools/list          return the catalogue of tools
//	tools/call          run a tool with arguments and return a result
//	notifications/*     one-way notifications (heartbeat, ...)
//
// Additional MCP features (resources, prompts, sampling) are not
// needed for a tools-only server, so we keep the surface tight.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const version = "0.1.0"

// rpcRequest carries a single MCP JSON-RPC envelope. ID may be a
// string or a number; we keep it as json.RawMessage so we can echo
// the exact bytes back in the response.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func main() {
	if err := serve(bufio.NewReader(os.Stdin), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "golars-mcp:", err)
		os.Exit(1)
	}
}

// serve runs the MCP loop over arbitrary streams. Injecting the
// reader + writer this way also makes the protocol layer unit-testable
// without spawning the real process.
func serve(in *bufio.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	enc := json.NewEncoder(out)
	for {
		var req rpcRequest
		if err := dec.Decode(&req); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode: %w", err)
		}
		resp := dispatch(&req)
		if resp == nil {
			// Notifications get no response.
			continue
		}
		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}
}

// dispatch routes an MCP request to the correct handler. Returns nil
// for notifications (no id) so the loop knows to skip the reply.
func dispatch(req *rpcRequest) *rpcResponse {
	isNotification := len(req.ID) == 0
	if isNotification {
		// We accept and ignore every notification.
		return nil
	}
	switch req.Method {
	case "initialize":
		return reply(req, initializeResult())
	case "tools/list":
		return reply(req, toolsList())
	case "tools/call":
		return handleToolCall(req)
	case "ping":
		return reply(req, map[string]any{})
	case "shutdown":
		return reply(req, map[string]any{})
	}
	return replyError(req, -32601, "method not found: "+req.Method)
}

func reply(req *rpcRequest, result any) *rpcResponse {
	return &rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
}

func replyError(req *rpcRequest, code int, msg string) *rpcResponse {
	return &rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: code, Message: msg}}
}

// initializeResult is the value returned from `initialize`.
func initializeResult() any {
	return map[string]any{
		"protocolVersion": "2025-06-18",
		"serverInfo": map[string]any{
			"name":    "golars-mcp",
			"version": version,
		},
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
	}
}

// handleToolCall unpacks the tools/call params and dispatches to
// the named tool's implementation.
func handleToolCall(req *rpcRequest) *rpcResponse {
	var args struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &args); err != nil {
		return replyError(req, -32602, "invalid params: "+err.Error())
	}
	tool := findTool(args.Name)
	if tool == nil {
		return replyError(req, -32601, "unknown tool: "+args.Name)
	}
	result, err := tool.Run(args.Arguments)
	if err != nil {
		return reply(req, toolError(err.Error()))
	}
	if _, err := json.Marshal(result); err != nil {
		return reply(req, toolError("tool result cannot be represented as JSON"))
	}
	return reply(req, result)
}

// toolError wraps an error string into MCP's structured tool result
// shape (isError: true + a single text content block).
func toolError(msg string) any {
	return map[string]any{
		"isError": true,
		"content": []any{textContent(msg)},
	}
}

func textContent(s string) any {
	return map[string]any{"type": "text", "text": s}
}

// toolsList returns the catalogue of every registered tool.
func toolsList() any {
	items := make([]any, 0, len(tools))
	for _, t := range tools {
		items = append(items, map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"inputSchema": t.InputSchema,
		})
	}
	return map[string]any{"tools": items}
}

// ---- simple helpers used by tool implementations -----------------

// asString reads the named string argument from a JSON args blob.
func asString(args json.RawMessage, key string) (string, error) {
	var m map[string]any
	if len(args) == 0 {
		return "", fmt.Errorf("missing argument %q", key)
	}
	if err := json.Unmarshal(args, &m); err != nil {
		return "", err
	}
	v, ok := m[key]
	if !ok {
		return "", fmt.Errorf("missing argument %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("argument %q must be a string", key)
	}
	return s, nil
}

// asInt reads the named int argument. Accepts JSON numbers and
// string numerals alike.
func asInt(args json.RawMessage, key string, def int) (int, error) {
	var m map[string]json.RawMessage
	if len(args) == 0 {
		return def, nil
	}
	if err := json.Unmarshal(args, &m); err != nil {
		return 0, err
	}
	v, ok := m[key]
	if !ok {
		return def, nil
	}
	text := string(v)
	if len(v) > 0 && v[0] == '"' {
		if err := json.Unmarshal(v, &text); err != nil {
			return 0, err
		}
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("argument %q must be an integer in range: %w", key, err)
	}
	return n, nil
}

// asStringSlice reads the named argument as []string. Accepts a
// single string (wraps in slice), a JSON array of strings, or an
// absent key (returns nil).
func asStringSlice(args json.RawMessage, key string) ([]string, error) {
	var m map[string]any
	if len(args) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(args, &m); err != nil {
		return nil, err
	}
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	switch x := v.(type) {
	case string:
		return []string{x}, nil
	case []any:
		out := make([]string, len(x))
		for i, e := range x {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("argument %q: non-string element", key)
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, fmt.Errorf("argument %q must be a string or array", key)
}

// structuredResult wraps both a text fallback and an optional
// structured payload in MCP's tool-result content shape.
func structuredResult(structured any, fallback string) any {
	return map[string]any{
		"content":           []any{textContent(fallback)},
		"structuredContent": structured,
	}
}

// verbatim is a minimal wrapper so we can embed a multi-line string
// in a tool result without worrying about trailing newlines.
func verbatim(s string) string { return strings.TrimRight(s, "\n") }
