package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

// TestInitialize exercises the base protocol handshake and verifies
// the server self-describes.
func TestInitialize(t *testing.T) {
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n")
	var out bytes.Buffer
	if err := serve(bufio.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v - raw: %s", err, out.String())
	}
	if resp.Error != nil {
		t.Fatalf("error: %v", resp.Error)
	}
	m, _ := resp.Result.(map[string]any)
	info, _ := m["serverInfo"].(map[string]any)
	if info["name"] != "golars-mcp" {
		t.Fatalf("serverInfo.name = %v, want golars-mcp", info["name"])
	}
}

// TestToolsList checks that every registered tool is discoverable.
func TestToolsList(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(req)), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	m, _ := resp.Result.(map[string]any)
	items, _ := m["tools"].([]any)
	if len(items) < 5 {
		t.Fatalf("tools: got %d items, want ≥ 5", len(items))
	}
}

// TestUnknownMethod surfaces a sensible error.
func TestUnknownMethod(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"no/such/thing"}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(req)), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil {
		t.Fatal("expected error, got success")
	}
	if resp.Error.Code != -32601 {
		t.Fatalf("error code: got %d want -32601", resp.Error.Code)
	}
}

func TestToolCallUnknownTool(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nope","arguments":{}}}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(req)), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Error.Code != -32601 {
		t.Fatalf("expected method-not-found, got %v", resp.Error)
	}
}

func TestIntegerArguments(t *testing.T) {
	for _, value := range []string{"2", `"2"`, strconv.Itoa(int(^uint(0) >> 1))} {
		got, err := asInt(json.RawMessage(`{"n":`+value+`}`), "n", 10)
		if err != nil || got < 2 {
			t.Fatalf("value %s: got %d, %v", value, got, err)
		}
	}
	for _, value := range []string{"2.5", "1e100", "9223372036854775808", "null", "true"} {
		if _, err := asInt(json.RawMessage(`{"n":`+value+`}`), "n", 10); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}

func TestUnrepresentableToolResultKeepsSessionAlive(t *testing.T) {
	previous := tools
	tools = append(append([]Tool(nil), tools...), Tool{
		Name: "test_nonfinite",
		Run: func(json.RawMessage) (any, error) {
			return structuredResult(map[string]any{"value": math.NaN()}, "NaN"), nil
		},
	})
	t.Cleanup(func() { tools = previous })
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"test_nonfinite"}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(input)), &out); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&out)
	var first, second rpcResponse
	if err := dec.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&second); err != nil {
		t.Fatal(err)
	}
	result, ok := first.Result.(map[string]any)
	if !ok || result["isError"] != true || string(second.ID) != "2" {
		t.Fatalf("unexpected responses: %+v %+v", first, second)
	}
}

func TestNotificationIgnored(t *testing.T) {
	in := strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n")
	var out bytes.Buffer
	if err := serve(bufio.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ID == nil || string(resp.ID) != "2" {
		t.Fatalf("notification should produce no reply; got %s", out.String())
	}
}
