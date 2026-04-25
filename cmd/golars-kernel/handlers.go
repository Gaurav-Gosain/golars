package main

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/go-zeromq/zmq4"

	"github.com/Gaurav-Gosain/golars/script"
)

// handleKernelInfo replies with language metadata so JupyterLab can
// configure syntax highlighting + the cell-language dropdown.
func (k *kernel) handleKernelInfo(sock zmq4.Socket, msg message) {
	content := map[string]any{
		"status":                 "ok",
		"protocol_version":       "5.3",
		"implementation":         "golars",
		"implementation_version": version,
		"language_info": map[string]any{
			"name":           "golars",
			"version":        version,
			"mimetype":       "text/x-glr",
			"file_extension": ".glr",
			"pygments_lexer": "text",
			"codemirror_mode": map[string]any{
				"name": "shell",
			},
		},
		"banner": fmt.Sprintf(
			"golars-kernel %s on %s/%s - pure-Go DataFrames with lazy plan + optimizer.\nLearn more: https://github.com/Gaurav-Gosain/golars",
			version, runtime.GOOS, runtime.GOARCH,
		),
		"help_links": []map[string]string{
			{"text": "golars docs", "url": "https://golars.gaurav.zip"},
			{"text": "scripting reference", "url": "https://golars.gaurav.zip/scripting"},
		},
	}
	_ = k.send(sock, reply(msg, "kernel_info_reply", content))
}

// handleExecute is the busy path: send execute_input on iopub, ship
// the cell to the kernel-host subprocess, fan stdout/stderr/html out
// as stream + display_data, then send execute_reply on shell.
func (k *kernel) handleExecute(msg message) {
	code, _ := msg.Content["code"].(string)
	silent, _ := msg.Content["silent"].(bool)
	storeHistory, _ := msg.Content["store_history"].(bool)

	count := k.execCount.Add(1)

	// Echo the cell on iopub so other clients (e.g. classic
	// notebook's side panel) can mirror it.
	if !silent {
		k.publish(broadcast(msg, "execute_input", map[string]any{
			"code":            code,
			"execution_count": count,
		}))
	}

	resp, err := k.host.Run(code)
	if err != nil {
		k.errorReply(msg, count, "HostError", err.Error())
		return
	}

	if resp.Text != "" {
		k.publish(broadcast(msg, "stream", map[string]any{
			"name": "stdout",
			"text": resp.Text,
		}))
	}
	if resp.Stderr != "" {
		k.publish(broadcast(msg, "stream", map[string]any{
			"name": "stderr",
			"text": resp.Stderr,
		}))
	}

	if resp.Error != "" {
		k.errorReply(msg, count, "GolarsError", resp.Error)
		return
	}

	// Auto-display the focused frame as the cell result. Suppress
	// when the user already explicitly displayed (heuristic: the cell
	// ends with show/head/tail/collect/save/describe/schema). This
	// avoids the boxed text + HTML table doubling up.
	if !silent && resp.HTML != "" && !endsWithDisplayCommand(code) {
		data := map[string]any{
			"text/html": resp.HTML,
		}
		if resp.Text == "" {
			// No stream output - include text/plain so consoles still see something.
			data["text/plain"] = fmt.Sprintf("<dataframe shape=%v>", resp.Shape)
		}
		k.publish(broadcast(msg, "execute_result", map[string]any{
			"execution_count": count,
			"data":            data,
			"metadata":        map[string]any{},
		}))
	}

	replyContent := map[string]any{
		"status":           "ok",
		"execution_count":  count,
		"payload":          []any{},
		"user_expressions": map[string]any{},
	}
	_ = k.send(k.shell, reply(msg, "execute_reply", replyContent))
	_ = storeHistory // accepted; we don't keep notebook history server-side
}

func (k *kernel) errorReply(msg message, count int64, name, evalue string) {
	tb := []string{evalue}
	k.publish(broadcast(msg, "error", map[string]any{
		"ename":     name,
		"evalue":    evalue,
		"traceback": tb,
	}))
	_ = k.send(k.shell, reply(msg, "execute_reply", map[string]any{
		"status":          "error",
		"execution_count": count,
		"ename":           name,
		"evalue":          evalue,
		"traceback":       tb,
	}))
}

// handleComplete: command-name + frame-name completion. We don't have
// a shared schema oracle (the kernel-host owns that state), so the
// suggestions are limited to commands and structural keywords. Worth
// a follow-up: let kernel-host expose a `complete` request.
func (k *kernel) handleComplete(msg message) {
	code, _ := msg.Content["code"].(string)
	posF, _ := msg.Content["cursor_pos"].(float64)
	pos := int(posF)
	pos = min(pos, len(code))

	// Walk back to the start of the current word.
	start := pos
	for start > 0 {
		c := code[start-1]
		if !isIdentByte(c) && c != '.' {
			break
		}
		start--
	}
	prefix := code[start:pos]
	prefix = strings.TrimPrefix(prefix, ".")

	matches := []string{}
	for _, c := range script.Commands {
		if strings.HasPrefix(c.Name, prefix) {
			matches = append(matches, c.Name)
		}
	}

	_ = k.send(k.shell, reply(msg, "complete_reply", map[string]any{
		"status":       "ok",
		"matches":      matches,
		"cursor_start": start,
		"cursor_end":   pos,
		"metadata":     map[string]any{},
	}))
}

// handleIsComplete: glr is line-oriented, so unless the cell ends with
// a trailing `\` (continuation marker) we always say "complete".
func (k *kernel) handleIsComplete(msg message) {
	code, _ := msg.Content["code"].(string)
	trim := strings.TrimRight(code, " \t\n")
	status := "complete"
	indent := ""
	if strings.HasSuffix(trim, "\\") {
		status = "incomplete"
		indent = "  "
	}
	_ = k.send(k.shell, reply(msg, "is_complete_reply", map[string]any{
		"status": status,
		"indent": indent,
	}))
}

// handleInspect: docs lookup keyed off the word at cursor_pos. Returns
// the CommandSpec long doc as text/markdown when available.
func (k *kernel) handleInspect(msg message) {
	code, _ := msg.Content["code"].(string)
	posF, _ := msg.Content["cursor_pos"].(float64)
	pos := int(posF)
	pos = min(pos, len(code))
	// Word under cursor: scan both directions across identifier chars.
	left, right := pos, pos
	for left > 0 && (isIdentByte(code[left-1]) || code[left-1] == '.') {
		left--
	}
	for right < len(code) && (isIdentByte(code[right]) || code[right] == '.') {
		right++
	}
	word := strings.TrimPrefix(code[left:right], ".")

	content := map[string]any{
		"status": "ok",
		"found":  false,
		"data":   map[string]any{},
		"metadata": map[string]any{},
	}
	if spec := script.FindCommand(word); spec != nil {
		content["found"] = true
		content["data"] = map[string]any{
			"text/markdown": fmt.Sprintf("**`%s`** - %s\n\n%s", spec.Signature, spec.Summary, spec.LongDoc),
			"text/plain":    fmt.Sprintf("%s\n\n%s\n\n%s", spec.Signature, spec.Summary, spec.LongDoc),
		}
	}
	_ = k.send(k.shell, reply(msg, "inspect_reply", content))
}

// endsWithDisplayCommand returns true when the cell's last non-empty
// non-comment line is a command that already prints its own output.
// We use that to skip the extra auto-displayed HTML execute_result so
// users don't see the same data rendered twice.
func endsWithDisplayCommand(code string) bool {
	lines := strings.Split(code, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		l = strings.TrimPrefix(l, ".")
		first, _, _ := strings.Cut(l, " ")
		switch first {
		case "show", "head", "tail", "collect", "save", "describe",
			"schema", "explain", "explain_tree", "tree", "graph",
			"show_graph", "mermaid", "frames", "info", "ishow", "browse",
			"glimpse", "size", "null_count", "null_count_all":
			return true
		}
		return false
	}
	return false
}

func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}
