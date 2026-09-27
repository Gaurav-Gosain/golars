package main

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/go-zeromq/zmq4"
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

	// Auto-display the focused frame as the cell result whenever the
	// host produced one. The HTML is theme-aware (uses currentColor +
	// border-only styling) so it renders fine on light or dark
	// JupyterLab; the captured stdout still shows alongside as stream.
	if !silent && resp.HTML != "" {
		data := map[string]any{"text/html": resp.HTML}
		if resp.Text == "" {
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

// handleComplete answers complete_request with the same completions
// golars-lsp offers: the kernel-host analyses the cell against the
// live session, so columns and frames are the ones loaded so far.
func (k *kernel) handleComplete(msg message) {
	code, _ := msg.Content["code"].(string)
	posF, _ := msg.Content["cursor_pos"].(float64)
	cursor := runeToByte(code, int(posF))
	r := k.ide("complete", code, cursor)
	types := make([]map[string]any, len(r.Types))
	for i, t := range r.Types {
		types[i] = map[string]any{
			"text": t.Text, "type": t.Type, "signature": t.Signature,
			"start": byteToRune(code, t.Start), "end": byteToRune(code, t.End),
		}
	}
	if r.Matches == nil {
		r.Matches = []string{}
	}
	_ = k.send(k.shell, reply(msg, "complete_reply", map[string]any{
		"status":       "ok",
		"matches":      r.Matches,
		"cursor_start": byteToRune(code, r.Start),
		"cursor_end":   byteToRune(code, r.End),
		"metadata":     map[string]any{"_jupyter_types_experimental": types},
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

// handleInspect answers inspect_request (shift+tab) with the hover
// text golars-lsp shows: command docs, function signatures and Go doc
// comments, and column dtypes from the live session.
func (k *kernel) handleInspect(msg message) {
	code, _ := msg.Content["code"].(string)
	posF, _ := msg.Content["cursor_pos"].(float64)
	r := k.ide("inspect", code, runeToByte(code, int(posF)))
	content := map[string]any{
		"status":   "ok",
		"found":    r.Markdown != "",
		"data":     map[string]any{},
		"metadata": map[string]any{},
	}
	if r.Markdown != "" {
		content["data"] = map[string]any{"text/markdown": r.Markdown, "text/plain": r.Markdown}
	}
	_ = k.send(k.shell, reply(msg, "inspect_reply", content))
}
