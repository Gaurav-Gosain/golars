package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server speaks JSON-RPC 2.0 (LSP base protocol) over stdio. Requests
// run in order on the reader goroutine; each needs one analysis of
// the document, which is cached per version. Diagnostics are
// published from a debounce timer so fast typing analyses once.
type server struct {
	in    *bufio.Reader
	out   *bufio.Writer
	log   io.Writer
	outMu sync.Mutex // serialises writes (replies and notifications)

	docs     *docStore
	shutdown bool
	writeErr error

	// debounce is the pause after a change before diagnostics are
	// computed and published.
	debounce time.Duration
	timerMu  sync.Mutex
	timers   map[string]*time.Timer
}

func newServer(in io.Reader, out io.Writer, log io.Writer) *server {
	return &server{
		in:       bufio.NewReader(in),
		out:      bufio.NewWriter(out),
		log:      log,
		docs:     newDocStore(),
		debounce: 120 * time.Millisecond,
		timers:   map[string]*time.Timer{},
	}
}

// Run drives the stdio loop until the peer sends `exit` or closes
// stdin. Any transport-level error short-circuits with that error.
func (s *server) Run() error {
	defer s.stopTimers()
	for {
		raw, err := s.readMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var msg rawMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			s.logf("invalid JSON: %v", err)
			continue
		}
		s.dispatch(&msg)
		if err := s.writeError(); err != nil {
			return err
		}
		if msg.Method == "exit" {
			return nil
		}
	}
}

func (s *server) stopTimers() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	for uri, t := range s.timers {
		t.Stop()
		delete(s.timers, uri)
	}
}

func (s *server) writeError() error {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	return s.writeErr
}

// -----------------------------------------------------------------
// Transport: LSP base protocol: "Content-Length: N\r\n\r\n{...}".
// -----------------------------------------------------------------

const maxMessageBytes = 8 * 1024 * 1024
const maxHeaderBytes = 8192

func (s *server) readMessage() ([]byte, error) {
	var contentLen, headerBytes int
	seenLength := false
	for {
		rawLine, err := s.in.ReadSlice('\n')
		headerBytes += len(rawLine)
		if headerBytes > maxHeaderBytes || errors.Is(err, bufio.ErrBufferFull) {
			return nil, errors.New("message headers too large")
		}
		if err != nil {
			if errors.Is(err, io.EOF) && headerBytes > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		line := strings.TrimRight(string(rawLine), "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			if strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
				if seenLength {
					return nil, errors.New("duplicate Content-Length header")
				}
				seenLength = true
				contentLen, err = strconv.Atoi(strings.TrimSpace(v))
				if err != nil {
					return nil, fmt.Errorf("bad Content-Length %q: %w", v, err)
				}
			}
		}
	}
	if contentLen <= 0 {
		return nil, errors.New("missing Content-Length header")
	}
	if contentLen > maxMessageBytes {
		return nil, errors.New("message body too large")
	}
	buf := make([]byte, contentLen)
	if _, err := io.ReadFull(s.in, buf); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}

func (s *server) writeMessage(payload any) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if s.writeErr != nil {
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		s.writeErr = fmt.Errorf("marshal reply: %w", err)
		return
	}
	if _, err = fmt.Fprintf(s.out, "Content-Length: %d\r\n\r\n", len(body)); err == nil {
		_, err = s.out.Write(body)
	}
	if err == nil {
		err = s.out.Flush()
	}
	s.writeErr = err
}

func (s *server) logf(format string, args ...any) {
	if s.log != nil {
		fmt.Fprintf(s.log, "golars-lsp: "+format+"\n", args...)
	}
}

// -----------------------------------------------------------------
// Dispatch: one request/notification → one handler.
// -----------------------------------------------------------------

type rawMessage struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
	// response fields (we never receive them in practice but tolerate them)
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// requestHandlers answer requests that need a document; each returns
// the result to send back.
var requestHandlers = map[string]func(s *server, params json.RawMessage) (any, error){
	"textDocument/completion":          (*server).completion,
	"textDocument/hover":               (*server).hover,
	"textDocument/signatureHelp":       (*server).signatureHelp,
	"textDocument/definition":          (*server).definition,
	"textDocument/references":          (*server).references,
	"textDocument/documentHighlight":   (*server).documentHighlight,
	"textDocument/prepareRename":       (*server).prepareRename,
	"textDocument/rename":              (*server).rename,
	"textDocument/documentSymbol":      (*server).documentSymbol,
	"textDocument/foldingRange":        (*server).foldingRange,
	"textDocument/semanticTokens/full": (*server).semanticTokens,
	"textDocument/formatting":          (*server).formatting,
	"textDocument/codeAction":          (*server).codeAction,
	"textDocument/inlayHint":           (*server).inlayHint,
}

func (s *server) dispatch(msg *rawMessage) {
	isRequest := msg.ID != nil
	if h, found := requestHandlers[msg.Method]; found {
		result, err := s.safeCall(h, msg.Params)
		if err != nil {
			s.replyError(msg, -32602, err.Error())
			return
		}
		s.reply(msg, result)
		return
	}
	switch msg.Method {
	case "initialize":
		s.handleInitialize(msg)
	case "initialized", "$/cancelRequest", "$/setTrace", "workspace/didChangeConfiguration":
	case "shutdown":
		s.shutdown = true
		s.reply(msg, nil)
	case "exit":
		// handled in Run after dispatch returns
	case "textDocument/didOpen":
		s.handleDidOpen(msg)
	case "textDocument/didChange":
		s.handleDidChange(msg)
	case "textDocument/didSave":
		s.handleDidSave(msg)
	case "textDocument/didClose":
		s.handleDidClose(msg)
	default:
		if isRequest {
			s.replyError(msg, -32601, "Method not found: "+msg.Method)
		}
	}
}

// safeCall runs a handler, turning a panic into an error reply so one
// bad document cannot take the server down.
func (s *server) safeCall(h func(*server, json.RawMessage) (any, error), params json.RawMessage) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logf("panic: %v", r)
			result, err = nil, fmt.Errorf("internal error: %v", r)
		}
	}()
	return h(s, params)
}

func (s *server) reply(req *rawMessage, result any) {
	if req.ID == nil {
		return
	}
	s.writeMessage(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result"`
	}{"2.0", *req.ID, result})
}

func (s *server) replyError(req *rawMessage, code int, msg string) {
	if req.ID == nil {
		return
	}
	s.writeMessage(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   rpcError        `json:"error"`
	}{"2.0", *req.ID, rpcError{code, msg}})
}

func (s *server) notify(method string, params any) {
	s.writeMessage(struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", method, params})
}

// -----------------------------------------------------------------
// initialize
// -----------------------------------------------------------------

type initializeParams struct {
	InitializationOptions struct {
		// DebounceMs overrides the diagnostics debounce.
		DebounceMs *int `json:"debounceMs"`
	} `json:"initializationOptions"`
}

func (s *server) handleInitialize(msg *rawMessage) {
	var p initializeParams
	_ = json.Unmarshal(msg.Params, &p)
	if d := p.InitializationOptions.DebounceMs; d != nil && *d >= 0 {
		s.debounce = time.Duration(*d) * time.Millisecond
	}
	s.reply(msg, map[string]any{
		"capabilities": map[string]any{
			"positionEncoding": "utf-16",
			"textDocumentSync": map[string]any{
				"openClose": true,
				"change":    1, // full text: documents are small
				"save":      map[string]any{"includeText": false},
			},
			"completionProvider": map[string]any{
				"triggerCharacters": []string{".", " ", "(", ",", ":", "/", "="},
				"resolveProvider":   false,
			},
			"hoverProvider": true,
			"signatureHelpProvider": map[string]any{
				"triggerCharacters":   []string{"(", ","},
				"retriggerCharacters": []string{","},
			},
			"definitionProvider":        true,
			"referencesProvider":        true,
			"documentHighlightProvider": true,
			"renameProvider":            map[string]any{"prepareProvider": true},
			"documentSymbolProvider":    true,
			"foldingRangeProvider":      true,
			"semanticTokensProvider": map[string]any{
				"legend": map[string]any{"tokenTypes": semanticTypes, "tokenModifiers": semanticModifiers},
				"full":   true,
			},
			"documentFormattingProvider": true,
			"codeActionProvider":         map[string]any{"codeActionKinds": []string{"quickfix"}},
			"inlayHintProvider":          true,
		},
		"serverInfo": map[string]any{"name": "golars-lsp", "version": "0.2.0"},
	})
}
