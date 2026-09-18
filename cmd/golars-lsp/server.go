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
)

// Server speaks JSON-RPC 2.0 (LSP base protocol) over stdio. It is
// deliberately compact: every LSP request we handle runs synchronously
// on the reader goroutine. Notifications (didOpen/didChange) are
// similarly serial; there's no request pipelining or cancellation
// support because .glr documents are tiny and requests complete in
// microseconds.
type server struct {
	in    *bufio.Reader
	out   *bufio.Writer
	log   io.Writer
	outMu sync.Mutex // serialises writes (LSP replies + notifications)

	docs     *docStore
	shutdown bool
	writeErr error
}

func newServer(in io.Reader, out io.Writer, log io.Writer) *server {
	return &server{
		in:   bufio.NewReader(in),
		out:  bufio.NewWriter(out),
		log:  log,
		docs: newDocStore(),
	}
}

// Run drives the stdio loop until the peer sends `exit` or closes
// stdin. Any transport-level error short-circuits with that error.
func (s *server) Run() error {
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
		if s.writeErr != nil {
			return s.writeErr
		}
		if msg.Method == "exit" {
			return nil
		}
	}
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

func (s *server) dispatch(msg *rawMessage) {
	// Notification: no id → no reply.
	isRequest := msg.ID != nil

	switch msg.Method {
	case "initialize":
		s.handleInitialize(msg)
	case "initialized":
		// no-op: client ack after initialize response
	case "shutdown":
		s.shutdown = true
		s.reply(msg, nil)
	case "exit":
		// handled in Run after dispatch returns
	case "textDocument/didOpen":
		s.handleDidOpen(msg)
	case "textDocument/didChange":
		s.handleDidChange(msg)
	case "textDocument/didClose":
		s.handleDidClose(msg)
	case "textDocument/completion":
		s.handleCompletion(msg)
	case "textDocument/hover":
		s.handleHover(msg)
	case "textDocument/definition":
		s.handleDefinition(msg)
	case "textDocument/inlayHint":
		s.handleInlayHint(msg)
	default:
		if isRequest {
			s.replyError(msg, -32601, "Method not found: "+msg.Method)
		}
	}
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

type initializeResult struct {
	Capabilities serverCapabilities `json:"capabilities"`
	ServerInfo   serverInfo         `json:"serverInfo"`
}

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type serverCapabilities struct {
	TextDocumentSync   int                 `json:"textDocumentSync"`
	CompletionProvider *completionProvider `json:"completionProvider,omitempty"`
	HoverProvider      bool                `json:"hoverProvider"`
	InlayHintProvider  bool                `json:"inlayHintProvider,omitempty"`
	DefinitionProvider bool                `json:"definitionProvider,omitempty"`
}

type completionProvider struct {
	TriggerCharacters []string `json:"triggerCharacters"`
	ResolveProvider   bool     `json:"resolveProvider"`
}

func (s *server) handleInitialize(msg *rawMessage) {
	s.reply(msg, initializeResult{
		Capabilities: serverCapabilities{
			TextDocumentSync: 1, // 1 = full text sync: simpler, fine for tiny docs
			CompletionProvider: &completionProvider{
				// '.' triggers command completion mid-line; space after
				// `.load ` or `.source ` triggers path/frame completion.
				TriggerCharacters: []string{".", " "},
			},
			HoverProvider:      true,
			InlayHintProvider:  true,
			DefinitionProvider: true,
		},
		ServerInfo: serverInfo{Name: "golars-lsp", Version: "0.1.0"},
	})
}
