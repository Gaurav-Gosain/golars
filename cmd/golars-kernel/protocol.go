package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// delimiter sits between the ZMQ identity frames and the signed
// header / parent / metadata / content frames in every Jupyter
// message. Frames after the delimiter are HMAC-SHA256 signed with
// the kernel key.
var delimiter = []byte("<IDS|MSG>")

// header is the minimal subset of the v5.3 message header we need.
// (date is a string so json roundtrips bit-identical.)
type header struct {
	MsgID    string `json:"msg_id"`
	Session  string `json:"session"`
	Username string `json:"username"`
	Date     string `json:"date"`
	MsgType  string `json:"msg_type"`
	Version  string `json:"version"`
}

// message is the parsed form of a wire message, with raw JSON content
// since each msg_type has its own schema and we'd rather pay the
// re-marshal cost than maintain a giant union.
type message struct {
	Identities [][]byte
	Header     header
	Parent     header
	Metadata   map[string]any
	Content    map[string]any
	Buffers    [][]byte
}

// signMessage computes the HMAC-SHA256 over the four signed JSON
// frames using the kernel key. Returned as lowercase hex per spec.
func signMessage(key []byte, hdr, parent, meta, content []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(hdr)
	mac.Write(parent)
	mac.Write(meta)
	mac.Write(content)
	return hex.EncodeToString(mac.Sum(nil))
}

// encode marshals a message to the wire frame layout:
//
//	[ids..., delimiter, signature, header, parent, metadata, content, buffers...]
//
// All four signed frames are JSON-serialised exactly once so the
// signature stays consistent with what the receiver verifies.
func encode(msg message, key []byte) ([][]byte, error) {
	hdr, err := json.Marshal(msg.Header)
	if err != nil {
		return nil, err
	}
	var parent []byte
	if msg.Parent.MsgID == "" {
		parent = []byte("{}")
	} else {
		parent, err = json.Marshal(msg.Parent)
		if err != nil {
			return nil, err
		}
	}
	meta, err := json.Marshal(orEmpty(msg.Metadata))
	if err != nil {
		return nil, err
	}
	content, err := json.Marshal(orEmpty(msg.Content))
	if err != nil {
		return nil, err
	}
	sig := signMessage(key, hdr, parent, meta, content)

	frames := make([][]byte, 0, 6+len(msg.Identities)+len(msg.Buffers))
	frames = append(frames, msg.Identities...)
	frames = append(frames, delimiter)
	frames = append(frames, []byte(sig))
	frames = append(frames, hdr)
	frames = append(frames, parent)
	frames = append(frames, meta)
	frames = append(frames, content)
	frames = append(frames, msg.Buffers...)
	return frames, nil
}

// decode parses a wire message and verifies the signature. The
// caller is expected to have arranged frames as a single ZMQ
// multipart message.
func decode(frames [][]byte, key []byte) (message, error) {
	idx := -1
	for i, f := range frames {
		if string(f) == string(delimiter) {
			idx = i
			break
		}
	}
	if idx < 0 || len(frames) < idx+6 {
		return message{}, fmt.Errorf("wire: missing delimiter or short frames (got %d)", len(frames))
	}
	identities := append([][]byte(nil), frames[:idx]...)
	signature := string(frames[idx+1])
	hdr := frames[idx+2]
	parent := frames[idx+3]
	meta := frames[idx+4]
	content := frames[idx+5]

	expected := signMessage(key, hdr, parent, meta, content)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return message{}, fmt.Errorf("wire: bad HMAC signature")
	}

	var msg message
	msg.Identities = identities
	if err := json.Unmarshal(hdr, &msg.Header); err != nil {
		return message{}, fmt.Errorf("wire: header: %w", err)
	}
	if len(parent) > 0 && string(parent) != "{}" {
		_ = json.Unmarshal(parent, &msg.Parent)
	}
	if len(meta) > 0 {
		_ = json.Unmarshal(meta, &msg.Metadata)
	}
	if len(content) > 0 {
		_ = json.Unmarshal(content, &msg.Content)
	}
	if len(frames) > idx+6 {
		msg.Buffers = append([][]byte(nil), frames[idx+6:]...)
	}
	return msg, nil
}

// reply builds a response message reusing the parent's session and a
// fresh msg_id, bound to msgType. Identities pass through so a
// ROUTER socket addresses the right caller.
func reply(parent message, msgType string, content map[string]any) message {
	return message{
		Identities: parent.Identities,
		Parent:     parent.Header,
		Header: header{
			MsgID:    uuid.NewString(),
			Session:  parent.Header.Session,
			Username: orStr(parent.Header.Username, "kernel"),
			Date:     time.Now().UTC().Format(time.RFC3339Nano),
			MsgType:  msgType,
			Version:  "5.3",
		},
		Content: content,
	}
}

// broadcast is reply but with no caller identity (iopub fanout).
func broadcast(parent message, msgType string, content map[string]any) message {
	m := reply(parent, msgType, content)
	m.Identities = nil
	return m
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func orStr(a, def string) string {
	if a == "" {
		return def
	}
	return a
}
