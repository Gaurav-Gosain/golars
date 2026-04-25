package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
)

// kernel owns the five Jupyter sockets and the long-lived `golars
// kernel-host` subprocess that actually executes glr code. The single
// goroutine model: shell + control loops run concurrently and both
// publish to iopub via a mutex; nothing else writes to iopub.
type kernel struct {
	conn *connectionFile
	key  []byte
	ctx  context.Context

	shell, iopub, control, stdin, hb zmq4.Socket

	host *kernelHost

	iopubMu sync.Mutex
	session string

	execCount atomic.Int64
	startTime time.Time
}

func newKernel(ctx context.Context, conn *connectionFile) (*kernel, error) {
	k := &kernel{
		conn:      conn,
		key:       []byte(conn.Key),
		ctx:       ctx,
		session:   uuid.NewString(),
		startTime: time.Now(),
	}

	// ROUTER for shell + control + stdin (we receive identity-prefixed
	// messages and reply to a specific peer). PUB for iopub (fanout).
	// REP for heartbeat (just echoes).
	k.shell = zmq4.NewRouter(ctx)
	k.iopub = zmq4.NewPub(ctx)
	k.control = zmq4.NewRouter(ctx)
	k.stdin = zmq4.NewRouter(ctx)
	k.hb = zmq4.NewRep(ctx)

	for _, b := range []struct {
		sock zmq4.Socket
		port int
		name string
	}{
		{k.shell, conn.ShellPort, "shell"},
		{k.iopub, conn.IOPubPort, "iopub"},
		{k.control, conn.ControlPort, "control"},
		{k.stdin, conn.StdinPort, "stdin"},
		{k.hb, conn.HBPort, "hb"},
	} {
		if err := b.sock.Listen(conn.addr(b.port)); err != nil {
			return nil, fmt.Errorf("listen %s: %w", b.name, err)
		}
	}

	host, err := startKernelHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("kernel-host: %w", err)
	}
	k.host = host

	return k, nil
}

func (k *kernel) Close() {
	if k.host != nil {
		k.host.Close()
	}
	for _, s := range []zmq4.Socket{k.shell, k.iopub, k.control, k.stdin, k.hb} {
		if s != nil {
			_ = s.Close()
		}
	}
}

// Run starts the heartbeat + control + shell loops and blocks until
// shutdown. shell is the main loop; control runs on its own goroutine
// so interrupt/shutdown work even when shell is busy executing a cell.
func (k *kernel) Run() error {
	go k.heartbeatLoop()
	go k.controlLoop()
	return k.shellLoop()
}

func (k *kernel) heartbeatLoop() {
	for {
		msg, err := k.hb.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("hb recv: %v", err)
			return
		}
		// Echo verbatim: heartbeat just measures liveness.
		if err := k.hb.Send(msg); err != nil {
			log.Printf("hb send: %v", err)
			return
		}
	}
}

// controlLoop handles shutdown_request and (in the future) interrupts.
// Runs on its own ZMQ socket so it stays responsive when shell is
// busy executing a cell.
func (k *kernel) controlLoop() {
	for {
		msg, err := k.recv(k.control)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("control recv: %v", err)
			return
		}
		switch msg.Header.MsgType {
		case "shutdown_request":
			restart, _ := msg.Content["restart"].(bool)
			_ = k.send(k.control, reply(msg, "shutdown_reply", map[string]any{
				"status":  "ok",
				"restart": restart,
			}))
			os.Exit(0)
		case "interrupt_request":
			// Best-effort: kill+respawn the host so the in-flight cell
			// dies cleanly. The current shell call will then come back
			// with a closed-pipe error and the loop continues.
			if k.host != nil {
				k.host.Restart()
			}
			_ = k.send(k.control, reply(msg, "interrupt_reply", map[string]any{
				"status": "ok",
			}))
		default:
			// kernel_info_request can also arrive on control - mirror
			// the shell handler.
			if msg.Header.MsgType == "kernel_info_request" {
				k.handleKernelInfo(k.control, msg)
				continue
			}
			log.Printf("control: unhandled %s", msg.Header.MsgType)
		}
	}
}

// shellLoop dispatches user requests one at a time. Cell execution
// blocks the loop intentionally - clients use control for interrupts.
func (k *kernel) shellLoop() error {
	// Tell everyone we're alive before accepting work. JupyterLab
	// otherwise sees a "starting" stub kernel forever.
	k.publishStatus(message{}, "starting")

	for {
		msg, err := k.recv(k.shell)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("shell recv: %w", err)
		}
		k.publishStatus(msg, "busy")
		switch msg.Header.MsgType {
		case "kernel_info_request":
			k.handleKernelInfo(k.shell, msg)
		case "execute_request":
			k.handleExecute(msg)
		case "complete_request":
			k.handleComplete(msg)
		case "is_complete_request":
			k.handleIsComplete(msg)
		case "inspect_request":
			k.handleInspect(msg)
		case "comm_info_request":
			_ = k.send(k.shell, reply(msg, "comm_info_reply", map[string]any{
				"status": "ok", "comms": map[string]any{},
			}))
		case "history_request":
			_ = k.send(k.shell, reply(msg, "history_reply", map[string]any{
				"status": "ok", "history": []any{},
			}))
		default:
			log.Printf("shell: unhandled %s", msg.Header.MsgType)
		}
		k.publishStatus(msg, "idle")
	}
}

// recv reads one ZMQ multipart message and decodes + signature-verifies
// it. zmq4 returns Frames as []zmq4.Msg with one frame per element;
// we flatten back to raw [][]byte for protocol decoding.
func (k *kernel) recv(s zmq4.Socket) (message, error) {
	m, err := s.Recv()
	if err != nil {
		return message{}, err
	}
	return decode(m.Frames, k.key)
}

func (k *kernel) send(s zmq4.Socket, msg message) error {
	frames, err := encode(msg, k.key)
	if err != nil {
		return err
	}
	return s.Send(zmq4.NewMsgFrom(frames...))
}

// publish broadcasts a message on iopub. Serialised via iopubMu so
// shell + control + execution goroutines don't interleave frames.
func (k *kernel) publish(msg message) {
	k.iopubMu.Lock()
	defer k.iopubMu.Unlock()
	if err := k.send(k.iopub, msg); err != nil {
		log.Printf("iopub send: %v", err)
	}
}

func (k *kernel) publishStatus(parent message, state string) {
	k.publish(broadcast(parent, "status", map[string]any{
		"execution_state": state,
	}))
}
