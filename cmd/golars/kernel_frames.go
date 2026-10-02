package main

import (
	"fmt"
	"slices"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Frame ops of the kernel-host let a notebook share frames with code in
// another language (gopyter's Go cells) without running script commands,
// so nothing is printed and the focus never moves:
//
//	{"op":"frames"}                          list the named frames and the focus
//	{"op":"export","name":"N","path":"P"}    write frame N ("" = the focus) to P as Arrow IPC
//	{"op":"import","name":"N","path":"P"}    read Arrow IPC from P as the named frame N
//
// Each listed frame has a generation number that changes whenever the
// frame is replaced, so a client can tell which frames a cell changed.

// hostFrame is one entry of an op=frames reply. The focus has Focus set
// and Name set to the name it was focused from ("" for an anonymous
// load).
type hostFrame struct {
	Name  string `json:"name"`
	Focus bool   `json:"focus,omitempty"`
	Rows  int    `json:"rows"`
	Cols  int    `json:"cols"`
	// Lazy marks a focused pipeline that has not been collected; Rows
	// is then unknown (-1).
	Lazy bool `json:"lazy,omitempty"`
	Gen  int  `json:"gen"`
}

// frameGens numbers the versions of frames. It keeps the last seen
// pointer of each frame, which also keeps the address from being reused
// by a later frame.
type frameGens struct {
	next  int
	named map[string]genEntry
	focus genEntry
}

type genEntry struct {
	df  *dataframe.DataFrame
	lf  *lazy.LazyFrame
	gen int
}

func (g *frameGens) bump(e genEntry, df *dataframe.DataFrame, lf *lazy.LazyFrame) genEntry {
	if e.gen != 0 && e.df == df && e.lf == lf {
		return e
	}
	g.next++
	return genEntry{df: df, lf: lf, gen: g.next}
}

// list returns the named frames sorted by name, then the focus.
func (g *frameGens) list(s *state) []hostFrame {
	if g.named == nil {
		g.named = map[string]genEntry{}
	}
	names := make([]string, 0, len(s.frames))
	for n := range s.frames {
		names = append(names, n)
	}
	slices.Sort(names)
	out := make([]hostFrame, 0, len(names)+1)
	for _, n := range names {
		f := s.frames[n]
		e := g.bump(g.named[n], f.df, nil)
		g.named[n] = e
		h, w := 0, 0
		if f.df != nil {
			h, w = f.df.Shape()
		}
		out = append(out, hostFrame{Name: n, Rows: h, Cols: w, Gen: e.gen})
	}
	for n := range g.named {
		if _, ok := s.frames[n]; !ok {
			delete(g.named, n)
		}
	}
	if !s.hasFocus() {
		g.focus = genEntry{}
		return out
	}
	g.focus = g.bump(g.focus, s.df, s.lf)
	f := hostFrame{Name: s.focused, Focus: true, Gen: g.focus.gen}
	if s.lf != nil {
		f.Lazy, f.Rows = true, -1
		if sch, err := s.lf.Schema(); err == nil {
			f.Cols = sch.Len()
		}
	} else {
		f.Rows, f.Cols = s.df.Shape()
	}
	return append(out, f)
}

// hostFrameOp answers op=frames, op=export and op=import.
func hostFrameOp(s *state, g *frameGens, req kernelRequest) kernelResponse {
	resp := kernelResponse{ID: req.ID}
	switch req.Op {
	case "frames":
		resp.Frames = g.list(s)
		if resp.Frames == nil {
			resp.Frames = []hostFrame{}
		}
	case "export":
		if req.Path == "" {
			resp.Error = "export: no path"
			return resp
		}
		var df *dataframe.DataFrame
		if req.Name == "" {
			if !s.hasFocus() {
				resp.Error = errNoFrame.Error()
				return resp
			}
			out, err := s.materialize()
			if err != nil {
				resp.Error = err.Error()
				return resp
			}
			defer out.Release()
			df = out
		} else {
			f, ok := s.frames[req.Name]
			if !ok || f.df == nil {
				resp.Error = fmt.Sprintf("no frame named %q", req.Name)
				return resp
			}
			df = f.df
		}
		if err := ipc.WriteFile(s.ctx, req.Path, df); err != nil {
			resp.Error = err.Error()
			return resp
		}
		h, w := df.Shape()
		resp.Shape = &[2]int{h, w}
	case "import":
		if req.Name == "" || req.Path == "" {
			resp.Error = "import: needs a name and a path"
			return resp
		}
		df, err := ipc.ReadFile(s.ctx, req.Path)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		s.stage(req.Name, req.Path, df)
		if g.named == nil {
			g.named = map[string]genEntry{}
		}
		e := g.bump(genEntry{}, df, nil)
		g.named[req.Name] = e
		resp.Gen = e.gen
		h, w := df.Shape()
		resp.Shape = &[2]int{h, w}
	}
	return resp
}
