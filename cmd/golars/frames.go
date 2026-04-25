package main

import (
	"fmt"
	"sort"
)

// Multi-source support: every frame loaded into the REPL / script
// session has a name. The "focused" frame is state.df (+ state.lf);
// all other frames are parked in state.frames until promoted via
// `.use NAME`. A zero-name slot is reserved for `.load PATH` (no
// `as`) so single-frame scripts work exactly as before.
//
// Commands:
//   load PATH              → focused frame (unnamed)
//   load PATH as NAME      → load into NAME slot, leave focus alone
//   use NAME               → focus becomes a clone of NAME (copy-on-
//                           promote; NAME stays in the registry so
//                           repeated `use NAME` lets scripts branch)
//   stash NAME             → materialise the current focus and save
//                           it under NAME for later `use`
//   frames                 → list loaded frames (name, path, shape)
//   drop_frame NAME        → release NAME from the registry

// loadAs reads PATH into a named registry slot without touching the
// currently-focused frame. If NAME already exists, the previous frame
// is released. Uses loadFile so the public `s.load` loader's "ok
// loaded ..." print doesn't duplicate the staged-frame summary.
func (s *state) loadAs(path, name string) error {
	if name == "" {
		return fmt.Errorf("load as: name must be non-empty")
	}
	df, err := s.loadFile(path)
	if err != nil {
		return err
	}
	if existing, ok := s.frames[name]; ok && existing != nil {
		if existing.df != nil {
			existing.df.Release()
		}
	}
	s.frames[name] = &namedFrame{df: df, lf: nil, path: path}
	fmt.Printf("%s loaded %s as %s (%d × %d)\n",
		successStyle.Render("✓"), path, cmdStyle.Render(name),
		df.Height(), df.Width())
	return nil
}

// cmdUse promotes a named frame to the focus. Copy-on-promote: the
// staged entry is cloned into focus rather than moved, so NAME stays
// available for repeated `.use NAME` (scripts branch off the same
// base frame). The prior focus is released: callers who want to
// preserve in-progress work must `.stash` it first.
func (s *state) cmdUse(name string) error {
	target, ok := s.frames[name]
	if !ok {
		return fmt.Errorf(".use: no frame named %q (try .frames)", name)
	}
	// Drop the previous focus. Lazy pipelines built on top of the
	// prior focus are discarded: users who want to keep them call
	// `.stash` before switching.
	if s.df != nil {
		s.df.Release()
	}
	s.df = target.df.Clone()
	s.lf = nil
	s.path = target.path
	s.focused = name
	fmt.Printf("%s focused %s (%d × %d)\n",
		successStyle.Render("✓"), cmdStyle.Render(name), s.df.Height(), s.df.Width())
	return nil
}

// cmdStash materialises the focused pipeline and stores a reference
// under NAME so later `.use NAME` can recall it. The focus is
// replaced with the materialised state (the lazy pipeline is
// consumed), so subsequent ops continue from the same rows that
// were just snapshotted.
//
// Overwrites any existing frame under NAME.
func (s *state) cmdStash(name string) error {
	if name == "" {
		return fmt.Errorf(".stash requires a non-empty name")
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	// Swap in the materialised frame as the new focus; release the
	// prior focus so we don't leak.
	if s.df != nil {
		s.df.Release()
	}
	if prev, ok := s.frames[name]; ok && prev.df != nil {
		prev.df.Release()
	}
	// Keep two independent references: one owned by the focus, one by
	// the registry.
	s.frames[name] = &namedFrame{df: df.Clone(), lf: nil, path: s.path}
	s.df = df
	s.lf = nil
	fmt.Printf("%s stashed %s (%d × %d)\n",
		successStyle.Render("✓"), cmdStyle.Render(name),
		df.Height(), df.Width())
	return nil
}

// cmdFrames prints the current frame registry.
func (s *state) cmdFrames() error {
	fmt.Println()
	if s.df == nil && len(s.frames) == 0 {
		fmt.Println(dimStyle.Render("  (no frames loaded)"))
		fmt.Println()
		return nil
	}
	// Focused first, then alphabetical.
	if s.df != nil {
		marker := "  *"
		name := s.focused
		if name == "" {
			name = "<default>"
		}
		fmt.Printf("%s  %s  %s  (%d × %d)\n", marker,
			cmdStyle.Render(padRight(name, 18)),
			dimStyle.Render(padRight(s.path, 40)),
			s.df.Height(), s.df.Width())
	}
	names := make([]string, 0, len(s.frames))
	for n := range s.frames {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := s.frames[n]
		fmt.Printf("   %s  %s  (%d × %d)\n",
			cmdStyle.Render(padRight(n, 18)),
			dimStyle.Render(padRight(f.path, 40)),
			f.df.Height(), f.df.Width())
	}
	fmt.Println()
	fmt.Println(dimStyle.Render("  *  = focused. Use .use NAME to switch."))
	fmt.Println()
	return nil
}

// cmdDropFrame releases NAME. Errors if NAME is the current focus (to
// avoid surprising `.show` failures: the caller should `.use` another
// frame first, or just rely on session exit to release everything).
func (s *state) cmdDropFrame(name string) error {
	if name == s.focused {
		return fmt.Errorf(".drop_frame: %q is focused; .use another frame first", name)
	}
	f, ok := s.frames[name]
	if !ok {
		return fmt.Errorf(".drop_frame: no frame named %q", name)
	}
	if f.df != nil {
		f.df.Release()
	}
	delete(s.frames, name)
	fmt.Printf("%s dropped %s\n", successStyle.Render("✓"), cmdStyle.Render(name))
	return nil
}

// ReleaseAllFrames releases every staged frame. The REPL drops all
// state on process exit so calling this is strictly a housekeeping
// hook for embedders; the current binary does not call it directly.
func (s *state) ReleaseAllFrames() {
	for _, f := range s.frames {
		if f.df != nil {
			f.df.Release()
		}
	}
	s.frames = nil
}
