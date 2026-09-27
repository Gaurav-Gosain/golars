package main

import (
	"fmt"
	"slices"
	"strings"
)

// Named frames let one session juggle several sources:
//
//	load PATH as NAME   stage a frame without touching the focus
//	use NAME            focus a clone of NAME (NAME stays staged, so
//	                    repeated `use NAME` branches off the same base)
//	stash NAME          materialise the focus and stage a copy as NAME
//	frames              list the focus and every staged frame
//	drop_frame NAME     release a staged frame

// cmdUse focuses a clone of a staged frame. The previous focus and any
// pending pipeline are discarded; `stash` first to keep them.
func cmdUse(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	name := c.args[0]
	target, found := s.frames[name]
	if !found {
		return fmt.Errorf("no frame named %q (see frames)", name)
	}
	s.replaceFocus(target.df.Clone())
	s.path = target.path
	s.focused = name
	ok("focused %s (%s)", cmdStyle.Render(name), shape(s.df))
	return nil
}

// cmdStash materialises the focus and stages a copy under NAME. The
// focus continues from the materialised rows, so later statements see
// exactly what was stashed.
func cmdStash(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	name := c.args[0]
	df, err := s.materialize()
	if err != nil {
		return err
	}
	s.stage(name, s.path, df.Clone())
	s.replaceFocus(df)
	ok("stashed %s (%s)", cmdStyle.Render(name), shape(df))
	return nil
}

// cmdFrames lists the focus first, then staged frames by name.
func cmdFrames(s *state, _ *call) error {
	fmt.Println()
	if !s.hasFocus() && len(s.frames) == 0 {
		fmt.Println(dimStyle.Render("  (no frames loaded)"))
		fmt.Println()
		return nil
	}
	if s.hasFocus() {
		name := s.focused
		if name == "" {
			name = "<default>"
		}
		size := "lazy"
		if s.lf == nil {
			size = shape(s.df)
		}
		fmt.Printf("  * %s  %s  (%s)\n",
			cmdStyle.Render(padRight(name, 18)),
			dimStyle.Render(padRight(s.path, 40)), size)
	}
	names := make([]string, 0, len(s.frames))
	for n := range s.frames {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		f := s.frames[n]
		fmt.Printf("    %s  %s  (%s)\n",
			cmdStyle.Render(padRight(n, 18)),
			dimStyle.Render(padRight(f.path, 40)), shape(f.df))
	}
	fmt.Println()
	fmt.Println(dimStyle.Render("  * = focused. Switch with use NAME."))
	fmt.Println()
	return nil
}

// cmdDropFrame releases a staged frame. The focus is a clone, so
// dropping the frame it came from is safe.
func cmdDropFrame(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	name := c.args[0]
	f, found := s.frames[name]
	if !found {
		return fmt.Errorf("no frame named %q", name)
	}
	f.df.Release()
	delete(s.frames, name)
	if s.focused == name {
		s.focused = ""
	}
	ok("dropped %s", cmdStyle.Render(name))
	return nil
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}
