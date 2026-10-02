package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
)

// state is one REPL, script or kernel session.
//
// The focused frame is df (its materialised source) plus lf (a lazy
// pipeline on top of it). After a lazy scan df is nil and lf holds
// the scan. Every other loaded frame is parked in frames until
// `use NAME` promotes a clone of it to the focus.
type state struct {
	ctx        context.Context
	df         *dataframe.DataFrame
	path       string
	lf         *lazy.LazyFrame
	focused    string // name of the focused frame; "" for an anonymous load
	frames     map[string]*namedFrame
	showTiming bool
	startTime  time.Time
	evalCount  int

	// cont buffers a statement continued over several REPL lines.
	cont strings.Builder
	// completion caches what completion knows about the session,
	// refreshed after each statement.
	completion sessionCache
}

// namedFrame is a frame staged with `load PATH as NAME`, `scan_* PATH
// as NAME` or `stash NAME`.
type namedFrame struct {
	df   *dataframe.DataFrame
	path string
}

var errNoFrame = errors.New("no frame loaded; run load PATH first")

func newState(timing bool) *state {
	return &state{
		ctx:        context.Background(),
		startTime:  time.Now(),
		showTiming: timing,
		frames:     make(map[string]*namedFrame),
	}
}

// close releases the focus and every staged frame.
func (s *state) close() {
	if s.df != nil {
		s.df.Release()
		s.df = nil
	}
	s.lf = nil
	for name, f := range s.frames {
		if f.df != nil {
			f.df.Release()
		}
		delete(s.frames, name)
	}
}

func (s *state) hasFocus() bool { return s.df != nil || s.lf != nil }

func (s *state) requireFocus() error {
	if !s.hasFocus() {
		return errNoFrame
	}
	return nil
}

// currentLazy returns the focused pipeline. Callers check hasFocus
// first; with nothing loaded the returned plan has no source.
func (s *state) currentLazy() lazy.LazyFrame {
	if s.lf != nil {
		return *s.lf
	}
	return lazy.FromDataFrame(s.df)
}

// schema returns the focused pipeline's output schema. A lazy file
// scan does not know its schema until it runs, so the fallback
// collects zero rows to observe it.
func (s *state) schema() (*schema.Schema, error) {
	lf := s.currentLazy()
	sch, err := lf.Schema()
	if err == nil {
		return sch, nil
	}
	df, cerr := lf.Head(0).Collect(s.ctx)
	if cerr != nil {
		return nil, err
	}
	defer df.Release()
	return df.Schema(), nil
}

// materialize collects the focused pipeline into a new frame that the
// caller must release.
func (s *state) materialize() (*dataframe.DataFrame, error) {
	if s.lf != nil {
		return s.lf.Collect(s.ctx)
	}
	if s.df == nil {
		return nil, errNoFrame
	}
	return s.df.Clone(), nil
}

// pushLazy appends a lazy step to the focused pipeline and prints a
// one-line confirmation.
func (s *state) pushLazy(lf lazy.LazyFrame, format string, args ...any) {
	s.lf = &lf
	ok(format, args...)
}

// replaceFocus makes df (owned by the caller until now) the focused
// frame, dropping the previous source and any pending pipeline. The
// focus keeps its name and path so `frames` still reports lineage.
func (s *state) replaceFocus(df *dataframe.DataFrame) {
	if s.df != nil {
		s.df.Release()
	}
	s.df = df
	s.lf = nil
}

// transformFocus materialises the focus, applies fn, and replaces the
// focus with the result. Used by commands that have no lazy
// equivalent (sample, pivot, explode, ...).
func (s *state) transformFocus(verb string, fn func(*dataframe.DataFrame) (*dataframe.DataFrame, error)) error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	out, err := fn(df)
	df.Release()
	if err != nil {
		return err
	}
	s.replaceFocus(out)
	ok("%s (%s)", verb, shape(out))
	return nil
}

// stage stores df under name, releasing whatever was there before.
func (s *state) stage(name, path string, df *dataframe.DataFrame) {
	if prev, found := s.frames[name]; found && prev.df != nil {
		prev.df.Release()
	}
	s.frames[name] = &namedFrame{df: df, path: path}
}

func shape(df *dataframe.DataFrame) string {
	return fmt.Sprintf("%d × %d", df.Height(), df.Width())
}

// ok prints a success line: a green check mark and the message.
func ok(format string, args ...any) {
	fmt.Println(successStyle.Render("✓") + " " + fmt.Sprintf(format, args...))
}
