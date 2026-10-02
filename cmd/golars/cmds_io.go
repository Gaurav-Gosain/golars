package main

import (
	"strings"

	"github.com/Gaurav-Gosain/golars/internal/fileio"
)

// asName returns NAME for the `PATH as NAME` form, or "" when absent.
// Any other trailing text is a usage error.
func asName(c *call) (string, error) {
	switch {
	case len(c.args) == 1:
		return "", nil
	case len(c.args) == 3 && strings.EqualFold(c.args[1], "as"):
		return c.args[2], nil
	}
	return "", c.usage()
}

// cmdLoad reads PATH eagerly. Without `as NAME` it replaces the focus;
// with it the frame is staged and the focus is left alone.
func cmdLoad(s *state, c *call) error {
	if len(c.args) == 0 {
		return c.usage()
	}
	name, err := asName(c)
	if err != nil {
		return err
	}
	path := c.args[0]
	df, err := fileio.Read(s.ctx, path)
	if err != nil {
		return err
	}
	if name != "" {
		s.stage(name, path, df)
		ok("loaded %s as %s (%s)", path, cmdStyle.Render(name), shape(df))
		return nil
	}
	s.replaceFocus(df)
	s.path = path
	s.focused = ""
	ok("loaded %s (%s)", path, shape(df))
	return nil
}

// cmdSave collects the focus and writes it; the format comes from the
// extension.
func cmdSave(s *state, c *call) error {
	if len(c.args) != 1 {
		return c.usage()
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	if err := fileio.Write(s.ctx, c.args[0], df); err != nil {
		return err
	}
	ok("wrote %s (%s)", c.args[0], shape(df))
	return nil
}

// scanCmd registers a lazy scan. An empty format infers the reader
// from the extension (scan_auto). Without `as NAME` the scan replaces
// the focus and the file is opened at collect time; with it the scan
// is collected and staged, matching `load PATH as NAME`.
func scanCmd(format string) handlerFunc {
	return func(s *state, c *call) error {
		if len(c.args) == 0 {
			return c.usage()
		}
		name, err := asName(c)
		if err != nil {
			return err
		}
		path := c.args[0]
		fromExt, known := fileio.FormatOf(path)
		var f fileio.Format
		switch {
		case format == "":
			if !known {
				_, err := fileio.Scan(path) // reports the unsupported extension
				return err
			}
			f = fromExt
		case format == "csv" && fromExt == fileio.TSV:
			f = fileio.TSV // scan_csv on a .tsv file keeps the tab delimiter
		default:
			f, _ = fileio.ParseFormat(format)
		}
		lf := fileio.ScanAs(f, path)
		if name != "" {
			df, err := lf.Collect(s.ctx)
			if err != nil {
				return err
			}
			s.stage(name, path, df)
			ok("scanned %s as %s (%s)", path, cmdStyle.Render(name), shape(df))
			return nil
		}
		s.replaceFocus(nil)
		s.lf = &lf
		s.path = path
		s.focused = ""
		ok("registered %s scan of %s", f, path)
		return nil
	}
}
