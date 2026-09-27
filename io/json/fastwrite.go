package json

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// writeBlockRows is the number of rows each writer task formats.
const writeBlockRows = 1 << 14

// appendRow appends row i as a JSON object. Common arrow types are
// formatted directly with the same output encoding/json produces; the
// rest go through arrowCellToGo and json.Marshal.
func (rw *rowWriter) appendRow(b []byte, i int) ([]byte, error) {
	b = append(b, '{')
	for c, col := range rw.cols {
		if c > 0 {
			b = append(b, ',')
		}
		b = append(b, rw.keys[c]...)
		b = append(b, ':')
		if col.IsNull(i) {
			b = append(b, "null"...)
			continue
		}
		switch a := col.(type) {
		case *array.Int64:
			b = strconv.AppendInt(b, a.Value(i), 10)
		case *array.Int32:
			b = strconv.AppendInt(b, int64(a.Value(i)), 10)
		case *array.Uint64:
			b = strconv.AppendUint(b, a.Value(i), 10)
		case *array.Float64:
			b = appendFloat(b, a.Value(i))
		case *array.Float32:
			b = appendFloat(b, float64(a.Value(i)))
		case *array.Boolean:
			b = strconv.AppendBool(b, a.Value(i))
		case *array.String:
			b = appendString(b, a.Value(i))
		case *array.LargeString:
			b = appendString(b, a.Value(i))
		default:
			v := arrowCellToGo(col, i)
			if f, isFloat := v.(float64); isFloat && (math.IsNaN(f) || math.IsInf(f, 0)) {
				v = nil
			}
			enc, err := json.Marshal(v)
			if err != nil {
				return b, fmt.Errorf("json: column %s row %d: %w", rw.keys[c], i, err)
			}
			b = append(b, enc...)
		}
	}
	return append(b, '}'), nil
}

// appendFloat formats f like encoding/json (NaN and infinities as null,
// which is how the writer has always emitted them), except that an
// integral value keeps a ".0" suffix as polars writes it, so the column
// reads back as float.
func appendFloat(b []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(b, "null"...)
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	start := len(b)
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'f' && !bytes.ContainsRune(b[start:], '.') {
		b = append(b, ".0"...)
	}
	if format == 'e' {
		// Clean up e-09 to e-9, as encoding/json does.
		if n := len(b); n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return b
}

// appendString quotes s. Printable ASCII without characters that
// encoding/json escapes is copied; anything else is marshaled.
func appendString(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			enc, _ := json.Marshal(s)
			return append(b, enc...)
		}
	}
	b = append(b, '"')
	b = append(b, s...)
	return append(b, '"')
}

// writeRows formats every row in parallel blocks and writes them in
// order. sep goes between rows and after the last one when trailing.
func (rw *rowWriter) writeRows(ctx context.Context, w io.Writer, n int, sep byte, trailing bool) error {
	nblocks := (n + writeBlockRows - 1) / writeBlockRows
	type block struct {
		buf  []byte
		err  error
		done chan struct{}
	}
	blocks := make([]*block, nblocks)
	for i := range blocks {
		blocks[i] = &block{done: make(chan struct{})}
	}
	workers := min(nblocks, runtime.GOMAXPROCS(0))
	// Bound the formatted blocks held in memory.
	inflight := make(chan struct{}, 2*workers)
	next := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for k := range next {
				bl := blocks[k]
				lo, hi := k*writeBlockRows, min((k+1)*writeBlockRows, n)
				buf := make([]byte, 0, (hi-lo)*64)
				for i := lo; i < hi && bl.err == nil; i++ {
					buf, bl.err = rw.appendRow(buf, i)
					if i < n-1 || trailing {
						buf = append(buf, sep)
					}
				}
				if bl.err == nil {
					bl.err = ctx.Err()
				}
				bl.buf = buf
				close(bl.done)
			}
		})
	}
	stop := make(chan struct{})
	go func() {
		defer close(next)
		for k := range nblocks {
			select {
			case inflight <- struct{}{}:
			case <-stop:
				return
			}
			select {
			case next <- k:
			case <-stop:
				return
			}
		}
	}()
	var err error
	for _, bl := range blocks {
		<-bl.done
		if err = bl.err; err != nil {
			break
		}
		if _, err = w.Write(bl.buf); err != nil {
			break
		}
		bl.buf = nil
		<-inflight
	}
	close(stop)
	wg.Wait()
	return err
}
