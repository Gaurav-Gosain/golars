package json

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// ndjsonBlock is the number of lines each decode task handles.
const ndjsonBlock = 4096

// readNDJSONBytes decodes newline-delimited JSON. Lines are unmarshaled
// in parallel; key order and error reporting follow document order.
// Line numbers in errors count non-blank lines, as before.
func readNDJSONBytes(ctx context.Context, data []byte, cfg config) (*dataframe.DataFrame, error) {
	var lines [][]byte
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		var line []byte
		if i < 0 {
			line, data = data, nil
		} else {
			line, data = data[:i], data[i+1:]
		}
		if t := trimSpaces(line); len(t) > 0 {
			lines = append(lines, t)
		}
	}
	rows := make([]map[string]any, len(lines))
	errs := make([]error, len(lines))
	nblocks := (len(lines) + ndjsonBlock - 1) / ndjsonBlock
	var next atomic.Int64
	var failed atomic.Bool
	var wg sync.WaitGroup
	for range min(nblocks, runtime.GOMAXPROCS(0)) {
		wg.Go(func() {
			for {
				k := int(next.Add(1)) - 1
				if k >= nblocks || failed.Load() || ctx.Err() != nil {
					return
				}
				for i := k * ndjsonBlock; i < min((k+1)*ndjsonBlock, len(lines)); i++ {
					if err := unmarshalNumbers(lines[i], &rows[i]); err != nil {
						errs[i] = err
						failed.Store(true)
						break
					}
				}
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var ord keyOrder
	for i, row := range rows {
		if errs[i] != nil {
			return nil, fmt.Errorf("json.ReadNDJSON: line %d: %w", i+1, errs[i])
		}
		if row == nil && failed.Load() {
			// A block after a failure was skipped; the failure is later
			// in this loop only if no earlier line failed, so decode it
			// here to report errors in document order.
			if err := unmarshalNumbers(lines[i], &rows[i]); err != nil {
				return nil, fmt.Errorf("json.ReadNDJSON: line %d: %w", i+1, err)
			}
			row = rows[i]
		}
		if err := ord.add(row, lines[i]); err != nil {
			return nil, fmt.Errorf("json.ReadNDJSON: line %d: %w", i+1, err)
		}
	}
	return buildFromRowsOrdered(ctx, rows, ord.names, cfg)
}
