package parquet

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readBatchSize is the number of rows decoded per NextBatch call. Larger
// batches mean fewer, larger output chunks and fewer builder resizes.
const readBatchSize = 1 << 20

// batchSizeFor returns the pqarrow decode batch size for pf: the
// largest row group, capped at readBatchSize. pqarrow reserves value
// and level buffers for a whole batch before decoding each row group,
// so a 1M-row batch on a file with 64K-row groups reserved (and zeroed)
// about 16x the memory each column needs, for every row group read in
// parallel.
func batchSizeFor(pf *file.Reader) int64 {
	batch := int64(1)
	for i := range pf.NumRowGroups() {
		batch = max(batch, pf.MetaData().RowGroup(i).NumRows())
	}
	return min(batch, readBatchSize)
}

// readTable decodes the projected columns of every row group with
// pqarrow. Columns are decoded in parallel (pqarrow's Parallel mode fans
// out one reader per column).
func readTable(ctx context.Context, fr *pqarrow.FileReader, nrg int, cfg config) (arrow.Table, error) {
	leaves, err := projectLeaves(fr.Manifest, cfg.columns)
	if err != nil {
		return nil, err
	}
	rgs := make([]int, nrg)
	for i := range rgs {
		rgs[i] = i
	}
	return readLeaves(ctx, fr, leaves, rgs)
}

// readRowGroupsParallel decodes row groups concurrently (each row group
// in turn fans out over its columns) and stitches the per-row-group
// tables into one table whose columns keep one chunk per row group.
// pqarrow alone only parallelizes over columns, which leaves most cores
// idle for narrow projections.
func readRowGroupsParallel(ctx context.Context, fr *pqarrow.FileReader, leaves []int, rgs []int) (arrow.Table, error) {
	nrg := len(rgs)
	tables := make([]arrow.Table, nrg)
	errs := make([]error, nrg)
	var next atomic.Int64
	var wg sync.WaitGroup
	workers := min(nrg, max(runtime.GOMAXPROCS(0)/max(len(leaves), 1), 1)*2)
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= nrg || ctx.Err() != nil {
					return
				}
				tables[i], errs[i] = fr.ReadRowGroups(ctx, leaves, []int{rgs[i]})
			}
		})
	}
	wg.Wait()
	release := func() {
		for _, t := range tables {
			if t != nil {
				t.Release()
			}
		}
	}
	if err := errors.Join(append(errs, ctx.Err())...); err != nil {
		release()
		return nil, err
	}
	defer release()

	sc := tables[0].Schema()
	cols := make([]arrow.Column, sc.NumFields())
	rows := int64(0)
	for _, t := range tables {
		rows += t.NumRows()
	}
	for c := range cols {
		var chunks []arrow.Array
		for _, t := range tables {
			chunks = append(chunks, t.Column(c).Data().Chunks()...)
		}
		chunked := arrow.NewChunked(sc.Field(c).Type, chunks)
		col := arrow.NewColumn(sc.Field(c), chunked)
		chunked.Release()
		cols[c] = *col
	}
	tbl := array.NewTable(sc, cols, rows)
	for i := range cols {
		cols[i].Release()
	}
	return tbl, nil
}

// projectLeaves maps top-level column names to parquet leaf column
// indices in the requested order. A nil names slice selects every leaf.
func projectLeaves(m *pqarrow.SchemaManifest, names []string) ([]int, error) {
	if names == nil {
		var out []int
		for i := range m.Fields {
			out = appendLeaves(out, &m.Fields[i])
		}
		return out, nil
	}
	byName := make(map[string]int, len(m.Fields))
	for i := range m.Fields {
		byName[m.Fields[i].Field.Name] = i
	}
	seen := make(map[string]bool, len(names))
	out := make([]int, 0, len(names))
	for _, n := range names {
		idx, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("column %q not found", n)
		}
		if seen[n] {
			return nil, fmt.Errorf("column %q requested more than once", n)
		}
		seen[n] = true
		out = appendLeaves(out, &m.Fields[idx])
	}
	return out, nil
}

func appendLeaves(out []int, f *pqarrow.SchemaField) []int {
	if len(f.Children) == 0 {
		return append(out, f.ColIndex)
	}
	for i := range f.Children {
		out = appendLeaves(out, &f.Children[i])
	}
	return out
}
