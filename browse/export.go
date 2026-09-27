package browse

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
	"github.com/Gaurav-Gosain/golars/series"
)

// exportView materialises the current view (filter + sort + hidden
// columns) into a new DataFrame, then writes it to path. Format is
// chosen from the extension: .csv .tsv .parquet .pq .arrow .ipc
// .json .ndjson .jsonl.
//
// Hidden columns are dropped from the export. An active filter /
// sort produces a reordered frame via compute.Take. When no reorder
// is needed and no columns are hidden, the source frame is written
// directly (zero-copy).
func (m *model) exportView(path string) error {
	// Check the extension first so a typo fails before a potentially
	// expensive Take.
	if _, known := fileio.FormatOf(path); !known {
		return fmt.Errorf("unsupported extension %q", filepath.Ext(path))
	}
	ctx := context.Background()

	df, cleanup, err := m.buildViewFrame(ctx)
	if err != nil {
		return err
	}
	if cleanup {
		defer df.Release()
	}
	return fileio.Write(ctx, path, df)
}

// buildViewFrame returns a DataFrame that matches the currently
// displayed view: visible columns only, in column order, with rows
// reordered per m.order when filter/sort is active. The cleanup
// bool indicates whether the caller is responsible for Release.
func (m *model) buildViewFrame(ctx context.Context) (*dataframe.DataFrame, bool, error) {
	cols := m.df.Columns()

	// Column selection: visible columns, in column order (frozen first
	// then the rest), matching visibleCols().
	vis := m.visibleCols()
	if len(vis) == 0 {
		return nil, false, fmt.Errorf("no visible columns to export")
	}

	// Fast path: no filter/sort and all columns visible in natural order.
	if m.order == nil && len(vis) == len(cols) {
		allNatural := true
		for i, ci := range vis {
			if m.cols[ci].orig != i {
				allNatural = false
				break
			}
		}
		if allNatural {
			return m.df, false, nil
		}
	}

	out := make([]*series.Series, 0, len(vis))
	if m.order == nil {
		for _, ci := range vis {
			out = append(out, cols[m.cols[ci].orig].Clone())
		}
	} else {
		for _, ci := range vis {
			s, err := compute.Take(ctx, cols[m.cols[ci].orig], m.order)
			if err != nil {
				for _, s := range out {
					s.Release()
				}
				return nil, false, err
			}
			out = append(out, s)
		}
	}

	df, err := dataframe.New(out...)
	if err != nil {
		for _, s := range out {
			s.Release()
		}
		return nil, false, err
	}
	return df, true, nil
}
