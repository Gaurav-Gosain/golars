package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// TakeInt32 is Take for int32 row indices, the width group-by and
// dictionary encoders keep their first rows in. String columns gather
// straight from the int32 indices; other dtypes widen the indices and
// go through Take. Out-of-range indices return an error.
func TakeInt32(ctx context.Context, s *series.Series, indices []int32, opts ...Option) (_ *series.Series, err error) {
	if s.DType().ID() != arrow.STRING {
		return Take(ctx, s, widenIndices(indices), opts...)
	}
	cfg := resolve(opts)
	n := s.Len()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("compute.Take: index out of range for Series of length %d (%v)", n, r)
		}
	}()
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	return takeStringIdx(cfg.outName(s.Name()), arr.(*array.String), indices, cfg.alloc)
}
