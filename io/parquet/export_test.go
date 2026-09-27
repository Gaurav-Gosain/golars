package parquet

// WithoutNative forces the pqarrow reader and writer.
func WithoutNative() Option { return func(c *config) { c.noNative = true } }

// PqarrowReads returns the number of columns and whole reads that the
// native reader handed to pqarrow so far.
func PqarrowReads() int64 { return pqarrowReads.Load() }
