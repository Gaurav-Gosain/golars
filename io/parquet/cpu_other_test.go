//go:build !unix

package parquet_test

import "testing"

func reportCPU(*testing.B) func() { return func() {} }
