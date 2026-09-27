//go:build !unix

package csv_test

import "testing"

func reportCPU(*testing.B) func() { return func() {} }
