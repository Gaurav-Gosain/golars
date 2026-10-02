//go:build !unix

package dataframe_test

import "testing"

func reportCPU(*testing.B) func() { return func() {} }
