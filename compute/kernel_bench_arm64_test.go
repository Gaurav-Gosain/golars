//go:build arm64 && !noasm

package compute

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkParSumInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024} {
		iv := make([]int64, n)
		for i := range iv {
			iv[i] = int64(i)
		}
		for _, par := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("n=%d/par=%d", n, par), func(b *testing.B) {
				b.SetBytes(int64(n * 8))
				for b.Loop() {
					if par == 1 {
						_ = simdSumInt64(iv)
					} else {
						_, _ = parallelSIMDSumInt64(ctx, iv, par)
					}
				}
			})
		}
	}
}

var kernelSizes = []int{16 * 1024, 256 * 1024, 1024 * 1024}

func BenchmarkKernelNEON(b *testing.B) {
	for _, n := range kernelSizes {
		iv := make([]int64, n)
		fv := make([]float64, n)
		for i := range iv {
			iv[i] = int64(i*7919) & 0xFFFFF
			fv[i] = float64(iv[i]) * 0.5
		}
		b.Run(fmt.Sprintf("SumInt64/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for b.Loop() {
				_ = simdSumInt64NEON(iv)
			}
		})
		b.Run(fmt.Sprintf("SumFloat64/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for b.Loop() {
				_ = simdSumFloat64NEON(fv)
			}
		})
		b.Run(fmt.Sprintf("MinFloat64/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for b.Loop() {
				_, _ = simdMinFloat64NEON(fv)
			}
		})
	}
}
