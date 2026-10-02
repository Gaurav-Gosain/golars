#!/bin/sh
# Regenerates eval/core_parity_gen_test.go from the pinned polars in
# bench/polars-compare. Run from anywhere inside the repository.
set -e
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root/bench/polars-compare"
uv run python "$root/eval/testdata/core_parity_gen.py" > "$root/eval/core_parity_gen_test.go.tmp"
mv "$root/eval/core_parity_gen_test.go.tmp" "$root/eval/core_parity_gen_test.go"
gofmt -w "$root/eval/core_parity_gen_test.go"
