# Memory model

A columnar analytics library lives or dies by how it manages memory. Go's garbage collector is capable but unforgiving of allocation-heavy hot loops. This document describes the rules golars follows to keep memory predictable.

## Buffer ownership

All column data ultimately sits in `memory.Buffer` (from arrow-go). A buffer is a reference-counted, opaque handle to a contiguous byte region. We never copy buffer contents when we can share them.

Reference counting rules:

- Creating a `Series` from an `arrow.Array` retains the array's buffers.
- Cloning a `Series` shares buffers, not copies them.
- Slicing a `Series` shares buffers with an offset and length.
- A `DataFrame` holds retains on every Series it owns.
- Release happens through `Series.Release()` and `DataFrame.Release()`. `Series` and `DataFrame` do not register finalizers. With the default Go-backed allocators, a buffer that is never released is still reclaimed by the GC, but a pooled buffer then never goes back to the pool, so the next kernel call allocates fresh memory. Release explicitly, especially in tight loops. (The one finalizer in the engine is on the `lazy` Cache node, which releases its memoised frame when the cache state is collected.)

## Allocator

Every Series, array, and kernel output is allocated through a `memory.Allocator`. The default is `memory.DefaultAllocator`. Hot kernels route the default through the process-wide pooled allocator in `internal/mempool` (`compute.PoolingMem`), which recycles size-bucketed buffers across calls. For tests we use `memory.NewCheckedAllocator` (wrapped by `testutil.NewCheckedAllocator`), which fails the test on unreleased buffers; a checked allocator bypasses the pool so leak detection keeps working. Every test that constructs Series should use one and verify zero leaks at teardown.

Allocator choice flows through functional options, not a global: `compute.WithAllocator`, `series.WithAllocator`, the IO packages' `WithAllocator`, and `lazy.WithExecAllocator` for plan execution. Expression evaluation takes the allocator from the execution config of the surrounding `Collect`.

## Immutability

`Series` and `DataFrame` are immutable to the user. Every mutating-looking method returns a new value. Under the hood we exploit ref-counted buffer sharing so that `df.Rename("a", "b")` is O(1) and does not copy column data.

This buys us a few things:

- Safe concurrent reads without locks.
- Easier reasoning about plan transformations.
- The optimizer can reorder and eliminate subplans without worrying about side effects.

## The chunked model

A Series is a sequence of chunks. Each chunk is an `arrow.Array` of the same dtype. Properties:

- All chunks share one dtype and one validity bitmap format.
- Total length is the sum of chunk lengths.
- Chunk boundaries are an implementation detail. Kernels must not depend on specific chunk sizes for correctness, only for scheduling.

Why chunks:

- Natural unit of parallelism.
- Natural unit of streaming (a morsel is a DataFrame-shaped collection of chunks, one per column).
- Enables append-without-copy: appending two Series concatenates chunk lists instead of copying.

The downside is that kernels must iterate over chunks. Most kernels handle this by walking `s.Chunks()` or by consolidating a multi-chunk input into a single array first (`s.Consolidated()` returns one `arrow.Array`; `s.Rechunk()` returns a single-chunk Series).

## Null masks

Arrow's validity bitmap is one bit per row, ones for valid, zeros for null. golars never materializes a null to a sentinel value. All kernels operate on `(data, bitmap)` pairs. Aggregations skip null positions. Comparisons propagate null per polars' semantics: `null == null` is null, `null < 1` is null, and so on.

Bitmaps are shared via buffer refcounts just like data buffers.

## Hot-loop allocation rules

Performance work follows a small set of rules:

1. **No allocation in inner loops.** Pre-allocate result buffers sized to the input. Use `memory.Allocator.Allocate(n)` once per chunk, not per row.
2. **No interface boxing in inner loops.** Hot kernels dispatch on dtype once at the outer level and then work on concrete `[]T` slices. The dtype-specialized kernels are hand-written (Go generics plus per-dtype type switches), not generated, so there is no interface dispatch cost per row.
3. **Avoid map operations in inner loops.** The int64-keyed hash paths in group-by and join use the dedicated open-addressing map in `internal/intmap` instead of `map[int64]int32`. String keys are dictionary-encoded without copying (`dataframe/strcodes.go`): keys of up to 7 bytes are packed into a uint64 and looked up in an integer table, longer ones go through a pointer-free table of views into the arrow data buffer (`dataframe/strtable.go`, hashed with `internal/strhash`), and the string-key hash join uses partitioned open-addressing tables (`dataframe/join_str.go`). Some less common join key types still use Go maps.
4. **Reuse buffers across calls.** Output buffers come from the pooled allocator in `internal/mempool`, so back-to-back kernel calls (and successive morsels in the streaming executor) reuse the same size-bucketed backing slices instead of hitting `mallocgc`.
5. **Bounded per-operator memory (planned).** Operators do not yet declare a memory budget and there is no spill to disk. Pipeline breakers (sort, group-by, join) materialise their full input in memory.

## Cross-operator sharing

Projection pushdown and common subexpression elimination mean that the same underlying column appears in multiple operator outputs. We never copy: the output Series of a projection shares buffers with the input. Refcounting ensures correctness.

## GC pressure management

Go's GC is concurrent and low-latency, but allocation pressure still drives pause frequency and throughput cost. golars keeps pressure low by:

- Working in large `[]T` slices instead of many small objects.
- Using `sync.Pool` for short-lived per-morsel scratch buffers (hash temp arrays, partition index buffers).
- Avoiding string allocation on the hot path. String columns are kept in arrow's native offset-plus-buffer layout and operated on as byte slices.
- Keeping the `Series` struct small (a name plus a pointer to an `arrow.Chunked`) so wrappers are cheap to create and share.

Allocation regressions are caught with `go test -benchmem` and by checking pprof profiles for `mallocgc` in hot kernels. CI runs the race detector over the hot packages.

## A note on off-heap

We do not use off-heap memory (mmap backed by anonymous regions) by default. arrow-go's `memory.GoAllocator` returns Go-managed slices. We switch to `memory.CgoArrowAllocator` only if profiling shows GC overhead is a problem on real workloads, and only if we decide to relax the no-cgo constraint. For now, staying on-heap is simpler and fast enough.

Out-of-core execution (spill to disk) is not implemented. If it lands, it would be a separate mechanism from off-heap allocation.
