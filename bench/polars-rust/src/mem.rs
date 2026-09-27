//! Per-workload memory accounting, mirroring cmd/bench/mem.go.
//!
//! A counting wrapper around the system allocator records the bytes and
//! allocations made while one call of a workload runs, plus the peak of
//! live bytes above the level at the start of the call. Counting is off
//! during the timed loop: the only cost there is one relaxed load of a
//! flag per allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};
use std::sync::Mutex;

pub struct Counting;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicI64 = AtomicI64::new(0);
static ALLOCS: AtomicI64 = AtomicI64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

#[inline]
fn on_alloc(size: usize) {
    if ENABLED.load(Relaxed) {
        let size = size as i64;
        ALLOC_BYTES.fetch_add(size, Relaxed);
        ALLOCS.fetch_add(1, Relaxed);
        let live = LIVE.fetch_add(size, Relaxed) + size;
        PEAK.fetch_max(live, Relaxed);
    }
}

#[inline]
fn on_free(size: usize) {
    if ENABLED.load(Relaxed) {
        LIVE.fetch_sub(size as i64, Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        on_alloc(layout.size());
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        on_alloc(layout.size());
        System.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_free(layout.size());
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A realloc counts as a fresh allocation of the new size plus a
        // free of the old block, matching how the Go runtime accounts a
        // grown slice.
        on_free(layout.size());
        on_alloc(new_size);
        System.realloc(ptr, layout, new_size)
    }
}

#[derive(Clone, Copy, Default)]
pub struct MemSample {
    pub alloc_bytes: i64,
    pub allocs: i64,
    pub peak_bytes: i64,
}

/// One sample per time_ns call, in call order.
pub static MEM_LOG: Mutex<Vec<MemSample>> = Mutex::new(Vec::new());

/// Runs f once with counting enabled and appends its sample to MEM_LOG.
/// Frees of memory allocated before the call can drive LIVE below zero;
/// the peak is taken relative to zero, so it is the high-water mark of
/// memory the call itself added.
pub fn measure<F: FnMut()>(mut f: F) {
    ALLOC_BYTES.store(0, Relaxed);
    ALLOCS.store(0, Relaxed);
    LIVE.store(0, Relaxed);
    PEAK.store(0, Relaxed);
    ENABLED.store(true, Relaxed);
    f();
    ENABLED.store(false, Relaxed);
    let s = MemSample {
        alloc_bytes: ALLOC_BYTES.load(Relaxed),
        allocs: ALLOCS.load(Relaxed),
        peak_bytes: PEAK.load(Relaxed),
    };
    MEM_LOG.lock().unwrap().push(s);
}
