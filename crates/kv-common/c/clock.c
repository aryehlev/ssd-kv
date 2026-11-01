/*
 * Clock reads for kv_common::clock (DESIGN §22, §23.10).
 *
 * std exposes no CLOCK_BOOTTIME reader, and the crate's only file allowed to
 * contain `unsafe` is src/ldouble.rs (DESIGN §24), which declares this
 * function. CLOCK_BOOTTIME counts time spent in suspend (leases must expire
 * across a VM pause); CLOCK_MONOTONIC drives timeouts; CLOCK_REALTIME is the
 * wall clock used only for TTLs and diagnostics.
 */
#define _GNU_SOURCE 1

#include <stdint.h>
#include <time.h>

#ifndef CLOCK_BOOTTIME
#define CLOCK_BOOTTIME 7
#endif

/* Clock selectors, mirrored in src/ldouble.rs. */
#define KV_CLOCK_MONOTONIC 0
#define KV_CLOCK_BOOTTIME 1
#define KV_CLOCK_REALTIME 2

/* Nanoseconds of the selected clock; 0 if the clock cannot be read (or, for
 * the wall clock, lies before 1970). CLOCK_BOOTTIME falls back to
 * CLOCK_MONOTONIC on kernels that lack it (both start at boot). */
uint64_t kv_clock_ns(int which) {
    struct timespec ts;
    clockid_t id;

    switch (which) {
    case KV_CLOCK_BOOTTIME: id = CLOCK_BOOTTIME; break;
    case KV_CLOCK_REALTIME: id = CLOCK_REALTIME; break;
    default: id = CLOCK_MONOTONIC; break;
    }
    if (clock_gettime(id, &ts) != 0) {
        if (which != KV_CLOCK_BOOTTIME || clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    }
    if (ts.tv_sec < 0) return 0;
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}
