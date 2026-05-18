package main

import "godfs/internal/observability"

// gcAbandonOnMaxAttempts reports whether delete-GC should drop a pending entry after max attempts.
// When strict mode is on, entries are retained and godfs_maint_gc_strict_hold_total is incremented.
func gcAbandonOnMaxAttempts(strict bool) bool {
	if strict {
		observability.IncGCStrictHold()
		return false
	}
	return true
}
