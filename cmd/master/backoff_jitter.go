package main

import (
	"math/rand/v2"
	"time"
)

// backoffWithJitter returns base plus a random extra in [0, base*jitterFrac).
// jitterFrac <= 0 disables jitter.
func backoffWithJitter(base time.Duration, jitterFrac float64) time.Duration {
	if jitterFrac <= 0 || base <= 0 {
		return base
	}
	extra := float64(base) * jitterFrac * rand.Float64()
	if extra <= 0 {
		return base
	}
	return base + time.Duration(extra)
}
