package main

import "testing"

func TestGCAbandonOnMaxAttempts(t *testing.T) {
	if gcAbandonOnMaxAttempts(true) {
		t.Fatal("strict mode must not abandon")
	}
	if !gcAbandonOnMaxAttempts(false) {
		t.Fatal("non-strict should abandon")
	}
}
