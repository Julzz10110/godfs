package domain

import "testing"

func TestDefaultReplicaConfig(t *testing.T) {
	t.Parallel()
	c := DefaultReplicaConfig()
	if c.Factor != 3 {
		t.Fatalf("Factor: got %d want 3", c.Factor)
	}
}
