package raft

import (
	"math/rand"
	"testing"
	"time"
)

func TestRandomElectionTimeoutStaysInRaftRange(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 1_000; i++ {
		timeout := RandomElectionTimeout(r)
		if timeout < MinElectionTimeout {
			t.Fatalf("timeout = %s, below minimum %s", timeout, MinElectionTimeout)
		}
		if timeout > MaxElectionTimeout {
			t.Fatalf("timeout = %s, above maximum %s", timeout, MaxElectionTimeout)
		}
	}
}

func TestDefaultHeartbeatIntervalIsShorterThanElectionTimeout(t *testing.T) {
	if DefaultHeartbeatInterval >= MinElectionTimeout {
		t.Fatalf("heartbeat interval = %s, want less than %s", DefaultHeartbeatInterval, MinElectionTimeout)
	}
}

func TestRandomElectionTimeoutAllowsNilRand(t *testing.T) {
	timeout := RandomElectionTimeout(nil)
	if timeout < 150*time.Millisecond || timeout > 300*time.Millisecond {
		t.Fatalf("timeout = %s, want inside default range", timeout)
	}
}
