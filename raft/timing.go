package raft

import (
	"math/rand"
	"time"
)

const (
	MinElectionTimeout       = 150 * time.Millisecond
	MaxElectionTimeout       = 300 * time.Millisecond
	DefaultHeartbeatInterval = 50 * time.Millisecond
)

func RandomElectionTimeout(r *rand.Rand) time.Duration {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	delta := MaxElectionTimeout - MinElectionTimeout
	return MinElectionTimeout + time.Duration(r.Int63n(int64(delta)+1))
}
