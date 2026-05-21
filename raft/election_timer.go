package raft

import (
	"context"
	"time"
)

type ElectionTimerConfig struct {
	NextTimeout func() time.Duration
	Reset       <-chan struct{}
}

func (n *RaftNode) RunElectionTimer(ctx context.Context, client VoteClient, cfg ElectionTimerConfig) error {
	nextTimeout := cfg.NextTimeout
	if nextTimeout == nil {
		nextTimeout = func() time.Duration {
			return RandomElectionTimeout(nil)
		}
	}

	for {
		timer := time.NewTimer(nextTimeout())
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-cfg.Reset:
			stopTimer(timer)
		case <-timer.C:
			if n.State() != Leader {
				if _, err := n.StartElection(ctx, client); err != nil {
					return err
				}
			}
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
