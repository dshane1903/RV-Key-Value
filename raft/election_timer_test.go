package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunElectionTimerStartsElectionAfterTimeout(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		errs <- node.RunElectionTimer(ctx, &fakeVoteClient{}, ElectionTimerConfig{
			NextTimeout: fixedTimeout(5 * time.Millisecond),
		})
	}()

	waitForState(t, node, Leader)
	cancel()

	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("timer err = %v, want context.Canceled", err)
	}
}

func TestRunElectionTimerResetDelaysElection(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reset := make(chan struct{}, 1)
	errs := make(chan error, 1)
	go func() {
		errs <- node.RunElectionTimer(ctx, &fakeVoteClient{}, ElectionTimerConfig{
			NextTimeout: fixedTimeout(30 * time.Millisecond),
			Reset:       reset,
		})
	}()

	time.Sleep(15 * time.Millisecond)
	reset <- struct{}{}
	time.Sleep(20 * time.Millisecond)
	if got := node.State(); got != Follower {
		t.Fatalf("state = %s, want %s before reset timeout elapses", got, Follower)
	}

	waitForState(t, node, Leader)
	cancel()

	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("timer err = %v, want context.Canceled", err)
	}
}

func fixedTimeout(timeout time.Duration) func() time.Duration {
	return func() time.Duration {
		return timeout
	}
}

func waitForState(t *testing.T, node *RaftNode, want State) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := node.State(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state = %s, want %s", node.State(), want)
}
