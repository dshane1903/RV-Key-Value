package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeAppendClient struct {
	mu        sync.Mutex
	responses map[string]AppendEntriesResponse
	requests  map[string][]AppendEntriesRequest
}

func (f *fakeAppendClient) AppendEntries(_ context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.requests == nil {
		f.requests = make(map[string][]AppendEntriesRequest)
	}
	f.requests[peerID] = append(f.requests[peerID], req)
	return f.responses[peerID], nil
}

func (f *fakeAppendClient) count(peerID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests[peerID])
}

func (f *fakeAppendClient) first(peerID string) AppendEntriesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[peerID][0]
}

func TestRunHeartbeatLoopSendsHeartbeatsToPeers(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeAppendClient{
		responses: map[string]AppendEntriesResponse{
			"n2": {Term: 1, Success: true},
			"n3": {Term: 1, Success: true},
		},
	}
	errs := make(chan error, 1)
	go func() {
		errs <- node.RunHeartbeatLoop(ctx, client, 5*time.Millisecond)
	}()

	waitForAppendCount(t, client, "n2", 1)
	waitForAppendCount(t, client, "n3", 1)
	cancel()

	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat err = %v, want context.Canceled", err)
	}

	req := client.first("n2")
	if req.Term != 1 {
		t.Fatalf("heartbeat term = %d, want 1", req.Term)
	}
	if req.LeaderID != "n1" {
		t.Fatalf("leader id = %q, want n1", req.LeaderID)
	}
	if len(req.Entries) != 0 {
		t.Fatalf("heartbeat entries = %d, want 0", len(req.Entries))
	}
}

func TestRunHeartbeatLoopStepsDownOnHigherTermResponse(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeAppendClient{
		responses: map[string]AppendEntriesResponse{
			"n2": {Term: 2, Success: false},
		},
	}
	errs := make(chan error, 1)
	go func() {
		errs <- node.RunHeartbeatLoop(ctx, client, 5*time.Millisecond)
	}()

	waitForState(t, node, Follower)
	cancel()

	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat err = %v, want context.Canceled", err)
	}
	if got := node.CurrentTerm(); got != 2 {
		t.Fatalf("term = %d, want 2", got)
	}
}

func waitForAppendCount(t *testing.T, client *fakeAppendClient, peerID string, want int) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := client.count(peerID); got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("append count for %s = %d, want at least %d", peerID, client.count(peerID), want)
}
