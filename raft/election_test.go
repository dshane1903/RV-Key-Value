package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeVoteClient struct {
	mu        sync.Mutex
	responses map[string]RequestVoteResponse
	errs      map[string]error
	requests  []RequestVoteRequest
}

func (f *fakeVoteClient) RequestVote(_ context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()

	if err := f.errs[peerID]; err != nil {
		return RequestVoteResponse{}, err
	}
	return f.responses[peerID], nil
}

func TestStartElectionBecomesLeaderWithMajority(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	client := &fakeVoteClient{
		responses: map[string]RequestVoteResponse{
			"n2": {Term: 1, VoteGranted: true},
			"n3": {Term: 1, VoteGranted: false},
		},
	}
	won, err := node.StartElection(context.Background(), client)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	if got := node.State(); got != Leader {
		t.Fatalf("state = %s, want %s", got, Leader)
	}
	if got := node.CurrentTerm(); got != 1 {
		t.Fatalf("term = %d, want 1", got)
	}
}

func TestStartElectionStepsDownOnHigherTermResponse(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	client := &fakeVoteClient{
		responses: map[string]RequestVoteResponse{
			"n2": {Term: 2, VoteGranted: false},
		},
	}
	won, err := node.StartElection(context.Background(), client)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if won {
		t.Fatal("won = true, want false")
	}
	if got := node.State(); got != Follower {
		t.Fatalf("state = %s, want %s", got, Follower)
	}
	if got := node.CurrentTerm(); got != 2 {
		t.Fatalf("term = %d, want 2", got)
	}
	if got := node.VotedFor(); got != "" {
		t.Fatalf("votedFor = %q, want empty", got)
	}
}

func TestStartElectionToleratesFailedPeer(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	client := &fakeVoteClient{
		responses: map[string]RequestVoteResponse{
			"n3": {Term: 1, VoteGranted: true},
		},
		errs: map[string]error{
			"n2": errors.New("peer unavailable"),
		},
	}
	won, err := node.StartElection(context.Background(), client)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	if got := node.State(); got != Leader {
		t.Fatalf("state = %s, want %s", got, Leader)
	}
}

func TestStartElectionRequestsVotesConcurrently(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	client := delayedVoteClient{
		delays: map[string]time.Duration{
			"n2": 300 * time.Millisecond,
		},
		responses: map[string]RequestVoteResponse{
			"n2": {Term: 1, VoteGranted: false},
			"n3": {Term: 1, VoteGranted: true},
		},
	}

	start := time.Now()
	won, err := node.StartElection(context.Background(), client)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	if elapsed := time.Since(start); elapsed >= 250*time.Millisecond {
		t.Fatalf("election took %s, slow peer appears to have blocked majority", elapsed)
	}
}

func TestStartElectionSingleNodeBecomesLeader(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	won, err := node.StartElection(context.Background(), &fakeVoteClient{})
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	if got := node.State(); got != Leader {
		t.Fatalf("state = %s, want %s", got, Leader)
	}
}

type delayedVoteClient struct {
	delays    map[string]time.Duration
	responses map[string]RequestVoteResponse
}

func (d delayedVoteClient) RequestVote(ctx context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	if delay := d.delays[peerID]; delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return RequestVoteResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
	return d.responses[peerID], nil
}
