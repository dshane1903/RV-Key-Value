package raftgrpc

import (
	"context"
	"testing"

	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
)

func TestServerRequestVoteDelegatesToNode(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	server := NewServer(node)

	resp, err := server.RequestVote(context.Background(), &raftkvpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "n2",
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if !resp.GetVoteGranted() {
		t.Fatal("vote granted = false, want true")
	}
	if got := node.VotedFor(); got != "n2" {
		t.Fatalf("node votedFor = %q, want n2", got)
	}
}

func TestServerAppendEntriesDelegatesToNode(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	server := NewServer(node)

	resp, err := server.AppendEntries(context.Background(), &raftkvpb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "n2",
		Entries: []*raftkvpb.LogEntry{
			{Term: 1, Command: []byte("set a 1")},
		},
	})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatal("success = false, want true")
	}
	if got := len(node.Log()); got != 1 {
		t.Fatalf("log length = %d, want 1", got)
	}
}

func TestServerAppendEntriesNotifiesElectionReset(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	reset := make(chan struct{}, 1)
	server := NewServerWithAppendReset(node, reset)
	resp, err := server.AppendEntries(context.Background(), &raftkvpb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "n2",
	})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatal("success = false, want true")
	}

	select {
	case <-reset:
	default:
		t.Fatal("append reset was not notified")
	}
}
