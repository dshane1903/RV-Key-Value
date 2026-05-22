package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProposeCommitsSingleNodeEntry(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	entry, err := node.Propose(context.Background(), nil, []byte("set a 1"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if entry.Index != 1 {
		t.Fatalf("entry index = %d, want 1", entry.Index)
	}
	if got := node.CommitIndex(); got != entry.Index {
		t.Fatalf("commitIndex = %d, want %d", got, entry.Index)
	}
}

func TestProposeRequiresLeader(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	_, err = node.Propose(context.Background(), nil, []byte("set a 1"))
	var notLeader ErrNotLeader
	if !errors.As(err, &notLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestProposeReplicatesToMajority(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")
	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}

	entry, err := cluster.nodes["n1"].ProposeWithRetryInterval(context.Background(), cluster, []byte("set a 1"), time.Millisecond)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if got := cluster.nodes["n1"].CommitIndex(); got != entry.Index {
		t.Fatalf("commitIndex = %d, want %d", got, entry.Index)
	}

	replicatedFollowers := 0
	for _, id := range []string{"n2", "n3"} {
		log := cluster.nodes[id].Log()
		if len(log) == 0 {
			continue
		}
		if got := string(log[0].Command); got != "set a 1" {
			t.Fatalf("%s command = %q, want set a 1", id, got)
		}
		replicatedFollowers++
	}
	if replicatedFollowers == 0 {
		t.Fatal("replicated followers = 0, want at least one follower for majority")
	}
}

func TestProposeReturnsContextErrorWithoutMajority(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")
	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	cluster.down["n2"] = true
	cluster.down["n3"] = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	entry, err := cluster.nodes["n1"].ProposeWithRetryInterval(ctx, cluster, []byte("set a 1"), time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context deadline exceeded", err)
	}
	if entry.Index != 1 {
		t.Fatalf("entry index = %d, want 1", entry.Index)
	}
	if got := cluster.nodes["n1"].CommitIndex(); got != 0 {
		t.Fatalf("commitIndex = %d, want 0", got)
	}
}
