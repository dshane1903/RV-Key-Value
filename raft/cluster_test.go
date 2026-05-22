package raft

import (
	"context"
	"errors"
	"testing"
)

type testCluster struct {
	nodes map[string]*RaftNode
	down  map[string]bool
}

func newTestCluster(t *testing.T, ids ...string) *testCluster {
	t.Helper()

	cluster := &testCluster{
		nodes: make(map[string]*RaftNode, len(ids)),
		down:  make(map[string]bool),
	}
	for _, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, peerID := range ids {
			if peerID != id {
				peers = append(peers, peerID)
			}
		}

		node, err := NewRaftNode(id, peers, nil)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		cluster.nodes[id] = node
	}
	return cluster
}

func (c *testCluster) RequestVote(_ context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	if c.down[peerID] {
		return RequestVoteResponse{}, errors.New("peer unavailable")
	}
	return c.nodes[peerID].RequestVote(req)
}

func (c *testCluster) PreVote(_ context.Context, peerID string, req PreVoteRequest) (PreVoteResponse, error) {
	if c.down[peerID] {
		return PreVoteResponse{}, errors.New("peer unavailable")
	}
	return c.nodes[peerID].PreVote(req)
}

func (c *testCluster) AppendEntries(_ context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if c.down[peerID] {
		return AppendEntriesResponse{}, errors.New("peer unavailable")
	}
	return c.nodes[peerID].HandleAppendEntries(req)
}

func (c *testCluster) leaderIDs() []string {
	var leaders []string
	for id, node := range c.nodes {
		if !c.down[id] && node.State() == Leader {
			leaders = append(leaders, id)
		}
	}
	return leaders
}

func TestThreeNodeClusterElectsLeader(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}

	leaders := cluster.leaderIDs()
	if len(leaders) != 1 {
		t.Fatalf("leaders = %v, want exactly one", leaders)
	}
	if leaders[0] != "n1" {
		t.Fatalf("leader = %s, want n1", leaders[0])
	}
}

func TestThreeNodeClusterReElectsAfterLeaderUnavailable(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("first election: %v", err)
	}
	if !won {
		t.Fatal("first election won = false, want true")
	}
	if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	cluster.down["n1"] = true
	won, err = cluster.nodes["n2"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("second election: %v", err)
	}
	if !won {
		t.Fatal("second election won = false, want true")
	}

	leaders := cluster.leaderIDs()
	if len(leaders) != 1 {
		t.Fatalf("available leaders = %v, want exactly one", leaders)
	}
	if leaders[0] != "n2" {
		t.Fatalf("leader = %s, want n2", leaders[0])
	}
	if got := cluster.nodes["n2"].CurrentTerm(); got != 2 {
		t.Fatalf("new leader term = %d, want 2", got)
	}
}

func TestThreeNodeClusterReplicatesLeaderEntryToFollowers(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}

	entry, err := cluster.nodes["n1"].AppendLocal([]byte("set a 1"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}
	if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("send heartbeats: %v", err)
	}

	for _, id := range []string{"n2", "n3"} {
		log := cluster.nodes[id].Log()
		if len(log) != 1 {
			t.Fatalf("%s log length = %d, want 1", id, len(log))
		}
		if got := string(log[0].Command); got != "set a 1" {
			t.Fatalf("%s command = %q, want set a 1", id, got)
		}
	}
	if got := cluster.nodes["n1"].CommitIndex(); got != entry.Index {
		t.Fatalf("leader commitIndex = %d, want %d", got, entry.Index)
	}
}

func TestThreeNodeClusterStaleFollowerCatchesUp(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")

	if err := cluster.nodes["n1"].BecomeFollower(1); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	if _, err := cluster.nodes["n1"].AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local 1: %v", err)
	}
	if _, err := cluster.nodes["n1"].AppendLocal([]byte("set b 2")); err != nil {
		t.Fatalf("append local 2: %v", err)
	}
	if err := cluster.nodes["n1"].BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := cluster.nodes["n1"].BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
			t.Fatalf("send heartbeat %d: %v", i, err)
		}
	}

	for _, id := range []string{"n2", "n3"} {
		log := cluster.nodes[id].Log()
		if len(log) != 2 {
			t.Fatalf("%s log length = %d, want 2", id, len(log))
		}
		if got := string(log[1].Command); got != "set b 2" {
			t.Fatalf("%s second command = %q, want set b 2", id, got)
		}
	}
}
