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
