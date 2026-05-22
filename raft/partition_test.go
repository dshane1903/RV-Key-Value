package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

type partitionCluster struct {
	nodes      map[string]*RaftNode
	partitions map[string]map[string]bool
}

func newPartitionCluster(t *testing.T, ids ...string) *partitionCluster {
	t.Helper()

	cluster := &partitionCluster{
		nodes:      make(map[string]*RaftNode, len(ids)),
		partitions: make(map[string]map[string]bool),
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

func (c *partitionCluster) RequestVote(_ context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	if c.isPartitioned(req.CandidateID, peerID) {
		return RequestVoteResponse{}, errors.New("network partition")
	}
	return c.nodes[peerID].RequestVote(req)
}

func (c *partitionCluster) AppendEntries(_ context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if c.isPartitioned(req.LeaderID, peerID) {
		return AppendEntriesResponse{}, errors.New("network partition")
	}
	return c.nodes[peerID].HandleAppendEntries(req)
}

func (c *partitionCluster) isolate(id string) {
	for peerID := range c.nodes {
		if peerID == id {
			continue
		}
		c.partition(id, peerID)
	}
}

func (c *partitionCluster) heal() {
	c.partitions = make(map[string]map[string]bool)
}

func (c *partitionCluster) partition(a, b string) {
	if c.partitions[a] == nil {
		c.partitions[a] = make(map[string]bool)
	}
	if c.partitions[b] == nil {
		c.partitions[b] = make(map[string]bool)
	}
	c.partitions[a][b] = true
	c.partitions[b][a] = true
}

func (c *partitionCluster) isPartitioned(a, b string) bool {
	return c.partitions[a][b] || c.partitions[b][a]
}

func TestMajorityPartitionElectsNewLeaderAndOldLeaderStepsDownAfterHeal(t *testing.T) {
	cluster := newPartitionCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("first election: %v", err)
	}
	if !won {
		t.Fatal("first election won = false, want true")
	}

	cluster.isolate("n1")
	won, err = cluster.nodes["n2"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("majority election: %v", err)
	}
	if !won {
		t.Fatal("majority election won = false, want true")
	}
	if got := cluster.nodes["n2"].State(); got != Leader {
		t.Fatalf("n2 state = %s, want leader", got)
	}
	if got := cluster.nodes["n1"].State(); got != Leader {
		t.Fatalf("isolated n1 state = %s, want still leader before heal", got)
	}

	cluster.heal()
	if err := cluster.nodes["n2"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("replicate after heal: %v", err)
	}
	if got := cluster.nodes["n1"].State(); got != Follower {
		t.Fatalf("n1 state after heal = %s, want follower", got)
	}
	if got := cluster.nodes["n1"].CurrentTerm(); got != cluster.nodes["n2"].CurrentTerm() {
		t.Fatalf("n1 term = %d, want %d", got, cluster.nodes["n2"].CurrentTerm())
	}
}

func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	cluster := newPartitionCluster(t, "n1", "n2", "n3")
	cluster.isolate("n1")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("minority election: %v", err)
	}
	if won {
		t.Fatal("minority election won = true, want false")
	}
	if got := cluster.nodes["n1"].State(); got != Candidate {
		t.Fatalf("n1 state = %s, want candidate", got)
	}
}

func TestOldLeaderCatchesUpAfterPartitionHeals(t *testing.T) {
	cluster := newPartitionCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("first election: %v", err)
	}
	if !won {
		t.Fatal("first election won = false, want true")
	}

	cluster.isolate("n1")
	won, err = cluster.nodes["n2"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("majority election: %v", err)
	}
	if !won {
		t.Fatal("majority election won = false, want true")
	}

	entry, err := cluster.nodes["n2"].ProposeWithRetryInterval(context.Background(), cluster, []byte("set partition 1"), time.Millisecond)
	if err != nil {
		t.Fatalf("propose in majority partition: %v", err)
	}
	if got := cluster.nodes["n2"].CommitIndex(); got != entry.Index {
		t.Fatalf("new leader commitIndex = %d, want %d", got, entry.Index)
	}
	if got := len(cluster.nodes["n1"].Log()); got != 0 {
		t.Fatalf("isolated old leader log length = %d, want 0 before heal", got)
	}

	cluster.heal()
	if err := cluster.nodes["n2"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("replicate after heal: %v", err)
	}

	if got := cluster.nodes["n1"].State(); got != Follower {
		t.Fatalf("old leader state = %s, want follower", got)
	}
	log := cluster.nodes["n1"].Log()
	if len(log) != 1 {
		t.Fatalf("old leader log length = %d, want 1", len(log))
	}
	if got := string(log[0].Command); got != "set partition 1" {
		t.Fatalf("old leader command = %q, want set partition 1", got)
	}
	if got := cluster.nodes["n1"].CommitIndex(); got != entry.Index {
		t.Fatalf("old leader commitIndex = %d, want %d", got, entry.Index)
	}
}
