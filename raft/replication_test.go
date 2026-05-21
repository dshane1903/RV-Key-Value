package raft

import (
	"errors"
	"testing"
)

func TestBecomeLeaderInitializesReplicationProgress(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeFollower(1); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	if got := node.NextIndex("n2"); got != 2 {
		t.Fatalf("nextIndex[n2] = %d, want 2", got)
	}
	if got := node.MatchIndex("n2"); got != 0 {
		t.Fatalf("matchIndex[n2] = %d, want 0", got)
	}
}

func TestBuildAppendEntriesUsesFollowerNextIndex(t *testing.T) {
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
	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local: %v", err)
	}

	req, err := node.BuildAppendEntries("n2")
	if err != nil {
		t.Fatalf("build append entries: %v", err)
	}
	if req.PrevLogIndex != 0 {
		t.Fatalf("prevLogIndex = %d, want 0", req.PrevLogIndex)
	}
	if len(req.Entries) != 1 {
		t.Fatalf("entries length = %d, want 1", len(req.Entries))
	}
	if got := string(req.Entries[0].Command); got != "set a 1" {
		t.Fatalf("entry command = %q, want set a 1", got)
	}
	if req.LeaderCommit != 0 {
		t.Fatalf("leaderCommit = %d, want 0", req.LeaderCommit)
	}
}

func TestRecordReplicationFailureBacksOffNextIndex(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeFollower(1); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	if err := node.RecordReplicationFailure("n2"); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if got := node.NextIndex("n2"); got != 1 {
		t.Fatalf("nextIndex[n2] = %d, want 1", got)
	}
	if err := node.RecordReplicationFailure("n2"); err != nil {
		t.Fatalf("record second failure: %v", err)
	}
	if got := node.NextIndex("n2"); got != 1 {
		t.Fatalf("nextIndex[n2] after second failure = %d, want 1", got)
	}
}

func TestRecordReplicationSuccessAdvancesCommitForCurrentTermMajority(t *testing.T) {
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
	entry, err := node.AppendLocal([]byte("set a 1"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}
	if got := node.CommitIndex(); got != 0 {
		t.Fatalf("commitIndex before majority = %d, want 0", got)
	}

	if err := node.RecordReplicationSuccess("n2", entry.Index); err != nil {
		t.Fatalf("record success: %v", err)
	}
	if got := node.CommitIndex(); got != entry.Index {
		t.Fatalf("commitIndex = %d, want %d", got, entry.Index)
	}
	if got := node.NextIndex("n2"); got != entry.Index+1 {
		t.Fatalf("nextIndex[n2] = %d, want %d", got, entry.Index+1)
	}
	if got := node.MatchIndex("n2"); got != entry.Index {
		t.Fatalf("matchIndex[n2] = %d, want %d", got, entry.Index)
	}
}

func TestRecordReplicationSuccessDoesNotCommitPreviousTermEntry(t *testing.T) {
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeFollower(1); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	entry, err := node.AppendLocal([]byte("set old 1"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	if err := node.RecordReplicationSuccess("n2", entry.Index); err != nil {
		t.Fatalf("record success: %v", err)
	}
	if got := node.CommitIndex(); got != 0 {
		t.Fatalf("commitIndex = %d, want 0", got)
	}
}

func TestBuildAppendEntriesReturnsUnknownPeer(t *testing.T) {
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

	_, err = node.BuildAppendEntries("n4")
	var unknown ErrUnknownPeer
	if !errors.As(err, &unknown) {
		t.Fatalf("err = %v, want ErrUnknownPeer", err)
	}
}
