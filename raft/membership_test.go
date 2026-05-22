package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMembershipChangeAddsPeerAfterCommit(t *testing.T) {
	cluster := &testCluster{
		nodes: map[string]*RaftNode{},
		down:  map[string]bool{},
	}
	cluster.nodes["n1"] = mustNewNode(t, "n1", []string{"n2"})
	cluster.nodes["n2"] = mustNewNode(t, "n2", []string{"n1"})
	cluster.nodes["n3"] = mustNewNode(t, "n3", []string{"n1", "n2"})

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}

	command, err := EncodeMembershipChange(MembershipChange{Type: MembershipAddPeer, PeerID: "n3"})
	if err != nil {
		t.Fatalf("encode membership change: %v", err)
	}
	if _, err := cluster.nodes["n1"].ProposeWithRetryInterval(context.Background(), cluster, command, time.Millisecond); err != nil {
		t.Fatalf("propose add peer: %v", err)
	}
	if err := cluster.nodes["n1"].ApplyCommitted(&fakeStateMachine{}); err != nil {
		t.Fatalf("apply leader membership: %v", err)
	}

	if got := cluster.nodes["n1"].Peers(); !samePeers(got, []string{"n2", "n3"}) {
		t.Fatalf("leader peers = %v, want [n2 n3]", got)
	}

	if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("replicate add commit: %v", err)
	}
	if err := cluster.nodes["n2"].ApplyCommitted(&fakeStateMachine{}); err != nil {
		t.Fatalf("apply follower membership: %v", err)
	}
	if got := cluster.nodes["n2"].Peers(); !samePeers(got, []string{"n1", "n3"}) {
		t.Fatalf("follower peers = %v, want [n1 n3]", got)
	}
}

func TestMembershipChangeRemovesPeerAndRejectsFutureVotes(t *testing.T) {
	cluster := newTestCluster(t, "n1", "n2", "n3")

	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}

	command, err := EncodeMembershipChange(MembershipChange{Type: MembershipRemovePeer, PeerID: "n3"})
	if err != nil {
		t.Fatalf("encode membership change: %v", err)
	}
	if _, err := cluster.nodes["n1"].ProposeWithRetryInterval(context.Background(), cluster, command, time.Millisecond); err != nil {
		t.Fatalf("propose remove peer: %v", err)
	}
	if err := cluster.nodes["n1"].ApplyCommitted(&fakeStateMachine{}); err != nil {
		t.Fatalf("apply leader membership: %v", err)
	}
	if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("replicate remove commit: %v", err)
	}
	if err := cluster.nodes["n2"].ApplyCommitted(&fakeStateMachine{}); err != nil {
		t.Fatalf("apply follower membership: %v", err)
	}

	if got := cluster.nodes["n1"].Peers(); !samePeers(got, []string{"n2"}) {
		t.Fatalf("leader peers = %v, want [n2]", got)
	}
	if got := cluster.nodes["n2"].Peers(); !samePeers(got, []string{"n1"}) {
		t.Fatalf("follower peers = %v, want [n1]", got)
	}

	won, err = cluster.nodes["n3"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("removed node election: %v", err)
	}
	if won {
		t.Fatal("removed node won election, want rejection by current members")
	}
}

func TestPendingMembershipChangeRejectsSecondChange(t *testing.T) {
	node := mustNewNode(t, "n1", []string{"n2"})
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	first, err := EncodeMembershipChange(MembershipChange{Type: MembershipAddPeer, PeerID: "n3"})
	if err != nil {
		t.Fatalf("encode first change: %v", err)
	}
	if _, err := node.AppendLocal(first); err != nil {
		t.Fatalf("append first change: %v", err)
	}

	second, err := EncodeMembershipChange(MembershipChange{Type: MembershipRemovePeer, PeerID: "n2"})
	if err != nil {
		t.Fatalf("encode second change: %v", err)
	}
	_, err = node.AppendLocal(second)
	var pending ErrMembershipChangePending
	if !errors.As(err, &pending) {
		t.Fatalf("append second err = %v, want ErrMembershipChangePending", err)
	}
}

func TestMembershipRemovalToSingleNodeStillRejectsRemovedPeerVote(t *testing.T) {
	node := mustNewNode(t, "n1", []string{"n2"})
	if err := node.ApplyMembershipChange(MembershipChange{Type: MembershipRemovePeer, PeerID: "n2"}); err != nil {
		t.Fatalf("remove peer: %v", err)
	}

	resp, err := node.RequestVote(RequestVoteRequest{
		Term:        2,
		CandidateID: "n2",
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatal("voteGranted = true, want false")
	}
}

func mustNewNode(t *testing.T, id string, peers []string) *RaftNode {
	t.Helper()

	node, err := NewRaftNode(id, peers, nil)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	return node
}

func samePeers(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
