package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
)

func TestParsePeers(t *testing.T) {
	ids, addrs, err := parsePeers("n2=localhost:9002,n3=localhost:9003")
	if err != nil {
		t.Fatalf("parse peers: %v", err)
	}

	if len(ids) != 2 {
		t.Fatalf("ids length = %d, want 2", len(ids))
	}
	if ids[0] != "n2" || ids[1] != "n3" {
		t.Fatalf("ids = %v, want [n2 n3]", ids)
	}
	if got := addrs["n2"]; got != "localhost:9002" {
		t.Fatalf("n2 addr = %q, want localhost:9002", got)
	}
	if got := addrs["n3"]; got != "localhost:9003" {
		t.Fatalf("n3 addr = %q, want localhost:9003", got)
	}
}

func TestParsePeersRejectsInvalidPeer(t *testing.T) {
	_, _, err := parsePeers("n2")
	if err == nil {
		t.Fatal("parse peers err = nil, want error")
	}
}

func TestParsePeersRejectsDuplicatePeer(t *testing.T) {
	_, _, err := parsePeers("n2=localhost:9002,n2=localhost:9102")
	if err == nil {
		t.Fatal("parse peers err = nil, want error")
	}
}

func TestApplyCommittedLoopAppliesOnCommitSignal(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	stateMachine := &notifyingStateMachine{applied: make(chan raft.LogEntry, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- applyCommittedLoop(ctx, node, stateMachine)
	}()

	entry, err := node.AppendLocal([]byte("set a 1"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}

	select {
	case applied := <-stateMachine.applied:
		if applied.Index != entry.Index {
			t.Fatalf("applied index = %d, want %d", applied.Index, entry.Index)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for applied entry")
	}

	cancel()
	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("apply loop err = %v, want context.Canceled", err)
	}
}

type notifyingStateMachine struct {
	applied chan raft.LogEntry
}

func (n *notifyingStateMachine) Apply(entry raft.LogEntry) error {
	n.applied <- entry
	return nil
}

func (n *notifyingStateMachine) Snapshot() ([]byte, error) {
	return nil, nil
}

func (n *notifyingStateMachine) RestoreSnapshot([]byte) error {
	return nil
}
