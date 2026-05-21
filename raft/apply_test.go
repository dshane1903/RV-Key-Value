package raft

import (
	"errors"
	"testing"
)

type fakeStateMachine struct {
	applied []LogEntry
	err     error
}

func (f *fakeStateMachine) Apply(entry LogEntry) error {
	if f.err != nil {
		return f.err
	}
	f.applied = append(f.applied, entry)
	return nil
}

func TestApplyCommittedAppliesEntriesInOrder(t *testing.T) {
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
	first, err := node.AppendLocal([]byte("set a 1"))
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	second, err := node.AppendLocal([]byte("set b 2"))
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := node.RecordReplicationSuccess("n2", second.Index); err != nil {
		t.Fatalf("record success: %v", err)
	}

	sm := &fakeStateMachine{}
	if err := node.ApplyCommitted(sm); err != nil {
		t.Fatalf("apply committed: %v", err)
	}

	if len(sm.applied) != 2 {
		t.Fatalf("applied length = %d, want 2", len(sm.applied))
	}
	if sm.applied[0].Index != first.Index || sm.applied[1].Index != second.Index {
		t.Fatalf("applied indexes = [%d %d], want [%d %d]", sm.applied[0].Index, sm.applied[1].Index, first.Index, second.Index)
	}
	if got := node.LastApplied(); got != second.Index {
		t.Fatalf("lastApplied = %d, want %d", got, second.Index)
	}
}

func TestApplyCommittedStopsOnStateMachineError(t *testing.T) {
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
	entry, err := node.AppendLocal([]byte("set a 1"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}
	if got := node.CommitIndex(); got != entry.Index {
		t.Fatalf("commitIndex = %d, want %d", got, entry.Index)
	}

	wantErr := errors.New("apply failed")
	err = node.ApplyCommitted(&fakeStateMachine{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("apply err = %v, want %v", err, wantErr)
	}
	if got := node.LastApplied(); got != 0 {
		t.Fatalf("lastApplied = %d, want 0", got)
	}
}
