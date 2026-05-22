package raft

import (
	"errors"
	"testing"
	"time"
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

func TestCommitReadySignalsLeaderCommit(t *testing.T) {
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

	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local: %v", err)
	}

	select {
	case <-node.CommitReady():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("commit ready signal timed out")
	}
}

func TestCommitReadySignalsFollowerCommitFromAppendEntries(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	resp, err := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		Entries:      []LogEntry{{Term: 1, Command: []byte("set a 1")}},
		LeaderCommit: 1,
	})
	if err != nil {
		t.Fatalf("handle append entries: %v", err)
	}
	if !resp.Success {
		t.Fatal("success = false, want true")
	}

	select {
	case <-node.CommitReady():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("commit ready signal timed out")
	}
}

func TestCompactRemovesAppliedLogPrefix(t *testing.T) {
	store := &memoryStore{}
	node, err := NewRaftNode("n1", []string{"n2"}, store)
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
		t.Fatalf("append first: %v", err)
	}
	second, err := node.AppendLocal([]byte("set b 2"))
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := node.RecordReplicationSuccess("n2", second.Index); err != nil {
		t.Fatalf("record replication: %v", err)
	}
	if err := node.ApplyCommitted(&fakeStateMachine{}); err != nil {
		t.Fatalf("apply committed: %v", err)
	}

	snapshot := []byte(`{"a":"b"}`)
	if err := node.Compact(snapshot); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if got := node.LastIncludedIndex(); got != second.Index {
		t.Fatalf("lastIncludedIndex = %d, want %d", got, second.Index)
	}
	if got := node.LastIncludedTerm(); got != second.Term {
		t.Fatalf("lastIncludedTerm = %d, want %d", got, second.Term)
	}
	if got := len(node.Log()); got != 0 {
		t.Fatalf("log length = %d, want 0", got)
	}
	if got := node.LastLogIndex(); got != second.Index {
		t.Fatalf("lastLogIndex = %d, want %d", got, second.Index)
	}
	if got := node.LastLogTerm(); got != second.Term {
		t.Fatalf("lastLogTerm = %d, want %d", got, second.Term)
	}
	if string(node.Snapshot()) != string(snapshot) {
		t.Fatalf("snapshot = %q, want %q", string(node.Snapshot()), string(snapshot))
	}
	if store.state.LastIncludedIndex != second.Index {
		t.Fatalf("persisted lastIncludedIndex = %d, want %d", store.state.LastIncludedIndex, second.Index)
	}
	if store.state.LastIncludedTerm != second.Term {
		t.Fatalf("persisted lastIncludedTerm = %d, want %d", store.state.LastIncludedTerm, second.Term)
	}
	if string(store.state.Snapshot) != string(snapshot) {
		t.Fatalf("persisted snapshot = %q, want %q", string(store.state.Snapshot), string(snapshot))
	}
}

func TestAppendLocalContinuesAfterSnapshotIndex(t *testing.T) {
	node, err := NewRaftNode("n1", nil, &memoryStore{state: PersistentState{
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
	}})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	entry, err := node.AppendLocal([]byte("set c 3"))
	if err != nil {
		t.Fatalf("append local: %v", err)
	}
	if entry.Index != 5 {
		t.Fatalf("entry index = %d, want 5", entry.Index)
	}
}

func TestNewRaftNodeRestoresSnapshotMetadata(t *testing.T) {
	snapshot := []byte(`{"name":"raft"}`)
	node, err := NewRaftNode("n1", nil, &memoryStore{state: PersistentState{
		CurrentTerm:       3,
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
		Snapshot:          snapshot,
		Log: []LogEntry{
			{Index: 5, Term: 3, Command: []byte("set c 3")},
		},
	}})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	if got := node.LastIncludedIndex(); got != 4 {
		t.Fatalf("lastIncludedIndex = %d, want 4", got)
	}
	if got := node.LastIncludedTerm(); got != 2 {
		t.Fatalf("lastIncludedTerm = %d, want 2", got)
	}
	if got := string(node.Snapshot()); got != string(snapshot) {
		t.Fatalf("snapshot = %q, want %q", got, string(snapshot))
	}
	if got := node.CommitIndex(); got != 4 {
		t.Fatalf("commitIndex = %d, want 4", got)
	}
	if got := node.LastApplied(); got != 4 {
		t.Fatalf("lastApplied = %d, want 4", got)
	}
	if got := node.LastLogIndex(); got != 5 {
		t.Fatalf("lastLogIndex = %d, want 5", got)
	}
	if got := node.LastLogTerm(); got != 3 {
		t.Fatalf("lastLogTerm = %d, want 3", got)
	}
}
