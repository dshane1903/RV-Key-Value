package raft

import (
	"errors"
	"testing"
)

type memoryStore struct {
	state PersistentState
}

func (m *memoryStore) Save(state PersistentState) error {
	m.state = PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         cloneLog(state.Log),
	}
	return nil
}

func (m *memoryStore) Load() (PersistentState, error) {
	return PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         cloneLog(m.state.Log),
	}, nil
}

func TestStateTransitionsPersistTermAndVote(t *testing.T) {
	store := &memoryStore{}
	node, err := NewRaftNode("n1", []string{"n2", "n3"}, store)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	if got := node.State(); got != Follower {
		t.Fatalf("initial state = %s, want %s", got, Follower)
	}

	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if got := node.State(); got != Candidate {
		t.Fatalf("state = %s, want %s", got, Candidate)
	}
	if got := node.CurrentTerm(); got != 1 {
		t.Fatalf("term = %d, want 1", got)
	}
	if got := node.VotedFor(); got != "n1" {
		t.Fatalf("votedFor = %q, want n1", got)
	}

	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}
	if got := node.State(); got != Leader {
		t.Fatalf("state = %s, want %s", got, Leader)
	}

	if err := node.BecomeFollower(3); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	if got := node.State(); got != Follower {
		t.Fatalf("state = %s, want %s", got, Follower)
	}
	if got := store.state.CurrentTerm; got != 3 {
		t.Fatalf("persisted term = %d, want 3", got)
	}
	if got := store.state.VotedFor; got != "" {
		t.Fatalf("persisted vote = %q, want empty", got)
	}
}

func TestAppendEntriesRejectsInconsistentPreviousEntry(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeFollower(2); err != nil {
		t.Fatalf("become follower: %v", err)
	}
	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local: %v", err)
	}

	err = node.AppendEntries(1, 99, []LogEntry{{Term: 2, Command: []byte("set b 2")}})
	var inconsistent ErrLogInconsistent
	if !errors.As(err, &inconsistent) {
		t.Fatalf("append err = %v, want ErrLogInconsistent", err)
	}
}

func TestAppendEntriesTruncatesConflictsAndAppends(t *testing.T) {
	node, err := NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	if err := node.BecomeFollower(1); err != nil {
		t.Fatalf("become follower term 1: %v", err)
	}
	if _, err := node.AppendLocal([]byte("set a 1")); err != nil {
		t.Fatalf("append local 1: %v", err)
	}
	if err := node.BecomeFollower(2); err != nil {
		t.Fatalf("become follower term 2: %v", err)
	}
	if _, err := node.AppendLocal([]byte("set stale 1")); err != nil {
		t.Fatalf("append local 2: %v", err)
	}

	err = node.AppendEntries(1, 1, []LogEntry{
		{Term: 3, Command: []byte("set b 2")},
		{Term: 3, Command: []byte("set c 3")},
	})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}

	log := node.Log()
	if len(log) != 3 {
		t.Fatalf("log length = %d, want 3", len(log))
	}
	if got := string(log[1].Command); got != "set b 2" {
		t.Fatalf("entry 2 command = %q, want set b 2", got)
	}
	if got := log[2].Index; got != 3 {
		t.Fatalf("entry 3 index = %d, want 3", got)
	}
}

func TestIsLogAtLeastUpToDate(t *testing.T) {
	tests := []struct {
		name               string
		candidateLastIndex uint64
		candidateLastTerm  uint64
		localLastIndex     uint64
		localLastTerm      uint64
		want               bool
	}{
		{
			name:               "newer term wins",
			candidateLastIndex: 1,
			candidateLastTerm:  3,
			localLastIndex:     10,
			localLastTerm:      2,
			want:               true,
		},
		{
			name:               "older term loses",
			candidateLastIndex: 100,
			candidateLastTerm:  1,
			localLastIndex:     1,
			localLastTerm:      2,
			want:               false,
		},
		{
			name:               "same term longer log wins",
			candidateLastIndex: 5,
			candidateLastTerm:  2,
			localLastIndex:     4,
			localLastTerm:      2,
			want:               true,
		},
		{
			name:               "same term shorter log loses",
			candidateLastIndex: 3,
			candidateLastTerm:  2,
			localLastIndex:     4,
			localLastTerm:      2,
			want:               false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsLogAtLeastUpToDate(
				tt.candidateLastIndex,
				tt.candidateLastTerm,
				tt.localLastIndex,
				tt.localLastTerm,
			)
			if got != tt.want {
				t.Fatalf("got %t, want %t", got, tt.want)
			}
		})
	}
}
