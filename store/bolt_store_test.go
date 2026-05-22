package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
)

func TestBoltRaftStorePersistsState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	first, err := NewBoltRaftStore(path, time.Second)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	state := raft.PersistentState{
		CurrentTerm:       7,
		VotedFor:          "n2",
		Peers:             []string{"n2", "n3"},
		SelfMember:        testBoolPtr(true),
		LastIncludedIndex: 3,
		LastIncludedTerm:  6,
		Snapshot:          []byte(`{"name":"raft"}`),
		Log: []raft.LogEntry{
			{Index: 4, Term: 6, Command: []byte("set a 1")},
			{Index: 5, Term: 7, Command: []byte("set b 2")},
		},
	}
	if err := first.Save(state); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	second, err := NewBoltRaftStore(path, time.Second)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer second.Close()

	loaded, err := second.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.CurrentTerm != state.CurrentTerm {
		t.Fatalf("term = %d, want %d", loaded.CurrentTerm, state.CurrentTerm)
	}
	if loaded.VotedFor != state.VotedFor {
		t.Fatalf("votedFor = %q, want %q", loaded.VotedFor, state.VotedFor)
	}
	if len(loaded.Peers) != len(state.Peers) || loaded.Peers[0] != "n2" || loaded.Peers[1] != "n3" {
		t.Fatalf("peers = %v, want %v", loaded.Peers, state.Peers)
	}
	if loaded.SelfMember == nil || !*loaded.SelfMember {
		t.Fatalf("selfMember = %v, want true", loaded.SelfMember)
	}
	if loaded.LastIncludedIndex != state.LastIncludedIndex {
		t.Fatalf("lastIncludedIndex = %d, want %d", loaded.LastIncludedIndex, state.LastIncludedIndex)
	}
	if loaded.LastIncludedTerm != state.LastIncludedTerm {
		t.Fatalf("lastIncludedTerm = %d, want %d", loaded.LastIncludedTerm, state.LastIncludedTerm)
	}
	if got := string(loaded.Snapshot); got != string(state.Snapshot) {
		t.Fatalf("snapshot = %q, want %q", got, string(state.Snapshot))
	}
	if got := string(loaded.Log[1].Command); got != "set b 2" {
		t.Fatalf("entry command = %q, want set b 2", got)
	}
}

func testBoolPtr(value bool) *bool {
	return &value
}
