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
		CurrentTerm: 7,
		VotedFor:    "n2",
		Log: []raft.LogEntry{
			{Index: 1, Term: 6, Command: []byte("set a 1")},
			{Index: 2, Term: 7, Command: []byte("set b 2")},
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
	if got := string(loaded.Log[1].Command); got != "set b 2" {
		t.Fatalf("entry command = %q, want set b 2", got)
	}
}
