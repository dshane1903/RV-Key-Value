package raft

import "fmt"

// State is the role a Raft node is currently playing.
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// LogEntry is the durable unit replicated through Raft.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

// PersistentState is the portion of node state that must survive restarts.
type PersistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// ErrLogInconsistent is returned when an append does not match the local log.
type ErrLogInconsistent struct {
	PrevLogIndex uint64
	PrevLogTerm  uint64
}

func (e ErrLogInconsistent) Error() string {
	return fmt.Sprintf("log is inconsistent at index %d term %d", e.PrevLogIndex, e.PrevLogTerm)
}

// StableStore persists and restores Raft's durable state.
type StableStore interface {
	Save(PersistentState) error
	Load() (PersistentState, error)
}

// IsLogAtLeastUpToDate implements Raft's RequestVote log freshness comparison.
func IsLogAtLeastUpToDate(candidateLastIndex, candidateLastTerm, localLastIndex, localLastTerm uint64) bool {
	if candidateLastTerm != localLastTerm {
		return candidateLastTerm > localLastTerm
	}
	return candidateLastIndex >= localLastIndex
}
