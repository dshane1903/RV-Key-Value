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

// RequestVoteRequest is the Raft vote request used during leader election.
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse reports whether the candidate received this node's vote.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest is sent by leaders for heartbeats and log replication.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse reports whether the follower accepted the append.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// ErrLogInconsistent is returned when an append does not match the local log.
type ErrLogInconsistent struct {
	PrevLogIndex uint64
	PrevLogTerm  uint64
}

func (e ErrLogInconsistent) Error() string {
	return fmt.Sprintf("log is inconsistent at index %d term %d", e.PrevLogIndex, e.PrevLogTerm)
}

// ErrUnknownPeer is returned when leader replication state is missing a peer.
type ErrUnknownPeer struct {
	PeerID string
}

func (e ErrUnknownPeer) Error() string {
	return fmt.Sprintf("unknown peer %q", e.PeerID)
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
