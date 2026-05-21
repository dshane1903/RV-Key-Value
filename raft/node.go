package raft

import "sync"

// RaftNode contains the local state needed for Raft consensus.
type RaftNode struct {
	mu sync.RWMutex

	id    string
	peers []string
	state State

	currentTerm uint64
	votedFor    string
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	store StableStore
}

// NewRaftNode builds a node from persisted state when a store is provided.
func NewRaftNode(id string, peers []string, store StableStore) (*RaftNode, error) {
	node := &RaftNode{
		id:    id,
		peers: append([]string(nil), peers...),
		state: Follower,
		store: store,
	}

	if store != nil {
		state, err := store.Load()
		if err != nil {
			return nil, err
		}
		node.currentTerm = state.CurrentTerm
		node.votedFor = state.VotedFor
		node.log = cloneLog(state.Log)
	}

	return node, nil
}

func (n *RaftNode) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *RaftNode) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *RaftNode) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *RaftNode) VotedFor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.votedFor
}

func (n *RaftNode) Log() []LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return cloneLog(n.log)
}

func (n *RaftNode) LastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return lastLogIndex(n.log)
}

func (n *RaftNode) LastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return lastLogTerm(n.log)
}

func (n *RaftNode) BecomeFollower(term uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Follower
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	return n.persistLocked()
}

func (n *RaftNode) BecomeCandidate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	return n.persistLocked()
}

func (n *RaftNode) BecomeLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Leader
	return n.persistLocked()
}

func (n *RaftNode) SetVotedFor(candidateID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.votedFor = candidateID
	return n.persistLocked()
}

func (n *RaftNode) RequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := RequestVoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}

	changed := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
		changed = true
	}

	resp.Term = n.currentTerm
	canVoteForCandidate := n.votedFor == "" || n.votedFor == req.CandidateID
	logIsFreshEnough := IsLogAtLeastUpToDate(req.LastLogIndex, req.LastLogTerm, lastLogIndex(n.log), lastLogTerm(n.log))
	if canVoteForCandidate && logIsFreshEnough {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		changed = true
	}

	if !changed {
		return resp, nil
	}
	return resp, n.persistLocked()
}

func (n *RaftNode) AppendLocal(command []byte) (LogEntry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log) + 1),
		Command: append([]byte(nil), command...),
	}
	n.log = append(n.log, entry)
	return entry, n.persistLocked()
}

// AppendEntries applies the Raft log consistency rule and appends entries after prevLogIndex.
func (n *RaftNode) AppendEntries(prevLogIndex, prevLogTerm uint64, entries []LogEntry) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if prevLogIndex > 0 {
		if prevLogIndex > uint64(len(n.log)) || n.log[prevLogIndex-1].Term != prevLogTerm {
			return ErrLogInconsistent{PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
		}
	}

	next := prevLogIndex + 1
	for i, entry := range entries {
		entry.Index = next + uint64(i)
		if entry.Index <= uint64(len(n.log)) {
			local := n.log[entry.Index-1]
			if local.Term != entry.Term {
				n.log = n.log[:entry.Index-1]
				n.log = append(n.log, normalizeEntries(entry.Index, entries[i:])...)
				return n.persistLocked()
			}
			continue
		}

		n.log = append(n.log, normalizeEntries(entry.Index, entries[i:])...)
		return n.persistLocked()
	}

	return n.persistLocked()
}

func (n *RaftNode) persistLocked() error {
	if n.store == nil {
		return nil
	}

	return n.store.Save(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         cloneLog(n.log),
	})
}

func normalizeEntries(startIndex uint64, entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, entry := range entries {
		entry.Index = startIndex + uint64(i)
		entry.Command = append([]byte(nil), entry.Command...)
		out[i] = entry
	}
	return out
}

func cloneLog(log []LogEntry) []LogEntry {
	out := make([]LogEntry, len(log))
	for i, entry := range log {
		entry.Command = append([]byte(nil), entry.Command...)
		out[i] = entry
	}
	return out
}

func lastLogIndex(log []LogEntry) uint64 {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Index
}

func lastLogTerm(log []LogEntry) uint64 {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Term
}
