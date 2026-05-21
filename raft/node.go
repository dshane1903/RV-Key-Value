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

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

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

func (n *RaftNode) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *RaftNode) NextIndex(peerID string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.nextIndex[peerID]
}

func (n *RaftNode) MatchIndex(peerID string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.matchIndex[peerID]
}

func (n *RaftNode) BecomeFollower(term uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Follower
	n.nextIndex = nil
	n.matchIndex = nil
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
	n.nextIndex = nil
	n.matchIndex = nil
	n.currentTerm++
	n.votedFor = n.id
	return n.persistLocked()
}

func (n *RaftNode) BecomeLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Leader
	n.initLeaderProgressLocked()
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
		n.nextIndex = nil
		n.matchIndex = nil
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

func (n *RaftNode) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}

	changed := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.nextIndex = nil
		n.matchIndex = nil
		changed = true
	}
	if n.state != Follower {
		n.nextIndex = nil
		n.matchIndex = nil
	}
	n.state = Follower

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) || n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			resp.Term = n.currentTerm
			if changed {
				return resp, n.persistLocked()
			}
			return resp, nil
		}
	}

	next := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		entry.Index = next + uint64(i)
		if entry.Index <= uint64(len(n.log)) {
			local := n.log[entry.Index-1]
			if local.Term != entry.Term {
				n.log = n.log[:entry.Index-1]
				n.log = append(n.log, normalizeEntries(entry.Index, req.Entries[i:])...)
				changed = true
				break
			}
			continue
		}

		n.log = append(n.log, normalizeEntries(entry.Index, req.Entries[i:])...)
		changed = true
		break
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, lastLogIndex(n.log))
	}

	resp.Term = n.currentTerm
	resp.Success = true
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
	if n.state == Leader {
		n.advanceCommitLocked()
	}
	return entry, n.persistLocked()
}

func (n *RaftNode) BuildAppendEntries(peerID string) (AppendEntriesRequest, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	next, ok := n.nextIndex[peerID]
	if !ok {
		return AppendEntriesRequest{}, ErrUnknownPeer{PeerID: peerID}
	}

	prevLogIndex := next - 1
	return AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  logTermAt(n.log, prevLogIndex),
		Entries:      cloneEntriesFrom(n.log, next),
		LeaderCommit: n.commitIndex,
	}, nil
}

func (n *RaftNode) RecordReplicationSuccess(peerID string, matchIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.nextIndex[peerID]; !ok {
		return ErrUnknownPeer{PeerID: peerID}
	}

	n.matchIndex[peerID] = matchIndex
	n.nextIndex[peerID] = matchIndex + 1
	n.advanceCommitLocked()
	return nil
}

func (n *RaftNode) RecordReplicationFailure(peerID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	next, ok := n.nextIndex[peerID]
	if !ok {
		return ErrUnknownPeer{PeerID: peerID}
	}
	if next > 1 {
		n.nextIndex[peerID] = next - 1
	}
	return nil
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

func (n *RaftNode) initLeaderProgressLocked() {
	next := lastLogIndex(n.log) + 1
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, peerID := range n.peers {
		n.nextIndex[peerID] = next
		n.matchIndex[peerID] = 0
	}
	n.advanceCommitLocked()
}

func (n *RaftNode) advanceCommitLocked() {
	if n.state != Leader {
		return
	}

	for index := lastLogIndex(n.log); index > n.commitIndex; index-- {
		if logTermAt(n.log, index) != n.currentTerm {
			continue
		}

		replicated := 1
		for _, peerID := range n.peers {
			if n.matchIndex[peerID] >= index {
				replicated++
			}
		}
		if replicated >= majority(len(n.peers)+1) {
			n.commitIndex = index
			return
		}
	}
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

func cloneEntriesFrom(log []LogEntry, startIndex uint64) []LogEntry {
	if startIndex == 0 {
		startIndex = 1
	}
	if startIndex > uint64(len(log)) {
		return nil
	}
	return cloneLog(log[startIndex-1:])
}

func lastLogIndex(log []LogEntry) uint64 {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Index
}

func logTermAt(log []LogEntry, index uint64) uint64 {
	if index == 0 || index > uint64(len(log)) {
		return 0
	}
	return log[index-1].Term
}

func lastLogTerm(log []LogEntry) uint64 {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Term
}
