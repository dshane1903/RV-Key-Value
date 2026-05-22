package raft

import (
	"fmt"
	"sync"
)

// RaftNode contains the local state needed for Raft consensus.
type RaftNode struct {
	mu sync.RWMutex

	id                   string
	peers                []string
	selfMember           bool
	membershipConfigured bool
	state                State
	leaderID             string

	currentTerm uint64
	votedFor    string
	log         []LogEntry

	commitIndex       uint64
	lastApplied       uint64
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshot          []byte

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	store       StableStore
	commitReady chan struct{}
}

// NewRaftNode builds a node from persisted state when a store is provided.
func NewRaftNode(id string, peers []string, store StableStore) (*RaftNode, error) {
	node := &RaftNode{
		id:                   id,
		peers:                clonePeers(peers),
		selfMember:           true,
		membershipConfigured: len(peers) > 0,
		state:                Follower,
		store:                store,
		commitReady:          make(chan struct{}, 1),
	}

	if store != nil {
		state, err := store.Load()
		if err != nil {
			return nil, err
		}
		node.currentTerm = state.CurrentTerm
		node.votedFor = state.VotedFor
		if state.Peers != nil {
			node.peers = clonePeers(state.Peers)
		}
		if state.SelfMember != nil {
			node.selfMember = *state.SelfMember
		}
		if state.Peers != nil || state.SelfMember != nil {
			node.membershipConfigured = true
		}
		node.lastIncludedIndex = state.LastIncludedIndex
		node.lastIncludedTerm = state.LastIncludedTerm
		node.snapshot = append([]byte(nil), state.Snapshot...)
		node.commitIndex = state.LastIncludedIndex
		node.lastApplied = state.LastIncludedIndex
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

func (n *RaftNode) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *RaftNode) Log() []LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return cloneLog(n.log)
}

func (n *RaftNode) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return clonePeers(n.peers)
}

func (n *RaftNode) Members() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	members := make([]string, 0, len(n.peers)+1)
	if n.selfMember {
		members = append(members, n.id)
	}
	members = append(members, n.peers...)
	return members
}

func (n *RaftNode) Snapshot() []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]byte(nil), n.snapshot...)
}

func (n *RaftNode) LastIncludedIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastIncludedIndex
}

func (n *RaftNode) LastIncludedTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastIncludedTerm
}

func (n *RaftNode) LastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogIndexLocked()
}

func (n *RaftNode) LastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogTermLocked()
}

func (n *RaftNode) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *RaftNode) CommitReady() <-chan struct{} {
	return n.commitReady
}

func (n *RaftNode) LastApplied() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
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
	n.leaderID = ""
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
	n.leaderID = ""
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
	n.leaderID = n.id
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
	if !n.selfMember {
		return resp, nil
	}
	if !n.isMemberLocked(req.CandidateID) {
		return resp, nil
	}

	changed := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.leaderID = ""
		n.nextIndex = nil
		n.matchIndex = nil
		n.state = Follower
		changed = true
	}

	resp.Term = n.currentTerm
	canVoteForCandidate := n.votedFor == "" || n.votedFor == req.CandidateID
	logIsFreshEnough := IsLogAtLeastUpToDate(req.LastLogIndex, req.LastLogTerm, n.lastLogIndexLocked(), n.lastLogTermLocked())
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

func (n *RaftNode) PreVote(req PreVoteRequest) (PreVoteResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	resp := PreVoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}
	if !n.selfMember {
		return resp, nil
	}
	if !n.isMemberLocked(req.CandidateID) {
		return resp, nil
	}

	logIsFreshEnough := IsLogAtLeastUpToDate(req.LastLogIndex, req.LastLogTerm, n.lastLogIndexLocked(), n.lastLogTermLocked())
	resp.VoteGranted = logIsFreshEnough
	return resp, nil
}

func (n *RaftNode) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}
	if !n.isMemberLocked(req.LeaderID) {
		return resp, nil
	}

	changed := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.leaderID = ""
		n.nextIndex = nil
		n.matchIndex = nil
		changed = true
	}
	if n.state != Follower {
		n.nextIndex = nil
		n.matchIndex = nil
	}
	n.state = Follower
	n.leaderID = req.LeaderID

	logChanged, err := n.appendEntriesToLogLocked(req.PrevLogIndex, req.PrevLogTerm, req.Entries)
	if err != nil {
		resp.Term = n.currentTerm
		if changed {
			return resp, n.persistLocked()
		}
		return resp, nil
	}
	changed = changed || logChanged

	if req.LeaderCommit > n.commitIndex {
		nextCommit := min(req.LeaderCommit, n.lastLogIndexLocked())
		if nextCommit > n.commitIndex {
			n.commitIndex = nextCommit
			n.signalCommitReadyLocked()
		}
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

	if _, ok, err := DecodeMembershipChange(command); ok {
		if err != nil {
			return LogEntry{}, err
		}
		if n.hasPendingMembershipChangeLocked() {
			return LogEntry{}, ErrMembershipChangePending{}
		}
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndexLocked() + 1,
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
	if next <= n.lastIncludedIndex {
		next = n.firstLogIndexLocked()
	}

	prevLogIndex := next - 1
	return AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  n.logTermAtLocked(prevLogIndex),
		Entries:      n.cloneEntriesFromLocked(next),
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
		if next <= n.lastIncludedIndex+1 {
			return nil
		}
		n.nextIndex[peerID] = next - 1
	}
	return nil
}

func (n *RaftNode) ShouldSnapshot(threshold uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return threshold > 0 && n.lastApplied >= n.lastIncludedIndex+threshold
}

func (n *RaftNode) Compact(snapshot []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastApplied <= n.lastIncludedIndex {
		return nil
	}
	index := n.lastApplied
	term := n.logTermAtLocked(index)
	if term == 0 {
		return ErrLogInconsistent{PrevLogIndex: index}
	}

	first := n.firstLogIndexLocked()
	switch {
	case index < first:
		n.log = nil
	case index >= n.lastLogIndexLocked():
		n.log = nil
	default:
		n.log = cloneLog(n.log[index-first+1:])
	}

	n.lastIncludedIndex = index
	n.lastIncludedTerm = term
	n.snapshot = append([]byte(nil), snapshot...)
	return n.persistLocked()
}

func (n *RaftNode) ApplyCommitted(sm StateMachine) error {
	for {
		entry, ok := n.nextCommittedEntry()
		if !ok {
			return nil
		}

		if change, ok, err := DecodeMembershipChange(entry.Command); ok {
			if err != nil {
				return err
			}
			if err := n.applyCommittedMembershipChange(change, entry.Index); err != nil {
				return err
			}
			continue
		}

		if err := sm.Apply(entry); err != nil {
			return err
		}
		n.markApplied(entry.Index)
	}
}

func (n *RaftNode) ApplyMembershipChange(change MembershipChange) error {
	if err := change.Validate(); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.applyMembershipChangeLocked(change); err != nil {
		return err
	}
	return n.persistLocked()
}

func (n *RaftNode) appendEntriesToLogLocked(prevLogIndex, prevLogTerm uint64, entries []LogEntry) (bool, error) {
	if prevLogIndex > 0 {
		if n.logTermAtLocked(prevLogIndex) != prevLogTerm {
			return false, ErrLogInconsistent{PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
		}
	}

	next := prevLogIndex + 1
	for i, entry := range entries {
		entry.Index = next + uint64(i)
		if entry.Index <= n.lastIncludedIndex {
			continue
		}

		if n.containsLogIndexLocked(entry.Index) {
			local := n.log[entry.Index-n.firstLogIndexLocked()]
			if local.Term != entry.Term {
				n.log = n.log[:entry.Index-n.firstLogIndexLocked()]
				n.log = append(n.log, normalizeEntries(entry.Index, entries[i:])...)
				return true, nil
			}
			continue
		}

		n.log = append(n.log, normalizeEntries(entry.Index, entries[i:])...)
		return true, nil
	}

	return false, nil
}

func (n *RaftNode) persistLocked() error {
	if n.store == nil {
		return nil
	}

	return n.store.Save(PersistentState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Peers:             clonePeers(n.peers),
		SelfMember:        boolPtr(n.selfMember),
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Snapshot:          append([]byte(nil), n.snapshot...),
		Log:               cloneLog(n.log),
	})
}

func (n *RaftNode) nextCommittedEntry() (LogEntry, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	next := n.lastApplied + 1
	if next > n.commitIndex || !n.containsLogIndexLocked(next) {
		return LogEntry{}, false
	}
	return cloneLog([]LogEntry{n.log[next-n.firstLogIndexLocked()]})[0], true
}

func (n *RaftNode) markApplied(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index > n.lastApplied {
		n.lastApplied = index
	}
}

func (n *RaftNode) initLeaderProgressLocked() {
	next := n.lastLogIndexLocked() + 1
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, peerID := range n.peers {
		n.nextIndex[peerID] = next
		n.matchIndex[peerID] = 0
	}
	n.advanceCommitLocked()
}

func (n *RaftNode) advanceCommitLocked() {
	if n.state != Leader || !n.selfMember {
		return
	}

	for index := n.lastLogIndexLocked(); index > n.commitIndex; index-- {
		if n.logTermAtLocked(index) != n.currentTerm {
			continue
		}

		replicated := 1
		for _, peerID := range n.peers {
			if n.matchIndex[peerID] >= index {
				replicated++
			}
		}
		if replicated >= majority(n.clusterSizeLocked()) {
			n.commitIndex = index
			n.signalCommitReadyLocked()
			return
		}
	}
}

func (n *RaftNode) applyMembershipChangeLocked(change MembershipChange) error {
	n.membershipConfigured = true

	switch change.Type {
	case MembershipAddPeer:
		if change.PeerID == n.id {
			n.selfMember = true
		} else if !containsPeer(n.peers, change.PeerID) {
			n.peers = append(n.peers, change.PeerID)
			if n.state == Leader && n.nextIndex != nil && n.matchIndex != nil {
				n.nextIndex[change.PeerID] = n.lastLogIndexLocked() + 1
				n.matchIndex[change.PeerID] = 0
			}
		}
	case MembershipRemovePeer:
		if change.PeerID == n.id {
			n.selfMember = false
			n.state = Follower
			n.leaderID = ""
			n.votedFor = ""
			n.nextIndex = nil
			n.matchIndex = nil
		} else {
			n.peers = removePeer(n.peers, change.PeerID)
			if n.nextIndex != nil {
				delete(n.nextIndex, change.PeerID)
			}
			if n.matchIndex != nil {
				delete(n.matchIndex, change.PeerID)
			}
		}
	default:
		return fmt.Errorf("unsupported membership change %q", change.Type)
	}

	n.advanceCommitLocked()
	return nil
}

func (n *RaftNode) applyCommittedMembershipChange(change MembershipChange, index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.applyMembershipChangeLocked(change); err != nil {
		return err
	}
	if index > n.lastApplied {
		n.lastApplied = index
	}
	return n.persistLocked()
}

func (n *RaftNode) hasPendingMembershipChangeLocked() bool {
	for _, entry := range n.log {
		if entry.Index <= n.lastApplied {
			continue
		}
		if _, ok, _ := DecodeMembershipChange(entry.Command); ok {
			return true
		}
	}
	return false
}

func (n *RaftNode) isMemberLocked(id string) bool {
	if id == n.id {
		return n.selfMember
	}
	if containsPeer(n.peers, id) {
		return true
	}
	return n.selfMember && !n.membershipConfigured
}

func (n *RaftNode) clusterSizeLocked() int {
	size := len(n.peers)
	if n.selfMember {
		size++
	}
	return size
}

func (n *RaftNode) firstLogIndexLocked() uint64 {
	return n.lastIncludedIndex + 1
}

func (n *RaftNode) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) containsLogIndexLocked(index uint64) bool {
	return index >= n.firstLogIndexLocked() && index <= n.lastLogIndexLocked()
}

func (n *RaftNode) logTermAtLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	if !n.containsLogIndexLocked(index) {
		return 0
	}
	return n.log[index-n.firstLogIndexLocked()].Term
}

func (n *RaftNode) cloneEntriesFromLocked(startIndex uint64) []LogEntry {
	if startIndex <= n.lastIncludedIndex {
		startIndex = n.firstLogIndexLocked()
	}
	if startIndex > n.lastLogIndexLocked() {
		return nil
	}
	return cloneLog(n.log[startIndex-n.firstLogIndexLocked():])
}

func (n *RaftNode) signalCommitReadyLocked() {
	select {
	case n.commitReady <- struct{}{}:
	default:
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

func clonePeers(peers []string) []string {
	return append([]string(nil), peers...)
}

func containsPeer(peers []string, id string) bool {
	for _, peerID := range peers {
		if peerID == id {
			return true
		}
	}
	return false
}

func removePeer(peers []string, id string) []string {
	out := peers[:0]
	for _, peerID := range peers {
		if peerID != id {
			out = append(out, peerID)
		}
	}
	return out
}

func boolPtr(value bool) *bool {
	return &value
}
