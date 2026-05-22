package raft

import (
	"context"
	"errors"
)

// VoteClient sends RequestVote RPCs to peer nodes.
type VoteClient interface {
	PreVote(ctx context.Context, peerID string, req PreVoteRequest) (PreVoteResponse, error)
	RequestVote(ctx context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
}

func (n *RaftNode) StartElection(ctx context.Context, client VoteClient) (bool, error) {
	if client == nil {
		return false, errors.New("vote client is nil")
	}

	preVoteWon, err := n.PreVoteElection(ctx, client)
	if err != nil || !preVoteWon {
		return false, err
	}

	if err := n.BecomeCandidate(); err != nil {
		return false, err
	}

	term, lastIndex, lastTerm, peers := n.electionSnapshot()
	votes := 1
	if votes >= majority(len(peers)+1) {
		return n.promoteCandidate(term)
	}

	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	responses := make(chan voteResult, len(peers))
	for _, peerID := range peers {
		peerID := peerID
		go func() {
			resp, err := client.RequestVote(ctx, peerID, req)
			responses <- voteResult{resp: resp, err: err}
		}()
	}

	for remaining := len(peers); remaining > 0; remaining-- {
		var result voteResult
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case result = <-responses:
		}
		if result.err != nil {
			continue
		}

		steppedDown, err := n.stepDownForHigherTerm(result.resp.Term)
		if err != nil || steppedDown {
			return false, err
		}
		if !n.isCandidateForTerm(term) {
			return false, nil
		}

		if result.resp.VoteGranted {
			votes++
			if votes >= majority(len(peers)+1) {
				return n.promoteCandidate(term)
			}
		}
	}

	return false, nil
}

func (n *RaftNode) PreVoteElection(ctx context.Context, client VoteClient) (bool, error) {
	if client == nil {
		return false, errors.New("vote client is nil")
	}

	currentTerm, lastIndex, lastTerm, peers := n.electionSnapshot()
	votes := 1
	if votes >= majority(len(peers)+1) {
		return true, nil
	}

	req := PreVoteRequest{
		Term:         currentTerm + 1,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	responses := make(chan preVoteResult, len(peers))
	for _, peerID := range peers {
		peerID := peerID
		go func() {
			resp, err := client.PreVote(ctx, peerID, req)
			responses <- preVoteResult{resp: resp, err: err}
		}()
	}

	for remaining := len(peers); remaining > 0; remaining-- {
		var result preVoteResult
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case result = <-responses:
		}
		if result.err != nil {
			continue
		}

		steppedDown, err := n.stepDownForHigherTerm(result.resp.Term)
		if err != nil || steppedDown {
			return false, err
		}
		if result.resp.VoteGranted {
			votes++
			if votes >= majority(len(peers)+1) {
				return true, nil
			}
		}
	}

	return false, nil
}

type preVoteResult struct {
	resp PreVoteResponse
	err  error
}

type voteResult struct {
	resp RequestVoteResponse
	err  error
}

func (n *RaftNode) electionSnapshot() (uint64, uint64, uint64, []string) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm, n.lastLogIndexLocked(), n.lastLogTermLocked(), append([]string(nil), n.peers...)
}

func (n *RaftNode) stepDownForHigherTerm(term uint64) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term <= n.currentTerm {
		return false, nil
	}

	n.currentTerm = term
	n.votedFor = ""
	n.state = Follower
	n.leaderID = ""
	n.nextIndex = nil
	n.matchIndex = nil
	return true, n.persistLocked()
}

func (n *RaftNode) isCandidateForTerm(term uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == Candidate && n.currentTerm == term
}

func (n *RaftNode) promoteCandidate(term uint64) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Candidate || n.currentTerm != term {
		return false, nil
	}

	n.state = Leader
	n.leaderID = n.id
	n.initLeaderProgressLocked()
	return true, n.persistLocked()
}

func majority(clusterSize int) int {
	return clusterSize/2 + 1
}
