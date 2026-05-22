package raft

import (
	"context"
	"errors"
	"time"
)

// AppendClient sends AppendEntries RPCs to peer nodes.
type AppendClient interface {
	AppendEntries(ctx context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

func (n *RaftNode) RunHeartbeatLoop(ctx context.Context, client AppendClient, interval time.Duration) error {
	if client == nil {
		return errors.New("append client is nil")
	}
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n.State() != Leader {
				continue
			}
			if err := n.ReplicateOnce(ctx, client); err != nil {
				return err
			}
		}
	}
}

func (n *RaftNode) ReplicateOnce(ctx context.Context, client AppendClient) error {
	return n.replicateOnce(ctx, client, 0)
}

func (n *RaftNode) replicateOnce(ctx context.Context, client AppendClient, stopWhenCommitted uint64) error {
	n.ensureLeaderProgress()
	peers := n.peerSnapshot()
	requests := make([]appendRequest, 0, len(peers))

	for _, peerID := range peers {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := n.BuildAppendEntries(peerID)
		if err != nil {
			return err
		}
		requests = append(requests, appendRequest{peerID: peerID, req: req})
	}

	results := make(chan appendResult, len(requests))
	for _, request := range requests {
		request := request
		go func() {
			resp, err := client.AppendEntries(ctx, request.peerID, request.req)
			results <- appendResult{peerID: request.peerID, req: request.req, resp: resp, err: err}
		}()
	}

	for remaining := len(requests); remaining > 0; remaining-- {
		var result appendResult
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result = <-results:
		}
		if result.err != nil {
			continue
		}

		steppedDown, err := n.stepDownForHigherTerm(result.resp.Term)
		if err != nil || steppedDown {
			return err
		}
		if result.resp.Success {
			matchIndex := result.req.PrevLogIndex + uint64(len(result.req.Entries))
			if err := n.RecordReplicationSuccess(result.peerID, matchIndex); err != nil {
				return err
			}
			if stopWhenCommitted > 0 && n.isCommitted(stopWhenCommitted) {
				return nil
			}
			continue
		}

		if err := n.RecordReplicationFailure(result.peerID); err != nil {
			return err
		}
	}

	return nil
}

type appendRequest struct {
	peerID string
	req    AppendEntriesRequest
}

type appendResult struct {
	peerID string
	req    AppendEntriesRequest
	resp   AppendEntriesResponse
	err    error
}

func (n *RaftNode) ensureLeaderProgress() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}
	if n.nextIndex == nil || n.matchIndex == nil {
		n.initLeaderProgressLocked()
		return
	}
	for _, peerID := range n.peers {
		if _, ok := n.nextIndex[peerID]; !ok {
			n.initLeaderProgressLocked()
			return
		}
		if _, ok := n.matchIndex[peerID]; !ok {
			n.initLeaderProgressLocked()
			return
		}
	}
}

func (n *RaftNode) peerSnapshot() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return append([]string(nil), n.peers...)
}
