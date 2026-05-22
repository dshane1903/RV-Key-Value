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
	n.ensureLeaderProgress()
	peers := n.peerSnapshot()

	for _, peerID := range peers {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := n.BuildAppendEntries(peerID)
		if err != nil {
			return err
		}
		resp, err := client.AppendEntries(ctx, peerID, req)
		if err != nil {
			continue
		}

		steppedDown, err := n.stepDownForHigherTerm(resp.Term)
		if err != nil || steppedDown {
			return err
		}
		if resp.Success {
			matchIndex := req.PrevLogIndex + uint64(len(req.Entries))
			if err := n.RecordReplicationSuccess(peerID, matchIndex); err != nil {
				return err
			}
			continue
		}

		if err := n.RecordReplicationFailure(peerID); err != nil {
			return err
		}
	}

	return nil
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
