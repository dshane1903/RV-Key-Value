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
			if err := n.sendHeartbeats(ctx, client); err != nil {
				return err
			}
		}
	}
}

func (n *RaftNode) sendHeartbeats(ctx context.Context, client AppendClient) error {
	term, prevLogIndex, prevLogTerm, commitIndex, peers := n.heartbeatSnapshot()
	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}

	for _, peerID := range peers {
		if err := ctx.Err(); err != nil {
			return err
		}

		resp, err := client.AppendEntries(ctx, peerID, req)
		if err != nil {
			continue
		}

		if _, err := n.stepDownForHigherTerm(resp.Term); err != nil {
			return err
		}
	}

	return nil
}

func (n *RaftNode) heartbeatSnapshot() (uint64, uint64, uint64, uint64, []string) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm, lastLogIndex(n.log), lastLogTerm(n.log), n.commitIndex, append([]string(nil), n.peers...)
}
