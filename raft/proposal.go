package raft

import (
	"context"
	"errors"
	"time"
)

func (n *RaftNode) Propose(ctx context.Context, client AppendClient, command []byte) (LogEntry, error) {
	return n.ProposeWithRetryInterval(ctx, client, command, DefaultHeartbeatInterval)
}

func (n *RaftNode) ProposeWithRetryInterval(ctx context.Context, client AppendClient, command []byte, retryInterval time.Duration) (LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return LogEntry{}, err
	}
	if retryInterval <= 0 {
		retryInterval = DefaultHeartbeatInterval
	}
	if state := n.State(); state != Leader {
		return LogEntry{}, ErrNotLeader{State: state}
	}

	entry, err := n.AppendLocal(command)
	if err != nil {
		return LogEntry{}, err
	}

	for {
		if n.isCommitted(entry.Index) {
			return entry, nil
		}
		if state := n.State(); state != Leader {
			return entry, ErrNotLeader{State: state}
		}
		if client == nil {
			return entry, errors.New("append client is nil")
		}

		if err := n.sendHeartbeats(ctx, client); err != nil {
			return entry, err
		}
		if n.isCommitted(entry.Index) {
			return entry, nil
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return entry, ctx.Err()
		case <-timer.C:
		}
	}
}

func (n *RaftNode) isCommitted(index uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex >= index
}
