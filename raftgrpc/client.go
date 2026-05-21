package raftgrpc

import (
	"context"
	"errors"
	"fmt"

	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	clients map[string]raftkvpb.RaftClient
}

func NewPeerClient(clients map[string]raftkvpb.RaftClient) *PeerClient {
	return &PeerClient{clients: clients}
}

func DialPeerClient(peers map[string]string) (*PeerClient, func() error, error) {
	clients := make(map[string]raftkvpb.RaftClient, len(peers))
	conns := make([]*grpc.ClientConn, 0, len(peers))

	for id, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeConnections(conns)
			return nil, nil, fmt.Errorf("dial peer %s at %s: %w", id, addr, err)
		}
		conns = append(conns, conn)
		clients[id] = raftkvpb.NewRaftClient(conn)
	}

	closeFn := func() error {
		return closeConnections(conns)
	}
	return NewPeerClient(clients), closeFn, nil
}

func (c *PeerClient) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	client, ok := c.clients[peerID]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("unknown peer %s", peerID)
	}

	resp, err := client.RequestVote(ctx, toProtoRequestVoteRequest(req))
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return fromProtoRequestVoteResponse(resp), nil
}

func (c *PeerClient) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	client, ok := c.clients[peerID]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("unknown peer %s", peerID)
	}

	resp, err := client.AppendEntries(ctx, toProtoAppendEntriesRequest(req))
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return fromProtoAppendEntriesResponse(resp), nil
}

func closeConnections(conns []*grpc.ClientConn) error {
	var joined error
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}
