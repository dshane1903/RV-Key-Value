package raftgrpc

import (
	"context"

	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
)

type Server struct {
	raftkvpb.UnimplementedRaftServer

	node *raft.RaftNode
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

func (s *Server) RequestVote(ctx context.Context, req *raftkvpb.RequestVoteRequest) (*raftkvpb.RequestVoteResponse, error) {
	resp, err := s.node.RequestVote(fromProtoRequestVoteRequest(req))
	if err != nil {
		return nil, err
	}
	return toProtoRequestVoteResponse(resp), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftkvpb.AppendEntriesRequest) (*raftkvpb.AppendEntriesResponse, error) {
	resp, err := s.node.HandleAppendEntries(fromProtoAppendEntriesRequest(req))
	if err != nil {
		return nil, err
	}
	return toProtoAppendEntriesResponse(resp), nil
}
