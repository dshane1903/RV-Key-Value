package raftgrpc

import (
	"context"

	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
)

type Server struct {
	raftkvpb.UnimplementedRaftServer

	node        *raft.RaftNode
	appendReset chan<- struct{}
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

func NewServerWithAppendReset(node *raft.RaftNode, appendReset chan<- struct{}) *Server {
	return &Server{node: node, appendReset: appendReset}
}

func (s *Server) RequestVote(ctx context.Context, req *raftkvpb.RequestVoteRequest) (*raftkvpb.RequestVoteResponse, error) {
	resp, err := s.node.RequestVote(fromProtoRequestVoteRequest(req))
	if err != nil {
		return nil, err
	}
	return toProtoRequestVoteResponse(resp), nil
}

func (s *Server) PreVote(ctx context.Context, req *raftkvpb.PreVoteRequest) (*raftkvpb.PreVoteResponse, error) {
	resp, err := s.node.PreVote(fromProtoPreVoteRequest(req))
	if err != nil {
		return nil, err
	}
	return toProtoPreVoteResponse(resp), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftkvpb.AppendEntriesRequest) (*raftkvpb.AppendEntriesResponse, error) {
	resp, err := s.node.HandleAppendEntries(fromProtoAppendEntriesRequest(req))
	if err != nil {
		return nil, err
	}
	if resp.Success {
		s.notifyAppendReset()
	}
	return toProtoAppendEntriesResponse(resp), nil
}

func (s *Server) notifyAppendReset() {
	if s.appendReset == nil {
		return
	}

	select {
	case s.appendReset <- struct{}{}:
	default:
	}
}
