package raftgrpc

import (
	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
)

func toProtoRequestVoteRequest(req raft.RequestVoteRequest) *raftkvpb.RequestVoteRequest {
	return &raftkvpb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
}

func fromProtoRequestVoteRequest(req *raftkvpb.RequestVoteRequest) raft.RequestVoteRequest {
	return raft.RequestVoteRequest{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	}
}

func toProtoRequestVoteResponse(resp raft.RequestVoteResponse) *raftkvpb.RequestVoteResponse {
	return &raftkvpb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}
}

func fromProtoRequestVoteResponse(resp *raftkvpb.RequestVoteResponse) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}
}

func toProtoPreVoteRequest(req raft.PreVoteRequest) *raftkvpb.PreVoteRequest {
	return &raftkvpb.PreVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
}

func fromProtoPreVoteRequest(req *raftkvpb.PreVoteRequest) raft.PreVoteRequest {
	return raft.PreVoteRequest{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	}
}

func toProtoPreVoteResponse(resp raft.PreVoteResponse) *raftkvpb.PreVoteResponse {
	return &raftkvpb.PreVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}
}

func fromProtoPreVoteResponse(resp *raftkvpb.PreVoteResponse) raft.PreVoteResponse {
	return raft.PreVoteResponse{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}
}

func toProtoAppendEntriesRequest(req raft.AppendEntriesRequest) *raftkvpb.AppendEntriesRequest {
	return &raftkvpb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      toProtoLogEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	}
}

func fromProtoAppendEntriesRequest(req *raftkvpb.AppendEntriesRequest) raft.AppendEntriesRequest {
	return raft.AppendEntriesRequest{
		Term:         req.GetTerm(),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      fromProtoLogEntries(req.GetEntries()),
		LeaderCommit: req.GetLeaderCommit(),
	}
}

func toProtoAppendEntriesResponse(resp raft.AppendEntriesResponse) *raftkvpb.AppendEntriesResponse {
	return &raftkvpb.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}
}

func fromProtoAppendEntriesResponse(resp *raftkvpb.AppendEntriesResponse) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{
		Term:    resp.GetTerm(),
		Success: resp.GetSuccess(),
	}
}

func toProtoLogEntries(entries []raft.LogEntry) []*raftkvpb.LogEntry {
	out := make([]*raftkvpb.LogEntry, len(entries))
	for i, entry := range entries {
		out[i] = &raftkvpb.LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: append([]byte(nil), entry.Command...),
		}
	}
	return out
}

func fromProtoLogEntries(entries []*raftkvpb.LogEntry) []raft.LogEntry {
	out := make([]raft.LogEntry, len(entries))
	for i, entry := range entries {
		out[i] = raft.LogEntry{
			Term:    entry.GetTerm(),
			Index:   entry.GetIndex(),
			Command: append([]byte(nil), entry.GetCommand()...),
		}
	}
	return out
}
