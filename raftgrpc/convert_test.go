package raftgrpc

import (
	"testing"

	"github.com/shaneduncan/rv-key-value/raft"
)

func TestAppendEntriesConversionCopiesLogCommands(t *testing.T) {
	req := raft.AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n1",
		PrevLogIndex: 4,
		PrevLogTerm:  2,
		Entries: []raft.LogEntry{
			{Term: 3, Index: 5, Command: []byte("set a 1")},
		},
		LeaderCommit: 5,
	}

	protoReq := toProtoAppendEntriesRequest(req)
	req.Entries[0].Command[0] = 'X'

	roundTrip := fromProtoAppendEntriesRequest(protoReq)
	if got := string(roundTrip.Entries[0].Command); got != "set a 1" {
		t.Fatalf("command = %q, want set a 1", got)
	}
	if roundTrip.Term != req.Term {
		t.Fatalf("term = %d, want %d", roundTrip.Term, req.Term)
	}
	if roundTrip.LeaderID != req.LeaderID {
		t.Fatalf("leader id = %q, want %q", roundTrip.LeaderID, req.LeaderID)
	}
	if roundTrip.LeaderCommit != req.LeaderCommit {
		t.Fatalf("leader commit = %d, want %d", roundTrip.LeaderCommit, req.LeaderCommit)
	}
}

func TestRequestVoteConversion(t *testing.T) {
	req := raft.RequestVoteRequest{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 7,
		LastLogTerm:  2,
	}

	roundTrip := fromProtoRequestVoteRequest(toProtoRequestVoteRequest(req))
	if roundTrip != req {
		t.Fatalf("roundTrip = %+v, want %+v", roundTrip, req)
	}
}

func TestPreVoteConversion(t *testing.T) {
	req := raft.PreVoteRequest{
		Term:         3,
		CandidateID:  "n2",
		LastLogIndex: 8,
		LastLogTerm:  3,
	}

	roundTrip := fromProtoPreVoteRequest(toProtoPreVoteRequest(req))
	if roundTrip != req {
		t.Fatalf("roundTrip = %+v, want %+v", roundTrip, req)
	}

	resp := raft.PreVoteResponse{Term: 3, VoteGranted: true}
	respRoundTrip := fromProtoPreVoteResponse(toProtoPreVoteResponse(resp))
	if respRoundTrip != resp {
		t.Fatalf("resp roundTrip = %+v, want %+v", respRoundTrip, resp)
	}
}
