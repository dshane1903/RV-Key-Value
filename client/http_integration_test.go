package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/store"
)

type httpTestCluster struct {
	nodes         map[string]*raft.RaftNode
	stateMachines map[string]*store.KVStateMachine
	down          map[string]bool
}

func newHTTPTestCluster(t *testing.T, ids ...string) *httpTestCluster {
	t.Helper()

	cluster := &httpTestCluster{
		nodes:         make(map[string]*raft.RaftNode, len(ids)),
		stateMachines: make(map[string]*store.KVStateMachine, len(ids)),
		down:          make(map[string]bool),
	}
	for _, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, peerID := range ids {
			if peerID != id {
				peers = append(peers, peerID)
			}
		}

		node, err := raft.NewRaftNode(id, peers, nil)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		cluster.nodes[id] = node
		cluster.stateMachines[id] = store.NewKVStateMachine()
	}
	return cluster
}

func (c *httpTestCluster) RequestVote(_ context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if c.down[peerID] {
		return raft.RequestVoteResponse{}, errors.New("peer unavailable")
	}
	return c.nodes[peerID].RequestVote(req)
}

func (c *httpTestCluster) AppendEntries(_ context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if c.down[peerID] {
		return raft.AppendEntriesResponse{}, errors.New("peer unavailable")
	}
	resp, err := c.nodes[peerID].HandleAppendEntries(req)
	if err != nil {
		return resp, err
	}
	if resp.Success {
		if err := c.nodes[peerID].ApplyCommitted(c.stateMachines[peerID]); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func TestHTTPFollowerWriteForwardsAndReplicatesAcrossCluster(t *testing.T) {
	cluster := newHTTPTestCluster(t, "n1", "n2", "n3")
	won, err := cluster.nodes["n1"].StartElection(context.Background(), cluster)
	if err != nil {
		t.Fatalf("start election: %v", err)
	}
	if !won {
		t.Fatal("won = false, want true")
	}
	for _, id := range []string{"n2", "n3"} {
		_, err := cluster.nodes[id].HandleAppendEntries(raft.AppendEntriesRequest{
			Term:     cluster.nodes["n1"].CurrentTerm(),
			LeaderID: "n1",
		})
		if err != nil {
			t.Fatalf("set known leader on %s: %v", id, err)
		}
	}

	handlers := make(map[string]http.Handler)
	servers := map[string]*httptest.Server{
		"n1": httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers["n1"].ServeHTTP(w, r)
		})),
		"n2": httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers["n2"].ServeHTTP(w, r)
		})),
		"n3": httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers["n3"].ServeHTTP(w, r)
		})),
	}
	for _, server := range servers {
		defer server.Close()
	}

	for id, node := range cluster.nodes {
		httpAddrs := make(map[string]string)
		for peerID, server := range servers {
			if peerID != id {
				httpAddrs[peerID] = server.URL
			}
		}
		handlers[id] = NewHTTPHandlerWithForwarding(
			node,
			cluster,
			cluster.stateMachines[id],
			NewLeaderForwarder(id, httpAddrs),
		)
	}

	req, err := http.NewRequest(http.MethodPut, servers["n2"].URL+"/kv/name", strings.NewReader("raft"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put through follower: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	if err := cluster.nodes["n1"].ReplicateOnce(context.Background(), cluster); err != nil {
		t.Fatalf("replicate leader commit: %v", err)
	}

	for _, id := range []string{"n1", "n2", "n3"} {
		resp, err := http.Get(servers[id].URL + "/kv/name")
		if err != nil {
			t.Fatalf("get from %s: %v", id, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s body: %v", id, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s get status = %d, want %d body=%q", id, resp.StatusCode, http.StatusOK, string(body))
		}
		if got := string(body); got != "raft" {
			t.Fatalf("%s body = %q, want raft", id, got)
		}
	}
}
