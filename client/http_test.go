package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/store"
)

type singleNodeAppendClient struct{}

func (singleNodeAppendClient) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, nil
}

func TestHTTPPutGetDeleteOnLeader(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	handler := NewHTTPHandler(node, singleNodeAppendClient{}, store.NewKVStateMachine())

	put := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("raft"))
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, put)
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("put status = %d, want %d body=%q", putResp.Code, http.StatusNoContent, putResp.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/kv/name", nil)
	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, get)
	if getResp.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getResp.Code, http.StatusOK)
	}
	if got := getResp.Body.String(); got != "raft" {
		t.Fatalf("get body = %q, want raft", got)
	}

	del := httptest.NewRequest(http.MethodDelete, "/kv/name", nil)
	delResp := httptest.NewRecorder()
	handler.ServeHTTP(delResp, del)
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d body=%q", delResp.Code, http.StatusNoContent, delResp.Body.String())
	}

	getResp = httptest.NewRecorder()
	handler.ServeHTTP(getResp, get)
	if getResp.Code != http.StatusNotFound {
		t.Fatalf("get after delete status = %d, want %d", getResp.Code, http.StatusNotFound)
	}
}

func TestHTTPWriteToFollowerReturnsConflict(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := NewHTTPHandler(node, singleNodeAppendClient{}, store.NewKVStateMachine())
	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("raft"))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusConflict)
	}
}

func TestHTTPWriteToFollowerForwardsToKnownLeader(t *testing.T) {
	follower, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	if _, err := follower.HandleAppendEntries(raft.AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	var forwardedMethod string
	var forwardedPath string
	var forwardedBody string
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		forwardedMethod = r.Method
		forwardedPath = r.URL.Path
		forwardedBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()

	forwarder := NewLeaderForwarder("n1", map[string]string{"n2": leader.URL})
	handler := NewHTTPHandlerWithForwarding(follower, singleNodeAppendClient{}, store.NewKVStateMachine(), forwarder)

	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("raft"))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d body=%q", resp.Code, http.StatusNoContent, resp.Body.String())
	}
	if forwardedMethod != http.MethodPut {
		t.Fatalf("forwarded method = %q, want PUT", forwardedMethod)
	}
	if forwardedPath != "/kv/name" {
		t.Fatalf("forwarded path = %q, want /kv/name", forwardedPath)
	}
	if forwardedBody != "raft" {
		t.Fatalf("forwarded body = %q, want raft", forwardedBody)
	}
}

func TestHTTPWriteToFollowerReturnsBadGatewayForUnknownLeader(t *testing.T) {
	follower, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	if _, err := follower.HandleAppendEntries(raft.AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	forwarder := NewLeaderForwarder("n1", nil)
	handler := NewHTTPHandlerWithForwarding(follower, singleNodeAppendClient{}, store.NewKVStateMachine(), forwarder)
	req := httptest.NewRequest(http.MethodDelete, "/kv/name", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadGateway)
	}
}

func TestHTTPRejectsInvalidKey(t *testing.T) {
	node, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := NewHTTPHandler(node, singleNodeAppendClient{}, store.NewKVStateMachine())
	req := httptest.NewRequest(http.MethodGet, "/kv/a/b", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
	}
}
