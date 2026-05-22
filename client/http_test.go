package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/store"
)

type singleNodeAppendClient struct{}

func (singleNodeAppendClient) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, nil
}

type unavailableAppendClient struct{}

func (unavailableAppendClient) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("peer unavailable")
}

type successfulAppendClient struct{}

func (successfulAppendClient) AppendEntries(_ context.Context, _ string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{Term: req.Term, Success: true}, nil
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

func TestHTTPClusterMembersListsMembers(t *testing.T) {
	node, err := raft.NewRaftNode("n1", []string{"n2"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := NewHTTPHandler(node, singleNodeAppendClient{}, store.NewKVStateMachine())
	req := httptest.NewRequest(http.MethodGet, "/cluster/members", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	var body struct {
		Members []string `json:"members"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Members) != 2 || body.Members[0] != "n1" || body.Members[1] != "n2" {
		t.Fatalf("members = %v, want [n1 n2]", body.Members)
	}
}

func TestHTTPClusterMemberAddAndRemove(t *testing.T) {
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

	handler := NewHTTPHandler(node, successfulAppendClient{}, store.NewKVStateMachine())
	add := httptest.NewRequest(http.MethodPut, "/cluster/members/n2", nil)
	addResp := httptest.NewRecorder()
	handler.ServeHTTP(addResp, add)
	if addResp.Code != http.StatusNoContent {
		t.Fatalf("add status = %d, want %d body=%q", addResp.Code, http.StatusNoContent, addResp.Body.String())
	}
	if peers := node.Peers(); len(peers) != 1 || peers[0] != "n2" {
		t.Fatalf("peers after add = %v, want [n2]", peers)
	}

	remove := httptest.NewRequest(http.MethodDelete, "/cluster/members/n2", nil)
	removeResp := httptest.NewRecorder()
	handler.ServeHTTP(removeResp, remove)
	if removeResp.Code != http.StatusNoContent {
		t.Fatalf("remove status = %d, want %d body=%q", removeResp.Code, http.StatusNoContent, removeResp.Body.String())
	}
	if peers := node.Peers(); len(peers) != 0 {
		t.Fatalf("peers after remove = %v, want []", peers)
	}
}

func TestHTTPReadOnLeaderFailsWithoutQuorum(t *testing.T) {
	node, err := raft.NewRaftNode("n1", []string{"n2", "n3"}, nil)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := node.BecomeCandidate(); err != nil {
		t.Fatalf("become candidate: %v", err)
	}
	if err := node.BecomeLeader(); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	handler := NewHTTPHandler(node, unavailableAppendClient{}, store.NewKVStateMachine())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/kv/name", nil).WithContext(ctx)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d body=%q", resp.Code, http.StatusGatewayTimeout, resp.Body.String())
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

func TestHTTPReadFromFollowerForwardsToKnownLeader(t *testing.T) {
	follower, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	if _, err := follower.HandleAppendEntries(raft.AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	var forwardedMethod string
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwardedMethod = r.Method
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("raft"))
	}))
	defer leader.Close()

	forwarder := NewLeaderForwarder("n1", map[string]string{"n2": leader.URL})
	handler := NewHTTPHandlerWithForwarding(follower, singleNodeAppendClient{}, store.NewKVStateMachine(), forwarder)

	req := httptest.NewRequest(http.MethodGet, "/kv/name", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%q", resp.Code, http.StatusOK, resp.Body.String())
	}
	if forwardedMethod != http.MethodGet {
		t.Fatalf("forwarded method = %q, want GET", forwardedMethod)
	}
	if got := resp.Body.String(); got != "raft" {
		t.Fatalf("body = %q, want raft", got)
	}
}

func TestHTTPWriteToFollowerPreservesLeaderErrorBody(t *testing.T) {
	follower, err := raft.NewRaftNode("n1", nil, nil)
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	if _, err := follower.HandleAppendEntries(raft.AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "proposal timed out", http.StatusGatewayTimeout)
	}))
	defer leader.Close()

	forwarder := NewLeaderForwarder("n1", map[string]string{"n2": leader.URL})
	handler := NewHTTPHandlerWithForwarding(follower, singleNodeAppendClient{}, store.NewKVStateMachine(), forwarder)

	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader("raft"))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusGatewayTimeout)
	}
	if got := resp.Body.String(); got != "proposal timed out\n" {
		t.Fatalf("body = %q, want leader error body", got)
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

func TestHTTPPutRejectsOversizedBody(t *testing.T) {
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
	req := httptest.NewRequest(http.MethodPut, "/kv/name", strings.NewReader(strings.Repeat("x", maxKVValueBytes+1)))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusRequestEntityTooLarge)
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
