package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/store"
)

type API struct {
	node            *raft.RaftNode
	appendClient    raft.AppendClient
	stateMachine    *store.KVStateMachine
	leaderForwarder *LeaderForwarder
	proposalTimeout time.Duration
	retryInterval   time.Duration
}

const (
	maxKVValueBytes         = 1 << 20
	maxForwardResponseBytes = 64 << 10
)

func NewHTTPHandler(node *raft.RaftNode, appendClient raft.AppendClient, stateMachine *store.KVStateMachine) http.Handler {
	return NewHTTPHandlerWithForwarding(node, appendClient, stateMachine, nil)
}

func NewHTTPHandlerWithForwarding(node *raft.RaftNode, appendClient raft.AppendClient, stateMachine *store.KVStateMachine, leaderForwarder *LeaderForwarder) http.Handler {
	api := &API{
		node:            node,
		appendClient:    appendClient,
		stateMachine:    stateMachine,
		leaderForwarder: leaderForwarder,
		proposalTimeout: 5 * time.Second,
		retryInterval:   25 * time.Millisecond,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", api.handleKV)
	return mux
}

type LeaderForwarder struct {
	httpAddrs map[string]string
	client    *http.Client
}

func NewLeaderForwarder(selfID string, httpAddrs map[string]string) *LeaderForwarder {
	return &LeaderForwarder{
		httpAddrs: httpAddrs,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.handleGet(w, r, key)
	case http.MethodPut:
		a.handlePut(w, r, key)
	case http.MethodDelete:
		a.handleDelete(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if a.node.State() != raft.Leader {
		if a.forwardToLeader(w, r, nil) {
			return
		}
		http.Error(w, "not leader", http.StatusConflict)
		return
	}

	if err := a.readBarrier(r.Context()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "read timed out", http.StatusGatewayTimeout)
			return
		}
		var notLeader raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			http.Error(w, "not leader", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	value, ok := a.stateMachine.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (a *API) readBarrier(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, a.proposalTimeout)
	defer cancel()

	command, err := store.EncodeCommand(store.Command{Operation: store.OperationNoop})
	if err != nil {
		return err
	}
	if _, err := a.node.ProposeWithRetryInterval(ctx, a.appendClient, command, a.retryInterval); err != nil {
		return err
	}
	return a.node.ApplyCommitted(a.stateMachine)
}

func (a *API) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxKVValueBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	command, err := store.EncodeCommand(store.Command{
		Operation: store.OperationPut,
		Key:       key,
		Value:     value,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !a.proposeAndApply(w, r, command, value) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	command, err := store.EncodeCommand(store.Command{
		Operation: store.OperationDelete,
		Key:       key,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !a.proposeAndApply(w, r, command, nil) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) proposeAndApply(w http.ResponseWriter, r *http.Request, command []byte, forwardBody []byte) bool {
	ctx, cancel := context.WithTimeout(r.Context(), a.proposalTimeout)
	defer cancel()

	_, err := a.node.ProposeWithRetryInterval(ctx, a.appendClient, command, a.retryInterval)
	if err != nil {
		var notLeader raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			if a.forwardToLeader(w, r, forwardBody) {
				return false
			}
			http.Error(w, "not leader", http.StatusConflict)
			return false
		}
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "proposal timed out", http.StatusGatewayTimeout)
			return false
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}

	if err := a.node.ApplyCommitted(a.stateMachine); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}
	return true
}

func (a *API) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) bool {
	if a.leaderForwarder == nil {
		return false
	}

	leaderID := a.node.LeaderID()
	if leaderID == "" || leaderID == a.node.ID() {
		return false
	}

	forwarded, err := a.leaderForwarder.Forward(r.Context(), leaderID, r.Method, r.URL.Path, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	for key, values := range forwarded.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(forwarded.StatusCode)
	_, _ = w.Write(forwarded.Body)
	return true
}

type ForwardResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (f *LeaderForwarder) Forward(ctx context.Context, leaderID, method, path string, body []byte) (ForwardResponse, error) {
	addr, ok := f.httpAddrs[leaderID]
	if !ok {
		return ForwardResponse{}, fmt.Errorf("unknown leader %s", leaderID)
	}

	base, err := url.Parse(addr)
	if err != nil {
		return ForwardResponse{}, fmt.Errorf("parse leader address: %w", err)
	}
	target := base.ResolveReference(&url.URL{Path: path})

	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return ForwardResponse{}, err
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return ForwardResponse{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxForwardResponseBytes+1))
	if err != nil {
		return ForwardResponse{}, err
	}
	if len(respBody) > maxForwardResponseBytes {
		respBody = respBody[:maxForwardResponseBytes]
	}
	return ForwardResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       respBody,
	}, nil
}
