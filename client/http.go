package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/store"
)

type API struct {
	node            *raft.RaftNode
	appendClient    raft.AppendClient
	stateMachine    *store.KVStateMachine
	proposalTimeout time.Duration
	retryInterval   time.Duration
}

func NewHTTPHandler(node *raft.RaftNode, appendClient raft.AppendClient, stateMachine *store.KVStateMachine) http.Handler {
	api := &API{
		node:            node,
		appendClient:    appendClient,
		stateMachine:    stateMachine,
		proposalTimeout: 5 * time.Second,
		retryInterval:   25 * time.Millisecond,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", api.handleKV)
	return mux
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.handleGet(w, key)
	case http.MethodPut:
		a.handlePut(w, r, key)
	case http.MethodDelete:
		a.handleDelete(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleGet(w http.ResponseWriter, key string) {
	value, ok := a.stateMachine.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (a *API) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
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

	if !a.proposeAndApply(w, r, command) {
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

	if !a.proposeAndApply(w, r, command) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) proposeAndApply(w http.ResponseWriter, r *http.Request, command []byte) bool {
	ctx, cancel := context.WithTimeout(r.Context(), a.proposalTimeout)
	defer cancel()

	_, err := a.node.ProposeWithRetryInterval(ctx, a.appendClient, command, a.retryInterval)
	if err != nil {
		var notLeader raft.ErrNotLeader
		if errors.As(err, &notLeader) {
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
