package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	httpapi "github.com/shaneduncan/rv-key-value/client"
	"github.com/shaneduncan/rv-key-value/observability"
	raftkvpb "github.com/shaneduncan/rv-key-value/proto"
	"github.com/shaneduncan/rv-key-value/raft"
	"github.com/shaneduncan/rv-key-value/raftgrpc"
	"github.com/shaneduncan/rv-key-value/store"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		id       = flag.String("id", "", "node id")
		raftAddr = flag.String("raft-addr", ":9001", "raft gRPC listen address")
		httpAddr = flag.String("http-addr", ":8080", "HTTP client API listen address")
		peerFlag = flag.String("peers", "", "comma-separated peer list, e.g. n2=localhost:9002,n3=localhost:9003")
		peerHTTP = flag.String("peer-http", "", "comma-separated peer HTTP list, e.g. n2=http://localhost:8002,n3=http://localhost:8003")
		dataPath = flag.String("data", "", "bbolt data path")
		kvPath   = flag.String("kv-data", "", "bbolt KV state machine data path")
	)
	flag.Parse()

	if *id == "" {
		return errors.New("missing required --id")
	}

	peerIDs, peerAddrs, err := parsePeers(*peerFlag)
	if err != nil {
		return err
	}
	_, peerHTTPAddrs, err := parsePeers(*peerHTTP)
	if err != nil {
		return fmt.Errorf("parse peer HTTP addresses: %w", err)
	}

	path := *dataPath
	if path == "" {
		path = filepath.Join("data", *id+".db")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	stableStore, err := store.NewBoltRaftStore(path, time.Second)
	if err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}
	defer stableStore.Close()

	node, err := raft.NewRaftNode(*id, peerIDs, stableStore)
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}

	kvDBPath := *kvPath
	if kvDBPath == "" {
		kvDBPath = filepath.Join("data", *id+"-kv.db")
	}
	if err := os.MkdirAll(filepath.Dir(kvDBPath), 0o755); err != nil {
		return fmt.Errorf("create kv data dir: %w", err)
	}

	kvDB, err := bbolt.Open(kvDBPath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open kv store: %w", err)
	}
	defer kvDB.Close()

	kvStateMachine, err := store.NewBoltKVStateMachine(kvDB)
	if err != nil {
		return fmt.Errorf("create kv state machine: %w", err)
	}
	metrics := observability.NewMetrics(*id)

	listener, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *raftAddr, err)
	}

	resetElection := make(chan struct{}, 1)
	grpcServer := grpc.NewServer()
	raftkvpb.RegisterRaftServer(grpcServer, raftgrpc.NewServerWithAppendReset(node, resetElection))

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()

	peerClient, closePeers, err := raftgrpc.DialPeerClient(peerAddrs)
	if err != nil {
		grpcServer.Stop()
		return err
	}
	defer closePeers()
	appendClient := metrics.InstrumentAppendClient(peerClient)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpMux := http.NewServeMux()
	httpMux.Handle("/kv/", metrics.InstrumentKV(httpapi.NewHTTPHandlerWithForwarding(node, appendClient, kvStateMachine, httpapi.NewLeaderForwarder(*id, peerHTTPAddrs))))
	httpMux.Handle("/metrics", metrics.Handler())
	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           httpMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
			return
		}
		httpErr <- nil
	}()

	loopErrs := make(chan error, 3)
	go func() {
		loopErrs <- node.RunElectionTimer(ctx, peerClient, raft.ElectionTimerConfig{Reset: resetElection})
	}()
	go func() {
		loopErrs <- node.RunHeartbeatLoop(ctx, appendClient, raft.DefaultHeartbeatInterval)
	}()
	go func() {
		loopErrs <- applyCommittedLoop(ctx, node, kvStateMachine)
	}()
	go logNodeState(ctx, node, metrics, 500*time.Millisecond)

	log.Printf("node %s listening on raft=%s http=%s with peers %v", *id, *raftAddr, *httpAddr, peerIDs)

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("raft grpc server: %w", err)
		}
	case err := <-httpErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	case err := <-loopErrs:
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}

	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	return nil
}

func parsePeers(value string) ([]string, map[string]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, map[string]string{}, nil
	}

	var ids []string
	addrs := make(map[string]string)
	for _, rawPeer := range strings.Split(value, ",") {
		peer := strings.TrimSpace(rawPeer)
		parts := strings.SplitN(peer, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, nil, fmt.Errorf("invalid peer %q, want id=address", rawPeer)
		}

		id := strings.TrimSpace(parts[0])
		addr := strings.TrimSpace(parts[1])
		if _, exists := addrs[id]; exists {
			return nil, nil, fmt.Errorf("duplicate peer id %q", id)
		}

		ids = append(ids, id)
		addrs[id] = addr
	}

	return ids, addrs, nil
}

func applyCommittedLoop(ctx context.Context, node *raft.RaftNode, stateMachine raft.StateMachine) error {
	for {
		if err := node.ApplyCommitted(stateMachine); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-node.CommitReady():
		}
	}
}

func logNodeState(ctx context.Context, node *raft.RaftNode, metrics *observability.Metrics, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		lastTerm  uint64
		lastState raft.State
		lastVote  string
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			term := node.CurrentTerm()
			state := node.State()
			votedFor := node.VotedFor()
			metrics.SetCurrentTerm(term)
			metrics.SetCommitIndex(node.CommitIndex())
			if term != lastTerm || state != lastState || votedFor != lastVote {
				log.Printf("node %s state=%s term=%d voted_for=%q", node.ID(), state, term, votedFor)
				if state == raft.Leader && lastState != raft.Leader {
					metrics.RecordLeaderElection()
				}
				lastTerm = term
				lastState = state
				lastVote = votedFor
			}
		}
	}
}
