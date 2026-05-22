package observability

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shaneduncan/rv-key-value/raft"
)

type Metrics struct {
	nodeID string

	registry        *prometheus.Registry
	leaderElections prometheus.Counter
	replicationTime prometheus.ObserverVec
	commitIndex     prometheus.Gauge
	currentTerm     prometheus.Gauge
	kvRequests      *prometheus.CounterVec
}

func NewMetrics(nodeID string) *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		nodeID:   nodeID,
		registry: registry,
		leaderElections: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "raft_leader_elections_total",
			Help:        "Total number of times this node became leader.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		replicationTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "raft_log_replication_latency_seconds",
			Help:        "AppendEntries round-trip latency by peer and result.",
			ConstLabels: prometheus.Labels{"node": nodeID},
			Buckets:     prometheus.DefBuckets,
		}, []string{"peer", "result"}),
		commitIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_commit_index",
			Help:        "Current Raft commit index for this node.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		currentTerm: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "raft_term_current",
			Help:        "Current Raft term for this node.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}),
		kvRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "kv_requests_total",
			Help:        "Total HTTP KV API requests by operation and status code.",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}, []string{"operation", "status"}),
	}

	registry.MustRegister(metrics.leaderElections, metrics.replicationTime, metrics.commitIndex, metrics.currentTerm, metrics.kvRequests)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RecordLeaderElection() {
	m.leaderElections.Inc()
}

func (m *Metrics) ObserveReplicationLatency(peerID, result string, duration time.Duration) {
	m.replicationTime.WithLabelValues(peerID, result).Observe(duration.Seconds())
}

func (m *Metrics) SetCommitIndex(index uint64) {
	m.commitIndex.Set(float64(index))
}

func (m *Metrics) SetCurrentTerm(term uint64) {
	m.currentTerm.Set(float64(term))
}

func (m *Metrics) InstrumentKV(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		m.kvRequests.WithLabelValues(operationForMethod(r.Method), strconv.Itoa(recorder.status)).Inc()
	})
}

func (m *Metrics) InstrumentAppendClient(next raft.AppendClient) raft.AppendClient {
	return appendClientFunc(func(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
		start := time.Now()
		resp, err := next.AppendEntries(ctx, peerID, req)
		result := "success"
		if err != nil {
			result = "error"
		} else if !resp.Success {
			result = "rejected"
		}
		m.ObserveReplicationLatency(peerID, result, time.Since(start))
		return resp, err
	})
}

type appendClientFunc func(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)

func (f appendClientFunc) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return f(ctx, peerID, req)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func operationForMethod(method string) string {
	switch strings.ToUpper(method) {
	case http.MethodGet:
		return "get"
	case http.MethodPut:
		return "put"
	case http.MethodDelete:
		return "delete"
	default:
		return "unknown"
	}
}
