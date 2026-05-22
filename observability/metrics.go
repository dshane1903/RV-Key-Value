package observability

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	nodeID string

	registry        *prometheus.Registry
	leaderElections prometheus.Counter
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

	registry.MustRegister(metrics.leaderElections, metrics.commitIndex, metrics.currentTerm, metrics.kvRequests)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RecordLeaderElection() {
	m.leaderElections.Inc()
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
