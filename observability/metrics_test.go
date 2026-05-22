package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandlerExposesRaftGaugesAndCounters(t *testing.T) {
	metrics := NewMetrics("n1")
	metrics.SetCurrentTerm(3)
	metrics.SetCommitIndex(7)
	metrics.RecordLeaderElection()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(resp, req)

	body := resp.Body.String()
	for _, want := range []string{
		`raft_term_current{node="n1"} 3`,
		`raft_commit_index{node="n1"} 7`,
		`raft_leader_elections_total{node="n1"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q\n%s", want, body)
		}
	}
}

func TestInstrumentKVRecordsOperationAndStatus(t *testing.T) {
	metrics := NewMetrics("n1")
	handler := metrics.InstrumentKV(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPut, "/kv/name", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	metricsResp := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(metricsResp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}

	want := `kv_requests_total{node="n1",operation="put",status="204"} 1`
	if !strings.Contains(string(body), want) {
		t.Fatalf("metrics body missing %q\n%s", want, string(body))
	}
}
