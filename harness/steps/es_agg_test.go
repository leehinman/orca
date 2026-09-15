package steps_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

const sampleAggResp = `{
  "aggregations": {
    "mem_size_min": {"value": 12345678.0},
    "mem_size_max": {"value": 23456789.0},
    "mem_size_avg": {"value": 18000000.0},
    "mem_size_p95": {"values": {"95.0": 22000000.0}}
  }
}`

func aggESClient(t *testing.T) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprint(w, sampleAggResp)
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func aggStep(id string) harness.Step {
	return harness.Step{
		ID: id,
		EsAgg: &harness.EsAggStep{
			Index: "metrics-*",
			Since: "${phase.execute.start}",
			Until: "${phase.collect.start}",
			Filter: map[string]string{
				"data_stream.dataset": "elastic_agent.elastic_agent",
				"component.id":        "elastic_agent",
			},
			Metrics: []elastic.AggMetricSpec{
				{Name: "mem_size", Field: "system.process.memory.size", Aggs: []string{"min", "max", "avg", "p95"}},
			},
		},
	}
}

func stateWithTimeRange() *harness.State {
	s := harness.NewState()
	s.SetPhaseStart("execute", time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC))
	s.SetPhaseStart("collect", time.Date(2024, 1, 15, 12, 5, 0, 0, time.UTC))
	return s
}

func TestEsAggFactory_OutputsWrittenToState(t *testing.T) {
	exec, _ := steps.NewEsAggFactory(aggESClient(t))(aggStep("metrics"))
	state := stateWithTimeRange()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, key := range []string{"mem_size_min", "mem_size_max", "mem_size_avg", "mem_size_p95"} {
		got, err := state.Resolve("${steps.metrics." + key + "}")
		if err != nil {
			t.Errorf("%s: unexpected resolve error: %v", key, err)
			continue
		}
		if got == "" {
			t.Errorf("%s: got empty value", key)
		}
	}
}

func TestEsAggFactory_SinceResolutionFailure(t *testing.T) {
	exec, _ := steps.NewEsAggFactory(aggESClient(t))(aggStep("metrics"))
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for unresolvable since")
	}
}

func TestEsAggFactory_UntilResolutionFailure(t *testing.T) {
	exec, _ := steps.NewEsAggFactory(aggESClient(t))(aggStep("metrics"))
	state := harness.NewState()
	state.SetPhaseStart("execute", time.Now())
	if err := exec.Run(context.Background(), state); err == nil {
		t.Fatal("expected error for unresolvable until")
	}
}

func TestEsAggFactory_ESError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error":{"type":"server_error"}}`)
	}))
	t.Cleanup(srv.Close)
	exec, _ := steps.NewEsAggFactory(harness.ESConfig{URL: srv.URL})(aggStep("metrics"))
	if err := exec.Run(context.Background(), stateWithTimeRange()); err == nil {
		t.Fatal("expected error for ES 500 response")
	}
}

func TestEsAggFactory_NoOutputWithoutID(t *testing.T) {
	step := aggStep("")
	exec, _ := steps.NewEsAggFactory(aggESClient(t))(step)
	state := stateWithTimeRange()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := state.Resolve("${steps.metrics.mem_size_min}"); err == nil {
		t.Error("expected no step outputs when step has no ID")
	}
}
