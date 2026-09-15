package steps_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func esCountClient(t *testing.T, count int64) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintf(w, `{"count":%d,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`, count)
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func docCountStep(since string, expected int, timeout time.Duration) harness.Step {
	return harness.Step{
		ID: "count",
		EsDocCount: &harness.EsDocCountStep{
			Index:    "logs-*",
			Since:    since,
			Expected: harness.VarInt(expected),
			Timeout:  harness.Duration{Duration: timeout},
		},
	}
}

func stateWithExecuteStart(t time.Time) *harness.State {
	s := harness.NewState()
	s.SetPhaseStart("execute", t)
	return s
}

func TestEsDocCountFactory_WritesActualCountToState(t *testing.T) {
	exec, _ := steps.NewEsDocCountFactory(esCountClient(t, 48000))(
		docCountStep("${phase.execute.start}", 48000, 5*time.Second),
	)
	state := stateWithExecuteStart(time.Now())
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := state.Resolve("${steps.count.actualCount}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "48000" {
		t.Errorf("actualCount: got %q, want %q", got, "48000")
	}
}

func TestEsDocCountFactory_WritesConfirmedAt(t *testing.T) {
	exec, _ := steps.NewEsDocCountFactory(esCountClient(t, 100))(
		docCountStep("${phase.execute.start}", 100, 5*time.Second),
	)
	state := stateWithExecuteStart(time.Now())
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v, err := state.Resolve("${steps.count.confirmedAt}")
	if err != nil || v == "" {
		t.Fatalf("confirmedAt not set in state: %v", err)
	}
}

func TestEsDocCountFactory_Timeout(t *testing.T) {
	exec, _ := steps.NewEsDocCountFactory(esCountClient(t, 0))(
		docCountStep("${phase.execute.start}", 48000, 50*time.Millisecond),
	)
	err := exec.Run(context.Background(), stateWithExecuteStart(time.Now()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestEsDocCountFactory_SinceResolutionFailure(t *testing.T) {
	exec, _ := steps.NewEsDocCountFactory(esCountClient(t, 0))(
		docCountStep("${phase.execute.start}", 48000, 5*time.Second),
	)
	err := exec.Run(context.Background(), harness.NewState())
	if err == nil {
		t.Fatal("expected error for unresolvable since")
	}
}

func TestEsDocCountFactory_NoOutputWithoutID(t *testing.T) {
	exec, _ := steps.NewEsDocCountFactory(esCountClient(t, 100))(harness.Step{
		EsDocCount: &harness.EsDocCountStep{
			Index:    "logs-*",
			Since:    "${phase.execute.start}",
			Expected: 100,
			Timeout:  harness.Duration{Duration: 5 * time.Second},
		},
	})
	state := stateWithExecuteStart(time.Now())
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := state.Resolve("${steps.count.actualCount}"); err == nil {
		t.Error("expected no output when step has no ID")
	}
}
