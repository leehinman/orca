package steps_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func beatDoc(ts time.Time, acked, uptimeMs, cpuMs, rss int) string {
	return fmt.Sprintf(`{"_source":{"type":"beats_stats","timestamp":%q,"beats_stats":{"metrics":{`+
		`"beat":{"info":{"uptime":{"ms":%d}},"cpu":{"total":{"time":{"ms":%d}}},"memstats":{"rss":%d}},`+
		`"libbeat":{"output":{"events":{"acked":%d,"total":%d}}}}}}}`,
		ts.UTC().Format(time.RFC3339Nano), uptimeMs, cpuMs, rss, acked, acked)
}

var msDate = regexp.MustCompile(`"gte":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z"`)

func beatsES(t *testing.T, docs ...string) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		body, _ := io.ReadAll(r.Body)
		if !msDate.Match(body) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, `{"hits":{"hits":[%s]}}`, strings.Join(docs, ","))
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func runWait(t *testing.T, es harness.ESConfig, since time.Time, expected int, timeout time.Duration) (*harness.State, error) {
	t.Helper()
	exec, err := steps.NewFilebeatWaitFactory(es)(harness.Step{ID: "gate", FilebeatWait: &harness.FilebeatWaitStep{
		Since: "${phase.warmup.start}", Expected: harness.VarInt(expected), Timeout: harness.Duration{Duration: timeout},
	}})
	if err != nil {
		t.Fatal(err)
	}
	state := harness.NewState()
	state.SetPhaseStart("warmup", since)
	return state, exec.Run(context.Background(), state)
}

func TestFilebeatWait_ReachedAtIsFirstSampleAtExpected(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	start := since.Add(10 * time.Second)
	at := func(s int) time.Time { return start.Add(time.Duration(s) * time.Second) }
	es := beatsES(t,
		beatDoc(at(5), 400, 5000, 900, 1e8),
		beatDoc(at(10), 1000, 10000, 1800, 1e8),
		beatDoc(at(15), 1000, 15000, 1900, 1e8),
	)
	state, err := runWait(t, es, since, 1000, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := state.Resolve("${steps.gate.reachedAt}")
	if want := at(10).Format(time.RFC3339Nano); got != want {
		t.Errorf("reachedAt: got %q, want %q", got, want)
	}
}

func TestFilebeatWait_IgnoresProcessStartedBeforeSince(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	old := beatDoc(since.Add(2*time.Second), 5000, 3600*1000, 1, 1)
	_, err := runWait(t, beatsES(t, old), since, 1, time.Second)
	if err == nil || !strings.Contains(err.Error(), "no filebeat samples") {
		t.Fatalf("want no-samples timeout, got %v", err)
	}
}

func TestFilebeatWait_TimeoutReportsLatestAcked(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	es := beatsES(t, beatDoc(since.Add(10*time.Second), 250, 5000, 1, 1))
	_, err := runWait(t, es, since, 1000, time.Second)
	if err == nil || !strings.Contains(err.Error(), "acked=250") {
		t.Fatalf("want timeout naming acked=250, got %v", err)
	}
}

func TestFilebeatWait_ZeroExpectedNeedsEventsSent(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	es := beatsES(t, beatDoc(since.Add(10*time.Second), 0, 5000, 1, 1))
	if _, err := runWait(t, es, since, 0, time.Second); err == nil {
		t.Fatal("acked=0 must not satisfy the warmup check")
	}
}

func TestFilebeatMetrics_RatesCPUAndMemory(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	start := since.Add(time.Second)
	at := func(s int) time.Time { return start.Add(time.Duration(s) * time.Second) }
	es := beatsES(t,
		beatDoc(at(5), 0, 5000, 1000, 100e6),
		beatDoc(at(10), 5000, 10000, 3500, 120e6),
		beatDoc(at(15), 15000, 15000, 6000, 140e6),
	)
	exec, err := steps.NewFilebeatMetricsFactory(es)(harness.Step{ID: "fb", FilebeatMetrics: &harness.FilebeatMetricsStep{
		Since: "${phase.warmup.start}", Until: "${phase.collect.start}", OutputDir: t.TempDir(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	state := harness.NewState()
	state.SetPhaseStart("warmup", since)
	state.SetPhaseStart("collect", at(20))
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	raw, _ := state.Resolve("${steps.fb.summary}")
	var sum harness.FilebeatMetricsSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Samples != 3 || sum.Events != 15000 {
		t.Errorf("samples/events: got %d/%d, want 3/15000", sum.Samples, sum.Events)
	}
	if sum.EventsPerSecAvg != 1500 || sum.EventsPerSecMax != 2000 {
		t.Errorf("events/s avg/max: got %v/%v, want 1500/2000", sum.EventsPerSecAvg, sum.EventsPerSecMax)
	}
	if sum.CPUAvg != 50 || sum.CPUMax != 50 {
		t.Errorf("cpu avg/max: got %v/%v, want 50/50", sum.CPUAvg, sum.CPUMax)
	}
	if sum.RSSMax != 140e6 {
		t.Errorf("rss max: got %v", sum.RSSMax)
	}
}

func TestFilebeatMetrics_WritesTSVAndCharts(t *testing.T) {
	since := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return since.Add(time.Duration(1+s) * time.Second) }
	es := beatsES(t,
		beatDoc(at(5), 0, 5000, 1000, 1e8),
		beatDoc(at(10), 5000, 10000, 3500, 1e8),
	)
	dir := t.TempDir()
	exec, _ := steps.NewFilebeatMetricsFactory(es)(harness.Step{ID: "fb", FilebeatMetrics: &harness.FilebeatMetricsStep{
		Since: "${phase.warmup.start}", Until: "${phase.collect.start}", OutputDir: dir,
	}})
	state := harness.NewState()
	state.SetPhaseStart("warmup", since)
	state.SetPhaseStart("collect", at(20))
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"filebeat-timeseries.tsv", "filebeat-cpu-chart.svg", "filebeat-memory-chart.svg", "filebeat-events-chart.svg"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
}
