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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

const samplesDir = "../../samples"

func loadSample(t *testing.T, rel string) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(samplesDir, rel))
	if err != nil {
		t.Fatalf("read sample %s: %v", rel, err)
	}
	var hits []struct {
		Source map[string]interface{} `json:"_source"`
	}
	if err := json.Unmarshal(data, &hits); err != nil {
		t.Fatalf("parse sample %s: %v", rel, err)
	}
	out := make([]map[string]interface{}, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.Source)
	}
	return out
}

func processModeDocs(t *testing.T) []map[string]interface{} {
	var docs []map[string]interface{}
	for _, f := range []string{"process_mode/sample-full.json", "process_mode/sample-metricbeat.json", "process_mode/sample-filebeat_input.json"} {
		docs = append(docs, loadSample(t, f)...)
	}
	return docs
}

func otelModeDocs(t *testing.T) []map[string]interface{} {
	var docs []map[string]interface{}
	for _, f := range []string{"otel_mode/sample-full.json", "otel_mode/sample-filebeat_input.json"} {
		docs = append(docs, loadSample(t, f)...)
	}
	return docs
}

func docMetricset(doc map[string]interface{}) string {
	ms, _ := doc["metricset"].(map[string]interface{})
	name, _ := ms["name"].(string)
	return name
}

func docComponentID(doc map[string]interface{}) string {
	c, _ := doc["component"].(map[string]interface{})
	id, _ := c["id"].(string)
	return id
}

// fixtureES serves the given documents for FetchDocs queries, filtered by the
// metricset.name term in the request, and canned responses for aggregations.
func fixtureES(t *testing.T, docs []map[string]interface{}, errorLogs map[string]int64, ingestCounts []int64) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		q := string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case strings.Contains(q, "date_histogram"):
			var buckets []string
			base := time.Date(2026, 9, 24, 18, 49, 0, 0, time.UTC)
			for i, c := range ingestCounts {
				buckets = append(buckets, fmt.Sprintf(`{"key":%d,"doc_count":%d}`, base.Add(time.Duration(i)*time.Minute).UnixMilli(), c))
			}
			fmt.Fprintf(w, `{"aggregations":{"hist":{"buckets":[%s]}}}`, strings.Join(buckets, ","))
		case strings.Contains(q, `"terms"`):
			var buckets []string
			for k, v := range errorLogs {
				buckets = append(buckets, fmt.Sprintf(`{"key":%q,"doc_count":%d}`, k, v))
			}
			fmt.Fprintf(w, `{"aggregations":{"groups":{"buckets":[%s]}}}`, strings.Join(buckets, ","))
		default:
			want := "stats"
			if strings.Contains(q, `"metricset.name":"json"`) {
				want = "json"
			}
			var hits []string
			for _, d := range docs {
				if docMetricset(d) != want {
					continue
				}
				src, _ := json.Marshal(d)
				hits = append(hits, fmt.Sprintf(`{"_source":%s}`, src))
			}
			fmt.Fprintf(w, `{"hits":{"hits":[%s]}}`, strings.Join(hits, ","))
		}
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func agentMetricsStep(outDir string, integrations []string) harness.Step {
	return harness.Step{
		ID: "am",
		EsAgentMetrics: &harness.EsAgentMetricsStep{
			Since:        "${phase.execute.start}",
			Until:        "${phase.collect.start}",
			OutputDir:    outDir,
			Integrations: integrations,
			Ingest:       []harness.IngestSpec{{Name: "prometheus", Index: "metrics-prometheus-*"}},
			Agents:       []harness.AgentSpec{{ID: "agent", Label: "bench", Filter: map[string]string{"host.hostname": "bench-control-plane"}}},
		},
	}
}

func runAgentMetrics(t *testing.T, es harness.ESConfig, step harness.Step) harness.AgentMetricsSummary {
	t.Helper()
	sum := runAgentMetricsRaw(t, es, step)
	if len(sum.Agents) != 1 {
		t.Fatalf("expected 1 agent summary, got %d", len(sum.Agents))
	}
	return sum
}

func runAgentMetricsRaw(t *testing.T, es harness.ESConfig, step harness.Step) harness.AgentMetricsSummary {
	t.Helper()
	exec, err := steps.NewEsAgentMetricsFactory(es, harness.KibanaConfig{})(step)
	if err != nil {
		t.Fatal(err)
	}
	state := stateWithTimeRange()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("run: %v", err)
	}
	raw, err := state.Resolve("${steps.am.summary}")
	if err != nil {
		t.Fatalf("summary not in state: %v", err)
	}
	var sum harness.AgentMetricsSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	return sum
}

func processIDs(a harness.AgentSummary) []string {
	ids := make([]string, len(a.Processes))
	for i, p := range a.Processes {
		ids[i] = p.ID
	}
	return ids
}

func findIntegration(t *testing.T, a harness.AgentSummary, id string) harness.IntegrationSummary {
	t.Helper()
	for _, in := range a.Integrations {
		if in.ID == id {
			return in
		}
	}
	t.Fatalf("integration %q not in summary: %+v", id, a.Integrations)
	return harness.IntegrationSummary{}
}

func findEvents(a harness.AgentSummary, dataset, id string) (harness.EventsSummary, bool) {
	for _, e := range a.Events {
		if e.Dataset == dataset && e.ID == id {
			return e, true
		}
	}
	return harness.EventsSummary{}, false
}

func assertFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("expected output file %s: %v", n, err)
		}
	}
}

func TestAgentMetrics_ProcessMode(t *testing.T) {
	outDir := t.TempDir()
	es := fixtureES(t, processModeDocs(t), map[string]int64{"prometheus/metrics-default": 1251}, []int64{600, 1200, 1800})
	sum := runAgentMetrics(t, es, agentMetricsStep(outDir, []string{"prometheus/metrics-default"}))
	a := sum.Agents[0]

	want := []string{"elastic-agent", "elastic-otel-collector", "prometheus/metrics-default", "total"}
	if got := processIDs(a); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("processes: got %v, want %v", got, want)
	}
	for _, p := range a.Processes {
		if p.Samples < 2 || p.MemAvg <= 0 || p.MemMax < p.MemAvg {
			t.Errorf("process %s: implausible summary %+v", p.ID, p)
		}
	}
	if a.Processes[2].Binary != "metricbeat" {
		t.Errorf("prometheus binary: got %q, want metricbeat", a.Processes[2].Binary)
	}

	in := findIntegration(t, a, "prometheus/metrics-default")
	if in.Mode != "process" {
		t.Errorf("mode: got %q, want process", in.Mode)
	}
	if in.ErrorLogs != 1251 || a.ErrorLogs != 1251 {
		t.Errorf("error logs: integration %d agent %d, want 1251", in.ErrorLogs, a.ErrorLogs)
	}
	ev, ok := findEvents(a, "elastic_agent.metricbeat", "prometheus/metrics-default")
	if !ok || ev.AvgPerSec <= 0 {
		t.Errorf("metricbeat pipeline events missing or zero: %+v", ev)
	}
	if in.EventsPerSecAvg != ev.AvgPerSec {
		t.Errorf("integration events/s %v should equal metricbeat series %v", in.EventsPerSecAvg, ev.AvgPerSec)
	}
	if _, ok := findEvents(a, "elastic_agent.elastic_agent", "http/metrics-monitoring"); !ok {
		t.Error("monitoring sub-component from dotted component.id not discovered")
	}
	if ev, ok := findEvents(a, "elastic_agent.filebeat_input", "filestream-monitoring"); !ok || ev.Members != 1 {
		t.Errorf("filebeat_input series should roll up to its parent input: %+v", a.Events)
	}

	if len(sum.Ingest) != 1 || sum.Ingest[0].Total != 3600 || sum.Ingest[0].MaxPerSec != 30 || sum.Ingest[0].AvgPerSec != 20 {
		t.Errorf("ingest summary: %+v", sum.Ingest)
	}

	assertFiles(t, outDir,
		"agent-elastic-agent.tsv", "agent-elastic-otel-collector.tsv", "agent-prometheus-metrics-default.tsv",
		"agent-processes.tsv", "agent-memory-chart.svg", "agent-cpu-chart.svg",
		"agent-events.tsv", "agent-events-chart.svg",
		"ingest-prometheus.tsv", "ingest-prometheus-chart.svg",
	)

	raw, _ := os.ReadFile(filepath.Join(outDir, "agent-prometheus-metrics-default.tsv"))
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 4 {
		t.Fatalf("process tsv rows: got %d, want header + 3", len(lines))
	}
	if !strings.HasPrefix(lines[0], "timestamp\tmem_size\tcpu_ms\tcpu_cores\tcpu_pct") {
		t.Errorf("header: %q", lines[0])
	}
	if cols := strings.Split(lines[2], "\t"); len(cols) != 5 || cols[4] == "" {
		t.Errorf("second row should carry cpu_pct: %q", lines[2])
	}

	raw, _ = os.ReadFile(filepath.Join(outDir, "agent-processes.tsv"))
	header := strings.Split(strings.SplitN(string(raw), "\n", 2)[0], "\t")
	wantHeader := "timestamp\tmem_elastic-agent\tmem_elastic-otel-collector\tmem_prometheus-metrics-default\tmem_total\tcpu_elastic-agent\tcpu_elastic-otel-collector\tcpu_prometheus-metrics-default\tcpu_total"
	if strings.Join(header, "\t") != wantHeader {
		t.Errorf("processes header:\n got %q\nwant %q", strings.Join(header, "\t"), wantHeader)
	}
}

func TestAgentMetrics_SingleAgentHasNoAllAgentsSummary(t *testing.T) {
	es := fixtureES(t, processModeDocs(t), nil, nil)
	sum := runAgentMetrics(t, es, agentMetricsStep(t.TempDir(), nil))
	if sum.AllAgents != nil {
		t.Errorf("expected no all-agents summary for a single agent, got %+v", sum.AllAgents)
	}
}

func TestAgentMetrics_MultipleAgentsAveraged(t *testing.T) {
	es := fixtureES(t, processModeDocs(t), nil, nil)
	step := agentMetricsStep(t.TempDir(), nil)
	step.EsAgentMetrics.Agents = []harness.AgentSpec{
		{ID: "agent-a", Label: "a", Filter: map[string]string{"host.hostname": "bench-control-plane"}},
		{ID: "agent-b", Label: "b", Filter: map[string]string{"host.hostname": "bench-control-plane"}},
	}
	sum := runAgentMetricsRaw(t, es, step)

	if len(sum.Agents) != 2 {
		t.Fatalf("agents: %+v", sum.Agents)
	}
	wantTotal := findProcess(t, sum.Agents[0], "total")
	if sum.AllAgents == nil {
		t.Fatalf("expected an all-agents summary for 2 agents")
	}
	if sum.AllAgents.Count != 2 {
		t.Errorf("count: got %d, want 2", sum.AllAgents.Count)
	}
	// Both agents are served identical fixture data, so the average across
	// agents should equal either agent's own total.
	if sum.AllAgents.MemAvg != wantTotal.MemAvg || sum.AllAgents.CPUAvg != wantTotal.CPUAvg {
		t.Errorf("all-agents average: got %+v, want mem=%v cpu=%v", sum.AllAgents, wantTotal.MemAvg, wantTotal.CPUAvg)
	}
}

func findProcess(t *testing.T, a harness.AgentSummary, id string) harness.ProcessSummary {
	t.Helper()
	for _, p := range a.Processes {
		if p.ID == id {
			return p
		}
	}
	t.Fatalf("process %q not found in %+v", id, a.Processes)
	return harness.ProcessSummary{}
}

func TestAgentMetrics_OtelMode(t *testing.T) {
	outDir := t.TempDir()
	es := fixtureES(t, otelModeDocs(t), map[string]int64{"prometheus/metrics-default": 42}, []int64{60})
	sum := runAgentMetrics(t, es, agentMetricsStep(outDir, []string{"prometheus/metrics-default"}))
	a := sum.Agents[0]

	want := []string{"elastic-agent", "elastic-otel-collector", "total"}
	if got := processIDs(a); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("processes: got %v, want %v", got, want)
	}
	in := findIntegration(t, a, "prometheus/metrics-default")
	if in.Mode != "otel" {
		t.Errorf("mode: got %q, want otel", in.Mode)
	}
	if in.ErrorLogs != 42 {
		t.Errorf("error logs: got %d, want 42", in.ErrorLogs)
	}
	recv, ok := findEvents(a, "elastic_agent.elastic_agent", "prometheus/metrics-default/prometheus")
	if !ok {
		t.Fatal("receiver series not discovered")
	}
	if in.EventsPerSecAvg != recv.AvgPerSec {
		t.Errorf("integration events/s %v should come from receiver %v", in.EventsPerSecAvg, recv.AvgPerSec)
	}
	if _, ok := findEvents(a, "elastic_agent.elastic_agent", "elasticsearch-default"); !ok {
		t.Error("exporter series not discovered")
	}
	assertFiles(t, outDir, "agent-processes.tsv", "agent-events.tsv", "agent-events-chart.svg")
	if _, err := os.Stat(filepath.Join(outDir, "agent-prometheus-metrics-default.tsv")); err == nil {
		t.Error("otel mode must not produce a process tsv for the integration")
	}
}

func TestAgentMetrics_MixedMode(t *testing.T) {
	docs := otelModeDocs(t)
	for _, d := range processModeDocs(t) {
		if docMetricset(d) == "json" && docComponentID(d) == "prometheus/metrics-default" {
			docs = append(docs, d)
		}
	}
	outDir := t.TempDir()
	es := fixtureES(t, docs, nil, nil)
	sum := runAgentMetrics(t, es, agentMetricsStep(outDir, []string{"prometheus/metrics-default", "http/metrics-monitoring", "nope/absent"}))
	a := sum.Agents[0]

	if got := findIntegration(t, a, "prometheus/metrics-default").Mode; got != "process" {
		t.Errorf("prometheus mode: got %q, want process (json doc wins)", got)
	}
	if got := findIntegration(t, a, "http/metrics-monitoring").Mode; got != "otel" {
		t.Errorf("http/metrics-monitoring mode: got %q, want otel", got)
	}
	if got := findIntegration(t, a, "nope/absent").Mode; got != "absent" {
		t.Errorf("unknown integration mode: got %q, want absent", got)
	}
	if got := processIDs(a); len(got) != 4 {
		t.Errorf("mixed mode processes: got %v", got)
	}
	if sum.Ingest[0].Total != 0 {
		t.Errorf("empty histogram should give zero total: %+v", sum.Ingest)
	}
}

func TestAgentMetrics_IngestRequiresIndex(t *testing.T) {
	step := agentMetricsStep(t.TempDir(), nil)
	step.EsAgentMetrics.Ingest = []harness.IngestSpec{{Name: "x"}}
	exec, _ := steps.NewEsAgentMetricsFactory(fixtureES(t, nil, nil, nil), harness.KibanaConfig{})(step)
	if err := exec.Run(context.Background(), stateWithTimeRange()); err == nil {
		t.Fatal("expected error for ingest without index")
	}
}

func TestAgentMetrics_NoDocsStillSucceeds(t *testing.T) {
	outDir := t.TempDir()
	step := agentMetricsStep(outDir, []string{"prometheus/metrics-default"})
	step.EsAgentMetrics.Ingest = nil
	sum := runAgentMetrics(t, fixtureES(t, nil, nil, nil), step)
	a := sum.Agents[0]
	if len(a.Processes) != 0 {
		t.Errorf("expected no processes, got %v", processIDs(a))
	}
	if got := a.Integrations[0].Mode; got != "absent" {
		t.Errorf("mode: got %q, want absent", got)
	}
	entries, _ := os.ReadDir(outDir)
	if len(entries) != 0 {
		t.Errorf("expected no output files, got %d", len(entries))
	}
}

type fakeKibanaReporting struct {
	mu         sync.Mutex
	jobParams  []string
	downloads  map[string]int
	readyAfter int
	failJobs   bool
	pngBody    string
}

func newFakeKibanaReporting(t *testing.T) (*fakeKibanaReporting, harness.KibanaConfig) {
	t.Helper()
	f := &fakeKibanaReporting{downloads: map[string]int{}, readyAfter: 2, pngBody: "\x89PNG fake"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.URL.Path == "/api/status":
			fmt.Fprint(w, `{"version":{"number":"9.5.4"}}`)
		case strings.HasPrefix(r.URL.Path, "/api/reporting/generate/pngV2"):
			if r.Header.Get("kbn-xsrf") == "" {
				t.Error("missing kbn-xsrf header")
			}
			f.jobParams = append(f.jobParams, r.URL.Query().Get("jobParams"))
			id := fmt.Sprintf("job-%d", len(f.jobParams))
			fmt.Fprintf(w, `{"path":"/api/reporting/jobs/download/%s","job":{"id":"%s"}}`, id, id)
		case strings.HasPrefix(r.URL.Path, "/api/reporting/jobs/download/"):
			id := strings.TrimPrefix(r.URL.Path, "/api/reporting/jobs/download/")
			f.downloads[id]++
			if f.downloads[id] <= f.readyAfter {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if f.failJobs {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, `{"statusCode":500,"message":"Reporting generation failed: boom"}`)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			fmt.Fprint(w, f.pngBody)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return f, harness.KibanaConfig{URL: srv.URL, Username: "elastic", Password: "changeme"}
}

func TestAgentMetrics_DashboardPNG(t *testing.T) {
	outDir := t.TempDir()
	step := agentMetricsStep(outDir, nil)
	step.EsAgentMetrics.Agents[0].Label = "${steps.infra.elastic_agent_hostname_0}"
	step.EsAgentMetrics.Dashboard = &harness.DashboardSpec{Timeout: harness.Duration{Duration: 30 * time.Second}}
	fk, kb := newFakeKibanaReporting(t)

	exec, err := steps.NewEsAgentMetricsFactory(fixtureES(t, processModeDocs(t), nil, []int64{1}), kb)(step)
	if err != nil {
		t.Fatal(err)
	}
	state := stateWithTimeRange()
	state.SetStepOutput("infra", "elastic_agent_hostname_0", "bench-elastic-agent-0")
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("run: %v", err)
	}

	dest := filepath.Join(outDir, "agent-dashboard.png")
	data, err := os.ReadFile(dest)
	if err != nil || string(data) != fk.pngBody {
		t.Fatalf("dashboard png not written correctly: %v %q", err, data)
	}
	raw, _ := state.Resolve("${steps.am.summary}")
	var sum harness.AgentMetricsSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Agents[0].DashboardPNG != dest {
		t.Errorf("summary dashboard_png: got %q, want %q", sum.Agents[0].DashboardPNG, dest)
	}
	if sum.Agents[0].Label != "bench-elastic-agent-0" {
		t.Errorf("label template not resolved: %q", sum.Agents[0].Label)
	}
	if len(fk.jobParams) != 1 {
		t.Fatalf("expected 1 report request, got %d", len(fk.jobParams))
	}
	jp := fk.jobParams[0]
	for _, want := range []string{
		"dashboardId:elastic_agent-f47f18cc-9c7d-4278-b2ea-a6dee816d395",
		"title:'bench-elastic-agent-0 agent metrics'",
		`query:'host.hostname:"bench-control-plane"'`,
		"version:'9.5.4'",
		"height:2400,width:1600",
		"timeRange:(from:'",
	} {
		if !strings.Contains(jp, want) {
			t.Errorf("jobParams missing %s:\n%s", want, jp)
		}
	}
	if fk.downloads["job-1"] < 3 {
		t.Errorf("expected polling through 503s, got %d downloads", fk.downloads["job-1"])
	}
}

func TestAgentMetrics_DashboardPNGFailure(t *testing.T) {
	outDir := t.TempDir()
	step := agentMetricsStep(outDir, nil)
	step.EsAgentMetrics.Dashboard = &harness.DashboardSpec{ID: "custom-dash", Width: 800, Height: 600}
	fk, kb := newFakeKibanaReporting(t)
	fk.failJobs = true
	fk.readyAfter = 0

	exec, _ := steps.NewEsAgentMetricsFactory(fixtureES(t, processModeDocs(t), nil, []int64{1}), kb)(step)
	state := stateWithTimeRange()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("dashboard failure must not fail the step: %v", err)
	}
	if !strings.Contains(fk.jobParams[0], "dashboardId:custom-dash") || !strings.Contains(fk.jobParams[0], "height:600,width:800") {
		t.Errorf("custom spec not applied: %s", fk.jobParams[0])
	}
	if _, err := os.Stat(filepath.Join(outDir, "agent-processes.tsv")); err != nil {
		t.Error("metrics TSVs should be written regardless of the dashboard")
	}
	raw, _ := state.Resolve("${steps.am.summary}")
	var sum harness.AgentMetricsSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Agents[0].DashboardPNG != "" || !strings.Contains(sum.Agents[0].DashboardErr, "boom") {
		t.Errorf("expected dashboard_error with boom, got png=%q err=%q", sum.Agents[0].DashboardPNG, sum.Agents[0].DashboardErr)
	}
}

func TestAgentMetrics_DashboardRequiresKibanaURL(t *testing.T) {
	step := agentMetricsStep(t.TempDir(), nil)
	step.EsAgentMetrics.Dashboard = &harness.DashboardSpec{}
	exec, _ := steps.NewEsAgentMetricsFactory(fixtureES(t, processModeDocs(t), nil, []int64{1}), harness.KibanaConfig{})(step)
	err := exec.Run(context.Background(), stateWithTimeRange())
	if err == nil || !strings.Contains(err.Error(), "kibana.url") {
		t.Fatalf("expected kibana.url error, got %v", err)
	}
}
