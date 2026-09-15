package harness

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/elastic/orca/elastic"
)

func sampleResult() *BenchmarkResult {
	return &BenchmarkResult{
		Name: "k8s-log-ingestion",
		Phases: []PhaseResult{
			{Name: "setup", DurationSec: 3.2},
			{Name: "configure", DurationSec: 12.1},
			{Name: "execute", DurationSec: 45.0},
			{Name: "collect", DurationSec: 8.3},
			{Name: "report", DurationSec: 1.4},
			{Name: "teardown", DurationSec: 2.1},
		},
		LogIngestion: LogIngestionResult{
			Expected:   48000,
			Actual:     48000,
			ElapsedSec: 6.2,
		},
		Metrics: []MetricResult{
			{
				Name: "mem_size",
				Aggs: []AggValue{
					{Name: "min", Value: 12345678},
					{Name: "max", Value: 23456789},
					{Name: "avg", Value: 18000000},
					{Name: "p95", Value: 22000000},
				},
			},
		},
	}
}

func TestPrint_ContainsBenchmarkName(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().Print(&buf)
	if !strings.Contains(buf.String(), "k8s-log-ingestion") {
		t.Errorf("output missing benchmark name:\n%s", buf.String())
	}
}

func TestPrint_ContainsAllPhaseNames(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().Print(&buf)
	out := buf.String()
	for _, name := range []string{"setup", "configure", "execute", "collect", "report", "teardown"} {
		if !strings.Contains(out, name) {
			t.Errorf("output missing phase %q:\n%s", name, out)
		}
	}
}

func TestPrint_ContainsPhaseDurations(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().Print(&buf)
	out := buf.String()
	for _, want := range []string{"3.200s", "45.000s"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing duration %q:\n%s", want, out)
		}
	}
}

func TestPrint_ContainsLogIngestionSection(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().Print(&buf)
	out := buf.String()
	for _, want := range []string{"log ingestion", "expected", "actual", "elapsed", "48000"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestPrint_ContainsMetricsSection(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().Print(&buf)
	out := buf.String()
	for _, want := range []string{"mem_size", "min", "max", "avg", "p95"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestPrint_EmptyPhasesSectionOmitted(t *testing.T) {
	r := &BenchmarkResult{Name: "empty"}
	var buf bytes.Buffer
	r.Print(&buf)
	if !strings.Contains(buf.String(), "empty") {
		t.Errorf("output missing name:\n%s", buf.String())
	}
}

func TestPrint_NoMetricsDoesNotPanic(t *testing.T) {
	r := &BenchmarkResult{Name: "no-metrics"}
	var buf bytes.Buffer
	r.Print(&buf)
}

func TestJSON_ValidOutput(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleResult().JSON(&buf); err != nil {
		t.Fatalf("JSON() error: %v", err)
	}
	if !json.Valid(buf.Bytes()) {
		t.Errorf("JSON() produced invalid JSON:\n%s", buf.String())
	}
}

func TestJSON_RoundTrip(t *testing.T) {
	r := sampleResult()
	var buf bytes.Buffer
	if err := r.JSON(&buf); err != nil {
		t.Fatalf("JSON() error: %v", err)
	}
	var got BenchmarkResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got.Name != r.Name {
		t.Errorf("Name: got %q, want %q", got.Name, r.Name)
	}
	if len(got.Phases) != len(r.Phases) {
		t.Errorf("Phases len: got %d, want %d", len(got.Phases), len(r.Phases))
	}
	if got.LogIngestion.Expected != r.LogIngestion.Expected {
		t.Errorf("Expected: got %d, want %d", got.LogIngestion.Expected, r.LogIngestion.Expected)
	}
	if len(got.Metrics) != len(r.Metrics) {
		t.Errorf("Metrics len: got %d, want %d", len(got.Metrics), len(r.Metrics))
	}
	if len(got.Metrics) > 0 && len(got.Metrics[0].Aggs) != len(r.Metrics[0].Aggs) {
		t.Errorf("Metrics[0].Aggs len: got %d, want %d", len(got.Metrics[0].Aggs), len(r.Metrics[0].Aggs))
	}
}

func TestJSON_TopLevelFieldNames(t *testing.T) {
	var buf bytes.Buffer
	sampleResult().JSON(&buf) //nolint
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	for _, key := range []string{"name", "phases", "log_ingestion", "metrics"} {
		if _, ok := m[key]; !ok {
			t.Errorf("JSON output missing top-level field %q", key)
		}
	}
}

// --- BuildResult ---

func TestBuildResult_Empty(t *testing.T) {
	cfg := &Config{Benchmark: BenchmarkMeta{Name: "mybench"}}
	res := cfg.BuildResult(NewState())
	if res.Name != "mybench" {
		t.Errorf("Name: got %q, want %q", res.Name, "mybench")
	}
	if len(res.Phases) != 0 {
		t.Errorf("Phases: got %d, want 0", len(res.Phases))
	}
}

func TestBuildResult_PhaseDurations(t *testing.T) {
	cfg := &Config{Benchmark: BenchmarkMeta{Name: "bench"}}
	state := NewState()
	state.SetPhaseDuration("setup", 3*time.Second)
	state.SetPhaseDuration("execute", 45500*time.Millisecond)

	res := cfg.BuildResult(state)

	if len(res.Phases) != 2 {
		t.Fatalf("Phases: got %d, want 2", len(res.Phases))
	}
	if res.Phases[0].Name != "setup" || res.Phases[0].DurationSec != 3.0 {
		t.Errorf("Phases[0]: got {%s %.3f}, want {setup 3.000}", res.Phases[0].Name, res.Phases[0].DurationSec)
	}
	if res.Phases[1].Name != "execute" || res.Phases[1].DurationSec != 45.5 {
		t.Errorf("Phases[1]: got {%s %.3f}, want {execute 45.500}", res.Phases[1].Name, res.Phases[1].DurationSec)
	}
}

func TestBuildResult_LogIngestion(t *testing.T) {
	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Collect: []Step{{
				ID: "count",
				EsDocCount: &EsDocCountStep{
					Index:    "logs-*",
					Since:    "2024-01-15T12:00:00Z",
					Expected: 1000,
					Timeout:  Duration{5 * time.Minute},
				},
			}},
		},
	}
	state := NewState()
	state.SetStepOutput("count", "actualCount", "48000")
	state.SetStepOutput("count", "confirmedAt", "2024-01-15T12:05:00Z")

	res := cfg.BuildResult(state)

	if res.LogIngestion.Actual != 48000 {
		t.Errorf("Actual: got %d, want 48000", res.LogIngestion.Actual)
	}
	if res.LogIngestion.Expected != 1000 {
		t.Errorf("Expected: got %d, want 1000", res.LogIngestion.Expected)
	}
	if res.LogIngestion.ElapsedSec != 300.0 {
		t.Errorf("ElapsedSec: got %.3f, want 300.000", res.LogIngestion.ElapsedSec)
	}
}

func TestBuildResult_Metrics(t *testing.T) {
	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Collect: []Step{{
				ID: "agg",
				EsAgg: &EsAggStep{
					Index: "metrics-*",
					Since: "2024-01-15T12:00:00Z",
					Until: "2024-01-15T12:05:00Z",
					Metrics: []elastic.AggMetricSpec{
						{Name: "mem_size", Field: "system.process.memory.size", Aggs: []string{"min", "max"}},
					},
				},
			}},
		},
	}
	state := NewState()
	state.SetStepOutput("agg", "mem_size_min", "12345678.0")
	state.SetStepOutput("agg", "mem_size_max", "23456789.0")

	res := cfg.BuildResult(state)

	if len(res.Metrics) != 1 {
		t.Fatalf("Metrics: got %d, want 1", len(res.Metrics))
	}
	if res.Metrics[0].Name != "mem_size" {
		t.Errorf("Metrics[0].Name: got %q, want %q", res.Metrics[0].Name, "mem_size")
	}
	if len(res.Metrics[0].Aggs) != 2 {
		t.Fatalf("Metrics[0].Aggs: got %d, want 2", len(res.Metrics[0].Aggs))
	}
	if res.Metrics[0].Aggs[0].Name != "min" || res.Metrics[0].Aggs[0].Value != 12345678.0 {
		t.Errorf("Aggs[0]: got {%s %g}, want {min 12345678}", res.Metrics[0].Aggs[0].Name, res.Metrics[0].Aggs[0].Value)
	}
}

func TestBuildResult_EsDocCountWithoutIDIgnored(t *testing.T) {
	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Collect: []Step{{
				EsDocCount: &EsDocCountStep{
					Index:    "logs-*",
					Since:    "2024-01-15T12:00:00Z",
					Expected: 1000,
					Timeout:  Duration{5 * time.Minute},
				},
			}},
		},
	}
	state := NewState()
	state.SetStepOutput("", "actualCount", "48000")

	res := cfg.BuildResult(state)

	if res.LogIngestion.Actual != 0 || res.LogIngestion.Expected != 0 {
		t.Errorf("expected zero log ingestion for step without ID, got actual=%d expected=%d",
			res.LogIngestion.Actual, res.LogIngestion.Expected)
	}
}

func TestPrint_AgentMetricsTable(t *testing.T) {
	r := BenchmarkResult{
		Name: "bench",
		AgentMetrics: []AgentMetricsSummary{{
			Agents: []AgentSummary{{
				ID: "agent",
				Processes: []ProcessSummary{
					{ID: "elastic-agent", Binary: "elastic-agent", Samples: 3, MemAvg: 33 << 20, MemMax: 34 << 20, MemLast: 34 << 20, CPUAvg: 1.5, CPUMax: 2},
					{ID: "total", Samples: 3, MemAvg: 100 << 20, MemMax: 110 << 20, MemLast: 105 << 20, CPUAvg: 4, CPUMax: 5},
				},
				Integrations: []IntegrationSummary{{ID: "prometheus/metrics-default", Mode: "process", EventsPerSecAvg: 210.5, ErrorLogs: 12}},
				Events:       []EventsSummary{{Dataset: "elastic_agent.metricbeat", ID: "prometheus/metrics-default", AvgPerSec: 210.5, MaxPerSec: 400}},
				ErrorLogs:    12,
			}},
			Ingest: []IngestSummary{{Name: "prometheus", Total: 357512, AvgPerSec: 210.5, MaxPerSec: 425.3}},
		}},
	}
	var buf bytes.Buffer
	r.Print(&buf)
	out := buf.String()
	for _, want := range []string{"agent agent processes:", "elastic-agent", "33.0 MB", "total", "process", "357512", "425.3", "error logs total: 12"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestBuildResult_DecodesAgentMetricsSummary(t *testing.T) {
	cfg := &Config{Benchmark: BenchmarkMeta{Name: "b"}, Phases: PhasesConfig{
		Collect: []Step{{ID: "am", EsAgentMetrics: &EsAgentMetricsStep{}}},
	}}
	st := NewState()
	st.SetStepOutput("am", "summary", `{"agents":[{"id":"agent","processes":[{"id":"total","mem_avg":1}]}]}`)
	res := cfg.BuildResult(st)
	if len(res.AgentMetrics) != 1 || res.AgentMetrics[0].Agents[0].ID != "agent" {
		t.Errorf("agent metrics not decoded: %+v", res.AgentMetrics)
	}
}

func TestPrint_VariablesAndVersions(t *testing.T) {
	r := BenchmarkResult{
		Name:      "bench",
		Variables: map[string]string{"VEGETA_EPS": "500", "ORCA_PROVIDER": "gcp"},
		Stack: &StackVersions{
			Elasticsearch: ComponentVersion{Version: "9.6.0-SNAPSHOT", BuildHash: "0123456789abcdef", BuildDate: "2026-10-01T03:12:00Z"},
			Kibana:        ComponentVersion{Version: "9.6.0-SNAPSHOT", BuildHash: "fedcba9876543210"},
			Agents: []AgentVersion{
				{Version: "9.2.8", Count: 5, Hosts: []string{"a0", "a1", "a2", "a3", "a4"}},
				{Version: "9.6.0-SNAPSHOT", Count: 1, Hosts: []string{"fleet"}},
			},
		},
	}
	var buf bytes.Buffer
	r.Print(&buf)
	out := buf.String()
	for _, want := range []string{
		"benchmark: bench\n\nvariables:\n  ORCA_PROVIDER  gcp\n  VEGETA_EPS     500\n",
		"versions:",
		"elasticsearch  9.6.0-SNAPSHOT  build 01234567 (2026-10-01T03:12:00Z)",
		"kibana         9.6.0-SNAPSHOT  build fedcba98",
		"elastic-agent  9.2.8           5 agents: a0, a1, a2, a3, a4",
		"elastic-agent  9.6.0-SNAPSHOT  1 agent: fleet",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	buf.Reset()
	(&BenchmarkResult{Name: "plain"}).Print(&buf)
	if strings.Contains(buf.String(), "variables") || strings.Contains(buf.String(), "versions") {
		t.Errorf("empty variables/versions must be omitted:\n%s", buf.String())
	}
}

func TestBuildResult_VariablesAndStackVersions(t *testing.T) {
	t.Setenv("ORCA_TEST_PROVIDER", "gcp")
	os.Unsetenv("ORCA_TEST_UNSET_EPS")
	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "b"},
		Variables: map[string]string{"ORCA_TEST_PROVIDER": "multipass", "ORCA_TEST_UNSET_EPS": "500"},
		Phases:    PhasesConfig{Collect: []Step{{ID: "versions", StackVersions: &StackVersionsStep{}}}},
	}
	st := NewState()
	st.SetStepOutput("versions", "summary", `{"elasticsearch":{"version":"9.6.0-SNAPSHOT"},"kibana":{"version":"9.6.0-SNAPSHOT"},"elastic_agent":[{"version":"9.2.8","count":5,"hosts":["a"]}]}`)

	res := cfg.BuildResult(st)

	if res.Variables["ORCA_TEST_PROVIDER"] != "gcp" || res.Variables["ORCA_TEST_UNSET_EPS"] != "500" {
		t.Errorf("variables: got %v", res.Variables)
	}
	if res.Stack == nil || res.Stack.Elasticsearch.Version != "9.6.0-SNAPSHOT" || len(res.Stack.Agents) != 1 {
		t.Errorf("stack versions not decoded: %+v", res.Stack)
	}

	var buf bytes.Buffer
	if err := res.JSON(&buf); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(buf.Bytes(), &m) //nolint:errcheck
	if m["variables"] == nil || m["stack_versions"] == nil {
		t.Errorf("json missing variables/stack_versions: %s", buf.String())
	}
}

func TestPrint_Error(t *testing.T) {
	var buf bytes.Buffer
	(&BenchmarkResult{Name: "b", Error: "collect: step agent-metrics: boom"}).Print(&buf)
	if !strings.HasPrefix(buf.String(), "benchmark: b\nerror: collect: step agent-metrics: boom\n") {
		t.Errorf("unexpected output:\n%s", buf.String())
	}
	buf.Reset()
	if err := (&BenchmarkResult{Name: "b", Error: "x"}).JSON(&buf); err != nil || !strings.Contains(buf.String(), `"error": "x"`) {
		t.Errorf("json error field missing: %s (%v)", buf.String(), err)
	}
}
