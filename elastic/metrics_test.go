package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	testSince = time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	testUntil = time.Date(2024, 1, 15, 12, 5, 0, 0, time.UTC)
)

var testMetrics = []AggMetricSpec{
	{Name: "mem_size", Field: "system.process.memory.size", Aggs: []string{"min", "max", "avg", "p95"}},
}

const sampleAggResponse = `{
  "aggregations": {
    "mem_size_min": {"value": 12345678.0},
    "mem_size_max": {"value": 23456789.0},
    "mem_size_avg": {"value": 18000000.0},
    "mem_size_p95": {"values": {"95.0": 22000000.0}}
  }
}`

func newAggServer(t *testing.T, body string) (*elasticsearch.Client, func() string) {
	t.Helper()
	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})
	if err != nil {
		t.Fatal(err)
	}
	return es, func() string { return captured }
}

var testFilter = map[string]string{
	"data_stream.dataset": "elastic_agent.elastic_agent",
	"component.id":        "elastic_agent",
}

func TestRunAgg_Values(t *testing.T) {
	es, _ := newAggServer(t, sampleAggResponse)
	result, _, err := RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cases := []struct{ key string; want float64 }{
		{"mem_size_min", 12345678},
		{"mem_size_max", 23456789},
		{"mem_size_avg", 18000000},
		{"mem_size_p95", 22000000},
	}
	for _, c := range cases {
		if result[c.key] != c.want {
			t.Errorf("%s: got %f, want %f", c.key, result[c.key], c.want)
		}
	}
}

func TestRunAgg_NullPercentile(t *testing.T) {
	body := strings.ReplaceAll(sampleAggResponse, `"95.0": 22000000.0`, `"95.0": null`)
	es, _ := newAggServer(t, body)
	result, _, err := RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics)
	if err != nil {
		t.Fatalf("unexpected error for null p95: %v", err)
	}
	if result["mem_size_p95"] != 0 {
		t.Errorf("p95: got %f, want 0 for null", result["mem_size_p95"])
	}
}

func TestRunAgg_ESError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error":{"type":"server_error"}}`)
	}))
	t.Cleanup(srv.Close)
	es, _ := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})
	_, _, err := RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics)
	if err == nil {
		t.Fatal("expected error for ES 500 response")
	}
}

func TestRunAgg_QueryContainsTimeRange(t *testing.T) {
	es, getBody := newAggServer(t, sampleAggResponse)
	RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics) //nolint
	body := getBody()
	for _, want := range []string{"2024-01-15T12:00:00Z", "2024-01-15T12:05:00Z", "gte", "lte"} {
		if !strings.Contains(body, want) {
			t.Errorf("request body missing %q: %s", want, body)
		}
	}
}

func TestRunAgg_QueryContainsTermFilters(t *testing.T) {
	es, getBody := newAggServer(t, sampleAggResponse)
	RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics) //nolint
	body := getBody()
	for _, want := range []string{"elastic_agent.elastic_agent", "component.id", "elastic_agent"} {
		if !strings.Contains(body, want) {
			t.Errorf("request body missing %q: %s", want, body)
		}
	}
}

func TestRunAgg_QueryContainsField(t *testing.T) {
	es, getBody := newAggServer(t, sampleAggResponse)
	RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics) //nolint
	body := getBody()
	if !strings.Contains(body, "system.process.memory.size") {
		t.Errorf("request body missing field name: %s", body)
	}
}

func TestRunAgg_QueryContainsPercentile(t *testing.T) {
	es, getBody := newAggServer(t, sampleAggResponse)
	RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics) //nolint
	body := getBody()
	if !strings.Contains(body, "percentiles") || !strings.Contains(body, "95") {
		t.Errorf("request body missing percentiles agg: %s", body)
	}
}

func TestRunAgg_QuerySizeZero(t *testing.T) {
	es, getBody := newAggServer(t, sampleAggResponse)
	RunAgg(context.Background(), es, "metrics-*", testSince, testUntil, testFilter, testMetrics) //nolint
	body := getBody()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if size, _ := m["size"].(float64); size != 0 {
		t.Errorf("size: got %v, want 0", size)
	}
}

func TestBuildAggQuery_InvalidAgg(t *testing.T) {
	_, err := buildAggQuery(testSince, testUntil, nil, []AggMetricSpec{
		{Name: "x", Field: "f", Aggs: []string{"median"}},
	})
	if err == nil {
		t.Fatal("expected error for unsupported agg type")
	}
}

// --- buildBoolFilter ---

func TestBuildBoolFilter_RangeOnly(t *testing.T) {
	clauses := buildBoolFilter(testSince, nil, nil, false)
	if len(clauses) != 1 {
		t.Fatalf("expected 1 clause, got %d", len(clauses))
	}
	b, _ := json.Marshal(clauses[0])
	s := string(b)
	if !strings.Contains(s, `"gte"`) {
		t.Errorf("range clause missing gte: %s", s)
	}
	if strings.Contains(s, `"lte"`) {
		t.Errorf("range clause should not have lte when until is nil: %s", s)
	}
}

func TestBuildBoolFilter_WithUntil(t *testing.T) {
	clauses := buildBoolFilter(testSince, &testUntil, nil, false)
	if len(clauses) != 1 {
		t.Fatalf("expected 1 clause, got %d", len(clauses))
	}
	b, _ := json.Marshal(clauses[0])
	s := string(b)
	if !strings.Contains(s, `"gte"`) {
		t.Errorf("range clause missing gte: %s", s)
	}
	if !strings.Contains(s, `"lte"`) {
		t.Errorf("range clause missing lte when until is set: %s", s)
	}
}

func TestBuildBoolFilter_WildcardAllowed(t *testing.T) {
	filter := map[string]string{"myfield": "val*"}
	clauses := buildBoolFilter(testSince, nil, filter, true)
	if len(clauses) != 2 {
		t.Fatalf("expected 2 clauses (range + wildcard), got %d", len(clauses))
	}
	b, _ := json.Marshal(clauses)
	s := string(b)
	if !strings.Contains(s, `"wildcard"`) {
		t.Errorf("expected wildcard clause for pattern value when allowWildcard=true: %s", s)
	}
}

func TestBuildBoolFilter_WildcardForbidden(t *testing.T) {
	filter := map[string]string{"myfield": "val*"}
	clauses := buildBoolFilter(testSince, nil, filter, false)
	if len(clauses) != 2 {
		t.Fatalf("expected 2 clauses (range + term), got %d", len(clauses))
	}
	b, _ := json.Marshal(clauses)
	s := string(b)
	if strings.Contains(s, `"wildcard"`) {
		t.Errorf("expected term clause (not wildcard) when allowWildcard=false: %s", s)
	}
	if !strings.Contains(s, `"term"`) {
		t.Errorf("expected term clause when allowWildcard=false: %s", s)
	}
}

func TestGetFieldValue_NestedDottedAndMixed(t *testing.T) {
	doc := map[string]interface{}{
		"component":    map[string]interface{}{"id": "elastic-otel-collector"},
		"component.id": "prometheus/metrics-default/prometheus",
		"beat.stats.libbeat.output.events.acked": 80.0,
		"system": map[string]interface{}{
			"process.memory": map[string]interface{}{"size": 42.0},
		},
	}
	cases := map[string]interface{}{
		"component.id":                           "prometheus/metrics-default/prometheus",
		"beat.stats.libbeat.output.events.acked": 80.0,
		"system.process.memory.size":             42.0,
	}
	for path, want := range cases {
		got, ok := GetFieldValue(doc, path)
		if !ok || got != want {
			t.Errorf("GetFieldValue(%q) = %v, %v; want %v", path, got, ok, want)
		}
	}
	if _, ok := GetFieldValue(doc, "system.process.cpu"); ok {
		t.Error("expected miss for absent path")
	}
}
