package steps_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

const twoDocResponse = `{
  "hits": {
    "total": {"value": 2},
    "hits": [
      {"_source": {
        "@timestamp": "2026-09-14T23:25:53.010Z",
        "system": {"process": {"memory": {"size": 1885576248},
                               "cgroup": {"memory": {"mem": {"usage": {"bytes": 11744403456}}}}}}
      }},
      {"_source": {
        "@timestamp": "2026-09-14T23:26:53.010Z",
        "system": {"process": {"memory": {"size": 1885686790}}}
      }}
    ]
  }
}`

func tsvESClient(t *testing.T, body string) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func tsvStep(t *testing.T, outPath string) harness.Step {
	t.Helper()
	return harness.Step{
		ID: "tsv-out",
		EsTsv: &harness.EsTsvStep{
			Index:  "metrics-*",
			Since:  "${phase.execute.start}",
			Until:  "${phase.collect.start}",
			Output: outPath,
			Filter: map[string]string{
				"data_stream.dataset": "elastic_agent.elastic_agent",
			},
			Metrics: []harness.TsvMetric{
				{Name: "timestamp", Field: "@timestamp", Required: true},
				{Name: "mem size", Field: "system.process.memory.size", Required: true},
				{Name: "cgroup mem", Field: "system.process.cgroup.memory.mem.usage.bytes"},
			},
		},
	}
}

func tsvState() *harness.State {
	s := harness.NewState()
	s.SetPhaseStart("execute", time.Date(2026, 9, 14, 23, 0, 0, 0, time.UTC))
	s.SetPhaseStart("collect", time.Date(2026, 9, 14, 23, 30, 0, 0, time.UTC))
	return s
}

func TestEsTsvFactory_CreatesFile(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	if err := exec.Run(context.Background(), tsvState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("output file not created: %v", err)
	}
}

func TestEsTsvFactory_HeaderRow(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	exec.Run(context.Background(), tsvState()) //nolint

	lines := readLines(t, outPath)
	if len(lines) < 1 {
		t.Fatal("expected at least a header row")
	}
	if lines[0] != "timestamp\tmem size\tcgroup mem" {
		t.Errorf("header: got %q", lines[0])
	}
}

func TestEsTsvFactory_TwoDataRows(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	exec.Run(context.Background(), tsvState()) //nolint

	lines := readLines(t, outPath)
	// header + 2 data rows
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines (header + 2 data), got %d: %v", len(lines), lines)
	}
}

func TestEsTsvFactory_FirstRowAllFields(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	exec.Run(context.Background(), tsvState()) //nolint

	lines := readLines(t, outPath)
	cols := strings.Split(lines[1], "\t")
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns in row 1, got %d: %v", len(cols), cols)
	}
	if cols[0] != "2026-09-14T23:25:53.010Z" {
		t.Errorf("col 0: got %q", cols[0])
	}
	if cols[1] != "1885576248" {
		t.Errorf("col 1: got %q", cols[1])
	}
	if cols[2] != "11744403456" {
		t.Errorf("col 2: got %q", cols[2])
	}
}

func TestEsTsvFactory_SecondRowMissingNonRequired(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	exec.Run(context.Background(), tsvState()) //nolint

	lines := readLines(t, outPath)
	cols := strings.Split(lines[2], "\t")
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns (including empty) in row 2, got %d: %v", len(cols), cols)
	}
	if cols[2] != "" {
		t.Errorf("missing non-required field should be empty, got %q", cols[2])
	}
}

func TestEsTsvFactory_SkipsDocMissingRequiredField(t *testing.T) {
	// Document with no memory field — should be skipped since mem size is required.
	body := `{"hits":{"total":{"value":1},"hits":[
		{"_source":{"@timestamp":"2026-09-14T23:25:53.010Z"}}
	]}}`
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, body))(tsvStep(t, outPath))
	exec.Run(context.Background(), tsvState()) //nolint

	lines := readLines(t, outPath)
	// Only header, no data rows.
	if len(lines) != 1 {
		t.Errorf("expected 1 line (header only), got %d: %v", len(lines), lines)
	}
}

func TestEsTsvFactory_SetsFilePathOutput(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	state := tsvState()
	exec.Run(context.Background(), state) //nolint

	v, err := state.Resolve("${steps.tsv-out.filePath}")
	if err != nil || v == "" {
		t.Fatalf("filePath not set in state: %v", err)
	}
}

func TestEsTsvFactory_SetsRowCount(t *testing.T) {
	outPath := t.TempDir() + "/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	state := tsvState()
	exec.Run(context.Background(), state) //nolint

	v, err := state.Resolve("${steps.tsv-out.rowCount}")
	if err != nil {
		t.Fatalf("rowCount not set: %v", err)
	}
	if v != "2" {
		t.Errorf("rowCount: got %q, want %q", v, "2")
	}
}

func TestEsTsvFactory_CreatesParentDir(t *testing.T) {
	outPath := t.TempDir() + "/nested/dir/out.tsv"
	exec, _ := steps.NewEsTsvFactory(tsvESClient(t, twoDocResponse))(tsvStep(t, outPath))
	if err := exec.Run(context.Background(), tsvState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("output file not found: %v", err)
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines
}
