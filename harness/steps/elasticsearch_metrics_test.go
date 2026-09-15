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
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func esNodeStatsDoc(t time.Time, node string, stats map[string]interface{}) string {
	doc := map[string]interface{}{
		"_source": map[string]interface{}{
			"@timestamp": t.Format(time.RFC3339),
			"elasticsearch": map[string]interface{}{
				"node": map[string]interface{}{
					"name":  node,
					"stats": stats,
				},
			},
		},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func nodeStatsDoc(t time.Time, node string, indexTotal, cpu, heap float64) string {
	return esNodeStatsDoc(t, node, map[string]interface{}{
		"indices": map[string]interface{}{
			"indexing": map[string]interface{}{"index_total": map[string]interface{}{"count": indexTotal}},
		},
		"process": map[string]interface{}{"cpu": map[string]interface{}{"pct": cpu}},
		"jvm":     map[string]interface{}{"mem": map[string]interface{}{"heap": map[string]interface{}{"used": map[string]interface{}{"bytes": heap}}}},
	})
}

func fixtureESMetrics(t *testing.T, nodeDocs []string, streams map[string]int64) (harness.ESConfig, *[]string) {
	t.Helper()
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, r.URL.Path+" "+string(body))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if strings.Contains(string(body), "multi_terms") {
			var buckets []string
			for name, n := range streams {
				parts := strings.SplitN(name, "-", 3)
				k, _ := json.Marshal(parts)
				buckets = append(buckets, fmt.Sprintf(`{"key":%s,"doc_count":%d}`, k, n))
			}
			fmt.Fprintf(w, `{"aggregations":{"ds":{"buckets":[%s]}}}`, strings.Join(buckets, ","))
			return
		}
		fmt.Fprintf(w, `{"hits":{"hits":[%s]}}`, strings.Join(nodeDocs, ","))
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}, &bodies
}

func runEsMetrics(t *testing.T, es harness.ESConfig, step harness.Step) (harness.EsMetricsSummary, *harness.State) {
	t.Helper()
	exec, err := steps.NewEsMetricsFactory(es)(step)
	if err != nil {
		t.Fatal(err)
	}
	state := stateWithTimeRange()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("run: %v", err)
	}
	raw, err := state.Resolve("${steps.esm.summary}")
	if err != nil {
		t.Fatalf("summary not in state: %v", err)
	}
	var sum harness.EsMetricsSummary
	if err := json.Unmarshal([]byte(raw), &sum); err != nil {
		t.Fatal(err)
	}
	return sum, state
}

func esMetricsStep(outDir string) harness.Step {
	return harness.Step{ID: "esm", EsMetrics: &harness.EsMetricsStep{
		Since: "${phase.execute.start}", Until: "${phase.collect.start}", OutputDir: outDir,
	}}
}

func TestEsMetrics_SingleNode(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 49, 0, 0, time.UTC)
	docs := []string{
		nodeStatsDoc(base, "es1", 1000, 10, 1e9),
		nodeStatsDoc(base.Add(time.Minute), "es1", 1600, 20, 2e9),
		nodeStatsDoc(base.Add(2*time.Minute), "es1", 2800, 30, 3e9),
	}
	es, bodies := fixtureESMetrics(t, docs, map[string]int64{"logs-apache.access-default": 900, "metrics-system.cpu-default": 300})
	outDir := t.TempDir()
	sum, state := runEsMetrics(t, es, esMetricsStep(outDir))

	if len(sum.Nodes) != 1 || sum.Nodes[0].Name != "es1" {
		t.Fatalf("nodes: %+v", sum.Nodes)
	}
	n := sum.Nodes[0]
	if n.Samples != 3 || n.IndexedDocs != 1800 || n.IndexingAvg != 15 || n.IndexingMax != 20 {
		t.Errorf("indexing summary wrong: %+v", n)
	}
	if n.CPUAvg != 20 || n.CPUMax != 30 || n.HeapMax != 3e9 {
		t.Errorf("cpu/heap summary wrong: %+v", n)
	}
	if sum.BulkPoolObserved || sum.IndexingPressureObserved {
		t.Errorf("expected no legacy bulk pool / indexing pressure observed: %+v", sum)
	}
	assertFiles(t, outDir, "es-node-timeseries.tsv", "es-node-indexing-chart.svg", "es-node-cpu-chart.svg", "es-node-heap-chart.svg",
		"es-node-indexing-cost-chart.svg", "es-node-threadpool-timeseries.tsv", "es-node-threadpool-chart.svg",
		"es-node-bulk-timeseries.tsv", "es-node-bulk-chart.svg", "es-data-streams.tsv")

	tp, _ := os.ReadFile(filepath.Join(outDir, "es-node-threadpool-timeseries.tsv"))
	tpLines := strings.Split(strings.TrimSpace(string(tp)), "\n")
	if tpLines[0] != "timestamp\twrite_active\twrite_queue\twrite_rejected" {
		t.Errorf("threadpool header (expected no bulk pool columns): %q", tpLines[0])
	}
	if _, err := os.Stat(filepath.Join(outDir, "es-node-bulk-pool-chart.svg")); err == nil {
		t.Errorf("did not expect legacy bulk pool chart when thread_pool.bulk is absent")
	}

	tsv, _ := os.ReadFile(filepath.Join(outDir, "es-node-timeseries.tsv"))
	lines := strings.Split(strings.TrimSpace(string(tsv)), "\n")
	wantHeader := "timestamp\tindex_total\tcpu_pct\theap_bytes\tindexing_rate\tindex_time_ms\tms_per_doc\tmerges_total\tmerges_time_ms\tsegments_count\tsegments_memory_bytes"
	if lines[0] != wantHeader {
		t.Errorf("header: %q", lines[0])
	}
	if len(lines) != 4 || !strings.HasPrefix(lines[2], fmt.Sprintf("%s\t1600\t20\t2000000000\t10\t", base.Add(time.Minute).UTC().Format(time.RFC3339))) ||
		!strings.HasPrefix(lines[3], fmt.Sprintf("%s\t2800\t30\t3000000000\t20\t", base.Add(2*time.Minute).UTC().Format(time.RFC3339))) {
		t.Errorf("rates not derived: %v", lines)
	}

	window := stateWindowSeconds(t, state)
	if sum.WindowSec != window {
		t.Errorf("window: got %v want %v", sum.WindowSec, window)
	}
	if len(sum.DataStreams) != 2 || sum.DataStreams[0].Name != "logs-apache.access-default" || sum.DataStreams[0].Docs != 900 {
		t.Fatalf("data streams: %+v", sum.DataStreams)
	}
	if sum.TotalDocs != 1200 || sum.TotalPerSec != round4f(1200/window) || sum.DataStreams[0].PerSec != round4f(900/window) {
		t.Errorf("per-second math wrong: total=%d %.4f first=%.4f window=%v", sum.TotalDocs, sum.TotalPerSec, sum.DataStreams[0].PerSec, window)
	}
	ds, _ := os.ReadFile(filepath.Join(outDir, "es-data-streams.tsv"))
	if !strings.HasPrefix(string(ds), "data_stream\tdocs\tper_sec\nlogs-apache.access-default\t900\t") || !strings.Contains(string(ds), "\ntotal\t1200\t") {
		t.Errorf("data streams tsv:\n%s", ds)
	}
	if v, _ := state.Resolve("${steps.esm.totalDocs}"); v != "1200" {
		t.Errorf("totalDocs output: %q", v)
	}

	var sawDefaultIndex, sawDSIndex bool
	for _, b := range *bodies {
		if strings.HasPrefix(b, "/metrics-*/_search") && strings.Contains(b, "elasticsearch.stack_monitoring.node_stats") {
			sawDefaultIndex = true
		}
		if strings.HasPrefix(b, "/logs-*,metrics-*,traces-*/_search") && strings.Contains(b, "multi_terms") {
			sawDSIndex = true
		}
	}
	if !sawDefaultIndex || !sawDSIndex {
		t.Errorf("default indices/filter not used: %v", *bodies)
	}
}

type fullNodeStatsParams struct {
	indexTotal, indexTimeMs                            float64
	writeActive, writeQueue, writeRejected             float64
	hasBulkPool                                        bool
	bulkPoolActive, bulkPoolQueue, bulkPoolRejected    float64
	bulkAvgSize, bulkAvgTimeMs, bulkTotalOps           float64
	mergesTotal, mergesTimeMs                          float64
	segmentsCount, segmentsMemBytes                    float64
	hasIndexingPressure                                bool
	ipCurrentBytes, ipPrimaryRejected, ipCoordRejected float64
}

func fullNodeStatsDoc(t time.Time, node string, p fullNodeStatsParams) string {
	threadPool := map[string]interface{}{
		"write": map[string]interface{}{
			"active":   map[string]interface{}{"count": p.writeActive},
			"queue":    map[string]interface{}{"count": p.writeQueue},
			"rejected": map[string]interface{}{"count": p.writeRejected},
		},
	}
	if p.hasBulkPool {
		threadPool["bulk"] = map[string]interface{}{
			"active":   map[string]interface{}{"count": p.bulkPoolActive},
			"queue":    map[string]interface{}{"count": p.bulkPoolQueue},
			"rejected": map[string]interface{}{"count": p.bulkPoolRejected},
		}
	}
	stats := map[string]interface{}{
		"indices": map[string]interface{}{
			"indexing": map[string]interface{}{
				"index_total": map[string]interface{}{"count": p.indexTotal},
				"index_time":  map[string]interface{}{"ms": p.indexTimeMs},
			},
			"bulk": map[string]interface{}{
				"avg_size":   map[string]interface{}{"bytes": p.bulkAvgSize},
				"avg_time":   map[string]interface{}{"ms": p.bulkAvgTimeMs},
				"operations": map[string]interface{}{"total": map[string]interface{}{"count": p.bulkTotalOps}},
			},
			"merges": map[string]interface{}{
				"total":      map[string]interface{}{"count": p.mergesTotal},
				"total_time": map[string]interface{}{"ms": p.mergesTimeMs},
			},
			"segments": map[string]interface{}{
				"count":  p.segmentsCount,
				"memory": map[string]interface{}{"bytes": p.segmentsMemBytes},
			},
		},
		"thread_pool": threadPool,
	}
	if p.hasIndexingPressure {
		stats["indexing_pressure"] = map[string]interface{}{
			"memory": map[string]interface{}{
				"current": map[string]interface{}{
					"combined_coordinating_and_primary": map[string]interface{}{"bytes": p.ipCurrentBytes},
				},
				"total": map[string]interface{}{
					"primary":      map[string]interface{}{"rejections": p.ipPrimaryRejected},
					"coordinating": map[string]interface{}{"rejections": p.ipCoordRejected},
				},
			},
		}
	}
	return esNodeStatsDoc(t, node, stats)
}

func TestEsMetrics_ThreadPoolAndBulkStats(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 49, 0, 0, time.UTC)
	docs := []string{
		fullNodeStatsDoc(base, "es1", fullNodeStatsParams{
			indexTotal: 1000, indexTimeMs: 2000,
			writeActive: 2, writeQueue: 5, writeRejected: 0,
			bulkAvgSize: 2048, bulkAvgTimeMs: 1, bulkTotalOps: 100,
			mergesTotal: 1, mergesTimeMs: 10,
			segmentsCount: 5, segmentsMemBytes: 1e6,
		}),
		fullNodeStatsDoc(base.Add(time.Minute), "es1", fullNodeStatsParams{
			indexTotal: 1600, indexTimeMs: 8000,
			writeActive: 3, writeQueue: 20, writeRejected: 7,
			bulkAvgSize: 4096, bulkAvgTimeMs: 2, bulkTotalOps: 700,
			mergesTotal: 4, mergesTimeMs: 40,
			segmentsCount: 5, segmentsMemBytes: 1e6,
		}),
	}
	es, _ := fixtureESMetrics(t, docs, nil)
	outDir := t.TempDir()
	sum, _ := runEsMetrics(t, es, esMetricsStep(outDir))

	if sum.BulkPoolObserved || sum.IndexingPressureObserved {
		t.Fatalf("did not expect legacy bulk pool / indexing pressure on a 9.6+-shaped doc: %+v", sum)
	}
	n := sum.Nodes[0]
	if n.IndexMsPerDocAvg != 10 {
		t.Errorf("index ms/doc: got %v want 10", n.IndexMsPerDocAvg)
	}
	if n.WriteActiveAvg != 2.5 || n.WriteActiveMax != 3 {
		t.Errorf("write active summary wrong: %+v", n)
	}
	if n.WriteQueueAvg != 12.5 || n.WriteQueueMax != 20 {
		t.Errorf("write queue summary wrong: %+v", n)
	}
	if n.WriteRejectedAvg != 0.1167 || n.WriteRejectedMax != 0.1167 {
		t.Errorf("write rejected summary wrong: %+v", n)
	}
	if n.BulkPoolQueueMax != 0 || n.BulkPoolRejected != 0 {
		t.Errorf("did not expect bulk pool stats: %+v", n)
	}
	if n.BulkAvgSizeBytesAvg != 3072 || n.BulkOpsPerSecAvg != 10 || n.BulkOpsPerSecMax != 10 {
		t.Errorf("bulk stats summary wrong: %+v", n)
	}
	if n.MergesCount != 3 || n.MergesTimeMs != 30 {
		t.Errorf("merges summary wrong: %+v", n)
	}
	if n.SegmentsAvg != 5 || n.SegmentsMax != 5 {
		t.Errorf("segments summary wrong: %+v", n)
	}

	tp, _ := os.ReadFile(filepath.Join(outDir, "es-node-threadpool-timeseries.tsv"))
	tpLines := strings.Split(strings.TrimSpace(string(tp)), "\n")
	if tpLines[0] != "timestamp\twrite_active\twrite_queue\twrite_rejected" {
		t.Errorf("threadpool header: %q", tpLines[0])
	}
	if !strings.HasSuffix(tpLines[2], "\t3\t20\t7") {
		t.Errorf("threadpool row: %q", tpLines[2])
	}
	if _, err := os.Stat(filepath.Join(outDir, "es-node-bulk-pool-chart.svg")); err == nil {
		t.Errorf("did not expect legacy bulk pool chart")
	}

	bulk, _ := os.ReadFile(filepath.Join(outDir, "es-node-bulk-timeseries.tsv"))
	bulkLines := strings.Split(strings.TrimSpace(string(bulk)), "\n")
	if bulkLines[0] != "timestamp\tavg_size_bytes\tavg_time_ms\ttotal_operations\tops_per_sec" {
		t.Errorf("bulk header: %q", bulkLines[0])
	}
	if !strings.HasSuffix(bulkLines[2], "\t4096\t2\t700\t10") {
		t.Errorf("bulk row: %q", bulkLines[2])
	}

	nodeTSV, _ := os.ReadFile(filepath.Join(outDir, "es-node-timeseries.tsv"))
	if strings.Contains(string(nodeTSV), "ip_current_bytes") {
		t.Errorf("did not expect indexing pressure columns:\n%s", nodeTSV)
	}
}

func TestEsMetrics_LegacyBulkPoolAndIndexingPressure(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 49, 0, 0, time.UTC)
	docs := []string{
		fullNodeStatsDoc(base, "es1", fullNodeStatsParams{
			indexTotal: 1000, indexTimeMs: 2000,
			writeActive: 1, writeQueue: 1, writeRejected: 0,
			hasBulkPool: true, bulkPoolActive: 2, bulkPoolQueue: 5, bulkPoolRejected: 0,
			bulkAvgSize: 500, bulkAvgTimeMs: 1, bulkTotalOps: 50,
			mergesTotal: 2, mergesTimeMs: 20,
			segmentsCount: 3, segmentsMemBytes: 2e6,
			hasIndexingPressure: true, ipCurrentBytes: 1e5, ipPrimaryRejected: 0, ipCoordRejected: 0,
		}),
		fullNodeStatsDoc(base.Add(time.Minute), "es1", fullNodeStatsParams{
			indexTotal: 1600, indexTimeMs: 8000,
			writeActive: 1, writeQueue: 1, writeRejected: 0,
			hasBulkPool: true, bulkPoolActive: 3, bulkPoolQueue: 9, bulkPoolRejected: 4,
			bulkAvgSize: 1500, bulkAvgTimeMs: 2, bulkTotalOps: 350,
			mergesTotal: 5, mergesTimeMs: 50,
			segmentsCount: 3, segmentsMemBytes: 2e6,
			hasIndexingPressure: true, ipCurrentBytes: 2e5, ipPrimaryRejected: 2, ipCoordRejected: 1,
		}),
	}
	es, _ := fixtureESMetrics(t, docs, nil)
	outDir := t.TempDir()
	sum, _ := runEsMetrics(t, es, esMetricsStep(outDir))

	if !sum.BulkPoolObserved || !sum.IndexingPressureObserved {
		t.Fatalf("expected legacy bulk pool and indexing pressure to be observed: %+v", sum)
	}
	n := sum.Nodes[0]
	if n.BulkPoolQueueAvg != 7 || n.BulkPoolQueueMax != 9 || n.BulkPoolRejected != 4 {
		t.Errorf("bulk pool summary wrong: %+v", n)
	}
	if n.IndexingPressureBytesAvg != 1.5e5 || n.IndexingPressureRejected != 3 {
		t.Errorf("indexing pressure summary wrong: %+v", n)
	}

	tp, _ := os.ReadFile(filepath.Join(outDir, "es-node-threadpool-timeseries.tsv"))
	tpLines := strings.Split(strings.TrimSpace(string(tp)), "\n")
	if tpLines[0] != "timestamp\twrite_active\twrite_queue\twrite_rejected\tbulk_pool_active\tbulk_pool_queue\tbulk_pool_rejected" {
		t.Errorf("threadpool header: %q", tpLines[0])
	}
	assertFiles(t, outDir, "es-node-bulk-pool-chart.svg")

	nodeTSV, _ := os.ReadFile(filepath.Join(outDir, "es-node-timeseries.tsv"))
	nodeLines := strings.Split(strings.TrimSpace(string(nodeTSV)), "\n")
	wantHeader := "timestamp\tindex_total\tcpu_pct\theap_bytes\tindexing_rate\tindex_time_ms\tms_per_doc\tmerges_total\tmerges_time_ms\tsegments_count\tsegments_memory_bytes\tip_current_bytes\tip_primary_rejected\tip_coordinating_rejected"
	if nodeLines[0] != wantHeader {
		t.Errorf("node header: %q", nodeLines[0])
	}
}

func TestEsMetrics_MultiNodePrefixesFiles(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 49, 0, 0, time.UTC)
	docs := []string{
		nodeStatsDoc(base, "node-a", 100, 1, 1), nodeStatsDoc(base.Add(time.Minute), "node-a", 160, 1, 1),
		nodeStatsDoc(base, "node-b", 500, 1, 1), nodeStatsDoc(base.Add(time.Minute), "node-b", 500, 1, 1),
	}
	es, _ := fixtureESMetrics(t, docs, nil)
	outDir := t.TempDir()
	sum, _ := runEsMetrics(t, es, esMetricsStep(outDir))
	if len(sum.Nodes) != 2 || sum.Nodes[0].Name != "node-a" || sum.Nodes[1].IndexedDocs != 0 {
		t.Fatalf("nodes: %+v", sum.Nodes)
	}
	assertFiles(t, outDir, "es-node-node-a-timeseries.tsv", "es-node-node-b-indexing-chart.svg", "es-data-streams.tsv")
	if sum.TotalDocs != 0 || len(sum.DataStreams) != 0 {
		t.Errorf("expected empty data stream summary, got %+v", sum)
	}
}

func TestEsMetrics_NoDocsStillSucceeds(t *testing.T) {
	es, _ := fixtureESMetrics(t, nil, nil)
	outDir := t.TempDir()
	sum, _ := runEsMetrics(t, es, esMetricsStep(outDir))
	if len(sum.Nodes) != 0 {
		t.Errorf("expected no nodes, got %+v", sum.Nodes)
	}
	assertFiles(t, outDir, "es-data-streams.tsv")
}

func TestEsMetrics_CustomIndexAndFilter(t *testing.T) {
	es, bodies := fixtureESMetrics(t, nil, nil)
	step := esMetricsStep(t.TempDir())
	step.EsMetrics.Index = "custom-*"
	step.EsMetrics.Filter = map[string]string{"host.name": "es-host"}
	step.EsMetrics.DataStreamIndex = "logs-*"
	runEsMetrics(t, es, step)
	joined := strings.Join(*bodies, "\n")
	if !strings.Contains(joined, "/custom-*/_search") || !strings.Contains(joined, `"host.name":"es-host"`) || !strings.Contains(joined, "/logs-*/_search") {
		t.Errorf("custom settings not applied:\n%s", joined)
	}
}

func TestEsMetrics_UntilBeforeSinceFails(t *testing.T) {
	es, _ := fixtureESMetrics(t, nil, nil)
	step := esMetricsStep(t.TempDir())
	step.EsMetrics.Since = "${phase.collect.start}"
	step.EsMetrics.Until = "${phase.execute.start}"
	exec, _ := steps.NewEsMetricsFactory(es)(step)
	if err := exec.Run(context.Background(), stateWithTimeRange()); err == nil || !strings.Contains(err.Error(), "not after") {
		t.Fatalf("expected until/since error, got %v", err)
	}
}

func stateWindowSeconds(t *testing.T, state *harness.State) float64 {
	t.Helper()
	s, _ := state.Resolve("${phase.execute.start}")
	u, _ := state.Resolve("${phase.collect.start}")
	st, err1 := time.Parse(time.RFC3339, s)
	ut, err2 := time.Parse(time.RFC3339, u)
	if err1 != nil || err2 != nil {
		t.Fatalf("bad times in state: %v %v", err1, err2)
	}
	return ut.Sub(st).Seconds()
}

func round4f(v float64) float64 {
	return float64(int64(v*10000+0.5)) / 10000
}
