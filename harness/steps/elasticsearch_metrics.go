package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

const (
	defaultEsNodeStatsIndex   = "metrics-*"
	defaultEsNodeStatsDataset = "elasticsearch.stack_monitoring.node_stats"
	defaultEsDataStreamIndex  = "logs-*,metrics-*,traces-*"
	esNodeNameField           = "elasticsearch.node.name"
	esIndexTotalField         = "elasticsearch.node.stats.indices.indexing.index_total.count"
	esIndexTimeMsField        = "elasticsearch.node.stats.indices.indexing.index_time.ms"
	esCPUPctField             = "elasticsearch.node.stats.process.cpu.pct"
	esHeapBytesField          = "elasticsearch.node.stats.jvm.mem.heap.used.bytes"

	esWriteActiveField   = "elasticsearch.node.stats.thread_pool.write.active.count"
	esWriteQueueField    = "elasticsearch.node.stats.thread_pool.write.queue.count"
	esWriteRejectedField = "elasticsearch.node.stats.thread_pool.write.rejected.count"

	// esBulkPool*Field read the legacy "bulk" thread pool, retired in
	// Elasticsearch 9.6.0 and replaced by "write"; kept for clusters older
	// than that.
	esBulkPoolActiveField   = "elasticsearch.node.stats.thread_pool.bulk.active.count"
	esBulkPoolQueueField    = "elasticsearch.node.stats.thread_pool.bulk.queue.count"
	esBulkPoolRejectedField = "elasticsearch.node.stats.thread_pool.bulk.rejected.count"

	esBulkAvgSizeBytesField = "elasticsearch.node.stats.indices.bulk.avg_size.bytes"
	esBulkAvgTimeMsField    = "elasticsearch.node.stats.indices.bulk.avg_time.ms"
	esBulkTotalOpsField     = "elasticsearch.node.stats.indices.bulk.operations.total.count"

	esMergesTotalField      = "elasticsearch.node.stats.indices.merges.total.count"
	esMergesTimeMsField     = "elasticsearch.node.stats.indices.merges.total_time.ms"
	esSegmentsCountField    = "elasticsearch.node.stats.indices.segments.count"
	esSegmentsMemBytesField = "elasticsearch.node.stats.indices.segments.memory.bytes"

	esIPCurrentBytesField         = "elasticsearch.node.stats.indexing_pressure.memory.current.combined_coordinating_and_primary.bytes"
	esIPPrimaryRejectedField      = "elasticsearch.node.stats.indexing_pressure.memory.total.primary.rejections"
	esIPCoordinatingRejectedField = "elasticsearch.node.stats.indexing_pressure.memory.total.coordinating.rejections"
)

type esNodeSample struct {
	t          time.Time
	indexTotal float64
	cpuPct     float64
	heapBytes  float64

	indexTimeMs float64

	writeActive, writeQueue, writeRejected          float64
	bulkPoolActive, bulkPoolQueue, bulkPoolRejected float64

	bulkAvgSizeBytes, bulkAvgTimeMs, bulkTotalOps float64

	mergesTotal, mergesTimeMs       float64
	segmentsCount, segmentsMemBytes float64

	ipCurrentBytes                            float64
	ipPrimaryRejected, ipCoordinatingRejected float64
}

// esCapabilities records which optional node_stats sections were actually
// observed across the fetched docs, so the TSV/summary output can omit a
// section Elasticsearch never reported instead of filling it with zeros.
type esCapabilities struct {
	bulkPool         bool // legacy thread_pool.bulk, retired in Elasticsearch 9.6.0
	indexingPressure bool
}

func NewEsMetricsFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("elasticsearch-metrics: %w", err)
			}
			cfg := step.EsMetrics

			index := defaultEsNodeStatsIndex
			if cfg.Index != "" {
				if index, err = state.Resolve(cfg.Index); err != nil {
					return err
				}
			}
			dsIndex := defaultEsDataStreamIndex
			if cfg.DataStreamIndex != "" {
				if dsIndex, err = state.Resolve(cfg.DataStreamIndex); err != nil {
					return err
				}
			}
			since, err := resolveTime(state, cfg.Since)
			if err != nil {
				return fmt.Errorf("since: %w", err)
			}
			until, err := resolveTime(state, cfg.Until)
			if err != nil {
				return fmt.Errorf("until: %w", err)
			}
			if !until.After(since) {
				return fmt.Errorf("until %s is not after since %s", until.Format(time.RFC3339), since.Format(time.RFC3339))
			}
			outDir, err := state.Resolve(cfg.OutputDir)
			if err != nil {
				return err
			}
			if outDir, err = filepath.Abs(outDir); err != nil {
				return fmt.Errorf("resolve output_dir: %w", err)
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return fmt.Errorf("create output dir: %w", err)
			}
			filter := map[string]string{"data_stream.dataset": defaultEsNodeStatsDataset}
			if len(cfg.Filter) > 0 {
				if filter, err = resolveFilter(state, cfg.Filter); err != nil {
					return err
				}
			}

			summary := harness.EsMetricsSummary{WindowSec: until.Sub(since).Seconds()}

			docs, err := elastic.FetchDocs(ctx, es, index, since, until, filter, 0)
			if err != nil {
				return fmt.Errorf("fetch node stats: %w", err)
			}
			nodes, caps := groupNodeStats(docs)
			if len(nodes) == 0 {
				fmt.Fprintf(os.Stderr, "[elasticsearch-metrics] warning: 0 node stats docs (index=%s since=%s until=%s)\n",
					index, since.Format(time.RFC3339), until.Format(time.RFC3339))
			}
			summary.BulkPoolObserved = caps.bulkPool
			summary.IndexingPressureObserved = caps.indexingPressure
			for _, name := range sortedKeys(nodes) {
				prefix := "es-node"
				if len(nodes) > 1 {
					prefix += "-" + componentSlug(name)
				}
				ns, err := writeNodeStats(outDir, prefix, name, nodes[name], caps)
				if err != nil {
					return fmt.Errorf("node %s: %w", name, err)
				}
				summary.Nodes = append(summary.Nodes, ns)
			}

			counts, err := elastic.CountByDataStream(ctx, es, dsIndex, since, until)
			if err != nil {
				return fmt.Errorf("count data streams: %w", err)
			}
			if err := writeDataStreamCounts(outDir, counts, &summary); err != nil {
				return err
			}

			if step.ID != "" {
				b, err := json.Marshal(summary)
				if err != nil {
					return fmt.Errorf("encode summary: %w", err)
				}
				state.SetStepOutput(step.ID, "summary", string(b))
				state.SetStepOutput(step.ID, "totalDocs", summary.TotalDocs)
				state.SetStepOutput(step.ID, "totalPerSec", fmtNum(summary.TotalPerSec))
			}
			return nil
		}), nil
	}
}

func groupNodeStats(docs []map[string]interface{}) (map[string][]esNodeSample, esCapabilities) {
	nodes := map[string][]esNodeSample{}
	var caps esCapabilities
	for _, doc := range docs {
		t, ok := docTime(doc)
		if !ok {
			continue
		}
		total, ok := numField(doc, esIndexTotalField)
		if !ok {
			continue
		}
		name := stringField(doc, esNodeNameField)
		if name == "" {
			name = "elasticsearch"
		}
		cpu, _ := numField(doc, esCPUPctField)
		heap, _ := numField(doc, esHeapBytesField)
		indexTimeMs, _ := numField(doc, esIndexTimeMsField)

		writeActive, _ := numField(doc, esWriteActiveField)
		writeQueue, _ := numField(doc, esWriteQueueField)
		writeRejected, _ := numField(doc, esWriteRejectedField)

		bulkPoolActive, bulkOk1 := numField(doc, esBulkPoolActiveField)
		bulkPoolQueue, bulkOk2 := numField(doc, esBulkPoolQueueField)
		bulkPoolRejected, bulkOk3 := numField(doc, esBulkPoolRejectedField)
		if bulkOk1 || bulkOk2 || bulkOk3 {
			caps.bulkPool = true
		}

		bulkAvgSizeBytes, _ := numField(doc, esBulkAvgSizeBytesField)
		bulkAvgTimeMs, _ := numField(doc, esBulkAvgTimeMsField)
		bulkTotalOps, _ := numField(doc, esBulkTotalOpsField)

		mergesTotal, _ := numField(doc, esMergesTotalField)
		mergesTimeMs, _ := numField(doc, esMergesTimeMsField)
		segmentsCount, _ := numField(doc, esSegmentsCountField)
		segmentsMemBytes, _ := numField(doc, esSegmentsMemBytesField)

		ipCurrentBytes, ipOk1 := numField(doc, esIPCurrentBytesField)
		ipPrimaryRejected, ipOk2 := numField(doc, esIPPrimaryRejectedField)
		ipCoordinatingRejected, ipOk3 := numField(doc, esIPCoordinatingRejectedField)
		if ipOk1 || ipOk2 || ipOk3 {
			caps.indexingPressure = true
		}

		nodes[name] = append(nodes[name], esNodeSample{
			t: t, indexTotal: total, cpuPct: cpu, heapBytes: heap,
			indexTimeMs: indexTimeMs,

			writeActive: writeActive, writeQueue: writeQueue, writeRejected: writeRejected,
			bulkPoolActive: bulkPoolActive, bulkPoolQueue: bulkPoolQueue, bulkPoolRejected: bulkPoolRejected,

			bulkAvgSizeBytes: bulkAvgSizeBytes, bulkAvgTimeMs: bulkAvgTimeMs, bulkTotalOps: bulkTotalOps,

			mergesTotal: mergesTotal, mergesTimeMs: mergesTimeMs,
			segmentsCount: segmentsCount, segmentsMemBytes: segmentsMemBytes,

			ipCurrentBytes:         ipCurrentBytes,
			ipPrimaryRejected:      ipPrimaryRejected,
			ipCoordinatingRejected: ipCoordinatingRejected,
		})
	}
	for _, s := range nodes {
		sort.Slice(s, func(i, j int) bool { return s[i].t.Before(s[j].t) })
	}
	return nodes, caps
}

// deltaRate returns the per-second rate of change of a cumulative counter
// between two samples, or 0 if the samples are not chronologically ordered.
func deltaRate(prevT, t time.Time, prevV, v float64) float64 {
	dt := t.Sub(prevT).Seconds()
	if dt <= 0 {
		return 0
	}
	return round4((v - prevV) / dt)
}

func writeNodeStats(outDir, prefix, name string, samples []esNodeSample, caps esCapabilities) (harness.EsNodeSummary, error) {
	ns := harness.EsNodeSummary{Name: name, Samples: len(samples)}
	times := make([]time.Time, len(samples))
	rows := make([][]float64, len(samples))
	var rates, msPerDocPts, bulkOpsRatePts, writeRejectedRatePts []tsPoint
	for i, s := range samples {
		rate, msDoc := 0.0, 0.0
		if i > 0 {
			prev := samples[i-1]
			rate = deltaRate(prev.t, s.t, prev.indexTotal, s.indexTotal)
			rates = append(rates, tsPoint{s.t, rate})
			bulkOpsRatePts = append(bulkOpsRatePts, tsPoint{s.t, deltaRate(prev.t, s.t, prev.bulkTotalOps, s.bulkTotalOps)})
			writeRejectedRatePts = append(writeRejectedRatePts, tsPoint{s.t, deltaRate(prev.t, s.t, prev.writeRejected, s.writeRejected)})
			if docDelta := s.indexTotal - prev.indexTotal; docDelta > 0 {
				msDoc = round4((s.indexTimeMs - prev.indexTimeMs) / docDelta)
				msPerDocPts = append(msPerDocPts, tsPoint{s.t, msDoc})
			}
		}
		times[i] = s.t
		row := []float64{s.indexTotal, s.cpuPct, s.heapBytes, rate,
			s.indexTimeMs, msDoc, s.mergesTotal, s.mergesTimeMs, s.segmentsCount, s.segmentsMemBytes}
		if caps.indexingPressure {
			row = append(row, s.ipCurrentBytes, s.ipPrimaryRejected, s.ipCoordinatingRejected)
		}
		rows[i] = row
	}
	if len(samples) > 1 {
		first, last := samples[0], samples[len(samples)-1]
		ns.IndexedDocs = int64(last.indexTotal - first.indexTotal)
		ns.MergesCount = int64(last.mergesTotal - first.mergesTotal)
		ns.MergesTimeMs = int64(last.mergesTimeMs - first.mergesTimeMs)
		if caps.bulkPool {
			ns.BulkPoolRejected = int64(last.bulkPoolRejected - first.bulkPoolRejected)
		}
		if caps.indexingPressure {
			ns.IndexingPressureRejected = int64((last.ipPrimaryRejected - first.ipPrimaryRejected) +
				(last.ipCoordinatingRejected - first.ipCoordinatingRejected))
		}
	}
	ns.IndexingAvg, ns.IndexingMax = avgMax(rates)
	ns.IndexMsPerDocAvg, _ = avgMax(msPerDocPts)
	ns.BulkOpsPerSecAvg, ns.BulkOpsPerSecMax = avgMax(bulkOpsRatePts)
	ns.WriteRejectedAvg, ns.WriteRejectedMax = avgMax(writeRejectedRatePts)

	cpu := make([]tsPoint, len(samples))
	heap := make([]tsPoint, len(samples))
	writeActive := make([]tsPoint, len(samples))
	writeQueue := make([]tsPoint, len(samples))
	segmentsCount := make([]tsPoint, len(samples))
	bulkAvgSize := make([]tsPoint, len(samples))
	var bulkPoolQueue, ipBytes []tsPoint
	for i, s := range samples {
		cpu[i] = tsPoint{s.t, s.cpuPct}
		heap[i] = tsPoint{s.t, s.heapBytes}
		writeActive[i] = tsPoint{s.t, s.writeActive}
		writeQueue[i] = tsPoint{s.t, s.writeQueue}
		segmentsCount[i] = tsPoint{s.t, s.segmentsCount}
		bulkAvgSize[i] = tsPoint{s.t, s.bulkAvgSizeBytes}
		if caps.bulkPool {
			bulkPoolQueue = append(bulkPoolQueue, tsPoint{s.t, s.bulkPoolQueue})
		}
		if caps.indexingPressure {
			ipBytes = append(ipBytes, tsPoint{s.t, s.ipCurrentBytes})
		}
	}
	ns.CPUAvg, ns.CPUMax = avgMax(cpu)
	ns.HeapAvg, ns.HeapMax = avgMax(heap)
	ns.WriteActiveAvg, ns.WriteActiveMax = avgMax(writeActive)
	ns.WriteQueueAvg, ns.WriteQueueMax = avgMax(writeQueue)
	ns.SegmentsAvg, ns.SegmentsMax = avgMax(segmentsCount)
	ns.BulkAvgSizeBytesAvg, _ = avgMax(bulkAvgSize)
	if caps.bulkPool {
		ns.BulkPoolQueueAvg, ns.BulkPoolQueueMax = avgMax(bulkPoolQueue)
	}
	if caps.indexingPressure {
		ns.IndexingPressureBytesAvg, _ = avgMax(ipBytes)
	}

	header := []string{"timestamp", "index_total", "cpu_pct", "heap_bytes", "indexing_rate",
		"index_time_ms", "ms_per_doc", "merges_total", "merges_time_ms", "segments_count", "segments_memory_bytes"}
	if caps.indexingPressure {
		header = append(header, "ip_current_bytes", "ip_primary_rejected", "ip_coordinating_rejected")
	}
	tsvPath := filepath.Join(outDir, prefix+"-timeseries.tsv")
	if err := writeAlignedTSV(tsvPath, header, times, rows); err != nil {
		return ns, err
	}
	charts := []struct{ file, title, col string }{
		{"-indexing-chart.svg", "Elasticsearch indexing rate (docs/s)", "indexing_rate"},
		{"-cpu-chart.svg", "Elasticsearch process CPU %", "cpu_pct"},
		{"-heap-chart.svg", "Elasticsearch JVM heap used (bytes)", "heap_bytes"},
		{"-indexing-cost-chart.svg", "Elasticsearch per-doc indexing cost (ms/doc)", "ms_per_doc"},
	}
	for _, c := range charts {
		title := c.title
		if prefix != "es-node" {
			title = name + " " + title
		}
		if err := renderLineChart(filepath.Join(outDir, prefix+c.file), title, tsvPath, "timestamp", []string{c.col}); err != nil {
			return ns, err
		}
	}

	if err := writeThreadPoolStats(outDir, prefix, name, samples, caps); err != nil {
		return ns, err
	}
	if err := writeBulkStats(outDir, prefix, name, samples); err != nil {
		return ns, err
	}

	fmt.Fprintf(os.Stderr, "[elasticsearch-metrics] node %s: %d samples, %d docs indexed, indexing avg %.1f/s max %.1f/s\n",
		name, ns.Samples, ns.IndexedDocs, ns.IndexingAvg, ns.IndexingMax)
	return ns, nil
}

// writeThreadPoolStats reports write thread-pool saturation (queue depth and
// rejections), plus the legacy "bulk" pool when present (Elasticsearch
// versions older than 9.6.0).
func writeThreadPoolStats(outDir, prefix, name string, samples []esNodeSample, caps esCapabilities) error {
	times := make([]time.Time, len(samples))
	rows := make([][]float64, len(samples))
	for i, s := range samples {
		row := []float64{s.writeActive, s.writeQueue, s.writeRejected}
		if caps.bulkPool {
			row = append(row, s.bulkPoolActive, s.bulkPoolQueue, s.bulkPoolRejected)
		}
		times[i] = s.t
		rows[i] = row
	}
	header := []string{"timestamp", "write_active", "write_queue", "write_rejected"}
	if caps.bulkPool {
		header = append(header, "bulk_pool_active", "bulk_pool_queue", "bulk_pool_rejected")
	}
	tsvPath := filepath.Join(outDir, prefix+"-threadpool-timeseries.tsv")
	if err := writeAlignedTSV(tsvPath, header, times, rows); err != nil {
		return err
	}

	title := "Elasticsearch write thread pool saturation (queue / rejected)"
	if prefix != "es-node" {
		title = name + " " + title
	}
	if err := renderLineChart(filepath.Join(outDir, prefix+"-threadpool-chart.svg"), title, tsvPath, "timestamp", []string{"write_queue", "write_rejected"}); err != nil {
		return err
	}
	if caps.bulkPool {
		title = "Elasticsearch legacy bulk thread pool saturation (queue / rejected)"
		if prefix != "es-node" {
			title = name + " " + title
		}
		if err := renderLineChart(filepath.Join(outDir, prefix+"-bulk-pool-chart.svg"), title, tsvPath, "timestamp", []string{"bulk_pool_queue", "bulk_pool_rejected"}); err != nil {
			return err
		}
	}
	return nil
}

// writeBulkStats reports the realized shape of _bulk requests as seen by
// Elasticsearch (indices.bulk.*): average request size/time and throughput.
func writeBulkStats(outDir, prefix, name string, samples []esNodeSample) error {
	times := make([]time.Time, len(samples))
	rows := make([][]float64, len(samples))
	for i, s := range samples {
		opsPerSec := 0.0
		if i > 0 {
			prev := samples[i-1]
			opsPerSec = deltaRate(prev.t, s.t, prev.bulkTotalOps, s.bulkTotalOps)
		}
		times[i] = s.t
		rows[i] = []float64{s.bulkAvgSizeBytes, s.bulkAvgTimeMs, s.bulkTotalOps, opsPerSec}
	}
	header := []string{"timestamp", "avg_size_bytes", "avg_time_ms", "total_operations", "ops_per_sec"}
	tsvPath := filepath.Join(outDir, prefix+"-bulk-timeseries.tsv")
	if err := writeAlignedTSV(tsvPath, header, times, rows); err != nil {
		return err
	}
	title := "Elasticsearch bulk request avg size (bytes)"
	if prefix != "es-node" {
		title = name + " " + title
	}
	return renderLineChart(filepath.Join(outDir, prefix+"-bulk-chart.svg"), title, tsvPath, "timestamp", []string{"avg_size_bytes"})
}

func writeDataStreamCounts(outDir string, counts []elastic.DataStreamCount, summary *harness.EsMetricsSummary) error {
	path := filepath.Join(outDir, "es-data-streams.tsv")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create data streams tsv: %w", err)
	}
	defer f.Close() //nolint:errcheck
	fmt.Fprintln(f, "data_stream\tdocs\tper_sec")
	for _, c := range counts {
		perSec := round4(float64(c.Count) / summary.WindowSec)
		fmt.Fprintf(f, "%s\t%d\t%s\n", c.Name, c.Count, fmtNum(perSec))
		summary.DataStreams = append(summary.DataStreams, harness.DataStreamSummary{Name: c.Name, Docs: c.Count, PerSec: perSec})
		summary.TotalDocs += c.Count
	}
	summary.TotalPerSec = round4(float64(summary.TotalDocs) / summary.WindowSec)
	fmt.Fprintf(f, "total\t%d\t%s\n", summary.TotalDocs, fmtNum(summary.TotalPerSec))
	fmt.Fprintf(os.Stderr, "[elasticsearch-metrics] %d data streams, %d docs over %.0fs = %.2f events/s; wrote %s\n",
		len(counts), summary.TotalDocs, summary.WindowSec, summary.TotalPerSec, path)
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
