package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

const (
	defaultBeatsIndex = ".monitoring-beats-*"
	beatsTimeField    = "timestamp"
	beatsAckedField   = "beats_stats.metrics.libbeat.output.events.acked"
	beatsCPUMsField   = "beats_stats.metrics.beat.cpu.total.time.ms"
	beatsRSSField     = "beats_stats.metrics.beat.memstats.rss"
	beatsUptimeField  = "beats_stats.metrics.beat.info.uptime.ms"
	beatsMaxDocs      = 10000
	beatsQueryTime    = "2006-01-02T15:04:05.000Z07:00"
)

type beatSample struct {
	t        time.Time
	uptimeMs float64
	cpuMs    float64
	rss      float64
	acked    float64
}

func parseBeatDoc(doc map[string]interface{}) (beatSample, bool) {
	v, ok := elastic.GetFieldValue(doc, beatsTimeField)
	if !ok {
		return beatSample{}, false
	}
	str, _ := v.(string)
	t, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return beatSample{}, false
	}
	acked, ok := numField(doc, beatsAckedField)
	if !ok {
		return beatSample{}, false
	}
	uptime, _ := numField(doc, beatsUptimeField)
	cpu, _ := numField(doc, beatsCPUMsField)
	rss, _ := numField(doc, beatsRSSField)
	return beatSample{t: t, uptimeMs: uptime, cpuMs: cpu, rss: rss, acked: acked}, true
}

// startedAfter reports whether the sampled process started at or after t.
// shortcut: compares the beat host clock to the operator clock, so clock skew
// larger than the gap between warmup start and filebeat start misclassifies;
// upgrade to a process id the harness can name if that bites.
func (s beatSample) startedAfter(t time.Time) bool {
	return !s.t.Add(-time.Duration(s.uptimeMs * float64(time.Millisecond))).Before(t)
}

// fetchBeatSamples returns beats_stats samples with since <= timestamp <
// until (until optional), oldest first, from processes started after since.
func fetchBeatSamples(ctx context.Context, es *elasticsearch.Client, index string, since time.Time, until *time.Time, filter map[string]string) ([]beatSample, error) {
	rng := map[string]interface{}{"gte": since.UTC().Format(beatsQueryTime)}
	if until != nil {
		rng["lt"] = until.UTC().Format(beatsQueryTime)
	}
	clauses := []interface{}{
		map[string]interface{}{"term": map[string]interface{}{"type": "beats_stats"}},
		map[string]interface{}{"range": map[string]interface{}{beatsTimeField: rng}},
	}
	for field, value := range filter {
		clauses = append(clauses, map[string]interface{}{"term": map[string]interface{}{field: value}})
	}
	body, err := json.Marshal(map[string]interface{}{
		"size":  beatsMaxDocs,
		"sort":  []interface{}{map[string]interface{}{beatsTimeField: map[string]interface{}{"order": "asc"}}},
		"query": map[string]interface{}{"bool": map[string]interface{}{"filter": clauses}},
	})
	if err != nil {
		return nil, err
	}
	resp, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(bytes.NewReader(body)),
		es.Search.WithIgnoreUnavailable(true),
		es.Search.WithAllowNoIndices(true),
	)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return nil, fmt.Errorf("search: %s", resp.Status())
	}
	var result struct {
		Hits struct {
			Hits []struct {
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	var out []beatSample
	for _, h := range result.Hits.Hits {
		if s, ok := parseBeatDoc(h.Source); ok && s.startedAfter(since) {
			out = append(out, s)
		}
	}
	return out, nil
}

func resolveBeatsIndex(state *harness.State, index string) (string, error) {
	if index == "" {
		return defaultBeatsIndex, nil
	}
	return state.Resolve(index)
}

func NewFilebeatWaitFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("filebeat-wait: %w", err)
			}
			cfg := step.FilebeatWait
			index, err := resolveBeatsIndex(state, cfg.Index)
			if err != nil {
				return err
			}
			since, err := resolveTime(state, cfg.Since)
			if err != nil {
				return fmt.Errorf("since: %w", err)
			}
			filter, err := resolveFilter(state, cfg.Filter)
			if err != nil {
				return err
			}
			want := float64(cfg.Expected)
			if want < 1 {
				want = 1
			}
			pollCtx, cancel := context.WithTimeout(ctx, cfg.Timeout.Duration)
			defer cancel()

			var reached beatSample
			var latest *beatSample
			for {
				samples, err := fetchBeatSamples(pollCtx, es, index, since, nil, filter)
				if err != nil {
					return fmt.Errorf("filebeat-wait: %w", err)
				}
				if n := len(samples); n > 0 {
					latest = &samples[n-1]
				}
				found := false
				for _, s := range samples {
					if s.acked >= want {
						reached, found = s, true
						break
					}
				}
				if found {
					break
				}
				select {
				case <-pollCtx.Done():
					if latest == nil {
						return fmt.Errorf("filebeat-wait: timed out, no filebeat samples since %s in %s", since.Format(time.RFC3339), index)
					}
					return fmt.Errorf("filebeat-wait: timed out waiting for acked >= %.0f (latest sample acked=%.0f at %s)", want, latest.acked, latest.t.Format(time.RFC3339))
				case <-time.After(defaultPollInterval):
				}
			}
			if step.ID != "" {
				state.SetStepOutput(step.ID, "reachedAt", reached.t.UTC().Format(time.RFC3339Nano))
				state.SetStepOutput(step.ID, "acked", int64(reached.acked))
				state.SetStepOutput(step.ID, "confirmedAt", time.Now().UTC().Format(time.RFC3339))
			}
			return nil
		}), nil
	}
}

func NewFilebeatMetricsFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("filebeat-metrics: %w", err)
			}
			cfg := step.FilebeatMetrics
			index, err := resolveBeatsIndex(state, cfg.Index)
			if err != nil {
				return err
			}
			since, err := resolveTime(state, cfg.Since)
			if err != nil {
				return fmt.Errorf("since: %w", err)
			}
			until, err := resolveTime(state, cfg.Until)
			if err != nil {
				return fmt.Errorf("until: %w", err)
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
			filter, err := resolveFilter(state, cfg.Filter)
			if err != nil {
				return err
			}
			samples, err := fetchBeatSamples(ctx, es, index, since, &until, filter)
			if err != nil {
				return fmt.Errorf("filebeat-metrics: %w", err)
			}
			sort.SliceStable(samples, func(i, j int) bool { return samples[i].t.Before(samples[j].t) })
			summary, err := writeFilebeatMetrics(outDir, samples)
			if err != nil {
				return err
			}
			if step.ID != "" {
				b, err := json.Marshal(summary)
				if err != nil {
					return fmt.Errorf("encode summary: %w", err)
				}
				state.SetStepOutput(step.ID, "summary", string(b))
			}
			return nil
		}), nil
	}
}

// writeFilebeatMetrics derives per-interval CPU % and events/s from the
// cumulative counters; the first sample is only a baseline.
func writeFilebeatMetrics(outDir string, samples []beatSample) (harness.FilebeatMetricsSummary, error) {
	sum := harness.FilebeatMetricsSummary{Samples: len(samples)}
	if len(samples) < 3 {
		fmt.Fprintf(os.Stderr, "[filebeat-metrics] warning: only %d samples in the window; the burst finished too fast for the monitoring period, use more lines\n", len(samples))
	}
	if len(samples) < 2 {
		return sum, nil
	}
	var cpu, rss, rate []tsPoint
	times := make([]time.Time, 0, len(samples)-1)
	rows := make([][]float64, 0, len(samples)-1)
	for i := 1; i < len(samples); i++ {
		prev, s := samples[i-1], samples[i]
		dtMs := float64(s.t.Sub(prev.t).Milliseconds())
		if dtMs <= 0 || s.acked < prev.acked {
			continue
		}
		cpuPct := round4((s.cpuMs - prev.cpuMs) / dtMs * 100)
		eps := deltaRate(prev.t, s.t, prev.acked, s.acked)
		cpu = append(cpu, tsPoint{s.t, cpuPct})
		rss = append(rss, tsPoint{s.t, s.rss})
		rate = append(rate, tsPoint{s.t, eps})
		times = append(times, s.t)
		rows = append(rows, []float64{cpuPct, s.rss, s.acked, eps})
	}
	sum.WindowSec = samples[len(samples)-1].t.Sub(samples[0].t).Seconds()
	sum.Events = int64(samples[len(samples)-1].acked)
	sum.CPUAvg, sum.CPUMax = avgMax(cpu)
	sum.RSSAvg, sum.RSSMax = avgMax(rss)
	sum.EventsPerSecAvg, sum.EventsPerSecMax = avgMax(rate)

	header := []string{"timestamp", "cpu_pct", "rss_bytes", "events_acked", "events_per_sec"}
	tsvPath := filepath.Join(outDir, "filebeat-timeseries.tsv")
	if err := writeAlignedTSV(tsvPath, header, times, rows); err != nil {
		return sum, err
	}
	charts := []struct{ file, title, col string }{
		{"filebeat-cpu-chart.svg", "Filebeat CPU %", "cpu_pct"},
		{"filebeat-memory-chart.svg", "Filebeat RSS (bytes)", "rss_bytes"},
		{"filebeat-events-chart.svg", "Filebeat output events/s", "events_per_sec"},
	}
	for _, c := range charts {
		if err := renderLineChart(filepath.Join(outDir, c.file), c.title, tsvPath, "timestamp", []string{c.col}); err != nil {
			return sum, err
		}
	}
	fmt.Fprintf(os.Stderr, "[filebeat-metrics] %d samples, %d events, avg %.1f/s max %.1f/s\n",
		sum.Samples, sum.Events, sum.EventsPerSecAvg, sum.EventsPerSecMax)
	return sum, nil
}
