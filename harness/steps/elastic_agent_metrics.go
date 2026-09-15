package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

const (
	defaultAgentMetricsIndex = "metrics-elastic_agent.*"
	defaultAgentLogsIndex    = "logs-elastic_agent*"
	agentMetricsPeriod       = time.Minute
	ingestInterval           = time.Minute
	agentDataset             = "elastic_agent.elastic_agent"
	datasetPrefix            = "elastic_agent."
	inputDatasetSuffix       = "_input"
	maxEventSeries           = 20
	otherSeriesID            = "other"
)

var statsCounterFields = []string{
	"beat.stats.libbeat.output.events.acked",
	"filebeat_input.events_pipeline_published_total",
}

const (
	defaultAgentDashboardID = "elastic_agent-f47f18cc-9c7d-4278-b2ea-a6dee816d395"
	defaultDashboardWidth   = 1600
	defaultDashboardHeight  = 2400
	defaultDashboardTimeout = 10 * time.Minute
	defaultDashboardPoll    = 5 * time.Second
	dashboardPNGExportType  = "pngV2"
)

func NewEsAgentMetricsFactory(esCfg harness.ESConfig, kbCfg harness.KibanaConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("elastic-agent-metrics: %w", err)
			}
			cfg := step.EsAgentMetrics

			index := defaultAgentMetricsIndex
			if cfg.Index != "" {
				if index, err = state.Resolve(cfg.Index); err != nil {
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
			outDir, err := state.Resolve(cfg.OutputDir)
			if err != nil {
				return err
			}
			outDir, err = filepath.Abs(outDir)
			if err != nil {
				return fmt.Errorf("resolve output_dir: %w", err)
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return fmt.Errorf("create output dir: %w", err)
			}

			var summary harness.AgentMetricsSummary
			for _, agent := range cfg.Agents {
				if agent.Label, err = state.Resolve(agent.Label); err != nil {
					return fmt.Errorf("agent %s label: %w", agent.ID, err)
				}
				filter, err := resolveFilter(state, agent.Filter)
				if err != nil {
					return fmt.Errorf("agent %s filter: %w", agent.ID, err)
				}
				as, err := collectAgentMetrics(ctx, es, index, since, until, outDir, agent, filter, cfg.Integrations)
				if err != nil {
					return fmt.Errorf("agent %s: %w", agent.ID, err)
				}
				summary.Agents = append(summary.Agents, as)
			}
			if len(summary.Agents) > 1 {
				summary.AllAgents = summarizeAllAgents(summary.Agents)
			}
			for _, ing := range cfg.Ingest {
				if ing.Index == "" {
					return fmt.Errorf("ingest %q: index is required", ing.Name)
				}
				idx, err := state.Resolve(ing.Index)
				if err != nil {
					return fmt.Errorf("ingest %s index: %w", ing.Name, err)
				}
				filter, err := resolveFilter(state, ing.Filter)
				if err != nil {
					return fmt.Errorf("ingest %s filter: %w", ing.Name, err)
				}
				name := ing.Name
				if name == "" {
					name = componentSlug(idx)
				}
				is, err := collectIngest(ctx, es, idx, since, until, outDir, name, filter)
				if err != nil {
					return fmt.Errorf("ingest %s: %w", name, err)
				}
				summary.Ingest = append(summary.Ingest, is)
			}

			if cfg.Dashboard != nil {
				if err := collectDashboardPNGs(ctx, state, kbCfg, cfg, since, until, outDir, summary.Agents); err != nil {
					if errors.Is(err, errDashboardConfig) {
						return fmt.Errorf("dashboard: %w", err)
					}
					fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] warning: dashboard: %v\n", err)
					for i := range summary.Agents {
						if summary.Agents[i].DashboardPNG == "" && summary.Agents[i].DashboardErr == "" {
							summary.Agents[i].DashboardErr = err.Error()
						}
					}
				}
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

type tsPoint struct {
	t time.Time
	v float64
}

type procSample struct {
	t     time.Time
	mem   float64
	cpuMS float64
	cores float64
}

type procSeries struct {
	id      string
	binary  string
	samples []procSample
}

type statsSeries struct {
	dataset string
	id      string
	parent  string
	field   string
	points  []tsPoint
}

type rateSeries struct {
	dataset string
	id      string
	field   string
	members int
	points  []tsPoint
}

func collectAgentMetrics(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	outDir string,
	agent harness.AgentSpec,
	filter map[string]string,
	integrations []string,
) (harness.AgentSummary, error) {
	label := agent.Label
	if label == "" {
		label = agent.ID
	}
	sum := harness.AgentSummary{ID: agent.ID, Label: label}

	procDocs, err := elastic.FetchDocs(ctx, es, index, since, until,
		mergeStringMaps(filter, map[string]string{"metricset.name": "json"}), 0)
	if err != nil {
		return sum, fmt.Errorf("fetch process metrics: %w", err)
	}
	statsDocs, err := elastic.FetchDocs(ctx, es, index, since, until,
		mergeStringMaps(filter, map[string]string{"metricset.name": "stats"}), 0)
	if err != nil {
		return sum, fmt.Errorf("fetch pipeline stats: %w", err)
	}
	errCounts, err := elastic.CountByGroup(ctx, es, defaultAgentLogsIndex, since, until,
		mergeStringMaps(filter, map[string]string{"log.level": "error"}), "component.id")
	if err != nil {
		return sum, fmt.Errorf("count error logs: %w", err)
	}

	procs := groupProcessDocs(procDocs)
	stats := groupStatsDocs(statsDocs)
	if len(procs) == 0 {
		fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] warning: agent %s: 0 process metric docs (index=%s since=%s until=%s)\n",
			agent.ID, index, since.Format(time.RFC3339), until.Format(time.RFC3339))
	}
	for _, n := range errCounts {
		sum.ErrorLogs += n
	}

	period := detectPeriod(procs, stats)
	cpuRates := make([][]tsPoint, len(procs))
	memPoints := make([][]tsPoint, len(procs))
	for i, p := range procs {
		cpuRates[i] = cpuPctPoints(p)
		memPoints[i] = make([]tsPoint, len(p.samples))
		for j, s := range p.samples {
			memPoints[i][j] = tsPoint{s.t, s.mem}
		}
		path := filepath.Join(outDir, agent.ID+"-"+componentSlug(p.id)+".tsv")
		if err := writeProcessTSV(path, p, cpuRates[i]); err != nil {
			return sum, err
		}
		sum.Processes = append(sum.Processes, summarizeProcess(p, cpuRates[i]))
	}

	if len(procs) > 0 {
		names := make([]string, len(procs))
		for i, p := range procs {
			names[i] = componentSlug(p.id)
		}
		times, aligned := alignSeries(append(append([][]tsPoint{}, memPoints...), cpuRates...), period)
		memVals, cpuVals := splitColumns(aligned, len(procs))
		memTotal := sumRows(memVals)
		cpuTotal := sumRows(cpuVals)

		memCols := prefixed("mem_", names)
		cpuCols := prefixed("cpu_", names)
		header := append(append(append([]string{"timestamp"}, memCols...), "mem_total"), append(cpuCols, "cpu_total")...)
		rows := make([][]float64, len(times))
		for i := range times {
			rows[i] = append(append(append([]float64{}, memVals[i]...), memTotal[i]), append(cpuVals[i], cpuTotal[i])...)
		}
		procPath := filepath.Join(outDir, agent.ID+"-processes.tsv")
		if err := writeAlignedTSV(procPath, header, times, rows); err != nil {
			return sum, err
		}
		if err := renderLineChart(filepath.Join(outDir, agent.ID+"-memory-chart.svg"), label+" memory (bytes)", procPath, "timestamp", append(memCols, "mem_total")); err != nil {
			return sum, err
		}
		if err := renderLineChart(filepath.Join(outDir, agent.ID+"-cpu-chart.svg"), label+" CPU %", procPath, "timestamp", append(cpuCols, "cpu_total")); err != nil {
			return sum, err
		}
		sum.Processes = append(sum.Processes, summarizeTotal(times, memTotal, cpuTotal))
	}

	rates := capEventSeries(rollUpInputSeries(stats, period), maxEventSeries, period)
	logDiscovery(agent.ID, procs, stats, rates)
	ratePoints := make([][]tsPoint, len(rates))
	eventCols := make([]string, len(rates))
	var chartCols []string
	for i, r := range rates {
		ratePoints[i] = r.points
		eventCols[i] = eventColumn(r.dataset, r.id)
		if !strings.Contains(r.id, "monitoring") {
			chartCols = append(chartCols, eventCols[i])
		}
		avg, max := avgMax(r.points)
		sum.Events = append(sum.Events, harness.EventsSummary{
			Dataset: r.dataset, ID: r.id, Field: r.field, Members: r.members, AvgPerSec: avg, MaxPerSec: max,
		})
	}
	if len(rates) > 0 {
		times, vals := alignSeries(ratePoints, period)
		eventsPath := filepath.Join(outDir, agent.ID+"-events.tsv")
		if err := writeAlignedTSV(eventsPath, append([]string{"timestamp"}, eventCols...), times, vals); err != nil {
			return sum, err
		}
		if len(chartCols) > 0 {
			if err := renderLineChart(filepath.Join(outDir, agent.ID+"-events-chart.svg"), label+" pipeline events/sec", eventsPath, "timestamp", chartCols); err != nil {
				return sum, err
			}
		}
	}

	for _, integ := range integrations {
		is := harness.IntegrationSummary{ID: integ, Mode: classifyIntegration(integ, procs, stats)}
		for id, n := range errCounts {
			if matchesIntegration(id, integ) {
				is.ErrorLogs += n
			}
		}
		for _, e := range integrationEvents(integ, sum.Events) {
			is.EventsPerSecAvg += e.AvgPerSec
		}
		sum.Integrations = append(sum.Integrations, is)
	}
	return sum, nil
}

var errDashboardConfig = errors.New("dashboard configuration")

func collectDashboardPNGs(
	ctx context.Context,
	state *harness.State,
	kbCfg harness.KibanaConfig,
	cfg *harness.EsAgentMetricsStep,
	since, until time.Time,
	outDir string,
	agents []harness.AgentSummary,
) error {
	spec := cfg.Dashboard
	kb, err := newKibanaClient(state, kbCfg)
	if err != nil {
		return fmt.Errorf("%w: %w", errDashboardConfig, err)
	}
	dashboardID := defaultAgentDashboardID
	if spec.ID != "" {
		if dashboardID, err = state.Resolve(spec.ID); err != nil {
			return fmt.Errorf("%w: id: %w", errDashboardConfig, err)
		}
	}
	width, height := spec.Width, spec.Height
	if width <= 0 {
		width = defaultDashboardWidth
	}
	if height <= 0 {
		height = defaultDashboardHeight
	}
	timeout := spec.Timeout.Duration
	if timeout <= 0 {
		timeout = defaultDashboardTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	version, err := kb.version(ctx)
	if err != nil {
		return fmt.Errorf("kibana version: %w", err)
	}

	fail := func(i int, err error) {
		agents[i].DashboardErr = err.Error()
		fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] warning: agent %s: dashboard PNG: %v\n", cfg.Agents[i].ID, err)
	}
	paths := make([]string, len(cfg.Agents))
	for i, agent := range cfg.Agents {
		filter, err := resolveFilter(state, agent.Filter)
		if err != nil {
			return fmt.Errorf("%w: agent %s filter: %w", errDashboardConfig, agent.ID, err)
		}
		title := agents[i].Label + " agent metrics"
		jobParams := dashboardPNGJobParams(dashboardID, title, kueryFilter(filter), since, until, width, height, version)
		if paths[i], err = kb.generateReport(ctx, dashboardPNGExportType, jobParams); err != nil {
			fail(i, fmt.Errorf("request report: %w", err))
			continue
		}
		fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] agent %s: requested dashboard PNG (%s)\n", agent.ID, paths[i])
	}
	for i, agent := range cfg.Agents {
		if paths[i] == "" {
			continue
		}
		dest := filepath.Join(outDir, agent.ID+"-dashboard.png")
		n, err := kb.waitReport(ctx, paths[i], dest, defaultDashboardPoll)
		if err != nil {
			fail(i, err)
			continue
		}
		if n == 0 {
			fail(i, fmt.Errorf("dashboard PNG is empty"))
			continue
		}
		agents[i].DashboardPNG = dest
		fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] agent %s: wrote dashboard PNG %s (%d bytes)\n", agent.ID, dest, n)
	}
	return nil
}

func collectIngest(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	outDir, name string,
	filter map[string]string,
) (harness.IngestSummary, error) {
	is := harness.IngestSummary{Name: name}
	buckets, err := elastic.DocCountHistogram(ctx, es, index, since, until, filter, ingestInterval)
	if err != nil {
		return is, err
	}
	path := filepath.Join(outDir, "ingest-"+componentSlug(name)+".tsv")
	f, err := os.Create(path)
	if err != nil {
		return is, fmt.Errorf("create ingest tsv: %w", err)
	}
	defer f.Close() //nolint:errcheck
	fmt.Fprintln(f, "timestamp\tdocs\tper_sec")
	secs := ingestInterval.Seconds()
	for _, b := range buckets {
		rate := round4(float64(b.Count) / secs)
		fmt.Fprintf(f, "%s\t%d\t%s\n", b.Time.Format(time.RFC3339), b.Count, fmtNum(rate))
		is.Total += b.Count
		if rate > is.MaxPerSec {
			is.MaxPerSec = rate
		}
	}
	if len(buckets) > 0 {
		is.AvgPerSec = round4(float64(is.Total) / (secs * float64(len(buckets))))
	}
	fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] ingest %s: %d docs in %d buckets, avg %.1f/s max %.1f/s\n",
		name, is.Total, len(buckets), is.AvgPerSec, is.MaxPerSec)
	if len(buckets) == 0 {
		return is, nil
	}
	return is, renderLineChart(filepath.Join(outDir, "ingest-"+componentSlug(name)+"-chart.svg"), name+" ingest docs/sec", path, "timestamp", []string{"per_sec"})
}

func groupProcessDocs(docs []map[string]interface{}) []*procSeries {
	byID := map[string]*procSeries{}
	for _, doc := range docs {
		t, ok := docTime(doc)
		if !ok {
			continue
		}
		mem, ok := numField(doc, "system.process.memory.size")
		if !ok {
			continue
		}
		id := stringField(doc, "component.id")
		if id == "" {
			continue
		}
		p := byID[id]
		if p == nil {
			p = &procSeries{id: id, binary: stringField(doc, "component.binary")}
			byID[id] = p
		}
		cpu, _ := numField(doc, "system.process.cpu.total.value")
		cores, _ := numField(doc, "system.cpu.cores")
		p.samples = append(p.samples, procSample{t: t, mem: mem, cpuMS: cpu, cores: cores})
	}
	out := make([]*procSeries, 0, len(byID))
	for _, p := range byID {
		sort.Slice(p.samples, func(i, j int) bool { return p.samples[i].t.Before(p.samples[j].t) })
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func groupStatsDocs(docs []map[string]interface{}) []*statsSeries {
	type key struct{ dataset, id string }
	byKey := map[key]*statsSeries{}
	for _, doc := range docs {
		t, ok := docTime(doc)
		if !ok {
			continue
		}
		id := statsComponentID(doc)
		if id == "" {
			continue
		}
		field := ""
		var v float64
		for _, f := range statsCounterFields {
			if n, ok := numField(doc, f); ok {
				field, v = f, n
				break
			}
		}
		if field == "" {
			continue
		}
		k := key{stringField(doc, "data_stream.dataset"), id}
		s := byKey[k]
		if s == nil {
			s = &statsSeries{dataset: k.dataset, id: id, parent: inputParent(k.dataset, id, doc), field: field}
			byKey[k] = s
		}
		s.points = append(s.points, tsPoint{t, v})
	}
	out := make([]*statsSeries, 0, len(byKey))
	for _, s := range byKey {
		sort.Slice(s.points, func(i, j int) bool { return s.points[i].t.Before(s.points[j].t) })
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].dataset != out[j].dataset {
			return out[i].dataset < out[j].dataset
		}
		return out[i].id < out[j].id
	})
	return out
}

// statsComponentID returns the sub-component id. The otel collector writes it
// as a dotted top-level "component.id" key beside the nested component object,
// and GetFieldValue checks the dotted key first.
func statsComponentID(doc map[string]interface{}) string {
	return stringField(doc, "component.id")
}

func classifyIntegration(integ string, procs []*procSeries, stats []*statsSeries) string {
	for _, p := range procs {
		if matchesIntegration(p.id, integ) {
			return "process"
		}
	}
	for _, s := range stats {
		if s.dataset == agentDataset && matchesIntegration(s.id, integ) {
			return "otel"
		}
	}
	return "absent"
}

func matchesIntegration(id, integ string) bool {
	return id == integ || strings.HasPrefix(id, integ+"/")
}

// integrationEvents returns the event series attributable to an integration,
// preferring output acked counters over input published counters so a beat
// reporting both is not counted twice.
func integrationEvents(integ string, events []harness.EventsSummary) []harness.EventsSummary {
	var acked, other []harness.EventsSummary
	for _, e := range events {
		if !matchesIntegration(e.ID, integ) {
			continue
		}
		if e.Field == statsCounterFields[0] {
			acked = append(acked, e)
		} else {
			other = append(other, e)
		}
	}
	if len(acked) > 0 {
		return acked
	}
	return other
}

// inputParent strips the per-input suffix from a *_input dataset id so every
// file harvested by one input rolls up to the input's parent component.
func inputParent(dataset, id string, doc map[string]interface{}) string {
	if !strings.HasSuffix(dataset, inputDatasetSuffix) {
		return id
	}
	if inputID := stringField(doc, "filebeat_input.id"); inputID != "" && strings.HasSuffix(id, "/"+inputID) {
		return strings.TrimSuffix(id, "/"+inputID)
	}
	return id
}

// detectPeriod returns the median spacing between consecutive samples across
// all series, rounded to the second, so rate roll-ups and the output grid match
// the agent's monitoring metrics_period. Falls back to agentMetricsPeriod.
func detectPeriod(procs []*procSeries, stats []*statsSeries) time.Duration {
	var deltas []time.Duration
	for _, p := range procs {
		for i := 1; i < len(p.samples); i++ {
			if d := p.samples[i].t.Sub(p.samples[i-1].t); d > 0 {
				deltas = append(deltas, d)
			}
		}
	}
	for _, s := range stats {
		for i := 1; i < len(s.points); i++ {
			if d := s.points[i].t.Sub(s.points[i-1].t); d > 0 {
				deltas = append(deltas, d)
			}
		}
	}
	if len(deltas) == 0 {
		return agentMetricsPeriod
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i] < deltas[j] })
	period := deltas[len(deltas)/2].Round(time.Second)
	if period < time.Second {
		return agentMetricsPeriod
	}
	return period
}

// rollUpInputSeries converts counters to rates per series, then sums the rates
// of series sharing a parent so per-file inputs collapse into one series.
func rollUpInputSeries(stats []*statsSeries, period time.Duration) []*rateSeries {
	type key struct{ dataset, parent string }
	groups := map[key][]*statsSeries{}
	var order []key
	for _, s := range stats {
		k := key{s.dataset, s.parent}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], s)
	}
	out := make([]*rateSeries, 0, len(order))
	for _, k := range order {
		members := groups[k]
		r := &rateSeries{dataset: k.dataset, id: k.parent, field: members[0].field, members: len(members)}
		if len(members) == 1 && members[0].id == k.parent {
			r.points = counterRatePoints(members[0].points)
		} else {
			rates := make([][]tsPoint, len(members))
			for i, m := range members {
				rates[i] = counterRatePoints(m.points)
			}
			r.points = sumRatesByBucket(rates, period)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].dataset != out[j].dataset {
			return out[i].dataset < out[j].dataset
		}
		return out[i].id < out[j].id
	})
	return out
}

// capEventSeries keeps the max series with the highest average rate and folds
// the rest into a single "other" series.
func capEventSeries(series []*rateSeries, max int, period time.Duration) []*rateSeries {
	if len(series) <= max {
		return series
	}
	byAvg := append([]*rateSeries{}, series...)
	sort.SliceStable(byAvg, func(i, j int) bool {
		ai, _ := avgMax(byAvg[i].points)
		aj, _ := avgMax(byAvg[j].points)
		return ai > aj
	})
	keep := map[*rateSeries]bool{}
	for _, s := range byAvg[:max] {
		keep[s] = true
	}
	out := make([]*rateSeries, 0, max+1)
	other := &rateSeries{id: otherSeriesID, field: series[0].field}
	var rest [][]tsPoint
	for _, s := range series {
		if keep[s] {
			out = append(out, s)
			continue
		}
		other.members += s.members
		rest = append(rest, s.points)
	}
	other.points = sumRatesByBucket(rest, period)
	return append(out, other)
}

// sumRatesByBucket sums rate points that fall in the same period-wide bucket
// across all series, without carrying values forward.
func sumRatesByBucket(series [][]tsPoint, period time.Duration) []tsPoint {
	sums := map[time.Time]float64{}
	for _, s := range series {
		for _, p := range s {
			sums[p.t.Truncate(period)] += p.v
		}
	}
	out := make([]tsPoint, 0, len(sums))
	for t, v := range sums {
		out = append(out, tsPoint{t, round4(v)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].t.Before(out[j].t) })
	return out
}

func cpuPctPoints(p *procSeries) []tsPoint {
	out := make([]tsPoint, 0, len(p.samples))
	for i := 1; i < len(p.samples); i++ {
		prev, cur := p.samples[i-1], p.samples[i]
		deltaMS := cur.t.Sub(prev.t).Seconds() * 1000
		if deltaMS <= 0 {
			continue
		}
		cores := cur.cores
		if cores == 0 {
			cores = 1
		}
		out = append(out, tsPoint{cur.t, round4((cur.cpuMS - prev.cpuMS) / deltaMS * 100 / cores)})
	}
	return out
}

func counterRatePoints(points []tsPoint) []tsPoint {
	out := make([]tsPoint, 0, len(points))
	for i := 1; i < len(points); i++ {
		deltaS := points[i].t.Sub(points[i-1].t).Seconds()
		if deltaS <= 0 {
			continue
		}
		out = append(out, tsPoint{points[i].t, round4((points[i].v - points[i-1].v) / deltaS)})
	}
	return out
}

// alignSeries places every series on a shared grid of width period, carrying
// each series' latest value forward for at most two periods. Missing cells are NaN.
func alignSeries(series [][]tsPoint, period time.Duration) ([]time.Time, [][]float64) {
	var minT, maxT time.Time
	for _, s := range series {
		for _, p := range s {
			if minT.IsZero() || p.t.Before(minT) {
				minT = p.t
			}
			if p.t.After(maxT) {
				maxT = p.t
			}
		}
	}
	if minT.IsZero() {
		return nil, nil
	}
	var times []time.Time
	var vals [][]float64
	idx := make([]int, len(series))
	for b := minT.Truncate(period); !b.After(maxT); b = b.Add(period) {
		end := b.Add(period)
		row := make([]float64, len(series))
		for i, s := range series {
			for idx[i] < len(s) && s[idx[i]].t.Before(end) {
				idx[i]++
			}
			row[i] = math.NaN()
			if idx[i] > 0 {
				p := s[idx[i]-1]
				if end.Sub(p.t) <= 2*period {
					row[i] = p.v
				}
			}
		}
		times = append(times, b)
		vals = append(vals, row)
	}
	return times, vals
}

func splitColumns(vals [][]float64, n int) (left, right [][]float64) {
	left = make([][]float64, len(vals))
	right = make([][]float64, len(vals))
	for i, row := range vals {
		left[i] = row[:n]
		right[i] = row[n:]
	}
	return left, right
}

func sumRows(vals [][]float64) []float64 {
	out := make([]float64, len(vals))
	for i, row := range vals {
		total, any := 0.0, false
		for _, v := range row {
			if !math.IsNaN(v) {
				total += v
				any = true
			}
		}
		out[i] = math.NaN()
		if any {
			out[i] = round4(total)
		}
	}
	return out
}

func summarizeProcess(p *procSeries, cpu []tsPoint) harness.ProcessSummary {
	s := harness.ProcessSummary{ID: p.id, Binary: p.binary, Samples: len(p.samples)}
	for _, x := range p.samples {
		s.MemAvg += x.mem
		if x.mem > s.MemMax {
			s.MemMax = x.mem
		}
		s.MemLast = x.mem
	}
	if len(p.samples) > 0 {
		s.MemAvg = round4(s.MemAvg / float64(len(p.samples)))
	}
	s.CPUAvg, s.CPUMax = avgMax(cpu)
	return s
}

func summarizeTotal(times []time.Time, mem, cpu []float64) harness.ProcessSummary {
	s := harness.ProcessSummary{ID: "total"}
	memPts := make([]tsPoint, 0, len(mem))
	cpuPts := make([]tsPoint, 0, len(cpu))
	for i := range times {
		if !math.IsNaN(mem[i]) {
			memPts = append(memPts, tsPoint{times[i], mem[i]})
		}
		if !math.IsNaN(cpu[i]) {
			cpuPts = append(cpuPts, tsPoint{times[i], cpu[i]})
		}
	}
	s.Samples = len(memPts)
	s.MemAvg, s.MemMax = avgMax(memPts)
	if len(memPts) > 0 {
		s.MemLast = memPts[len(memPts)-1].v
	}
	s.CPUAvg, s.CPUMax = avgMax(cpuPts)
	return s
}

// summarizeAllAgents averages each agent's total memory/cpu footprint (its
// "total" ProcessSummary row) across all agents.
func summarizeAllAgents(agents []harness.AgentSummary) *harness.AllAgentsSummary {
	var memSum, cpuSum float64
	var n int
	for _, a := range agents {
		for _, p := range a.Processes {
			if p.ID == "total" {
				memSum += p.MemAvg
				cpuSum += p.CPUAvg
				n++
				break
			}
		}
	}
	if n == 0 {
		return nil
	}
	return &harness.AllAgentsSummary{
		Count:  n,
		MemAvg: round4(memSum / float64(n)),
		CPUAvg: round4(cpuSum / float64(n)),
	}
}

func avgMax(points []tsPoint) (avg, max float64) {
	if len(points) == 0 {
		return 0, 0
	}
	for _, p := range points {
		avg += p.v
		if p.v > max {
			max = p.v
		}
	}
	return round4(avg / float64(len(points))), max
}

func writeProcessTSV(path string, p *procSeries, cpu []tsPoint) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create process tsv: %w", err)
	}
	defer f.Close() //nolint:errcheck
	fmt.Fprintln(f, "timestamp\tmem_size\tcpu_ms\tcpu_cores\tcpu_pct")
	cpuAt := make(map[time.Time]float64, len(cpu))
	for _, c := range cpu {
		cpuAt[c.t] = c.v
	}
	for _, s := range p.samples {
		pct := ""
		if v, ok := cpuAt[s.t]; ok {
			pct = fmtNum(v)
		}
		fmt.Fprintf(f, "%s\t%s\t%s\t%s\t%s\n", s.t.UTC().Format(time.RFC3339Nano), fmtNum(s.mem), fmtNum(s.cpuMS), fmtNum(s.cores), pct)
	}
	fmt.Fprintf(os.Stderr, "[metrics] wrote %d rows to %s\n", len(p.samples), path)
	return nil
}

func writeAlignedTSV(path string, header []string, times []time.Time, rows [][]float64) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create tsv: %w", err)
	}
	defer f.Close() //nolint:errcheck
	fmt.Fprintln(f, strings.Join(header, "\t"))
	for i, t := range times {
		cells := make([]string, 0, len(rows[i])+1)
		cells = append(cells, t.UTC().Format(time.RFC3339))
		for _, v := range rows[i] {
			cells = append(cells, fmtNum(v))
		}
		fmt.Fprintln(f, strings.Join(cells, "\t"))
	}
	fmt.Fprintf(os.Stderr, "[metrics] wrote %d rows to %s\n", len(times), path)
	return nil
}

func logDiscovery(agentID string, procs []*procSeries, stats []*statsSeries, rates []*rateSeries) {
	names := make([]string, len(procs))
	for i, p := range procs {
		names[i] = fmt.Sprintf("%s (%s, %d docs)", p.id, p.binary, len(p.samples))
	}
	fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] agent %s processes: %s\n", agentID, strings.Join(names, ", "))
	cols := make([]string, len(rates))
	for i, r := range rates {
		cols[i] = eventColumn(r.dataset, r.id)
		if r.members > 1 {
			cols[i] += fmt.Sprintf(" (%d series)", r.members)
		}
	}
	fmt.Fprintf(os.Stderr, "[elastic-agent-metrics] agent %s pipeline components (%d raw series): %s\n", agentID, len(stats), strings.Join(cols, ", "))
}

func eventColumn(dataset, id string) string {
	if dataset == "" {
		return componentSlug(id)
	}
	return strings.TrimPrefix(dataset, datasetPrefix) + ":" + componentSlug(id)
}

func prefixed(prefix string, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = prefix + n
	}
	return out
}

func docTime(doc map[string]interface{}) (time.Time, bool) {
	v, ok := elastic.GetFieldValue(doc, "@timestamp")
	if !ok {
		return time.Time{}, false
	}
	s, _ := v.(string)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func numField(doc map[string]interface{}, path string) (float64, bool) {
	v, ok := elastic.GetFieldValue(doc, path)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

func stringField(doc map[string]interface{}, path string) string {
	v, ok := elastic.GetFieldValue(doc, path)
	if !ok {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case []interface{}:
		if len(s) > 0 {
			str, _ := s[0].(string)
			return str
		}
	}
	return ""
}

func round4(v float64) float64 {
	return math.Round(v*1e4) / 1e4
}

func fmtNum(v float64) string {
	if math.IsNaN(v) {
		return ""
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// componentSlug turns a component.id into a safe filename segment.
func componentSlug(compID string) string {
	r := strings.NewReplacer("/", "-", ".", "-", "*", "", ":", "-")
	return r.Replace(compID)
}

// mergeStringMaps returns a new map combining base and extra; extra wins on conflict.
func mergeStringMaps(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
