package harness

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

type BenchmarkResult struct {
	Name         string                `json:"name"`
	Error        string                `json:"error,omitempty"`
	Variables    map[string]string     `json:"variables,omitempty"`
	Stack        *StackVersions        `json:"stack_versions,omitempty"`
	Phases       []PhaseResult         `json:"phases"`
	LogIngestion LogIngestionResult    `json:"log_ingestion"`
	Metrics      []MetricResult        `json:"metrics,omitempty"`
	AgentMetrics []AgentMetricsSummary `json:"agent_metrics,omitempty"`
	EsMetrics    []EsMetricsSummary    `json:"elasticsearch_metrics,omitempty"`

	FilebeatMetrics []FilebeatMetricsSummary `json:"filebeat_metrics,omitempty"`
}

// FilebeatMetricsSummary is the structured output of one filebeat-metrics
// step, stored in state as JSON under steps.<id>.summary. Events is the
// cumulative acked count at the last kept sample.
type FilebeatMetricsSummary struct {
	Samples         int     `json:"samples"`
	WindowSec       float64 `json:"window_sec"`
	Events          int64   `json:"events"`
	EventsPerSecAvg float64 `json:"events_per_sec_avg"`
	EventsPerSecMax float64 `json:"events_per_sec_max"`
	CPUAvg          float64 `json:"cpu_avg_pct"`
	CPUMax          float64 `json:"cpu_max_pct"`
	RSSAvg          float64 `json:"rss_avg_bytes"`
	RSSMax          float64 `json:"rss_max_bytes"`
}

func (s FilebeatMetricsSummary) print(w io.Writer) {
	fmt.Fprintln(w, "\nfilebeat:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  samples\tevents\tevents/s avg\tevents/s max\tcpu avg\tcpu max\trss avg\trss max")
	fmt.Fprintf(tw, "  %d\t%d\t%.1f\t%.1f\t%.1f%%\t%.1f%%\t%s\t%s\n",
		s.Samples, s.Events, s.EventsPerSecAvg, s.EventsPerSecMax, s.CPUAvg, s.CPUMax, formatBytes(s.RSSAvg), formatBytes(s.RSSMax))
	tw.Flush() //nolint:errcheck
}

// StackVersions is the structured output of one stack-versions step, stored
// in state as JSON under steps.<id>.summary.
type StackVersions struct {
	Elasticsearch ComponentVersion `json:"elasticsearch"`
	Kibana        ComponentVersion `json:"kibana"`
	Agents        []AgentVersion   `json:"elastic_agent,omitempty"`
}

type ComponentVersion struct {
	Version   string `json:"version"`
	BuildHash string `json:"build_hash,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
}

// AgentVersion groups the Fleet-enrolled agents that report one version.
type AgentVersion struct {
	Version string   `json:"version"`
	Build   string   `json:"build,omitempty"`
	Count   int      `json:"count"`
	Hosts   []string `json:"hosts"`
}

// EsMetricsSummary is the structured output of one elasticsearch-metrics
// step, stored in state as JSON under steps.<id>.summary.
type EsMetricsSummary struct {
	WindowSec   float64             `json:"window_sec"`
	Nodes       []EsNodeSummary     `json:"nodes"`
	DataStreams []DataStreamSummary `json:"data_streams"`
	TotalDocs   int64               `json:"total_docs"`
	TotalPerSec float64             `json:"total_per_sec"`

	// BulkPoolObserved is true when any node reported the legacy
	// thread_pool.bulk stats, retired in Elasticsearch 9.6.0 in favor of
	// thread_pool.write.
	BulkPoolObserved bool `json:"bulk_pool_observed,omitempty"`
	// IndexingPressureObserved is true when any node reported
	// indexing_pressure memory stats.
	IndexingPressureObserved bool `json:"indexing_pressure_observed,omitempty"`
}

type EsNodeSummary struct {
	Name        string  `json:"name"`
	Samples     int     `json:"samples"`
	IndexedDocs int64   `json:"indexed_docs"`
	IndexingAvg float64 `json:"indexing_avg_per_sec"`
	IndexingMax float64 `json:"indexing_max_per_sec"`
	CPUAvg      float64 `json:"cpu_avg_pct"`
	CPUMax      float64 `json:"cpu_max_pct"`
	HeapAvg     float64 `json:"heap_avg_bytes"`
	HeapMax     float64 `json:"heap_max_bytes"`

	// IndexMsPerDocAvg is the average per-document indexing cost
	// (index_time delta ÷ index_total delta) across sample intervals.
	IndexMsPerDocAvg float64 `json:"index_ms_per_doc_avg"`

	WriteActiveAvg float64 `json:"write_active_avg"`
	WriteActiveMax float64 `json:"write_active_max"`
	WriteQueueAvg  float64 `json:"write_queue_avg"`
	WriteQueueMax  float64 `json:"write_queue_max"`
	// WriteRejected{Avg,Max} are the average/max rejected-requests-per-second
	// rate across sample intervals (thread_pool.write.rejected is a
	// cumulative counter, so it's reported as a rate like indexing_rate).
	WriteRejectedAvg float64 `json:"write_rejected_avg"`
	WriteRejectedMax float64 `json:"write_rejected_max"`

	// BulkPool* report the legacy thread_pool.bulk stats; zero/omitted
	// unless EsMetricsSummary.BulkPoolObserved is true.
	BulkPoolQueueAvg float64 `json:"bulk_pool_queue_avg,omitempty"`
	BulkPoolQueueMax float64 `json:"bulk_pool_queue_max,omitempty"`
	BulkPoolRejected int64   `json:"bulk_pool_rejected,omitempty"`

	BulkAvgSizeBytesAvg float64 `json:"bulk_avg_size_bytes_avg"`
	BulkOpsPerSecAvg    float64 `json:"bulk_ops_per_sec_avg"`
	BulkOpsPerSecMax    float64 `json:"bulk_ops_per_sec_max"`

	MergesCount  int64 `json:"merges_count"`
	MergesTimeMs int64 `json:"merges_time_ms"`

	SegmentsAvg float64 `json:"segments_avg"`
	SegmentsMax float64 `json:"segments_max"`

	// IndexingPressure* are zero/omitted unless
	// EsMetricsSummary.IndexingPressureObserved is true.
	IndexingPressureBytesAvg float64 `json:"indexing_pressure_bytes_avg,omitempty"`
	IndexingPressureRejected int64   `json:"indexing_pressure_rejected,omitempty"`
}

type DataStreamSummary struct {
	Name   string  `json:"name"`
	Docs   int64   `json:"docs"`
	PerSec float64 `json:"per_sec"`
}

// AgentMetricsSummary is the structured output of one elastic-agent-metrics
// step, stored in state as JSON under steps.<id>.summary.
type AgentMetricsSummary struct {
	Agents []AgentSummary  `json:"agents"`
	Ingest []IngestSummary `json:"ingest,omitempty"`
	// AllAgents averages each agent's total memory/cpu footprint across all
	// agents; only populated when there is more than one agent.
	AllAgents *AllAgentsSummary `json:"all_agents,omitempty"`
}

type AgentSummary struct {
	ID           string               `json:"id"`
	Label        string               `json:"label"`
	Processes    []ProcessSummary     `json:"processes"`
	Events       []EventsSummary      `json:"events,omitempty"`
	Integrations []IntegrationSummary `json:"integrations,omitempty"`
	ErrorLogs    int64                `json:"error_logs"`
	DashboardPNG string               `json:"dashboard_png,omitempty"`
	DashboardErr string               `json:"dashboard_error,omitempty"`
}

// AllAgentsSummary is the mean, across all agents in a run, of each agent's
// own total memory/cpu average (the "total" ProcessSummary row).
type AllAgentsSummary struct {
	Count  int     `json:"count"`
	MemAvg float64 `json:"mem_avg"`
	CPUAvg float64 `json:"cpu_avg"`
}

type ProcessSummary struct {
	ID      string  `json:"id"`
	Binary  string  `json:"binary,omitempty"`
	Samples int     `json:"samples"`
	MemAvg  float64 `json:"mem_avg"`
	MemMax  float64 `json:"mem_max"`
	MemLast float64 `json:"mem_last"`
	CPUAvg  float64 `json:"cpu_avg"`
	CPUMax  float64 `json:"cpu_max"`
}

type EventsSummary struct {
	Dataset   string  `json:"dataset"`
	ID        string  `json:"id"`
	Field     string  `json:"field"`
	Members   int     `json:"members,omitempty"`
	AvgPerSec float64 `json:"avg_per_sec"`
	MaxPerSec float64 `json:"max_per_sec"`
}

type IntegrationSummary struct {
	ID              string  `json:"id"`
	Mode            string  `json:"mode"`
	EventsPerSecAvg float64 `json:"events_per_sec_avg"`
	ErrorLogs       int64   `json:"error_logs"`
}

type IngestSummary struct {
	Name      string  `json:"name"`
	Total     int64   `json:"total"`
	AvgPerSec float64 `json:"avg_per_sec"`
	MaxPerSec float64 `json:"max_per_sec"`
}

type PhaseResult struct {
	Name        string  `json:"name"`
	DurationSec float64 `json:"duration_sec"`
}

type LogIngestionResult struct {
	Expected   int     `json:"expected"`
	Actual     int64   `json:"actual"`
	ElapsedSec float64 `json:"elapsed_sec"`
}

type MetricResult struct {
	Name string     `json:"name"`
	Aggs []AggValue `json:"aggs"`
}

type AggValue struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

func (r *BenchmarkResult) Print(w io.Writer) {
	fmt.Fprintf(w, "benchmark: %s\n", r.Name) //nolint:errcheck
	if r.Error != "" {
		fmt.Fprintf(w, "error: %s\n", r.Error)
	}
	if len(r.Variables) > 0 {
		fmt.Fprintln(w, "\nvariables:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		names := make([]string, 0, len(r.Variables))
		for k := range r.Variables {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fmt.Fprintf(tw, "  %s\t%s\n", k, r.Variables[k])
		}
		tw.Flush() //nolint:errcheck
	}
	if r.Stack != nil {
		r.Stack.print(w)
	}

	if len(r.Phases) > 0 {
		fmt.Fprintln(w, "\nphases:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, p := range r.Phases {
			fmt.Fprintf(tw, "  %s\t%.3fs\n", p.Name, p.DurationSec)
		}
		tw.Flush() //nolint:errcheck
	}

	if r.LogIngestion.Expected > 0 || r.LogIngestion.Actual > 0 {
		fmt.Fprintln(w, "\nlog ingestion:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintf(tw, "  expected\t%d\n", r.LogIngestion.Expected)
		fmt.Fprintf(tw, "  actual\t%d\n", r.LogIngestion.Actual)
		fmt.Fprintf(tw, "  elapsed\t%.3fs\n", r.LogIngestion.ElapsedSec)
		tw.Flush() //nolint:errcheck
	}

	for _, m := range r.Metrics {
		fmt.Fprintf(w, "\n%s:\n", m.Name)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, av := range m.Aggs {
			fmt.Fprintf(tw, "  %s\t%.6g\n", av.Name, av.Value)
		}
		tw.Flush() //nolint:errcheck
	}

	for _, am := range r.AgentMetrics {
		am.print(w)
	}
	for _, em := range r.EsMetrics {
		em.print(w)
	}
	for _, fm := range r.FilebeatMetrics {
		fm.print(w)
	}
}

func (s StackVersions) print(w io.Writer) {
	fmt.Fprintln(w, "\nversions:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  elasticsearch\t%s\t%s\n", s.Elasticsearch.Version, s.Elasticsearch.build())
	if s.Kibana.Version != "" {
		fmt.Fprintf(tw, "  kibana\t%s\t%s\n", s.Kibana.Version, s.Kibana.build())
	}
	for _, a := range s.Agents {
		noun := "agents"
		if a.Count == 1 {
			noun = "agent"
		}
		fmt.Fprintf(tw, "  elastic-agent\t%s\t%d %s: %s\n", a.Version, a.Count, noun, strings.Join(a.Hosts, ", "))
	}
	tw.Flush() //nolint:errcheck
}

func (c ComponentVersion) build() string {
	if c.BuildHash == "" {
		return ""
	}
	hash := c.BuildHash
	if len(hash) > 8 {
		hash = hash[:8]
	}
	if c.BuildDate == "" {
		return "build " + hash
	}
	return "build " + hash + " (" + c.BuildDate + ")"
}

func (s EsMetricsSummary) print(w io.Writer) {
	if len(s.Nodes) > 0 {
		fmt.Fprintln(w, "\nelasticsearch nodes:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  node\tsamples\tindexed docs\tindexing avg\tindexing max\tms/doc\tcpu avg\tcpu max\theap avg\theap max")
		for _, n := range s.Nodes {
			fmt.Fprintf(tw, "  %s\t%d\t%d\t%.1f/s\t%.1f/s\t%.2f\t%.1f%%\t%.1f%%\t%s\t%s\n",
				n.Name, n.Samples, n.IndexedDocs, n.IndexingAvg, n.IndexingMax, n.IndexMsPerDocAvg, n.CPUAvg, n.CPUMax, formatBytes(n.HeapAvg), formatBytes(n.HeapMax))
		}
		tw.Flush() //nolint:errcheck

		fmt.Fprintln(w, "\nelasticsearch write path:")
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		header := "  node\twrite active avg\twrite active max\twrite queue avg\twrite queue max\twrite rejected avg\twrite rejected max"
		if s.BulkPoolObserved {
			header += "\tbulk pool queue avg\tbulk pool queue max\tbulk pool rejected"
		}
		fmt.Fprintln(tw, header)
		for _, n := range s.Nodes {
			row := fmt.Sprintf("  %s\t%.1f\t%.1f\t%.1f\t%.1f\t%.2f\t%.2f",
				n.Name, n.WriteActiveAvg, n.WriteActiveMax, n.WriteQueueAvg, n.WriteQueueMax, n.WriteRejectedAvg, n.WriteRejectedMax)
			if s.BulkPoolObserved {
				row += fmt.Sprintf("\t%.1f\t%.1f\t%d", n.BulkPoolQueueAvg, n.BulkPoolQueueMax, n.BulkPoolRejected)
			}
			fmt.Fprintln(tw, row)
		}
		tw.Flush() //nolint:errcheck

		fmt.Fprintln(w, "\nelasticsearch bulk requests:")
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  node\tavg size\tops/sec avg\tops/sec max")
		for _, n := range s.Nodes {
			fmt.Fprintf(tw, "  %s\t%s\t%.1f\t%.1f\n", n.Name, formatBytes(n.BulkAvgSizeBytesAvg), n.BulkOpsPerSecAvg, n.BulkOpsPerSecMax)
		}
		tw.Flush() //nolint:errcheck

		if s.IndexingPressureObserved {
			fmt.Fprintln(w, "\nelasticsearch indexing pressure:")
			tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "  node\tmemory avg\trejected")
			for _, n := range s.Nodes {
				fmt.Fprintf(tw, "  %s\t%s\t%d\n", n.Name, formatBytes(n.IndexingPressureBytesAvg), n.IndexingPressureRejected)
			}
			tw.Flush() //nolint:errcheck
		}
	}
	if len(s.DataStreams) > 0 {
		fmt.Fprintf(w, "\ndata streams (%.0fs window):\n", s.WindowSec)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  data stream\tdocs\tevents/s")
		for _, d := range s.DataStreams {
			fmt.Fprintf(tw, "  %s\t%d\t%.2f\n", d.Name, d.Docs, d.PerSec)
		}
		fmt.Fprintf(tw, "  total\t%d\t%.2f\n", s.TotalDocs, s.TotalPerSec)
		tw.Flush() //nolint:errcheck
	}
}

func (s AgentMetricsSummary) print(w io.Writer) {
	for _, a := range s.Agents {
		fmt.Fprintf(w, "\nagent %s processes:\n", a.ID)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  component\tbinary\tmem avg\tmem max\tmem last\tcpu avg\tcpu max")
		for _, p := range a.Processes {
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%.2f%%\t%.2f%%\n",
				p.ID, p.Binary, formatBytes(p.MemAvg), formatBytes(p.MemMax), formatBytes(p.MemLast), p.CPUAvg, p.CPUMax)
		}
		tw.Flush() //nolint:errcheck
		if a.DashboardPNG != "" {
			fmt.Fprintf(w, "  dashboard: %s\n", a.DashboardPNG)
		}
		if a.DashboardErr != "" {
			fmt.Fprintf(w, "  dashboard: failed: %s\n", a.DashboardErr)
		}

		if len(a.Integrations) > 0 {
			fmt.Fprintf(w, "\nagent %s integrations:\n", a.ID)
			tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "  integration\tmode\tevents/s avg\terror logs")
			for _, in := range a.Integrations {
				fmt.Fprintf(tw, "  %s\t%s\t%.1f\t%d\n", in.ID, in.Mode, in.EventsPerSecAvg, in.ErrorLogs)
			}
			tw.Flush() //nolint:errcheck
		}

		if len(a.Events) > 0 {
			fmt.Fprintf(w, "\nagent %s pipeline events/s:\n", a.ID)
			tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "  dataset\tcomponent\tseries\tavg\tmax")
			for _, e := range a.Events {
				fmt.Fprintf(tw, "  %s\t%s\t%d\t%.1f\t%.1f\n", e.Dataset, e.ID, e.Members, e.AvgPerSec, e.MaxPerSec)
			}
			tw.Flush() //nolint:errcheck
		}
		fmt.Fprintf(w, "  error logs total: %d\n", a.ErrorLogs)
	}

	if s.AllAgents != nil {
		fmt.Fprintln(w, "\nall agents:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  agents\tmem avg\tcpu avg")
		fmt.Fprintf(tw, "  %d\t%s\t%.2f%%\n", s.AllAgents.Count, formatBytes(s.AllAgents.MemAvg), s.AllAgents.CPUAvg)
		tw.Flush() //nolint:errcheck
	}

	if len(s.Ingest) > 0 {
		fmt.Fprintln(w, "\ningest:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  name\ttotal docs\tavg/s\tmax/s")
		for _, in := range s.Ingest {
			fmt.Fprintf(tw, "  %s\t%d\t%.1f\t%.1f\n", in.Name, in.Total, in.AvgPerSec, in.MaxPerSec)
		}
		tw.Flush() //nolint:errcheck
	}
}

func formatBytes(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func (r *BenchmarkResult) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// BuildResult extracts benchmark measurements from state after a run.
func (cfg *Config) BuildResult(state *State) BenchmarkResult {
	res := BenchmarkResult{Name: cfg.Benchmark.Name}
	for name, def := range cfg.Variables {
		if res.Variables == nil {
			res.Variables = map[string]string{}
		}
		res.Variables[name] = def
		if v, err := state.Resolve("${" + name + "}"); err == nil {
			res.Variables[name] = v
		}
	}

	for _, name := range phaseNames {
		dur, err := state.Resolve("${phase." + name + ".duration}")
		if err != nil || dur == "" {
			continue
		}
		d, err := time.ParseDuration(dur)
		if err != nil {
			continue
		}
		res.Phases = append(res.Phases, PhaseResult{
			Name:        name,
			DurationSec: d.Seconds(),
		})
	}

	for _, step := range cfg.Phases.AllSteps() {
		if step.EsDocCount != nil && step.ID != "" {
			v, _ := state.Resolve("${steps." + step.ID + ".actualCount}")
			if v != "" {
				count, _ := strconv.ParseInt(v, 10, 64)
				res.LogIngestion.Actual = count
				res.LogIngestion.Expected = int(step.EsDocCount.Expected)

				sinceStr, _ := state.Resolve(step.EsDocCount.Since)
				confirmedStr, _ := state.Resolve("${steps." + step.ID + ".confirmedAt}")
				if sinceStr != "" && confirmedStr != "" {
					since, serr := time.Parse(time.RFC3339, sinceStr)
					confirmed, cerr := time.Parse(time.RFC3339, confirmedStr)
					if serr == nil && cerr == nil && confirmed.After(since) {
						res.LogIngestion.ElapsedSec = confirmed.Sub(since).Seconds()
					}
				}
			}
		}

		if step.EsAgentMetrics != nil && step.ID != "" {
			if v, _ := state.Resolve("${steps." + step.ID + ".summary}"); v != "" {
				var am AgentMetricsSummary
				if err := json.Unmarshal([]byte(v), &am); err == nil {
					res.AgentMetrics = append(res.AgentMetrics, am)
				}
			}
		}

		if step.EsMetrics != nil && step.ID != "" {
			if v, _ := state.Resolve("${steps." + step.ID + ".summary}"); v != "" {
				var em EsMetricsSummary
				if err := json.Unmarshal([]byte(v), &em); err == nil {
					res.EsMetrics = append(res.EsMetrics, em)
				}
			}
		}

		if step.FilebeatMetrics != nil && step.ID != "" {
			if v, _ := state.Resolve("${steps." + step.ID + ".summary}"); v != "" {
				var fm FilebeatMetricsSummary
				if err := json.Unmarshal([]byte(v), &fm); err == nil {
					res.FilebeatMetrics = append(res.FilebeatMetrics, fm)
				}
			}
		}

		if step.StackVersions != nil && step.ID != "" {
			if v, _ := state.Resolve("${steps." + step.ID + ".summary}"); v != "" {
				var sv StackVersions
				if err := json.Unmarshal([]byte(v), &sv); err == nil {
					res.Stack = &sv
				}
			}
		}

		if step.EsAgg != nil && step.ID != "" {
			for _, m := range step.EsAgg.Metrics {
				mr := MetricResult{Name: m.Name}
				for _, agg := range m.Aggs {
					stateKey := m.Name + "_" + agg
					v, err := state.Resolve("${steps." + step.ID + "." + stateKey + "}")
					if err == nil && v != "" {
						f, _ := strconv.ParseFloat(v, 64)
						mr.Aggs = append(mr.Aggs, AggValue{Name: agg, Value: f})
					}
				}
				if len(mr.Aggs) > 0 {
					res.Metrics = append(res.Metrics, mr)
				}
			}
		}
	}

	return res
}
