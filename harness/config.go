package harness

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/orca/elastic"
	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration for YAML unmarshaling from strings like "5m", "30s".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

// VarInt is an int field that also accepts ${...} references to declared
// variables or environment variables, resolved at config-load time, e.g.
// `expected_groups: ${TF_VAR_agent_count}`. It also accepts a `*`-separated
// product of such references, e.g.
// `expected: ${TF_VAR_agent_count}*${FLOG_FILE_COUNT}*${FLOG_LINES_PER_FILE}`.
type VarInt int

func (v *VarInt) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	resolved, err := NewState().Resolve(s)
	if err != nil {
		return err
	}
	n, err := parseVarIntExpr(resolved)
	if err != nil {
		return fmt.Errorf("invalid int %q: %w", resolved, err)
	}
	*v = VarInt(n)
	return nil
}

// parseVarIntExpr parses s as a plain integer, or, failing that, as a
// `*`-separated product of integers (e.g. "2*5*1000").
func parseVarIntExpr(s string) (int, error) {
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return n, nil
	}
	terms := strings.Split(s, "*")
	if len(terms) < 2 {
		return 0, fmt.Errorf("not an integer or product of integers")
	}
	product := 1
	for _, t := range terms {
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err != nil {
			return 0, fmt.Errorf("term %q: %w", t, err)
		}
		product *= n
	}
	return product, nil
}

type Config struct {
	Benchmark     BenchmarkMeta     `yaml:"benchmark"`
	Variables     map[string]string `yaml:"variables"`
	Elasticsearch ESConfig          `yaml:"elasticsearch"`
	Kibana        KibanaConfig      `yaml:"kibana"`
	Phases        PhasesConfig      `yaml:"phases"`
}

type BenchmarkMeta struct {
	Name string `yaml:"name"`
}

type ESConfig struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	CAPath   string `yaml:"ca_path"`
}

// KibanaConfig holds connection settings for the Kibana Fleet API.
type KibanaConfig struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	CAPath   string `yaml:"ca_path"`
}

type PhasesConfig struct {
	Setup     []Step `yaml:"setup"`
	Configure []Step `yaml:"configure"`
	Seed      []Step `yaml:"seed"`
	Warmup    []Step `yaml:"warmup"`
	Execute   []Step `yaml:"execute"`
	Collect   []Step `yaml:"collect"`
	Report    []Step `yaml:"report"`
	Teardown  []Step `yaml:"teardown"`
}

// Phase groups executed by the up, bench, and down lifecycle commands.
var (
	upPhases    = []string{"setup", "configure", "seed"}
	benchPhases = []string{"warmup", "execute", "collect", "report"}
	downPhases  = []string{"teardown"}
)

type phase struct {
	name  string
	steps []Step
}

// ordered returns all phases in execution order. It is the single source of
// truth for phase names and their order.
func (p *PhasesConfig) ordered() []phase {
	return []phase{
		{"setup", p.Setup},
		{"configure", p.Configure},
		{"seed", p.Seed},
		{"warmup", p.Warmup},
		{"execute", p.Execute},
		{"collect", p.Collect},
		{"report", p.Report},
		{"teardown", p.Teardown},
	}
}

func (p *PhasesConfig) pick(names []string) []phase {
	var out []phase
	for _, ph := range p.ordered() {
		for _, n := range names {
			if ph.name == n {
				out = append(out, ph)
				break
			}
		}
	}
	return out
}

func (p *PhasesConfig) all() [][]Step {
	phases := p.ordered()
	out := make([][]Step, len(phases))
	for i, ph := range phases {
		out[i] = ph.steps
	}
	return out
}

// AllSteps returns every step across all phases in execution order.
func (p *PhasesConfig) AllSteps() []Step {
	var out []Step
	for _, ph := range p.ordered() {
		out = append(out, ph.steps...)
	}
	return out
}

var phaseNames = func() []string {
	phases := (&PhasesConfig{}).ordered()
	names := make([]string, len(phases))
	for i, ph := range phases {
		names[i] = ph.name
	}
	return names
}()

// Step is a single benchmark step. Exactly one step-type field must be non-nil.
type Step struct {
	ID              string               `yaml:"id"`
	Optional        bool                 `yaml:"optional"`
	Noop            *NoopStep            `yaml:"noop"`
	Wait            *WaitStep            `yaml:"wait"`
	Kind            *KindStep            `yaml:"kind"`
	KindDelete      *KindDeleteStep      `yaml:"kind-delete"`
	KindLoad        *KindLoadStep        `yaml:"kind-load"`
	Kubectl         *KubectlStep         `yaml:"kubectl"`
	WaitPods        *WaitPodsStep        `yaml:"wait-pods"`
	WatchJob        *WatchJobStep        `yaml:"watch-job"`
	WriteIni        *WriteIniStep        `yaml:"write-ini"`
	EsDocCount      *EsDocCountStep      `yaml:"es-doc-count"`
	EsLogSpan       *EsLogSpanStep       `yaml:"es-log-span"`
	EsAgg           *EsAggStep           `yaml:"es-agg"`
	EsTsv           *EsTsvStep           `yaml:"es-tsv"`
	EsAgentMetrics  *EsAgentMetricsStep  `yaml:"elastic-agent-metrics"`
	EsMetrics       *EsMetricsStep       `yaml:"elasticsearch-metrics"`
	FilebeatWait    *FilebeatWaitStep    `yaml:"filebeat-wait"`
	FilebeatMetrics *FilebeatMetricsStep `yaml:"filebeat-metrics"`
	StackVersions   *StackVersionsStep   `yaml:"stack-versions"`
	TsvDerive       *TsvDeriveStep       `yaml:"tsv-derive"`
	Chart           *ChartStep           `yaml:"chart"`
	Shell           *ShellStep           `yaml:"shell"`
	OpenTofu        *OpenTofuStep        `yaml:"opentofu"`
	Ansible         *AnsibleStep         `yaml:"ansible"`
	FleetDiag       *FleetDiagStep       `yaml:"fleet-agent-diagnostics"`
}

func (s *Step) typeName() string {
	switch {
	case s.Noop != nil:
		return "noop"
	case s.Wait != nil:
		return "wait"
	case s.Kind != nil:
		return "kind"
	case s.KindDelete != nil:
		return "kind-delete"
	case s.KindLoad != nil:
		return "kind-load"
	case s.Kubectl != nil:
		return "kubectl"
	case s.WaitPods != nil:
		return "wait-pods"
	case s.WatchJob != nil:
		return "watch-job"
	case s.WriteIni != nil:
		return "write-ini"
	case s.EsDocCount != nil:
		return "es-doc-count"
	case s.EsLogSpan != nil:
		return "es-log-span"
	case s.EsAgg != nil:
		return "es-agg"
	case s.EsTsv != nil:
		return "es-tsv"
	case s.EsAgentMetrics != nil:
		return "elastic-agent-metrics"
	case s.EsMetrics != nil:
		return "elasticsearch-metrics"
	case s.FilebeatWait != nil:
		return "filebeat-wait"
	case s.FilebeatMetrics != nil:
		return "filebeat-metrics"
	case s.StackVersions != nil:
		return "stack-versions"
	case s.TsvDerive != nil:
		return "tsv-derive"
	case s.Chart != nil:
		return "chart"
	case s.Shell != nil:
		return "shell"
	case s.OpenTofu != nil:
		return "opentofu"
	case s.Ansible != nil:
		return "ansible"
	case s.FleetDiag != nil:
		return "fleet-agent-diagnostics"
	default:
		return ""
	}
}

// OutputSpec describes how a step output value is captured.
type OutputSpec struct {
	From string `yaml:"from"` // "file"
	Path string `yaml:"path"`
}

type NoopStep struct {
	State map[string]string `yaml:"state"`
}

// WaitStep pauses for a fixed wall-clock duration.
type WaitStep struct {
	Duration Duration `yaml:"duration"`
}

type KindStep struct {
	Name    string                `yaml:"name"`
	Outputs map[string]OutputSpec `yaml:"outputs"`
}

type KindDeleteStep struct {
	Name string `yaml:"name"`
}

type KindLoadStep struct {
	Name  string `yaml:"name"`  // kind cluster name
	Image string `yaml:"image"` // path to image archive file
}

type KubectlStep struct {
	Kubeconfig string `yaml:"kubeconfig"`
	Run        string `yaml:"run"` // kubectl subcommand and args, e.g. "apply -f ./agent.yaml"
}

type WaitPodsStep struct {
	Kubeconfig string   `yaml:"kubeconfig"`
	Namespace  string   `yaml:"namespace"`
	Selector   string   `yaml:"selector"`
	Timeout    Duration `yaml:"timeout"`
}

type WatchJobStep struct {
	Kubeconfig string `yaml:"kubeconfig"`
	Namespace  string `yaml:"namespace"`
	Job        string `yaml:"job"`
}

type EsDocCountStep struct {
	Index          string            `yaml:"index"`
	Since          string            `yaml:"since"`
	Filter         map[string]string `yaml:"filter"`
	Expected       VarInt            `yaml:"expected"`
	Timeout        Duration          `yaml:"timeout"`
	GroupBy        string            `yaml:"group_by"`
	ExpectedGroups int               `yaml:"expected_groups"`
}

// EsLogSpanStep polls until each of the expected agent groups has produced
// logs spanning at least MinDuration (max @timestamp - min @timestamp >= MinDuration).
type EsLogSpanStep struct {
	Index          string            `yaml:"index"`
	Since          string            `yaml:"since"`
	Filter         map[string]string `yaml:"filter"`
	GroupBy        string            `yaml:"group_by"`
	MinDuration    Duration          `yaml:"min_duration"`
	ExpectedGroups VarInt            `yaml:"expected_groups"`
	Timeout        Duration          `yaml:"timeout"`
}

// TsvDeriveStep reads a TSV and appends columns computed as the rate of
// change of source columns divided by the elapsed time between rows.
type TsvDeriveStep struct {
	Input   string         `yaml:"input"`
	Output  string         `yaml:"output"`
	Time    string         `yaml:"time"` // column with RFC3339 timestamps
	Columns []DeriveColumn `yaml:"columns"`
}

// DeriveColumn specifies one derived column: (delta_from / delta_ms / divisor) * scale.
// Divisor is an optional column name whose value at each row is used as an
// additional divisor (e.g. cpu_cores to normalise CPU time to % of total capacity).
type DeriveColumn struct {
	Name    string  `yaml:"name"`    // output column name
	From    string  `yaml:"from"`    // source column (cumulative counter)
	Divisor string  `yaml:"divisor"` // optional column to divide by (e.g. cpu_cores)
	Scale   float64 `yaml:"scale"`   // multiplier; default 1.0
}

// ChartStep reads a TSV file and renders an XY line chart image.
// X selects the column used as the X axis; Y lists the series columns
// (empty = every column that is not X). Output format is detected from the
// file extension (.png, .svg, .jpg).
type ChartStep struct {
	Input  string   `yaml:"input"`  // path to TSV file; supports ${...} templates
	Output string   `yaml:"output"` // image output path; supports ${...} templates
	Title  string   `yaml:"title"`
	X      string   `yaml:"x"`      // column name for the X axis
	Y      []string `yaml:"y"`      // Y series column names; empty = all non-X columns
	Width  int      `yaml:"width"`  // default 800
	Height int      `yaml:"height"` // default 400
}

// EsTsvStep fetches raw documents from ES and writes them as a TSV file.
// Each TsvMetric maps a document field to a TSV column. Required fields cause
// a document to be skipped entirely if absent; non-required fields produce an
// empty column (tab delimiter is still written).
type EsTsvStep struct {
	Index   string            `yaml:"index"`
	Since   string            `yaml:"since"`
	Until   string            `yaml:"until"`
	Filter  map[string]string `yaml:"filter"`
	Output  string            `yaml:"output"` // file path for TSV; supports ${...} templates
	Size    int               `yaml:"size"`   // max documents to fetch; default 10000
	Metrics []TsvMetric       `yaml:"metrics"`
}

// TsvMetric describes one column in the TSV output.
type TsvMetric struct {
	Name     string `yaml:"name"`
	Field    string `yaml:"field"`
	Required bool   `yaml:"required"`
}

// EsAgentMetricsStep discovers the elastic-agent processes and pipeline
// components reporting from each agent, classifies the named integrations as
// process or otel mode, and writes per-agent TSV files and charts.
type EsAgentMetricsStep struct {
	Index        string         `yaml:"index"`
	Since        string         `yaml:"since"`
	Until        string         `yaml:"until"`
	OutputDir    string         `yaml:"output_dir"`
	Integrations []string       `yaml:"integrations"`
	Ingest       []IngestSpec   `yaml:"ingest"`
	Agents       []AgentSpec    `yaml:"agents"`
	Dashboard    *DashboardSpec `yaml:"dashboard"`
}

// DashboardSpec requests a PNG of a Kibana dashboard per agent, filtered by
// the agent's filter over the step's time range. ID defaults to the Elastic
// Agent metrics dashboard shipped with the elastic_agent integration.
type DashboardSpec struct {
	ID      string   `yaml:"id"`
	Width   int      `yaml:"width"`   // default 1600
	Height  int      `yaml:"height"`  // default 2400
	Timeout Duration `yaml:"timeout"` // default 10m
}

// FilebeatWaitStep polls Filebeat's legacy stack-monitoring documents until a
// sample from a Filebeat process started after Since reports
// libbeat.output.events.acked >= max(Expected, 1). With Expected unset it
// confirms that metrics arrive and the output is sending. Index defaults to
// .monitoring-beats-*. Outputs: reachedAt (the matching sample's timestamp,
// RFC3339Nano), acked, confirmedAt.
type FilebeatWaitStep struct {
	Index    string            `yaml:"index"`
	Since    string            `yaml:"since"`
	Filter   map[string]string `yaml:"filter"`
	Expected VarInt            `yaml:"expected"`
	Timeout  Duration          `yaml:"timeout"`
}

// FilebeatMetricsStep writes Filebeat CPU, memory, and output event-rate
// TSVs and charts from legacy stack-monitoring documents. Samples at or after
// Until are dropped, so passing the filebeat-wait reachedAt excludes the
// partial final interval and leaves constant-rate samples.
type FilebeatMetricsStep struct {
	Index     string            `yaml:"index"`
	Since     string            `yaml:"since"`
	Until     string            `yaml:"until"`
	OutputDir string            `yaml:"output_dir"`
	Filter    map[string]string `yaml:"filter"`
}

// EsMetricsStep collects Elasticsearch node stats (indexing rate, CPU, heap)
// as TSV files and charts, and counts documents per data stream over the
// time range to derive observed events per second.
type EsMetricsStep struct {
	Index           string            `yaml:"index"` // node stats index; default metrics-*
	Since           string            `yaml:"since"`
	Until           string            `yaml:"until"`
	OutputDir       string            `yaml:"output_dir"`
	Filter          map[string]string `yaml:"filter"`            // node stats filter; default stack_monitoring.node_stats dataset
	DataStreamIndex string            `yaml:"data_stream_index"` // default logs-*,metrics-*,traces-*
}

// StackVersionsStep records the Elasticsearch and Kibana build versions and
// the version of every Fleet-enrolled elastic-agent at the time it runs.
type StackVersionsStep struct{}

// IngestSpec names a destination data stream whose document rate measures
// benchmark throughput independently of agent mode.
type IngestSpec struct {
	Name   string            `yaml:"name"`
	Index  string            `yaml:"index"`
	Filter map[string]string `yaml:"filter"`
}

// AgentSpec identifies one elastic-agent instance for metric collection.
type AgentSpec struct {
	ID     string            `yaml:"id"`
	Label  string            `yaml:"label"`
	Filter map[string]string `yaml:"filter"`
}

// EsAggStep runs a date-range + term-filter aggregation against ES.
// Each entry in Metrics produces one or more step outputs named <name>_<agg>.
type EsAggStep struct {
	Index   string                  `yaml:"index"`
	Since   string                  `yaml:"since"`
	Until   string                  `yaml:"until"`
	Filter  map[string]string       `yaml:"filter"` // ES field → exact value (term queries)
	Metrics []elastic.AggMetricSpec `yaml:"metrics"`
}

// WriteIniStep writes an INI-format file from a list of named sections, each
// containing one or more entry lines. All strings support ${...} templates.
type WriteIniStep struct {
	Output   string       `yaml:"output"`
	Sections []IniSection `yaml:"sections"`
}

// IniSection is one [name] block in the output INI file.
type IniSection struct {
	Name    string   `yaml:"name"`
	Entries []string `yaml:"entries"`
}

type ShellStep struct {
	Run     string   `yaml:"run"`
	Timeout Duration `yaml:"timeout"`
}

type OpenTofuStep struct {
	Dir     string                `yaml:"dir"`
	Destroy bool                  `yaml:"destroy"`
	Vars    map[string]string     `yaml:"vars"`
	Outputs map[string]OutputSpec `yaml:"outputs"`
}

// AnsibleStep runs a playbook. Outputs, when set, names a JSON object file
// written by the playbook whose top-level keys become step outputs.
type AnsibleStep struct {
	Playbook     string            `yaml:"playbook"`
	Inventory    string            `yaml:"inventory"`
	Requirements string            `yaml:"requirements"`
	Vars         map[string]string `yaml:"vars"`
	Outputs      *OutputSpec       `yaml:"outputs"`
}

// FleetDiagStep requests a diagnostics bundle from each Fleet-managed agent
// via the Kibana Fleet API, waits for the uploads to complete, and downloads
// the zip archives into OutputDir.
type FleetDiagStep struct {
	OutputDir    string           `yaml:"output_dir"`
	Timeout      Duration         `yaml:"timeout"`       // default 10m
	PollInterval Duration         `yaml:"poll_interval"` // default 10s
	CPUProfile   bool             `yaml:"cpu_profile"`
	Agents       []FleetDiagAgent `yaml:"agents"`
}

// FleetDiagAgent identifies one agent by Fleet agent ID or, when ID is empty,
// by its enrolled hostname.
type FleetDiagAgent struct {
	Name     string `yaml:"name"`
	ID       string `yaml:"id"`
	Hostname string `yaml:"hostname"`
}

// parseConfig reads and parses a benchmark config file. It exports the
// declared variables (see applyVariables) using orca.env next to the config
// plus envFiles before expanding any !repeat directives (see expandRepeats),
// since their `count` fields may reference those variables; by the time the
// document is decoded into Config, every list is already a concrete,
// fixed-size list like any other step type expects.
func parseConfig(path string, envFiles []string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	root := &doc
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	var pre struct {
		Variables map[string]string `yaml:"variables"`
	}
	if err := root.Decode(&pre); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := applyVariables(pre.Variables, path, envFiles); err != nil {
		return nil, err
	}
	if err := expandRepeats(root); err != nil {
		return nil, fmt.Errorf("expand !repeat: %w", err)
	}
	var cfg Config
	if err := root.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// LoadConfig parses the config file, exports its variables (see
// applyVariables) using orca.env next to the config plus envFiles, and
// validates all references.
func LoadConfig(path string, envFiles ...string) (*Config, error) {
	cfg, err := parseConfig(path, envFiles)
	if err != nil {
		return nil, err
	}
	return cfg, validate(cfg)
}

// validateStructure checks that every step has exactly one type and all step
// IDs are unique. Returns the set of step IDs for use in ref validation.
func validateStructure(cfg *Config) (map[string]bool, error) {
	if cfg.Benchmark.Name == "" {
		return nil, fmt.Errorf("benchmark.name is required")
	}
	stepIDs := map[string]bool{}
	for _, steps := range cfg.Phases.all() {
		for i, step := range steps {
			if step.typeName() == "" {
				label := step.ID
				if label == "" {
					label = fmt.Sprintf("#%d", i+1)
				}
				return nil, fmt.Errorf("step %s: must have exactly one step type", label)
			}
			if step.ID != "" {
				if stepIDs[step.ID] {
					return nil, fmt.Errorf("duplicate step id %q", step.ID)
				}
				stepIDs[step.ID] = true
			}
		}
	}
	return stepIDs, nil
}

func validate(cfg *Config) error {
	stepIDs, err := validateStructure(cfg)
	if err != nil {
		return err
	}
	for _, ref := range collectRefs(cfg) {
		if err := validateRef(ref, stepIDs); err != nil {
			return err
		}
	}
	return nil
}

// LoadConfigUnchecked parses the config file and validates step structure
// (types, unique IDs) but skips reference validation. Use for bench and down
// runs where env vars for earlier phases may not be set.
func LoadConfigUnchecked(path string, envFiles ...string) (*Config, error) {
	cfg, err := parseConfig(path, envFiles)
	if err != nil {
		return nil, err
	}
	if _, err := validateStructure(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

var refRE = regexp.MustCompile(`\$\{([^}]+)\}`)

// collectRefs extracts all ${...} reference keys from string fields in cfg.
func collectRefs(cfg *Config) []string {
	var refs []string
	add := func(s string) {
		for _, m := range refRE.FindAllStringSubmatch(s, -1) {
			refs = append(refs, m[1])
		}
	}
	add(cfg.Elasticsearch.URL)
	add(cfg.Elasticsearch.Username)
	add(cfg.Elasticsearch.Password)
	add(cfg.Elasticsearch.CAPath)
	add(cfg.Kibana.URL)
	add(cfg.Kibana.Username)
	add(cfg.Kibana.Password)
	add(cfg.Kibana.CAPath)
	for _, steps := range cfg.Phases.all() {
		for _, step := range steps {
			collectStepRefs(step, add)
		}
	}
	return refs
}

func collectStepRefs(step Step, add func(string)) {
	switch {
	case step.Noop != nil:
		for _, v := range step.Noop.State {
			add(v)
		}
	case step.Kind != nil:
		add(step.Kind.Name)
		for _, o := range step.Kind.Outputs {
			add(o.Path)
		}
	case step.KindDelete != nil:
		add(step.KindDelete.Name)
	case step.KindLoad != nil:
		add(step.KindLoad.Name)
		add(step.KindLoad.Image)
	case step.Kubectl != nil:
		add(step.Kubectl.Kubeconfig)
		add(step.Kubectl.Run)
	case step.WaitPods != nil:
		add(step.WaitPods.Kubeconfig)
		add(step.WaitPods.Namespace)
		add(step.WaitPods.Selector)
	case step.WatchJob != nil:
		add(step.WatchJob.Kubeconfig)
		add(step.WatchJob.Namespace)
		add(step.WatchJob.Job)
	case step.EsDocCount != nil:
		add(step.EsDocCount.Index)
		add(step.EsDocCount.Since)
		for _, v := range step.EsDocCount.Filter {
			add(v)
		}
	case step.EsLogSpan != nil:
		add(step.EsLogSpan.Index)
		add(step.EsLogSpan.Since)
		for _, v := range step.EsLogSpan.Filter {
			add(v)
		}
	case step.EsAgg != nil:
		add(step.EsAgg.Index)
		add(step.EsAgg.Since)
		add(step.EsAgg.Until)
		for _, v := range step.EsAgg.Filter {
			add(v)
		}
	case step.EsAgentMetrics != nil:
		add(step.EsAgentMetrics.Index)
		add(step.EsAgentMetrics.Since)
		add(step.EsAgentMetrics.Until)
		add(step.EsAgentMetrics.OutputDir)
		if step.EsAgentMetrics.Dashboard != nil {
			add(step.EsAgentMetrics.Dashboard.ID)
		}
		for _, agent := range step.EsAgentMetrics.Agents {
			add(agent.Label)
			for _, v := range agent.Filter {
				add(v)
			}
		}
		for _, ing := range step.EsAgentMetrics.Ingest {
			add(ing.Index)
			for _, v := range ing.Filter {
				add(v)
			}
		}
	case step.EsMetrics != nil:
		add(step.EsMetrics.Index)
		add(step.EsMetrics.Since)
		add(step.EsMetrics.Until)
		add(step.EsMetrics.OutputDir)
		add(step.EsMetrics.DataStreamIndex)
		for _, v := range step.EsMetrics.Filter {
			add(v)
		}
	case step.FilebeatWait != nil:
		add(step.FilebeatWait.Index)
		add(step.FilebeatWait.Since)
		for _, v := range step.FilebeatWait.Filter {
			add(v)
		}
	case step.FilebeatMetrics != nil:
		add(step.FilebeatMetrics.Index)
		add(step.FilebeatMetrics.Since)
		add(step.FilebeatMetrics.Until)
		add(step.FilebeatMetrics.OutputDir)
		for _, v := range step.FilebeatMetrics.Filter {
			add(v)
		}
	case step.EsTsv != nil:
		add(step.EsTsv.Index)
		add(step.EsTsv.Since)
		add(step.EsTsv.Until)
		add(step.EsTsv.Output)
		for _, v := range step.EsTsv.Filter {
			add(v)
		}
	case step.TsvDerive != nil:
		add(step.TsvDerive.Input)
		add(step.TsvDerive.Output)
	case step.Chart != nil:
		add(step.Chart.Input)
		add(step.Chart.Output)
		add(step.Chart.Title)
	case step.WriteIni != nil:
		add(step.WriteIni.Output)
		for _, sec := range step.WriteIni.Sections {
			for _, e := range sec.Entries {
				add(e)
			}
		}
	case step.Shell != nil:
		add(step.Shell.Run)
	case step.OpenTofu != nil:
		add(step.OpenTofu.Dir)
		for _, v := range step.OpenTofu.Vars {
			add(v)
		}
		for _, o := range step.OpenTofu.Outputs {
			add(o.Path)
		}
	case step.Ansible != nil:
		add(step.Ansible.Playbook)
		add(step.Ansible.Inventory)
		add(step.Ansible.Requirements)
		for _, v := range step.Ansible.Vars {
			add(v)
		}
		if step.Ansible.Outputs != nil {
			add(step.Ansible.Outputs.Path)
		}
	case step.FleetDiag != nil:
		add(step.FleetDiag.OutputDir)
		for _, a := range step.FleetDiag.Agents {
			add(a.ID)
			add(a.Hostname)
		}
	}
}

func validateRef(ref string, stepIDs map[string]bool) error {
	ref, _, hasDefault := splitRefDefault(ref)
	switch {
	case ref == "scratch" || ref == "results" || ref == "run":
		return nil
	case strings.HasPrefix(ref, "phase."):
		parts := strings.SplitN(ref, ".", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid phase reference ${%s}: expected phase.<name>.<field>", ref)
		}
		valid := false
		for _, p := range phaseNames {
			if p == parts[1] {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("unknown phase %q in ${%s}", parts[1], ref)
		}
		if parts[2] != "start" && parts[2] != "end" && parts[2] != "duration" {
			return fmt.Errorf("invalid phase field %q in ${%s}: must be start, end, or duration", parts[2], ref)
		}
		return nil
	case strings.HasPrefix(ref, "steps."):
		parts := strings.SplitN(ref, ".", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid step reference ${%s}: expected steps.<id>.<key>", ref)
		}
		if !stepIDs[parts[1]] {
			return fmt.Errorf("${%s}: no step with id %q", ref, parts[1])
		}
		return nil
	default:
		if _, ok := os.LookupEnv(ref); !ok && !hasDefault {
			return fmt.Errorf("${%s}: env var not set", ref)
		}
		return nil
	}
}
