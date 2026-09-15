package harness

import (
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// --- helpers ---

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func minimalConfig() *Config {
	return &Config{Benchmark: BenchmarkMeta{Name: "bench"}}
}

// --- LoadConfig ---

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	f := writeTempConfig(t, ":::not yaml:::")
	_, err := LoadConfig(f)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadConfig_Valid(t *testing.T) {
	t.Setenv("ES_URL", "http://localhost:9200")
	t.Setenv("ES_USER", "elastic")
	t.Setenv("ES_PASSWORD", "changeme")
	f := writeTempConfig(t, `
benchmark:
  name: my-bench
elasticsearch:
  url: ${ES_URL}
  username: ${ES_USER}
  password: ${ES_PASSWORD}
phases:
  setup:
    - id: cluster
      kind:
        name: bench
        outputs:
          kubeconfigPath: {from: file, path: "${scratch}/kubeconfig"}
  teardown:
    - kind-delete:
        name: bench
`)
	cfg, err := LoadConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Benchmark.Name != "my-bench" {
		t.Errorf("name: got %q, want %q", cfg.Benchmark.Name, "my-bench")
	}
	if len(cfg.Phases.Setup) != 1 {
		t.Errorf("setup steps: got %d, want 1", len(cfg.Phases.Setup))
	}
	if len(cfg.Phases.Teardown) != 1 {
		t.Errorf("teardown steps: got %d, want 1", len(cfg.Phases.Teardown))
	}
}

// --- validate ---

func TestValidate_MissingName(t *testing.T) {
	cfg := minimalConfig()
	cfg.Benchmark.Name = ""
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for missing benchmark.name")
	}
}

func TestValidate_NoStepType(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Setup = []Step{{ID: "x"}}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for step with no type")
	}
}

func TestValidate_DuplicateID(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Setup = []Step{
		{ID: "x", Noop: &NoopStep{}},
		{ID: "x", Noop: &NoopStep{}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for duplicate step id")
	}
}

func TestValidate_UnknownPhaseRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Collect = []Step{
		{EsDocCount: &EsDocCountStep{
			Index:    "logs-*",
			Since:    "${phase.badphase.start}",
			Expected: 100,
			Timeout:  Duration{5 * time.Minute},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for unknown phase name")
	}
}

func TestValidate_InvalidPhaseField(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Collect = []Step{
		{EsDocCount: &EsDocCountStep{
			Index:    "logs-*",
			Since:    "${phase.execute.notafield}",
			Expected: 100,
			Timeout:  Duration{5 * time.Minute},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for invalid phase field")
	}
}

func TestValidate_ValidPhaseRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Collect = []Step{
		{EsDocCount: &EsDocCountStep{
			Index:    "logs-*",
			Since:    "${phase.execute.start}",
			Expected: 100,
			Timeout:  Duration{5 * time.Minute},
		}},
	}
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_UnknownStepRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Configure = []Step{
		{Kubectl: &KubectlStep{
			Kubeconfig: "${steps.missing.kubeconfigPath}",
			Run:        "apply -f agent.yaml",
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for reference to unknown step id")
	}
}

func TestValidate_ValidStepRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Setup = []Step{
		{ID: "cluster", Kind: &KindStep{Name: "bench"}},
	}
	cfg.Phases.Configure = []Step{
		{Kubectl: &KubectlStep{
			Kubeconfig: "${steps.cluster.kubeconfigPath}",
			Run:        "apply -f agent.yaml",
		}},
	}
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_ScratchRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Setup = []Step{
		{ID: "cluster", Kind: &KindStep{
			Name: "bench",
			Outputs: map[string]OutputSpec{
				"kubeconfigPath": {From: "file", Path: "${scratch}/kubeconfig"},
			},
		}},
	}
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_EnvVarNotSet(t *testing.T) {
	const key = "ORCA_TEST_UNSET_VAR_XYZZY"
	os.Unsetenv(key)
	cfg := minimalConfig()
	cfg.Elasticsearch.URL = "${" + key + "}"
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for unset env var")
	}
}

func TestValidate_EnvVarSet(t *testing.T) {
	t.Setenv("ORCA_TEST_ES_URL", "http://localhost:9200")
	cfg := minimalConfig()
	cfg.Elasticsearch.URL = "${ORCA_TEST_ES_URL}"
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_EnvVarSetToEmpty(t *testing.T) {
	// os.LookupEnv distinguishes "not set" from "set to empty string".
	// A var explicitly set to "" should pass validation (the value is available,
	// just empty — the step itself will fail if it needs a non-empty value).
	t.Setenv("ORCA_TEST_EMPTY_VAR_XYZZY", "")
	cfg := minimalConfig()
	cfg.Elasticsearch.URL = "${ORCA_TEST_EMPTY_VAR_XYZZY}"
	if err := validate(cfg); err != nil {
		t.Fatalf("expected no error for env var set to empty string, got: %v", err)
	}
}

// --- Duration ---

func TestDuration_ValidYAML(t *testing.T) {
	type wrapper struct {
		T Duration `yaml:"t"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("t: 5m30s"), &w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := 5*time.Minute + 30*time.Second
	if w.T.Duration != want {
		t.Errorf("got %v, want %v", w.T.Duration, want)
	}
}

func TestDuration_InvalidYAML(t *testing.T) {
	type wrapper struct {
		T Duration `yaml:"t"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("t: notaduration"), &w); err == nil {
		t.Fatal("expected error for invalid duration string")
	}
}

// --- fleet-agent-diagnostics / ansible outputs ---

func TestValidate_FleetDiagStepRefs(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Configure = []Step{
		{ID: "provision", Ansible: &AnsibleStep{
			Playbook:  "play.yml",
			Inventory: "inv.ini",
			Outputs:   &OutputSpec{From: "file", Path: "${scratch}/agent-ids.json"},
		}},
	}
	cfg.Phases.Collect = []Step{
		{ID: "diag", FleetDiag: &FleetDiagStep{
			OutputDir: "${results}",
			Agents: []FleetDiagAgent{
				{Name: "agent0", ID: "${steps.provision.agent0}"},
				{Name: "agent1", Hostname: "bench-elastic-agent-1"},
			},
		}},
	}
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := cfg.Phases.Collect[0].typeName(); got != "fleet-agent-diagnostics" {
		t.Errorf("typeName: got %q", got)
	}
}

func TestValidate_FleetDiagUnknownStepRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Collect = []Step{
		{FleetDiag: &FleetDiagStep{
			OutputDir: "${results}",
			Agents:    []FleetDiagAgent{{Name: "agent0", ID: "${steps.missing.agent0}"}},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for reference to unknown step id in agent id")
	}
}

func TestValidate_AnsibleOutputsUnknownRef(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Configure = []Step{
		{ID: "provision", Ansible: &AnsibleStep{
			Playbook: "play.yml",
			Outputs:  &OutputSpec{Path: "${steps.missing.dir}/ids.json"},
		}},
	}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for unknown step ref in ansible outputs path")
	}
}

func TestValidate_KibanaRefs(t *testing.T) {
	cfg := minimalConfig()
	cfg.Kibana = KibanaConfig{URL: "https://${steps.missing.kibana_ip}:5601"}
	if err := validate(cfg); err == nil {
		t.Fatal("expected error for unknown step ref in kibana.url")
	}
	cfg.Phases.Setup = []Step{{ID: "infra", OpenTofu: &OpenTofuStep{Dir: "./tofu"}}}
	cfg.Kibana.URL = "https://${steps.infra.kibana_ip}:5601"
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_FleetDiagYAML(t *testing.T) {
	path := writeTempConfig(t, `
benchmark:
  name: bench
kibana:
  url: https://kibana:5601
  username: elastic
  password: changeme
phases:
  configure:
    - id: provision
      ansible:
        playbook: play.yml
        inventory: inv.ini
        outputs:
          from: file
          path: ${scratch}/agent-ids.json
  collect:
    - id: diag
      fleet-agent-diagnostics:
        output_dir: ${results}
        timeout: 5m
        poll_interval: 2s
        cpu_profile: true
        agents:
          - name: agent0
            id: ${steps.provision.agent0}
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Kibana.URL != "https://kibana:5601" {
		t.Errorf("kibana.url: got %q", cfg.Kibana.URL)
	}
	d := cfg.Phases.Collect[0].FleetDiag
	if d == nil {
		t.Fatal("fleet-agent-diagnostics step not parsed")
	}
	if d.Timeout.Duration != 5*time.Minute || d.PollInterval.Duration != 2*time.Second || !d.CPUProfile {
		t.Errorf("unexpected step fields: %+v", d)
	}
	if len(d.Agents) != 1 || d.Agents[0].ID != "${steps.provision.agent0}" {
		t.Errorf("unexpected agents: %+v", d.Agents)
	}
	if cfg.Phases.Configure[0].Ansible.Outputs.Path != "${scratch}/agent-ids.json" {
		t.Errorf("ansible outputs not parsed: %+v", cfg.Phases.Configure[0].Ansible.Outputs)
	}
}

// --- ${VAR:-default} ---

func TestValidate_EnvVarWithDefault(t *testing.T) {
	cfg := minimalConfig()
	cfg.Phases.Setup = []Step{{OpenTofu: &OpenTofuStep{Dir: "./tofu/${ORCA_TEST_UNSET_PROVIDER:-multipass}"}}}
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cfg.Phases.Setup = []Step{{OpenTofu: &OpenTofuStep{Dir: "./tofu/${steps.missing.dir:-x}"}}}
	if err := validate(cfg); err == nil {
		t.Fatal("default must not mask an unknown step reference")
	}
}

func TestState_ResolveDefault(t *testing.T) {
	s := NewState()
	s.SetScratch("/tmp/s")
	cases := map[string]string{
		"./tofu/${ORCA_TEST_UNSET_PROVIDER:-multipass}": "./tofu/multipass",
		"${scratch:-/nope}/x":                           "/tmp/s/x",
		"${ORCA_TEST_UNSET_PROVIDER:-}":                 "",
		"${ORCA_TEST_UNSET_PROVIDER:-a:-b}":             "a:-b",
	}
	for in, want := range cases {
		got, err := s.Resolve(in)
		if err != nil || got != want {
			t.Errorf("%s: got %q (%v), want %q", in, got, err, want)
		}
	}
	t.Setenv("ORCA_TEST_UNSET_PROVIDER", "gcp")
	if got, _ := s.Resolve("${ORCA_TEST_UNSET_PROVIDER:-multipass}"); got != "gcp" {
		t.Errorf("env var should win over default, got %q", got)
	}
	if _, err := s.Resolve("${ORCA_TEST_REALLY_UNSET}"); err == nil {
		t.Error("reference without default must still fail")
	}
}

func TestLoadConfig_StackVersionsStep(t *testing.T) {
	path := writeTempConfig(t, `
benchmark:
  name: bench
phases:
  collect:
    - id: versions
      stack-versions: {}
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Phases.Collect[0].StackVersions == nil || cfg.Phases.Collect[0].typeName() != "stack-versions" {
		t.Errorf("stack-versions step not parsed: %+v", cfg.Phases.Collect[0])
	}
}
