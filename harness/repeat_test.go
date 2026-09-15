package harness

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func mustRepeatDoc(t *testing.T, src string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(src), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	root := &doc
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	return root
}

func TestExpandRepeats_ScalarSequence(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: 3
  as: i
  template: "agent${i}"
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Entries []string `yaml:"entries"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []string{"agent0", "agent1", "agent2"}
	if len(w.Entries) != len(want) {
		t.Fatalf("got %v, want %v", w.Entries, want)
	}
	for i, v := range want {
		if w.Entries[i] != v {
			t.Errorf("entry %d: got %q, want %q", i, w.Entries[i], v)
		}
	}
}

func TestExpandRepeats_MappingSequence(t *testing.T) {
	root := mustRepeatDoc(t, `
agents: !repeat
  count: 2
  as: i
  template:
    name: agent${i}
    id: ${steps.provision.agent${i}}
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Agents []struct {
			Name string `yaml:"name"`
			ID   string `yaml:"id"`
		} `yaml:"agents"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(w.Agents) != 2 {
		t.Fatalf("got %d agents, want 2", len(w.Agents))
	}
	if w.Agents[0].Name != "agent0" || w.Agents[0].ID != "${steps.provision.agent0}" {
		t.Errorf("agent0: %+v", w.Agents[0])
	}
	if w.Agents[1].Name != "agent1" || w.Agents[1].ID != "${steps.provision.agent1}" {
		t.Errorf("agent1: %+v", w.Agents[1])
	}
}

func TestExpandRepeats_ItemInExistingSequence(t *testing.T) {
	root := mustRepeatDoc(t, `
items:
  - literal
  - !repeat
    count: 2
    as: i
    template: "gen${i}"
  - tail
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Items []string `yaml:"items"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []string{"literal", "gen0", "gen1", "tail"}
	if len(w.Items) != len(want) {
		t.Fatalf("got %v, want %v", w.Items, want)
	}
	for i, v := range want {
		if w.Items[i] != v {
			t.Errorf("item %d: got %q, want %q", i, w.Items[i], v)
		}
	}
}

func TestExpandRepeats_ZeroCount(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: 0
  template: "x${i}"
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Entries []string `yaml:"entries"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(w.Entries) != 0 {
		t.Errorf("got %v, want empty", w.Entries)
	}
}

func TestExpandRepeats_CountFromVariable(t *testing.T) {
	t.Setenv("ORCA_TEST_REPEAT_COUNT", "4")
	root := mustRepeatDoc(t, `
entries: !repeat
  count: ${ORCA_TEST_REPEAT_COUNT}
  template: "x${i}"
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Entries []string `yaml:"entries"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(w.Entries) != 4 {
		t.Fatalf("got %d entries, want 4", len(w.Entries))
	}
}

func TestExpandRepeats_MissingTemplate(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: 2
`)
	if err := expandRepeats(root); err == nil {
		t.Fatal("expected error for missing template")
	}
}

func TestExpandRepeats_NonIntegerCount(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: notanumber
  template: "x${i}"
`)
	if err := expandRepeats(root); err == nil {
		t.Fatal("expected error for non-integer count")
	}
}

func TestExpandRepeats_CountCannotReferenceSteps(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: ${steps.infra.agent_count}
  template: "x${i}"
`)
	if err := expandRepeats(root); err == nil {
		t.Fatal("expected error: steps.* is not available at config-load time")
	}
}

func TestExpandRepeats_CustomIndexName(t *testing.T) {
	root := mustRepeatDoc(t, `
entries: !repeat
  count: 2
  as: n
  template: "agent${n}-${n}"
`)
	if err := expandRepeats(root); err != nil {
		t.Fatalf("expandRepeats: %v", err)
	}
	var w struct {
		Entries []string `yaml:"entries"`
	}
	if err := root.Decode(&w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []string{"agent0-0", "agent1-1"}
	for i, v := range want {
		if w.Entries[i] != v {
			t.Errorf("entry %d: got %q, want %q", i, w.Entries[i], v)
		}
	}
}

// --- VarInt ---

func TestVarInt_Literal(t *testing.T) {
	type wrapper struct {
		N VarInt `yaml:"n"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("n: 4"), &w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.N != 4 {
		t.Errorf("got %d, want 4", w.N)
	}
}

func TestVarInt_VariableReference(t *testing.T) {
	t.Setenv("ORCA_TEST_VARINT", "7")
	type wrapper struct {
		N VarInt `yaml:"n"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("n: ${ORCA_TEST_VARINT}"), &w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.N != 7 {
		t.Errorf("got %d, want 7", w.N)
	}
}

func TestVarInt_Product(t *testing.T) {
	t.Setenv("ORCA_TEST_VARINT_A", "2")
	t.Setenv("ORCA_TEST_VARINT_B", "5")
	type wrapper struct {
		N VarInt `yaml:"n"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("n: ${ORCA_TEST_VARINT_A}*${ORCA_TEST_VARINT_B}*1000"), &w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.N != 10000 {
		t.Errorf("got %d, want 10000", w.N)
	}
}

func TestVarInt_ProductInvalidTerm(t *testing.T) {
	type wrapper struct {
		N VarInt `yaml:"n"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("n: 2*notanumber"), &w); err == nil {
		t.Fatal("expected error for non-integer term in product")
	}
}

func TestVarInt_Invalid(t *testing.T) {
	type wrapper struct {
		N VarInt `yaml:"n"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte("n: notanumber"), &w); err == nil {
		t.Fatal("expected error for non-integer value")
	}
}

// --- end-to-end via LoadConfig ---

func TestLoadConfig_RepeatEndToEnd(t *testing.T) {
	t.Setenv("ORCA_TEST_AGENT_COUNT", "2")
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
  warmup:
    - es-log-span:
        index: logs-*
        group_by: agent.name
        min_duration: 1m
        expected_groups: ${ORCA_TEST_AGENT_COUNT}
        timeout: 5m
  collect:
    - id: diag
      fleet-agent-diagnostics:
        output_dir: ${results}
        agents: !repeat
          count: ${ORCA_TEST_AGENT_COUNT}
          as: i
          template:
            name: agent${i}
            id: ${steps.provision.agent${i}}
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d := cfg.Phases.Collect[0].FleetDiag
	if d == nil || len(d.Agents) != 2 {
		t.Fatalf("fleet-agent-diagnostics agents: got %+v", d)
	}
	if d.Agents[0].Name != "agent0" || d.Agents[0].ID != "${steps.provision.agent0}" {
		t.Errorf("agent0: %+v", d.Agents[0])
	}
	if d.Agents[1].Name != "agent1" || d.Agents[1].ID != "${steps.provision.agent1}" {
		t.Errorf("agent1: %+v", d.Agents[1])
	}
	if cfg.Phases.Warmup[0].EsLogSpan.ExpectedGroups != 2 {
		t.Errorf("expected_groups: got %d, want 2", cfg.Phases.Warmup[0].EsLogSpan.ExpectedGroups)
	}
}
