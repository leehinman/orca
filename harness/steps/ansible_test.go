package steps_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func fakeAnsiblePlaybook(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ansible-playbook")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestAnsibleFactory_OutputsRequireID(t *testing.T) {
	_, err := steps.NewAnsibleFactory()(harness.Step{Ansible: &harness.AnsibleStep{
		Playbook: "p.yml",
		Outputs:  &harness.OutputSpec{Path: "/tmp/out.json"},
	}})
	if err == nil {
		t.Fatal("expected error for outputs without step id")
	}
}

func TestAnsibleFactory_OutputsRejectUnknownFrom(t *testing.T) {
	_, err := steps.NewAnsibleFactory()(harness.Step{ID: "p", Ansible: &harness.AnsibleStep{
		Playbook: "p.yml",
		Outputs:  &harness.OutputSpec{From: "stdout", Path: "/tmp/out.json"},
	}})
	if err == nil {
		t.Fatal("expected error for outputs.from other than file")
	}
}

func TestAnsibleFactory_CapturesOutputs(t *testing.T) {
	fakeAnsiblePlaybook(t, `
for a in "$@"; do
  case "$a" in out_path=*) out="${a#out_path=}";; esac
done
printf '{"agent0":"id-0","agent1":"id-1","count":2}' > "$out"
`)
	state := harness.NewState()
	state.SetScratch(t.TempDir())
	exec, err := steps.NewAnsibleFactory()(harness.Step{ID: "provision", Ansible: &harness.AnsibleStep{
		Playbook:  "play.yml",
		Inventory: "inv.ini",
		Vars:      map[string]string{"out_path": "${scratch}/agent-ids.json"},
		Outputs:   &harness.OutputSpec{From: "file", Path: "${scratch}/agent-ids.json"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for key, want := range map[string]string{"agent0": "id-0", "agent1": "id-1", "count": "2"} {
		got, err := state.Resolve("${steps.provision." + key + "}")
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", key, got, want)
		}
	}
}

func TestAnsibleFactory_OutputsFileMissing(t *testing.T) {
	fakeAnsiblePlaybook(t, "exit 0")
	state := harness.NewState()
	state.SetScratch(t.TempDir())
	exec, _ := steps.NewAnsibleFactory()(harness.Step{ID: "provision", Ansible: &harness.AnsibleStep{
		Playbook: "play.yml",
		Outputs:  &harness.OutputSpec{Path: "${scratch}/missing.json"},
	}})
	if err := exec.Run(context.Background(), state); err == nil {
		t.Fatal("expected error when outputs file is missing")
	}
}

func TestAnsibleFactory_PlaybookFailure(t *testing.T) {
	fakeAnsiblePlaybook(t, "exit 2")
	exec, _ := steps.NewAnsibleFactory()(harness.Step{Ansible: &harness.AnsibleStep{Playbook: "play.yml"}})
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for failing playbook")
	}
}

func TestAnsibleFactory_UsesConfigNextToPlaybook(t *testing.T) {
	fakeAnsiblePlaybook(t, `
for a in "$@"; do
  case "$a" in env_out=*) out="${a#env_out=}";; esac
done
printf '%s' "${ANSIBLE_CONFIG-unset}" > "$out"
`)
	dir := t.TempDir()
	playbook := filepath.Join(dir, "play.yml")
	if err := os.WriteFile(playbook, []byte("[]"), 0o644); err != nil {
		t.Fatal(err)
	}
	envOut := filepath.Join(dir, "env.txt")
	run := func() string {
		exec, err := steps.NewAnsibleFactory()(harness.Step{Ansible: &harness.AnsibleStep{
			Playbook: playbook,
			Vars:     map[string]string{"env_out": envOut},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := exec.Run(context.Background(), harness.NewState()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := os.ReadFile(envOut)
		if err != nil {
			t.Fatal(err)
		}
		return string(got)
	}

	if got := run(); got != "unset" {
		t.Errorf("without ansible.cfg: ANSIBLE_CONFIG should be unset, got %q", got)
	}

	cfgPath := filepath.Join(dir, "ansible.cfg")
	if err := os.WriteFile(cfgPath, []byte("[defaults]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := run(); got != cfgPath {
		t.Errorf("with ansible.cfg: got %q, want %q", got, cfgPath)
	}

	t.Setenv("ANSIBLE_CONFIG", "/explicit/ansible.cfg")
	if got := run(); got != "/explicit/ansible.cfg" {
		t.Errorf("explicit ANSIBLE_CONFIG should win, got %q", got)
	}
}
