package steps_test

import (
	"context"
	"testing"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func TestNoopFactory_SetsStateOutput(t *testing.T) {
	exec, _ := steps.NewNoopFactory()(harness.Step{
		ID:   "cluster",
		Noop: &harness.NoopStep{State: map[string]string{"kubeconfigPath": "/tmp/kubeconfig"}},
	})
	state := harness.NewState()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := state.Resolve("${steps.cluster.kubeconfigPath}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "/tmp/kubeconfig" {
		t.Errorf("got %q, want %q", got, "/tmp/kubeconfig")
	}
}

func TestNoopFactory_MultipleOutputs(t *testing.T) {
	exec, _ := steps.NewNoopFactory()(harness.Step{
		ID: "infra",
		Noop: &harness.NoopStep{State: map[string]string{
			"host": "10.0.0.1",
			"port": "22",
		}},
	})
	state := harness.NewState()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	host, _ := state.Resolve("${steps.infra.host}")
	port, _ := state.Resolve("${steps.infra.port}")
	if host != "10.0.0.1" {
		t.Errorf("host: got %q, want %q", host, "10.0.0.1")
	}
	if port != "22" {
		t.Errorf("port: got %q, want %q", port, "22")
	}
}

func TestNoopFactory_ResolvesTemplateValue(t *testing.T) {
	exec, _ := steps.NewNoopFactory()(harness.Step{
		ID:   "derived",
		Noop: &harness.NoopStep{State: map[string]string{"configPath": "${scratch}/config"}},
	})
	state := harness.NewState()
	state.SetScratch("/tmp/bench")
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, _ := state.Resolve("${steps.derived.configPath}")
	if got != "/tmp/bench/config" {
		t.Errorf("got %q, want %q", got, "/tmp/bench/config")
	}
}

func TestNoopFactory_EmptyState(t *testing.T) {
	exec, _ := steps.NewNoopFactory()(harness.Step{ID: "x", Noop: &harness.NoopStep{}})
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNoopFactory_UnresolvableValue(t *testing.T) {
	exec, _ := steps.NewNoopFactory()(harness.Step{
		ID:   "x",
		Noop: &harness.NoopStep{State: map[string]string{"k": "${steps.missing.val}"}},
	})
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for unresolvable template value")
	}
}
