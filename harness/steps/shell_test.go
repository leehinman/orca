package steps_test

import (
	"context"
	"testing"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func TestShellFactory_Success(t *testing.T) {
	exec, _ := steps.NewShellFactory()(harness.Step{Shell: &harness.ShellStep{Run: "true"}})
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShellFactory_NonZeroExit(t *testing.T) {
	exec, _ := steps.NewShellFactory()(harness.Step{Shell: &harness.ShellStep{Run: "false"}})
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for non-zero exit")
	}
}

func TestShellFactory_ResolvesRunTemplate(t *testing.T) {
	exec, _ := steps.NewShellFactory()(harness.Step{Shell: &harness.ShellStep{Run: "test -d ${scratch}"}})
	state := harness.NewState()
	state.SetScratch(t.TempDir())
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShellFactory_UnresolvableTemplate(t *testing.T) {
	exec, _ := steps.NewShellFactory()(harness.Step{Shell: &harness.ShellStep{Run: "echo ${steps.missing.val}"}})
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for unresolvable template in run")
	}
}

func TestShellFactory_ContextCancelled(t *testing.T) {
	exec, _ := steps.NewShellFactory()(harness.Step{Shell: &harness.ShellStep{Run: "sleep 60"}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := exec.Run(ctx, harness.NewState()); err == nil {
		t.Fatal("expected error for cancelled context")
	}
}
