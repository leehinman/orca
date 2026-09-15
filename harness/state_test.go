package harness

import (
	"path/filepath"
	"testing"
	"time"
)

func TestState_LiteralString(t *testing.T) {
	s := NewState()
	got, err := s.Resolve("no references here")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "no references here" {
		t.Errorf("got %q, want %q", got, "no references here")
	}
}

func TestState_StepOutput(t *testing.T) {
	s := NewState()
	s.SetStepOutput("cluster", "kubeconfigPath", "/tmp/kubeconfig")
	got, err := s.Resolve("${steps.cluster.kubeconfigPath}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "/tmp/kubeconfig" {
		t.Errorf("got %q, want %q", got, "/tmp/kubeconfig")
	}
}

func TestState_Scratch(t *testing.T) {
	s := NewState()
	s.SetScratch("/tmp/bench-run")
	got, err := s.Resolve("${scratch}/kubeconfig")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "/tmp/bench-run/kubeconfig" {
		t.Errorf("got %q, want %q", got, "/tmp/bench-run/kubeconfig")
	}
}

func TestState_Results(t *testing.T) {
	s := NewState()
	s.SetResults("/tmp/results-2026-Jan-01--12_00_00")
	got, err := s.Resolve("${results}/summary.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "/tmp/results-2026-Jan-01--12_00_00/summary.txt" {
		t.Errorf("got %q, want %q", got, "/tmp/results-2026-Jan-01--12_00_00/summary.txt")
	}
}

func TestState_ResultsDoesNotFallBackToEnv(t *testing.T) {
	t.Setenv("results", "should-not-resolve")
	s := NewState()
	_, err := s.Resolve("${results}")
	if err == nil {
		t.Fatal("expected error: results ref must not fall back to env")
	}
}

func TestState_PhaseStart(t *testing.T) {
	s := NewState()
	s.SetPhaseStart("execute", time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC))
	got, err := s.Resolve("${phase.execute.start}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2024-01-15T12:00:00Z" {
		t.Errorf("got %q, want %q", got, "2024-01-15T12:00:00Z")
	}
}

func TestState_PhaseEnd(t *testing.T) {
	s := NewState()
	s.SetPhaseEnd("execute", time.Date(2024, 1, 15, 12, 5, 0, 0, time.UTC))
	got, err := s.Resolve("${phase.execute.end}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2024-01-15T12:05:00Z" {
		t.Errorf("got %q, want %q", got, "2024-01-15T12:05:00Z")
	}
}

func TestState_PhaseDuration(t *testing.T) {
	s := NewState()
	s.SetPhaseDuration("execute", 3*time.Minute+42*time.Second+500*time.Millisecond)
	got, err := s.Resolve("${phase.execute.duration}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "3m42.5s" {
		t.Errorf("got %q, want %q", got, "3m42.5s")
	}
}

func TestState_EnvVar(t *testing.T) {
	t.Setenv("ORCA_TEST_ES_URL", "http://localhost:9200")
	s := NewState()
	got, err := s.Resolve("${ORCA_TEST_ES_URL}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "http://localhost:9200" {
		t.Errorf("got %q, want %q", got, "http://localhost:9200")
	}
}

func TestState_MultipleRefs(t *testing.T) {
	s := NewState()
	s.SetScratch("/tmp/run")
	s.SetStepOutput("cluster", "name", "bench")
	got, err := s.Resolve("${steps.cluster.name} at ${scratch}/config")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "bench at /tmp/run/config"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestState_UnresolvedStepRef(t *testing.T) {
	s := NewState()
	_, err := s.Resolve("${steps.missing.key}")
	if err == nil {
		t.Fatal("expected error for unresolved step reference")
	}
}

func TestState_UnresolvedPhaseRef(t *testing.T) {
	s := NewState()
	_, err := s.Resolve("${phase.execute.start}")
	if err == nil {
		t.Fatal("expected error for unresolved phase reference")
	}
}

func TestState_UnresolvedEnvVar(t *testing.T) {
	const key = "ORCA_TEST_UNSET_VAR_XYZZY"
	t.Setenv(key, "") // ensure empty
	s := NewState()
	_, err := s.Resolve("${" + key + "}")
	if err == nil {
		t.Fatal("expected error for unset env var")
	}
}

func TestState_StepRefDoesNotFallBackToEnv(t *testing.T) {
	// Env var named "steps.x.y" must not satisfy a ${steps.x.y} reference;
	// step outputs must come from SetStepOutput.
	t.Setenv("steps.cluster.key", "should-not-resolve")
	s := NewState()
	_, err := s.Resolve("${steps.cluster.key}")
	if err == nil {
		t.Fatal("expected error: step refs must not fall back to env")
	}
}

func TestState_SecondRefUnresolved(t *testing.T) {
	s := NewState()
	s.SetStepOutput("cluster", "name", "bench")
	_, err := s.Resolve("${steps.cluster.name} and ${steps.missing.key}")
	if err == nil {
		t.Fatal("expected error when any reference is unresolved")
	}
}

func TestState_SaveAndLoad(t *testing.T) {
	s := NewState()
	s.SetScratch("/tmp/run-bench/scratch")
	s.SetResults("/tmp/run-bench")
	s.SetStepOutput("cluster", "name", "bench")

	path := filepath.Join(t.TempDir(), "orca-state.json")
	if err := s.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}

	for _, ref := range []string{"${scratch}", "${results}", "${steps.cluster.name}"} {
		want, _ := s.Resolve(ref)
		got, err := loaded.Resolve(ref)
		if err != nil {
			t.Errorf("loaded.Resolve(%q): %v", ref, err)
			continue
		}
		if got != want {
			t.Errorf("loaded.Resolve(%q) = %q, want %q", ref, got, want)
		}
	}
}

func TestState_PhaseTimestampsStoredAsUTC(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	ts := time.Date(2024, 6, 1, 8, 0, 0, 0, loc) // 08:00 Eastern = 12:00 UTC
	s := NewState()
	s.SetPhaseStart("setup", ts)
	got, err := s.Resolve("${phase.setup.start}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2024-06-01T12:00:00Z" {
		t.Errorf("got %q, want %q", got, "2024-06-01T12:00:00Z")
	}
}

func TestState_ClearStepOutputs(t *testing.T) {
	s := NewState()
	s.SetStepOutput("m", "summary", "old")
	s.SetStepOutput("m", "totalDocs", 5)
	s.SetStepOutput("infra", "ip", "10.0.0.1")
	s.SetStepOutput("metrics", "summary", "keep")
	s.ClearStepOutputs([]string{"m"})
	if _, err := s.Resolve("${steps.m.summary}"); err == nil {
		t.Error("steps.m.summary should be cleared")
	}
	if _, err := s.Resolve("${steps.m.totalDocs}"); err == nil {
		t.Error("steps.m.totalDocs should be cleared")
	}
	for _, ref := range []string{"${steps.infra.ip}", "${steps.metrics.summary}"} {
		if _, err := s.Resolve(ref); err != nil {
			t.Errorf("%s should survive: %v", ref, err)
		}
	}
}
