package harness

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// --- test helpers ---

func newTestRunner() *Runner {
	r := NewRunner()
	r.stderr = &bytes.Buffer{}
	return r
}

func okFactory(_ Step) (Executor, error) {
	return ExecutorFunc(func(_ context.Context, _ *State) error { return nil }), nil
}

func errFactory(err error) ExecutorFactory {
	return func(_ Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error { return err }), nil
	}
}

func idRecorder(log *[]string) ExecutorFactory {
	return func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			*log = append(*log, stepLabel(step))
			return nil
		}), nil
	}
}

func minimalCfg() *Config {
	return &Config{Benchmark: BenchmarkMeta{Name: "bench"}}
}

// cdTemp changes the working directory to a temp dir for the duration of the
// test, then restores it. Use in tests that call r.Run() to avoid creating
// run-* directories in the source tree.
func cdTemp(t *testing.T) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(orig) }) //nolint:errcheck
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

// --- empty phases ---

func TestRunner_EmptyPhasesSucceed(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	if err := r.Run(context.Background(), minimalCfg()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunner_EmptyPhasesNoOutput(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Run(context.Background(), minimalCfg()) //nolint
	if out := r.stderr.(*bytes.Buffer).String(); out != "" {
		t.Errorf("expected no output for all-empty phases, got: %q", out)
	}
}

// --- phase ordering ---

func TestRunner_PhasesRunInOrder(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:     []Step{{ID: "s1", Noop: &NoopStep{}}},
			Configure: []Step{{ID: "s2", Noop: &NoopStep{}}},
			Execute:   []Step{{ID: "s3", Noop: &NoopStep{}}},
			Collect:   []Step{{ID: "s4", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"s1", "s2", "s3", "s4"}
	if !equalSlices(log, want) {
		t.Errorf("execution order: got %v, want %v", log, want)
	}
}

func TestRunner_MultipleStepsInPhaseRunInOrder(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{
				{ID: "a", Noop: &NoopStep{}},
				{ID: "b", Noop: &NoopStep{}},
				{ID: "c", Noop: &NoopStep{}},
			},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalSlices(log, []string{"a", "b", "c"}) {
		t.Errorf("got %v, want [a b c]", log)
	}
}

// --- teardown behavior on failure ---

func TestRunner_TeardownRunsOnStepError(t *testing.T) {
	cdTemp(t)
	var teardownRan bool
	boom := errors.New("setup failed")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "td" {
				teardownRan = true
				return nil
			}
			return boom
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:    []Step{{ID: "fail", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if !teardownRan {
		t.Error("teardown should run automatically after step failure")
	}
}

func TestRunner_TeardownRunsOnContextCancel(t *testing.T) {
	cdTemp(t)
	ctx, cancel := context.WithCancel(context.Background())
	var teardownRan bool
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "td" {
				teardownRan = true
				return nil
			}
			cancel()
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:     []Step{{ID: "setup", Noop: &NoopStep{}}},
			Configure: []Step{{ID: "configure", Noop: &NoopStep{}}},
			Teardown:  []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	err := r.Run(ctx, cfg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if !teardownRan {
		t.Error("teardown should run automatically after context cancellation")
	}
}

func TestRunner_SkipTeardownSkipsTeardownOnError(t *testing.T) {
	cdTemp(t)
	var teardownRan bool
	boom := errors.New("setup failed")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "td" {
				teardownRan = true
				return nil
			}
			return boom
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:    []Step{{ID: "fail", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg, RunOptions{SkipTeardown: true})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if teardownRan {
		t.Error("teardown must not run when SkipTeardown is set")
	}
}

func TestRunner_SkipTeardownSkipsTeardownOnSuccess(t *testing.T) {
	cdTemp(t)
	var teardownRan bool
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "td" {
				teardownRan = true
			}
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:    []Step{{ID: "setup", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg, RunOptions{SkipTeardown: true}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if teardownRan {
		t.Error("teardown must not run when SkipTeardown is set, even on success")
	}
}

func TestRunner_SkipTeardownRunsReport(t *testing.T) {
	cdTemp(t)
	var reportRan, teardownRan bool
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			switch step.ID {
			case "td":
				teardownRan = true
			case "rpt":
				reportRan = true
			}
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:    []Step{{ID: "setup", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
			Report:   []Step{{ID: "rpt", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg, RunOptions{SkipTeardown: true}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if teardownRan {
		t.Error("teardown must not run when SkipTeardown is set")
	}
	if !reportRan {
		t.Error("report must run when SkipTeardown is set and main phases succeed")
	}
}

func TestRunner_SkipTeardownWritesStateFile(t *testing.T) {
	cdTemp(t)
	boom := errors.New("setup failed")
	r := newTestRunner()
	r.Register("noop", errFactory(boom))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{{Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg, RunOptions{SkipTeardown: true}); !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}

	state := r.State()
	if state == nil {
		t.Fatal("state should be set after run")
	}
	runDir, err := state.Resolve("${results}")
	if err != nil {
		t.Fatalf("could not resolve results: %v", err)
	}
	stateFile := filepath.Join(runDir, "orca-state.json")
	if _, err := os.Stat(stateFile); err != nil {
		t.Errorf("state file %q not found: %v", stateFile, err)
	}
}

func TestRunner_TeardownErrorReturnedWhenNoMainError(t *testing.T) {
	cdTemp(t)
	boom := errors.New("teardown failed")
	r := newTestRunner()
	r.Register("noop", errFactory(boom))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Teardown: []Step{{Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
}

func TestRunner_MainErrorTakesPrecedenceOverTeardownError(t *testing.T) {
	cdTemp(t)
	mainBoom := errors.New("main failure")
	tdBoom := errors.New("teardown failure")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "td" {
				return tdBoom
			}
			return mainBoom
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:    []Step{{ID: "fail", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg)
	if !errors.Is(err, mainBoom) {
		t.Fatalf("got %v, want mainBoom", err)
	}
}

// --- report phase ---

func TestRunner_ReportRunsOnSuccess(t *testing.T) {
	cdTemp(t)
	var reportRan bool
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "rpt" {
				reportRan = true
			}
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Report: []Step{{ID: "rpt", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reportRan {
		t.Error("report did not run on success")
	}
}

func TestRunner_ReportSkippedOnError(t *testing.T) {
	cdTemp(t)
	var reportRan bool
	boom := errors.New("execute failed")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "rpt" {
				reportRan = true
				return nil
			}
			return boom
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Execute: []Step{{ID: "fail", Noop: &NoopStep{}}},
			Report:  []Step{{ID: "rpt", Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if reportRan {
		t.Error("report must not run after execute failure")
	}
}

// --- scratch dir ---

func TestRunner_ScratchDirAvailableToSteps(t *testing.T) {
	cdTemp(t)
	var scratchPath string
	r := newTestRunner()
	r.Register("noop", func(_ Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, state *State) error {
			p, err := state.Resolve("${scratch}")
			if err != nil {
				return err
			}
			scratchPath = p
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{{Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scratchPath == "" {
		t.Fatal("scratch path not set in state")
	}
	if _, err := os.Stat(scratchPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("scratch dir %q still exists after Run", scratchPath)
	}
}

func TestRunner_ScratchPreservedOnSkipTeardown(t *testing.T) {
	cdTemp(t)
	boom := errors.New("setup failed")
	r := newTestRunner()
	r.Register("noop", errFactory(boom))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{{Noop: &NoopStep{}}},
		},
	}
	r.Run(context.Background(), cfg, RunOptions{SkipTeardown: true}) //nolint

	scratch, err := r.State().Resolve("${scratch}")
	if err != nil {
		t.Fatalf("could not resolve scratch: %v", err)
	}
	if _, err := os.Stat(scratch); err != nil {
		t.Errorf("scratch dir %q should be preserved when teardown is skipped: %v", scratch, err)
	}
}

// --- phase state and logging ---

func TestRunner_PhaseTimestampsAvailableToLaterSteps(t *testing.T) {
	cdTemp(t)
	var gotStart string
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, state *State) error {
			if step.ID == "check" {
				v, err := state.Resolve("${phase.setup.start}")
				if err != nil {
					return err
				}
				gotStart = v
			}
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:   []Step{{Noop: &NoopStep{}}},
			Collect: []Step{{ID: "check", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotStart == "" {
		t.Fatal("phase.setup.start not available in collect phase")
	}
	if _, err := time.Parse(time.RFC3339, gotStart); err != nil {
		t.Errorf("phase.setup.start %q is not valid RFC3339: %v", gotStart, err)
	}
}

func TestRunner_PhaseLogsStartAndDone(t *testing.T) {
	cdTemp(t)
	buf := &bytes.Buffer{}
	r := NewRunner()
	r.stderr = buf
	r.Register("noop", okFactory)

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{{Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "[setup] starting") {
		t.Errorf("missing [setup] starting in output: %q", out)
	}
	if !strings.Contains(out, "[setup] done in") {
		t.Errorf("missing [setup] done in output: %q", out)
	}
}

// --- error cases ---

func TestRunner_UnregisteredStepType(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{{Noop: &NoopStep{}}},
		},
	}
	err := r.Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unregistered step type")
	}
	if !strings.Contains(err.Error(), "noop") {
		t.Errorf("error should mention step type name, got: %v", err)
	}
}

func TestRunner_StepErrorStopsPhase(t *testing.T) {
	cdTemp(t)
	var log []string
	boom := errors.New("step b failed")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			log = append(log, step.ID)
			if step.ID == "b" {
				return boom
			}
			return nil
		}), nil
	})

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup: []Step{
				{ID: "a", Noop: &NoopStep{}},
				{ID: "b", Noop: &NoopStep{}},
				{ID: "c", Noop: &NoopStep{}},
			},
		},
	}
	err := r.Run(context.Background(), cfg)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if equalSlices(log, []string{"a", "b", "c"}) {
		t.Error("step c should not have run after step b failed")
	}
	if !equalSlices(log, []string{"a", "b"}) {
		t.Errorf("expected [a b], got %v", log)
	}
}

// --- phase groups ---

func TestPhaseGroups_CoverAllPhasesInOrder(t *testing.T) {
	var names []string
	for _, group := range [][]string{upPhases, benchPhases, downPhases} {
		for _, ph := range minimalCfg().Phases.pick(group) {
			names = append(names, ph.name)
		}
	}
	if !equalSlices(names, phaseNames) {
		t.Errorf("up+bench+down: got %v, want %v", names, phaseNames)
	}
	if phaseNames[len(phaseNames)-1] != "teardown" {
		t.Errorf("teardown must be the last phase, got %v", phaseNames)
	}
}

func TestRunner_ReportRunsBeforeTeardown(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))

	cfg := &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Report:   []Step{{ID: "rep", Noop: &NoopStep{}}},
			Teardown: []Step{{ID: "td", Noop: &NoopStep{}}},
		},
	}
	if err := r.Run(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalSlices(log, []string{"rep", "td"}) {
		t.Errorf("order: got %v, want [rep td]", log)
	}
}

// --- up / bench / down lifecycle ---

func lifecycleCfg() *Config {
	return &Config{
		Benchmark: BenchmarkMeta{Name: "bench"},
		Phases: PhasesConfig{
			Setup:     []Step{{ID: "setup", Noop: &NoopStep{}}},
			Configure: []Step{{ID: "configure", Noop: &NoopStep{}}},
			Seed:      []Step{{ID: "seed", Noop: &NoopStep{}}},
			Warmup:    []Step{{ID: "warmup", Noop: &NoopStep{}}},
			Execute:   []Step{{ID: "execute", Noop: &NoopStep{}}},
			Collect:   []Step{{ID: "collect", Noop: &NoopStep{}}},
			Report:    []Step{{ID: "report", Noop: &NoopStep{}}},
			Teardown:  []Step{{ID: "teardown", Noop: &NoopStep{}}},
		},
	}
}

func TestRunner_UpRunsOnlyUpPhases(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))

	if err := r.Up(context.Background(), lifecycleCfg()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalSlices(log, []string{"setup", "configure", "seed"}) {
		t.Errorf("up phases: got %v", log)
	}
}

func TestRunner_UpSavesStateAndCurrentRun(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Register("noop", okFactory)

	if err := r.Up(context.Background(), lifecycleCfg()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	runDir := runDirOf(r.State())
	if runDir == "" {
		t.Fatal("state has no run dir")
	}
	if _, err := os.Stat(filepath.Join(runDir, stateFileName)); err != nil {
		t.Errorf("state file not written: %v", err)
	}
	cur, err := CurrentRun()
	if err != nil {
		t.Fatalf("CurrentRun: %v", err)
	}
	if cur != runDir {
		t.Errorf("current run: got %q, want %q", cur, runDir)
	}
}

func TestRunner_UpFailureSavesStateWithoutTeardown(t *testing.T) {
	cdTemp(t)
	boom := errors.New("seed failed")
	var teardownRan bool
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			switch step.ID {
			case "teardown":
				teardownRan = true
			case "seed":
				return boom
			}
			return nil
		}), nil
	})

	if err := r.Up(context.Background(), lifecycleCfg()); !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if teardownRan {
		t.Error("up must never run teardown")
	}
	if _, err := os.Stat(filepath.Join(runDirOf(r.State()), stateFileName)); err != nil {
		t.Errorf("state file not written after failure: %v", err)
	}
	if _, err := CurrentRun(); err != nil {
		t.Errorf("current run should remain set after failure: %v", err)
	}
}

func TestRunner_BenchRunsBenchPhasesIntoNumberedDirs(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))
	cfg := lifecycleCfg()

	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	runDir := runDirOf(r.State())
	log = nil

	for i := 1; i <= 2; i++ {
		state, err := LoadRun(runDir)
		if err != nil {
			t.Fatalf("LoadRun: %v", err)
		}
		if err := r.Bench(context.Background(), cfg, state); err != nil {
			t.Fatalf("bench %d: %v", i, err)
		}
		results, _ := state.Resolve("${results}")
		want := filepath.Join(runDir, "bench-"+strconv.Itoa(i))
		if results != want {
			t.Errorf("bench %d results: got %q, want %q", i, results, want)
		}
		if _, err := os.Stat(want); err != nil {
			t.Errorf("bench %d results dir missing: %v", i, err)
		}
	}
	if !equalSlices(log, []string{"warmup", "execute", "collect", "report", "warmup", "execute", "collect", "report"}) {
		t.Errorf("bench phases: got %v", log)
	}
}

func TestRunner_BenchPreservesUpState(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, st *State) error {
			if step.ID == "setup" {
				st.SetStepOutput("setup", "cluster", "kind-bench")
			}
			return nil
		}), nil
	})
	cfg := lifecycleCfg()
	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	state, err := LoadRun(runDirOf(r.State()))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Bench(context.Background(), cfg, state); err != nil {
		t.Fatal(err)
	}
	got, err := state.Resolve("${steps.setup.cluster}")
	if err != nil || got != "kind-bench" {
		t.Errorf("setup output lost across bench: %q, %v", got, err)
	}
}

func TestRunner_DownRunsTeardownAndCleansUp(t *testing.T) {
	cdTemp(t)
	var log []string
	r := newTestRunner()
	r.Register("noop", idRecorder(&log))
	cfg := lifecycleCfg()

	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	runDir := runDirOf(r.State())
	scratch, _ := r.State().Resolve("${scratch}")
	log = nil

	state, err := LoadRun(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Down(context.Background(), cfg, state); err != nil {
		t.Fatalf("down: %v", err)
	}
	if !equalSlices(log, []string{"teardown"}) {
		t.Errorf("down phases: got %v", log)
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Errorf("scratch should be removed after down: %v", err)
	}
	if _, err := CurrentRun(); err == nil {
		t.Error("current run should be cleared after down")
	}
	if _, err := os.Stat(runDir); err != nil {
		t.Errorf("run dir should be kept after down: %v", err)
	}
}

func TestRunner_DownFailureKeepsScratchAndCurrentRun(t *testing.T) {
	cdTemp(t)
	boom := errors.New("teardown failed")
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, _ *State) error {
			if step.ID == "teardown" {
				return boom
			}
			return nil
		}), nil
	})
	cfg := lifecycleCfg()
	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	scratch, _ := r.State().Resolve("${scratch}")
	state, err := LoadRun(runDirOf(r.State()))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Down(context.Background(), cfg, state); !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
	if _, err := os.Stat(scratch); err != nil {
		t.Errorf("scratch should be kept after failed down: %v", err)
	}
	if _, err := CurrentRun(); err != nil {
		t.Errorf("current run should be kept after failed down: %v", err)
	}
}

func TestRunner_DownOnlyClearsMatchingCurrentRun(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Register("noop", okFactory)
	cfg := lifecycleCfg()

	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	first := runDirOf(r.State())
	time.Sleep(1100 * time.Millisecond)
	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	second := runDirOf(r.State())
	if first == second {
		t.Fatal("expected distinct run dirs")
	}

	state, err := LoadRun(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Down(context.Background(), cfg, state); err != nil {
		t.Fatal(err)
	}
	cur, err := CurrentRun()
	if err != nil || cur != second {
		t.Errorf("current run: got %q, %v; want %q", cur, err, second)
	}
}

func TestRunner_RunClearsCurrentRunAfterTeardown(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Register("noop", okFactory)
	if err := r.Run(context.Background(), lifecycleCfg()); err != nil {
		t.Fatal(err)
	}
	if _, err := CurrentRun(); err == nil {
		t.Error("current run should be cleared after a full run")
	}
	results, _ := r.State().Resolve("${results}")
	if filepath.Base(results) != "bench-1" {
		t.Errorf("run results dir: got %q, want bench-1", results)
	}
}

func TestRunner_SkipTeardownKeepsCurrentRun(t *testing.T) {
	cdTemp(t)
	r := newTestRunner()
	r.Register("noop", okFactory)
	if err := r.Run(context.Background(), lifecycleCfg(), RunOptions{SkipTeardown: true}); err != nil {
		t.Fatal(err)
	}
	cur, err := CurrentRun()
	if err != nil || cur != runDirOf(r.State()) {
		t.Errorf("current run should point at the skipped run: %q, %v", cur, err)
	}
}

func TestCurrentRun_MissingPointer(t *testing.T) {
	cdTemp(t)
	if _, err := CurrentRun(); err == nil {
		t.Error("expected error when no current run exists")
	}
}

func TestLoadRun_FillsRunDirForOldState(t *testing.T) {
	dir := t.TempDir()
	st := NewState()
	st.SetScratch(filepath.Join(dir, "scratch"))
	if err := st.Save(filepath.Join(dir, stateFileName)); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := runDirOf(loaded); got != dir {
		t.Errorf("run dir: got %q, want %q", got, dir)
	}
}

// --- helpers ---

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRunner_BenchClearsPreviousStepOutputs(t *testing.T) {
	cdTemp(t)
	cfg := &Config{Benchmark: BenchmarkMeta{Name: "b"}, Phases: PhasesConfig{
		Setup:   []Step{{ID: "infra", Noop: &NoopStep{}}},
		Collect: []Step{{ID: "m", Noop: &NoopStep{}}},
	}}
	r := newTestRunner()
	r.Register("noop", func(step Step) (Executor, error) {
		return ExecutorFunc(func(_ context.Context, st *State) error {
			st.SetStepOutput(step.ID, "summary", "first")
			return nil
		}), nil
	})
	if err := r.Up(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	state := r.State()
	if err := r.Bench(context.Background(), cfg, state); err != nil {
		t.Fatal(err)
	}
	if v, _ := state.Resolve("${steps.m.summary}"); v != "first" {
		t.Fatalf("first bench output missing: %q", v)
	}

	r.Register("noop", errFactory(errors.New("boom")))
	if err := r.Bench(context.Background(), cfg, state); err == nil {
		t.Fatal("expected second bench to fail")
	}
	if _, err := state.Resolve("${steps.m.summary}"); err == nil {
		t.Error("failed bench must not keep the previous bench's step output")
	}
	if v, _ := state.Resolve("${steps.infra.summary}"); v != "first" {
		t.Errorf("setup step output must survive bench: %q", v)
	}
}
