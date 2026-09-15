package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// Executor runs a single benchmark step.
type Executor interface {
	Run(ctx context.Context, state *State) error
}

// ExecutorFunc adapts a function to the Executor interface.
type ExecutorFunc func(ctx context.Context, state *State) error

func (f ExecutorFunc) Run(ctx context.Context, state *State) error { return f(ctx, state) }

// ExecutorFactory builds an Executor from a step's config.
type ExecutorFactory func(Step) (Executor, error)

// RunOptions controls optional behavior of Run.
type RunOptions struct {
	SkipTeardown bool
}

// Runner sequences phases and steps for a benchmark run.
type Runner struct {
	factories map[string]ExecutorFactory
	stderr    io.Writer
	lastState *State
}

// NewRunner returns a Runner that writes phase logs to os.Stderr.
func NewRunner() *Runner {
	return &Runner{
		factories: make(map[string]ExecutorFactory),
		stderr:    os.Stderr,
	}
}

// Register associates a step type name with its executor factory.
func (r *Runner) Register(typeName string, f ExecutorFactory) {
	r.factories[typeName] = f
}

// State returns the state from the most recent lifecycle call, or nil if none
// has been made. It is safe to inspect after the call returns.
func (r *Runner) State() *State {
	return r.lastState
}

// Run executes the full benchmark lifecycle: the up phases, one bench
// iteration, and teardown. A new run directory is created.
//
// Teardown runs automatically after a failure unless SkipTeardown is set, in
// which case state is left in the run directory for a later `orca down`.
func (r *Runner) Run(ctx context.Context, cfg *Config, opts ...RunOptions) error {
	opt := RunOptions{}
	if len(opts) > 0 {
		opt = opts[0]
	}

	state, err := r.newRun()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mainErr := r.runPhases(ctx, cfg, state, upPhases)
	if mainErr == nil {
		mainErr = r.bench(ctx, cfg, state)
	}
	r.saveState(state)

	if opt.SkipTeardown {
		fmt.Fprintf(r.stderr, "[teardown] skipped — run: orca down --config <file> --run %s\n", runDirOf(state))
		return mainErr
	}

	tdErr := r.down(context.Background(), cfg, state)
	if tdErr != nil {
		fmt.Fprintf(r.stderr, "[teardown] error: %v\n", tdErr)
	}
	if mainErr != nil {
		return mainErr
	}
	return tdErr
}

// Up creates a new run directory and executes the setup, configure, and seed
// phases. State is saved to the run directory, which becomes the current run.
// Teardown never runs automatically; use Down.
func (r *Runner) Up(ctx context.Context, cfg *Config) error {
	state, err := r.newRun()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err = r.runPhases(ctx, cfg, state, upPhases)
	r.saveState(state)
	if err != nil {
		fmt.Fprintf(r.stderr, "[up] failed — clean up with: orca down --config <file> --run %s\n", runDirOf(state))
	}
	return err
}

// Bench executes the warmup, execute, collect, and report phases against an
// existing run. Each call writes into a fresh bench-<n> directory under the run
// directory, exposed to steps as ${results}. State is saved afterwards.
func (r *Runner) Bench(ctx context.Context, cfg *Config, state *State) error {
	r.lastState = state

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err := r.bench(ctx, cfg, state)
	r.saveState(state)
	return err
}

// Down executes the teardown phase against an existing run. On success the
// scratch directory is removed and the current-run pointer is cleared.
func (r *Runner) Down(ctx context.Context, cfg *Config, state *State) error {
	r.lastState = state

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return r.down(ctx, cfg, state)
}

func (r *Runner) newRun() (*State, error) {
	runDir := "run-" + time.Now().UTC().Format("2006-Jan-02--15_04_05")
	runDir, err := filepath.Abs(runDir)
	if err != nil {
		return nil, fmt.Errorf("resolve run dir: %w", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}

	scratch := filepath.Join(runDir, "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return nil, fmt.Errorf("create scratch dir: %w", err)
	}

	state := NewState()
	state.SetRunDir(runDir)
	state.SetScratch(scratch)
	state.SetResults(runDir)
	r.lastState = state

	if err := setCurrentRun(runDir); err != nil {
		return nil, fmt.Errorf("record current run: %w", err)
	}
	return state, nil
}

func (r *Runner) bench(ctx context.Context, cfg *Config, state *State) error {
	runDir := runDirOf(state)
	if runDir == "" {
		return fmt.Errorf("state has no run directory")
	}
	results, err := nextBenchDir(runDir)
	if err != nil {
		return err
	}
	state.SetResults(results)
	var ids []string
	for _, p := range cfg.Phases.pick(benchPhases) {
		for _, step := range p.steps {
			if step.ID != "" {
				ids = append(ids, step.ID)
			}
		}
	}
	state.ClearStepOutputs(ids)
	return r.runPhases(ctx, cfg, state, benchPhases)
}

func (r *Runner) down(ctx context.Context, cfg *Config, state *State) error {
	if err := r.runPhases(ctx, cfg, state, downPhases); err != nil {
		return err
	}
	if scratch, err := state.Resolve("${scratch}"); err == nil {
		os.RemoveAll(scratch) //nolint:errcheck
	}
	if runDir := runDirOf(state); runDir != "" {
		clearCurrentRun(runDir)
	}
	return nil
}

func (r *Runner) runPhases(ctx context.Context, cfg *Config, state *State, names []string) error {
	for _, p := range cfg.Phases.pick(names) {
		if err := r.runPhase(ctx, p.name, p.steps, state); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) saveState(state *State) {
	runDir := runDirOf(state)
	if runDir == "" {
		return
	}
	if err := state.Save(filepath.Join(runDir, stateFileName)); err != nil {
		fmt.Fprintf(r.stderr, "warning: could not save state: %v\n", err)
	}
}

func runDirOf(state *State) string {
	dir, err := state.Resolve("${run}")
	if err != nil {
		return ""
	}
	return dir
}

func nextBenchDir(runDir string) (string, error) {
	for n := 1; ; n++ {
		dir := filepath.Join(runDir, fmt.Sprintf("bench-%d", n))
		err := os.Mkdir(dir, 0o755)
		if err == nil {
			return dir, nil
		}
		if !os.IsExist(err) {
			return "", fmt.Errorf("create results dir: %w", err)
		}
	}
}

func (r *Runner) runPhase(ctx context.Context, name string, steps []Step, state *State) error {
	if len(steps) == 0 {
		return nil
	}
	fmt.Fprintf(r.stderr, "[%s] starting\n", name)
	start := time.Now()
	state.SetPhaseStart(name, start)

	var stepErr error
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			stepErr = err
			break
		}
		label := stepLabel(step)
		fmt.Fprintf(r.stderr, "[%s/%s] starting\n", name, label)
		stepStart := time.Now()
		err := r.runStep(ctx, step, state)
		stepElapsed := time.Since(stepStart).Round(time.Millisecond)
		if err != nil {
			if step.Optional {
				fmt.Fprintf(r.stderr, "[%s/%s] skipped (optional): %v\n", name, label, err)
			} else {
				fmt.Fprintf(r.stderr, "[%s/%s] failed after %s\n", name, label, stepElapsed)
				stepErr = err
				break
			}
		} else {
			fmt.Fprintf(r.stderr, "[%s/%s] done in %s\n", name, label, stepElapsed)
		}
	}

	end := time.Now()
	elapsed := end.Sub(start)
	state.SetPhaseEnd(name, end)
	state.SetPhaseDuration(name, elapsed)

	if stepErr != nil {
		return fmt.Errorf("%s: %w", name, stepErr)
	}
	fmt.Fprintf(r.stderr, "[%s] done in %s\n", name, elapsed.Round(time.Millisecond))
	return nil
}

func (r *Runner) runStep(ctx context.Context, step Step, state *State) error {
	typeName := step.typeName()
	factory, ok := r.factories[typeName]
	if !ok {
		return fmt.Errorf("no executor registered for step type %q", typeName)
	}
	exec, err := factory(step)
	if err != nil {
		return fmt.Errorf("step %s: build executor: %w", stepLabel(step), err)
	}
	if err := exec.Run(ctx, state); err != nil {
		return fmt.Errorf("step %s: %w", stepLabel(step), err)
	}
	return nil
}

func stepLabel(step Step) string {
	if step.ID != "" {
		return step.ID
	}
	return step.typeName()
}
