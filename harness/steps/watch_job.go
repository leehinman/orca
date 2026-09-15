package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewWatchJobFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.WatchJob
			kubeconfig, err := state.Resolve(cfg.Kubeconfig)
			if err != nil {
				return err
			}
			namespace, err := state.Resolve(cfg.Namespace)
			if err != nil {
				return err
			}
			job, err := state.Resolve(cfg.Job)
			if err != nil {
				return err
			}
			// --timeout=-1s: wait indefinitely; overall timeout is managed by ctx.
			cmd := exec.CommandContext(ctx, "kubectl", "wait",
				"--for=condition=complete",
				"job/"+job,
				"-n", namespace,
				"--kubeconfig", kubeconfig,
				"--timeout=-1s",
			)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("kubectl wait job: %w", err)
			}
			return nil
		}), nil
	}
}
