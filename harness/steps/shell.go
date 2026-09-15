package steps

import (
	"context"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewShellFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			run, err := state.Resolve(step.Shell.Run)
			if err != nil {
				return err
			}
			if d := step.Shell.Timeout.Duration; d > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, d)
				defer cancel()
			}
			cmd := exec.CommandContext(ctx, "sh", "-c", run)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		}), nil
	}
}
