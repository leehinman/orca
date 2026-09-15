package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewKindDeleteFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			name, err := state.Resolve(step.KindDelete.Name)
			if err != nil {
				return err
			}
			cmd := exec.CommandContext(ctx, "kind", "delete", "cluster", "--name", name)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("kind delete cluster: %w", err)
			}
			return nil
		}), nil
	}
}
