package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewKindLoadFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.KindLoad
			name, err := state.Resolve(cfg.Name)
			if err != nil {
				return err
			}
			image, err := state.Resolve(cfg.Image)
			if err != nil {
				return err
			}
			cmd := exec.CommandContext(ctx, "kind", "load", "image-archive", image, "--name", name)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("kind load image-archive: %w", err)
			}
			return nil
		}), nil
	}
}
