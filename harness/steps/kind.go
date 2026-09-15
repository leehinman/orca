package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewKindFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.Kind
			name, err := state.Resolve(cfg.Name)
			if err != nil {
				return err
			}

			type out struct {
				key  string
				path string
			}
			var outs []out
			for k, spec := range cfg.Outputs {
				p, err := state.Resolve(spec.Path)
				if err != nil {
					return fmt.Errorf("output %q path: %w", k, err)
				}
				outs = append(outs, out{k, p})
			}
			if len(outs) == 0 {
				return fmt.Errorf("kind step requires at least one output for the kubeconfig path")
			}

			cmd := exec.CommandContext(ctx, "kind", "create", "cluster",
				"--name", name,
				"--kubeconfig", outs[0].path,
			)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("kind create cluster: %w", err)
			}

			if step.ID != "" {
				state.SetStepOutput(step.ID, "name", name)
			}
			for _, o := range outs {
				state.SetStepOutput(step.ID, o.key, o.path)
			}
			return nil
		}), nil
	}
}
