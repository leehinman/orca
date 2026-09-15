package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/elastic/orca/harness"
)

func NewKubectlFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.Kubectl
			kubeconfig, err := state.Resolve(cfg.Kubeconfig)
			if err != nil {
				return err
			}
			run, err := state.Resolve(cfg.Run)
			if err != nil {
				return err
			}
			// Single-quote the kubeconfig path so it is safe against spaces.
			// Pass run through sh -c so the shell handles any quoting within it
			// (e.g. -p='[{"op":...}]' in kubectl patch commands).
			script := fmt.Sprintf("kubectl --kubeconfig '%s' %s",
				strings.ReplaceAll(kubeconfig, "'", "'\\''"),
				run,
			)
			cmd := exec.CommandContext(ctx, "sh", "-c", script)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("kubectl: %w", err)
			}
			return nil
		}), nil
	}
}
