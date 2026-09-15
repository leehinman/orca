package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/elastic/orca/harness"
)

func NewOpenTofuFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.OpenTofu
			dir, err := state.Resolve(cfg.Dir)
			if err != nil {
				return err
			}

			var varArgs []string
			for k, v := range cfg.Vars {
				resolved, err := state.Resolve(v)
				if err != nil {
					return fmt.Errorf("var %q: %w", k, err)
				}
				varArgs = append(varArgs, "-var", k+"="+resolved)
			}

			initCmd := exec.CommandContext(ctx, "tofu", "init")
			initCmd.Dir = dir
			initCmd.Stdout = os.Stdout
			initCmd.Stderr = os.Stderr
			if err := initCmd.Run(); err != nil {
				return fmt.Errorf("tofu init: %w", err)
			}

			sub := "apply"
			if cfg.Destroy {
				sub = "destroy"
			}
			args := append([]string{sub, "-auto-approve"}, varArgs...)

			cmd := exec.CommandContext(ctx, "tofu", args...)
			cmd.Dir = dir
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("tofu %s: %w", sub, err)
			}

			if len(cfg.Outputs) == 0 || step.ID == "" {
				return nil
			}

			outCmd := exec.CommandContext(ctx, "tofu", "output", "-json")
			outCmd.Dir = dir
			var buf bytes.Buffer
			outCmd.Stdout = &buf
			outCmd.Stderr = os.Stderr
			if err := outCmd.Run(); err != nil {
				return fmt.Errorf("tofu output: %w", err)
			}

			var tofuOutputs map[string]struct {
				Value any `json:"value"`
			}
			if err := json.Unmarshal(buf.Bytes(), &tofuOutputs); err != nil {
				return fmt.Errorf("parse tofu output: %w", err)
			}
			for k := range cfg.Outputs {
				if v, ok := tofuOutputs[k]; ok {
					state.SetStepOutput(step.ID, k, fmt.Sprintf("%v", v.Value))
				}
			}
			return nil
		}), nil
	}
}
