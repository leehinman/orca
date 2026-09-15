package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/elastic/orca/harness"
)

func NewAnsibleFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		if out := step.Ansible.Outputs; out != nil {
			if step.ID == "" {
				return nil, fmt.Errorf("ansible: outputs require a step id")
			}
			if out.From != "" && out.From != "file" {
				return nil, fmt.Errorf("ansible: outputs.from must be \"file\", got %q", out.From)
			}
			if out.Path == "" {
				return nil, fmt.Errorf("ansible: outputs.path is required")
			}
		}
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.Ansible
			playbook, err := state.Resolve(cfg.Playbook)
			if err != nil {
				return err
			}
			env := ansibleEnv(playbook)
			if cfg.Requirements != "" {
				reqs, err := state.Resolve(cfg.Requirements)
				if err != nil {
					return fmt.Errorf("requirements: %w", err)
				}
				galaxyCmd := exec.CommandContext(ctx, "ansible-galaxy", "collection", "install", "-r", reqs)
				galaxyCmd.Env = env
				galaxyCmd.Stdout = os.Stdout
				galaxyCmd.Stderr = os.Stderr
				if err := galaxyCmd.Run(); err != nil {
					return fmt.Errorf("ansible-galaxy: %w", err)
				}
			}
			inventory, err := state.Resolve(cfg.Inventory)
			if err != nil {
				return err
			}
			args := []string{"-i", inventory, playbook}
			for k, v := range cfg.Vars {
				resolved, err := state.Resolve(v)
				if err != nil {
					return fmt.Errorf("var %q: %w", k, err)
				}
				args = append(args, "-e", k+"="+resolved)
			}
			cmd := exec.CommandContext(ctx, "ansible-playbook", args...)
			cmd.Env = env
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("ansible-playbook: %w", err)
			}
			if cfg.Outputs == nil {
				return nil
			}
			return readAnsibleOutputs(state, step.ID, cfg.Outputs.Path)
		}), nil
	}
}

func ansibleEnv(playbook string) []string {
	env := os.Environ()
	if _, set := os.LookupEnv("ANSIBLE_CONFIG"); set {
		return env
	}
	cfgPath := filepath.Join(filepath.Dir(playbook), "ansible.cfg")
	if _, err := os.Stat(cfgPath); err != nil {
		return env
	}
	if abs, err := filepath.Abs(cfgPath); err == nil {
		cfgPath = abs
	}
	fmt.Fprintf(os.Stderr, "[ansible] using config %s\n", cfgPath)
	return append(env, "ANSIBLE_CONFIG="+cfgPath)
}

func readAnsibleOutputs(state *harness.State, stepID, path string) error {
	resolved, err := state.Resolve(path)
	if err != nil {
		return fmt.Errorf("outputs path: %w", err)
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return fmt.Errorf("read outputs file: %w", err)
	}
	var outputs map[string]any
	if err := json.Unmarshal(data, &outputs); err != nil {
		return fmt.Errorf("parse outputs file %s: %w", resolved, err)
	}
	for k, v := range outputs {
		state.SetStepOutput(stepID, k, fmt.Sprintf("%v", v))
	}
	fmt.Fprintf(os.Stderr, "[ansible] captured %d output(s) from %s\n", len(outputs), resolved)
	return nil
}
