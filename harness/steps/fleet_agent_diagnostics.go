package steps

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/elastic/orca/harness"
)

const (
	defaultFleetDiagTimeout = 10 * time.Minute
	defaultFleetDiagPoll    = 10 * time.Second
)

type fleetDiagTarget struct {
	name     string
	agentID  string
	actionID string
	dest     string
	done     bool
}

func NewFleetDiagFactory(kbCfg harness.KibanaConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		cfg := step.FleetDiag
		if len(cfg.Agents) == 0 {
			return nil, fmt.Errorf("fleet-agent-diagnostics: at least one agent is required")
		}
		for i, a := range cfg.Agents {
			if a.Name == "" {
				return nil, fmt.Errorf("fleet-agent-diagnostics: agent #%d: name is required", i+1)
			}
			if a.ID == "" && a.Hostname == "" {
				return nil, fmt.Errorf("fleet-agent-diagnostics: agent %s: id or hostname is required", a.Name)
			}
		}
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			return runFleetDiag(ctx, state, kbCfg, step)
		}), nil
	}
}

func runFleetDiag(ctx context.Context, state *harness.State, kbCfg harness.KibanaConfig, step harness.Step) error {
	cfg := step.FleetDiag
	outDir, err := state.Resolve(cfg.OutputDir)
	if err != nil {
		return err
	}
	if outDir, err = filepath.Abs(outDir); err != nil {
		return fmt.Errorf("resolve output_dir: %w", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	client, err := newKibanaClient(state, kbCfg)
	if err != nil {
		return fmt.Errorf("fleet-agent-diagnostics: %w", err)
	}

	timeout := cfg.Timeout.Duration
	if timeout <= 0 {
		timeout = defaultFleetDiagTimeout
	}
	poll := cfg.PollInterval.Duration
	if poll <= 0 {
		poll = defaultFleetDiagPoll
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var errs []error
	var targets []*fleetDiagTarget
	for _, a := range cfg.Agents {
		t, err := requestFleetDiag(ctx, client, state, a, cfg.CPUProfile, outDir)
		if err != nil {
			errs = append(errs, fmt.Errorf("agent %s: %w", a.Name, err))
			continue
		}
		targets = append(targets, t)
	}

	pending := len(targets)
	for pending > 0 {
		for _, t := range targets {
			if t.done {
				continue
			}
			finished, err := checkFleetDiag(ctx, client, t)
			if err != nil {
				errs = append(errs, fmt.Errorf("agent %s: %w", t.name, err))
			}
			if finished {
				t.done = true
				pending--
			}
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			for _, t := range targets {
				if !t.done {
					errs = append(errs, fmt.Errorf("agent %s: diagnostics not ready after %s", t.name, timeout))
				}
			}
			pending = 0
		case <-time.After(poll):
		}
	}

	if len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] warning: some diagnostics were not collected:\n%v\n", errors.Join(errs...))
	}
	if step.ID != "" {
		for _, t := range targets {
			if _, err := os.Stat(t.dest); err == nil {
				state.SetStepOutput(step.ID, t.name, t.dest)
			}
		}
	}
	return nil
}

func requestFleetDiag(ctx context.Context, client *kibanaClient, state *harness.State, a harness.FleetDiagAgent, cpuProfile bool, outDir string) (*fleetDiagTarget, error) {
	agentID, err := state.Resolve(a.ID)
	if err != nil {
		return nil, fmt.Errorf("id: %w", err)
	}
	if agentID == "" {
		hostname, err := state.Resolve(a.Hostname)
		if err != nil {
			return nil, fmt.Errorf("hostname: %w", err)
		}
		if hostname == "" {
			return nil, fmt.Errorf("id and hostname both resolved to empty")
		}
		if agentID, err = client.findAgentByHostname(ctx, hostname); err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: hostname %s resolved to agent %s\n", a.Name, hostname, agentID)
	}
	actionID, err := client.requestDiagnostics(ctx, agentID, cpuProfile)
	if err != nil {
		return nil, fmt.Errorf("request diagnostics: %w", err)
	}
	fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: requested diagnostics from agent %s (action %s)\n", a.Name, agentID, actionID)
	return &fleetDiagTarget{
		name:     a.Name,
		agentID:  agentID,
		actionID: actionID,
		dest:     filepath.Join(outDir, a.Name+"-diagnostics.zip"),
	}, nil
}

func checkFleetDiag(ctx context.Context, client *kibanaClient, t *fleetDiagTarget) (bool, error) {
	uploads, err := client.listUploads(ctx, t.agentID)
	if err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: list uploads: %v\n", t.name, err)
		return false, nil
	}
	for _, u := range uploads {
		if u.ActionID != t.actionID {
			continue
		}
		switch u.Status {
		case "READY":
			n, err := client.downloadFile(ctx, u.ID, u.Name, t.dest)
			if err != nil {
				return true, fmt.Errorf("download %s: %w", u.Name, err)
			}
			if n == 0 {
				return true, fmt.Errorf("downloaded %s is empty", u.Name)
			}
			fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: downloaded %s (%d bytes) to %s\n", t.name, u.Name, n, t.dest)
			return true, nil
		case "FAILED", "EXPIRED", "DELETED":
			msg := u.Status
			if u.Error != "" {
				msg += ": " + u.Error
			}
			return true, fmt.Errorf("diagnostics %s", msg)
		default:
			fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: %s\n", t.name, u.Status)
			return false, nil
		}
	}
	fmt.Fprintf(os.Stderr, "[fleet-agent-diagnostics] %s: waiting for agent to pick up action\n", t.name)
	return false, nil
}
