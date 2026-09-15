package steps

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/elastic/orca/harness"
)

func NewWaitPodsFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.WaitPods
			kubeconfig, err := state.Resolve(cfg.Kubeconfig)
			if err != nil {
				return err
			}
			namespace, err := state.Resolve(cfg.Namespace)
			if err != nil {
				return err
			}
			selector, err := state.Resolve(cfg.Selector)
			if err != nil {
				return err
			}

			// Bound the whole operation to the configured timeout.
			waitCtx, cancel := context.WithTimeout(ctx, cfg.Timeout.Duration)
			defer cancel()

			timeout := fmt.Sprintf("%ds", int(cfg.Timeout.Seconds()))
			for {
				var stderr bytes.Buffer
				cmd := exec.CommandContext(waitCtx, "kubectl", "wait",
					"--for=condition=ready", "pod",
					"-l", selector,
					"-n", namespace,
					"--kubeconfig", kubeconfig,
					"--timeout="+timeout,
				)
				cmd.Stdout = os.Stdout
				cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
				runErr := cmd.Run()
				if runErr == nil {
					return nil
				}
				// Context expired: kubectl was killed mid-wait; report a clean timeout.
				if waitCtx.Err() != nil {
					return fmt.Errorf("kubectl wait pods: timed out waiting for pods with selector %q in %s", selector, namespace)
				}
				// kubectl exits immediately with "no matching resources found" when
				// the scheduler hasn't created the pods yet (e.g. DaemonSet just
				// applied). Retry every 5 s until pods appear or the deadline fires.
				if !strings.Contains(stderr.String(), "no matching resources found") {
					return fmt.Errorf("kubectl wait pods: %w", runErr)
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("kubectl wait pods: timed out waiting for pods with selector %q in %s", selector, namespace)
				case <-time.After(5 * time.Second):
				}
			}
		}), nil
	}
}
