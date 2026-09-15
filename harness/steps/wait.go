package steps

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/elastic/orca/harness"
)

const waitProgressInterval = time.Minute

func NewWaitFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		d := step.Wait.Duration.Duration
		if d <= 0 {
			return nil, fmt.Errorf("wait: duration must be positive")
		}
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			return waitFor(ctx, d, waitProgressInterval, os.Stderr)
		}), nil
	}
}

func waitFor(ctx context.Context, d, progress time.Duration, log interface{ Write([]byte) (int, error) }) error {
	deadline := time.Now().Add(d)
	fmt.Fprintf(log, "[wait] waiting %s (until %s)\n", d, deadline.UTC().Format(time.RFC3339))
	ticker := time.NewTicker(progress)
	defer ticker.Stop()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-ticker.C:
			fmt.Fprintf(log, "[wait] %s remaining\n", time.Until(deadline).Round(time.Second))
		}
	}
}
